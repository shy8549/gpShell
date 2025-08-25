#!/usr/bin/env bash
# gpload_parallel_resume.sh
# - 纯 Bash/AWK；无 Python 依赖
# - 自动识别生成列（identity/nextval/uuid/now）
# - Header 自动识别；字段映射；输入列类型与目标列匹配
# - 分片/并发 可开关；断点续传；详细日志
# - LOG_ERRORS 支持（启用时 ERROR_LIMIT >= 2）
set -euo pipefail

#################### 可配置参数 ####################
CSV_DELIM=","
CSV_QUOTE='"'
CSV_ESCAPE="\\"
CSV_NULL="\\N"
CSV_ENCODING="UTF8"

ENABLE_CHUNKING=${ENABLE_CHUNKING:-true}     # true: 切片；false: 不切片（单文件）
CHUNK_LINES=${CHUNK_LINES:-200000}

ENABLE_PARALLEL=${ENABLE_PARALLEL:-true}     # true: 并发；false: 串行
MAX_JOBS=${MAX_JOBS:-4}
RETRY_TIMES=${RETRY_TIMES:-2}

ENABLE_LOG_ERRORS=${ENABLE_LOG_ERRORS:-true}
GPLOAD_ERROR_LIMIT=${GPLOAD_ERROR_LIMIT:-0}
MIN_ERROR_LIMIT_WHEN_LOG_ERRORS=${MIN_ERROR_LIMIT_WHEN_LOG_ERRORS:-2}
ENABLE_REUSE_TABLES=${ENABLE_REUSE_TABLES:-true}
GPFDIST_PORT=${GPFDIST_PORT:-8800}           # 基础端口（每分片自动分配不同端口：8800、8802…）

ALLOW_CSV_OVERRIDE_GENERATED=${ALLOW_CSV_OVERRIDE_GENERATED:-true}

# 单文件模式使用软链接还是复制：symlink|copy
SINGLE_FILE_LINK_MODE=${SINGLE_FILE_LINK_MODE:-symlink}

LOG_LEVEL="${LOG_LEVEL:-DEBUG}"              # DEBUG/INFO/WARN/ERROR

#################### 日志工具 ####################
RUN_ID="$(date +'%Y%m%d_%H%M%S')_$$"
ROOT_DIR="$(pwd)"
LOG_DIR="$ROOT_DIR/logs/$(basename "$0" .sh)/$RUN_ID"
mkdir -p "$LOG_DIR"
MAIN_LOG="$LOG_DIR/main.log"

lvl_int(){ case "$1" in DEBUG) echo 10;; INFO) echo 20;; WARN) echo 30;; ERROR) echo 40;; *) echo 20;; esac; }
should_log(){ [ "$(lvl_int "$1")" -ge "$(lvl_int "$LOG_LEVEL")" ]; }
_ts(){ date +'%Y-%m-%d %H:%M:%S'; }
_now_ms(){ date +%s%3N 2>/dev/null || { perl -MTime::HiRes=time -e 'printf("%.0f\n", time()*1000)'; }; }

_log(){ local L="$1"; shift; local T="$1"; shift; local M="$*"
  if should_log "$L"; then echo "$(_ts) $L [$T]: $M" | tee -a "$MAIN_LOG"; else echo "$(_ts) $L [$T]: $M" >> "$MAIN_LOG"; fi; }
log_dbg(){ _log DEBUG "$@"; }; log_info(){ _log INFO "$@"; }; log_warn(){ _log WARN "$@"; }; log_error(){ _log ERROR "$@"; }

declare -A __T0; step_start(){ __T0["$1"]="$(_now_ms)"; log_info "$1" "▶ start"; }
step_end(){ local k="$1"; local t1="$(_now_ms)"; local t0="${__T0[$k]:-0}"; log_info "$k" "✔ done ($((t1-t0))ms)"; }

#################### 全局变量 ####################
SCHEMA=""; TABLE=""; CSV_PATH=""
WORKDIR=""; STATE_FILE=""; CSV_HASH=""
LOCAL_IP_FOR_GPFDIST=""
CHUNK_YML_SKIP=0

declare -a TABLE_COLS=()
declare -A GENERATED_SET=()
declare -A NOTNULL_NO_DEFAULT=()
declare -A TARGET_TYPE=()

INPUT_COLS=()
INPUT_TYPES=()
MAPPING_KV=()
SKIP_LINES=0

TOTAL_START="$(_now_ms)"

#################### 工具/公共 ####################
require_env(){ : "${DB_HOST:?need DB_HOST}" "${DB_PORT:?need DB_PORT}" "${DB_NAME:?need DB_NAME}" "${DB_USER:?need DB_USER}" "${DB_PASSWORD:?need DB_PASSWORD}"; }
parse_args(){ SCHEMA="${1:-}"; TABLE="${2:-}"; CSV_PATH="${3:-}"
  [[ -z "$SCHEMA" || -z "$TABLE" || -z "$CSV_PATH" ]] && { log_error "args" "Usage: $0 <schema> <table> <csv_file>"; exit 2; }
  [[ -f "$CSV_PATH" ]] || { log_error "args" "CSV not found: $CSV_PATH"; exit 2; } }
psql_cmd(){ PGPASSWORD="$DB_PASSWORD" psql -X -v ON_ERROR_STOP=1 -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" "$@"; }
ensure_gpload(){ if ! command -v gpload >/dev/null 2>&1; then
    [[ -f /usr/local/greenplum-db/greenplum_path.sh ]] && source /usr/local/greenplum-db/greenplum_path.sh || true; fi
  command -v gpload >/dev/null 2>&1 || { log_error "ensure_gpload" "gpload not found"; exit 6; } }
sha256(){ command -v sha256sum >/dev/null 2>&1 && sha256sum "$1" | awk '{print $1}' || shasum -a 256 "$1" | awk '{print $1}'; }
yaml_escape(){ local s="${1//\\/\\\\}"; s="${s//\"/\\\"}"; printf '"%s"' "$s"; }
read_first_line(){ head -n 1 "$CSV_PATH"; }
abs_path(){ command -v readlink >/dev/null 2>&1 && readlink -f "$1" || (cd "$(dirname "$1")"; echo "$PWD/$(basename "$1")"); }

get_local_hosts_for_gpfdist(){
  if [[ -n "${GPLOAD_LOCAL_HOSTS:-}" ]]; then printf "%s\n" "$GPLOAD_LOCAL_HOSTS"; return; fi
  local ip=""
  if command -v ip >/dev/null 2>&1; then ip=$(ip route get 8.8.8.8 2>/dev/null | awk '/src/{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1); exit}}'); fi
  [[ -z "$ip" ]] && ip=$(hostname -I 2>/dev/null | awk '{print $1}')
  [[ -z "$ip" ]] && ip=$(getent hosts "$(hostname -f 2>/dev/null || hostname)" | awk '{print $1}' | head -n1)
  [[ -z "$ip" ]] && ip="127.0.0.1"; printf "%s\n" "$ip"
}

split_csv_line_tabs(){ local line="$1"
  echo "$line" | awk -v d="$CSV_DELIM" -v q="$CSV_QUOTE" 'BEGIN{FS="";OFS="\t"}{
    s=$0;n=length(s);inq=0;field="";out="";
    for(i=1;i<=n;i++){c=substr(s,i,1);
      if(inq){ if(c==q){ if(i<n && substr(s,i+1,1)==q){field=field q;i++} else{inq=0} } else{field=field c} }
      else{ if(c==q){inq=1} else if(c==d){out=(out==""?field:out OFS field);field=""} else{field=field c} }
    }
    out=(out==""?field:out OFS field); print out }'
}

map_input_type(){ local dt="$(echo "$1" | tr 'A-Z' 'a-z')"
  case "$dt" in
    integer|int4) echo "integer";;
    bigint|int8) echo "bigint";;
    smallint|int2) echo "smallint";;
    numeric|decimal*) echo "numeric";;
    "double precision") echo "double precision";;
    real|float4) echo "real";;
    boolean|bool) echo "boolean";;
    date) echo "date";;
    time*|timetz) echo "time";;
    timestamp*|timestamptz) echo "timestamp";;
    uuid) echo "uuid";;
    *) echo "text";;
  esac
}

#################### 元数据 & 映射 ####################
fetch_table_meta(){
  local schema_esc table_esc
  schema_esc=$(printf "%s" "$SCHEMA" | sed "s/'/''/g")
  table_esc=$(printf "%s" "$TABLE"  | sed "s/'/''/g")
  local sql="
select column_name, lower(data_type), ordinal_position, is_nullable,
       (column_default is not null) as has_default,
       (case when (is_identity is not null and is_identity in ('YES','ALWAYS','BY DEFAULT')) then true else false end) as is_identity,
       coalesce(column_default,'') as column_default
from information_schema.columns
where table_schema='$schema_esc' and table_name='$table_esc'
order by ordinal_position;"
  psql_cmd -Atc "$sql" | while IFS='|' read -r name dtype ord isnull hasdef isid def; do
    local lowdef is_gen="false" lc
    lowdef="$(echo "$def" | tr 'A-Z' 'a-z')"; lc="$(echo "$name" | tr 'A-Z' 'a-z')"
    if [[ "$isid" == "t" ]]; then is_gen="true"
    elif [[ "$lowdef" =~ nextval\(|unique_rowid\(|identity ]]; then is_gen="true"
    elif [[ "$lowdef" =~ gen_random_uuid\(\)|uuid_generate_|uuid_ossp ]]; then is_gen="true"
    elif [[ "$lowdef" =~ now\(\)|current_timestamp|clock_timestamp\(\)|statement_timestamp\(\) ]]; then is_gen="true"
    fi
    echo "${name}|${dtype}|${ord}|${isnull}|${hasdef}|${isid}|${def}|${is_gen}"
  done
}

detect_header(){
  local first_line="$1"; declare -A tset=(); local c lc
  for c in "${TABLE_COLS[@]}"; do lc="$(echo "$c" | tr 'A-Z' 'a-z')"; tset["$lc"]=1; done
  IFS=$'\t' read -r -a head_cols <<< "$(split_csv_line_tabs "$first_line")"
  [[ ${#head_cols[@]} -eq 0 ]] && { echo 0; return; }
  local match=0 all_alpha=1 h lc_h
  for h in "${head_cols[@]}"; do
    h="$(echo "$h" | sed 's/^"//;s/"$//' | awk '{$1=$1;print}')"
    lc_h="$(echo "$h" | tr 'A-Z' 'a-z')"
    [[ -n "${tset[$lc_h]:-}" ]] && ((match++))
    [[ "$lc_h" =~ ^[0-9]+$ ]] && all_alpha=0
  done
  if (( match == ${#head_cols[@]} )) || { (( match>=1 )) && (( all_alpha==1 )); }; then echo 1; else echo 0; fi
}

build_mapping_with_header(){
  local first_line="$1"; declare -A HEADER_IDX=(); declare -A HEADER_POS=()
  IFS=$'\t' read -r -a header_cols <<< "$(split_csv_line_tabs "$first_line")"
  INPUT_COLS=(); INPUT_TYPES=()
  local i hn lc
  for ((i=0;i<${#header_cols[@]};i++)); do
    hn="$(echo "${header_cols[i]}" | sed 's/^"//;s/"$//' | awk '{$1=$1;print}')"
    INPUT_COLS+=("$hn"); lc="$(echo "$hn" | tr 'A-Z' 'a-z')"
    HEADER_IDX["$lc"]="$hn"; HEADER_POS["$lc"]="$i"
  done
  MAPPING_KV=()
  local missing_required=(); local tgt lc_tgt is_gen src_name dtype
  for tgt in "${TABLE_COLS[@]}"; do
    lc_tgt="$(echo "$tgt" | tr 'A-Z' 'a-z')"; is_gen="${GENERATED_SET[$lc_tgt]:-0}"
    if [[ "$is_gen" == "1" ]]; then
      if [[ "$ALLOW_CSV_OVERRIDE_GENERATED" == "true" && -n "${HEADER_IDX[$lc_tgt]:-}" ]]; then
        src_name="${HEADER_IDX[$lc_tgt]}"; MAPPING_KV+=("$tgt: ${src_name}")
        dtype="${TARGET_TYPE[$lc_tgt]:-text}"; INPUT_TYPES["${HEADER_POS[$lc_tgt]}"]="$(map_input_type "$dtype")"
      fi
      continue
    fi
    if [[ -n "${HEADER_IDX[$lc_tgt]:-}" ]]; then
      src_name="${HEADER_IDX[$lc_tgt]}"; MAPPING_KV+=("$tgt: ${src_name}")
      dtype="${TARGET_TYPE[$lc_tgt]:-text}"; INPUT_TYPES["${HEADER_POS[$lc_tgt]}"]="$(map_input_type "$dtype")"
    else
      [[ "${NOTNULL_NO_DEFAULT[$lc_tgt]:-0}" == "1" ]] && missing_required+=("$tgt")
    fi
  done
  for ((i=0;i<${#INPUT_COLS[@]};i++)); do [[ -n "${INPUT_TYPES[$i]:-}" ]] || INPUT_TYPES[$i]="text"; done
  (( ${#missing_required[@]} == 0 )) || { log_error "mapping" "Missing NOT NULL: ${missing_required[*]}"; exit 5; }
  SKIP_LINES=1
}

build_mapping_positional(){
  local first_line="$1"; IFS=$'\t' read -r -a row1 <<< "$(split_csv_line_tabs "$first_line")"
  local csv_n=${#row1[@]} usable_targets=() tgt lc dtype
  for tgt in "${TABLE_COLS[@]}"; do lc="$(echo "$tgt" | tr 'A-Z' 'a-z')"; [[ "${GENERATED_SET[$lc]:-0}" == "0" ]] && usable_targets+=("$tgt"); done
  (( csv_n <= ${#usable_targets[@]} )) || { log_error "mapping" "CSV cols $csv_n > target ${#usable_targets[@]}"; exit 5; }
  INPUT_COLS=(); INPUT_TYPES=(); MAPPING_KV=()
  local i; for ((i=1;i<=csv_n;i++)); do
    INPUT_COLS+=("col$i"); dtype="${TARGET_TYPE[$(echo "${usable_targets[i-1]}" | tr 'A-Z' 'a-z')]:-text}"
    INPUT_TYPES+=("$(map_input_type "$dtype")"); MAPPING_KV+=("${usable_targets[i-1]}: col$((i))")
  done
  local missing_required=()
  for ((i=csv_n;i<${#usable_targets[@]};i++)); do tgt="${usable_targets[i]}"; lc="$(echo "$tgt" | tr 'A-Z' 'a-z')"
    [[ "${NOTNULL_NO_DEFAULT[$lc]:-0}" == "1" ]] && missing_required+=("$tgt"); done
  (( ${#missing_required[@]} == 0 )) || { log_error "mapping" "Missing NOT NULL: ${missing_required[*]}"; exit 5; }
  SKIP_LINES=0
}

#################### YAML 生成（每分片独立端口 + HEADER 支持） ####################
write_yaml(){
  local yml="${1:-}"; local skip_lines="${2:-0}"; local schema="${3:-}"; local table="${4:-}"; local file_path="${5:-}"

  local EFFECTIVE_ERROR_LIMIT="${GPLOAD_ERROR_LIMIT:-0}"
  if [[ "${ENABLE_LOG_ERRORS:-false}" == "true" && "${EFFECTIVE_ERROR_LIMIT:-0}" -lt "${MIN_ERROR_LIMIT_WHEN_LOG_ERRORS:-2}" ]]; then
    EFFECTIVE_ERROR_LIMIT="${MIN_ERROR_LIMIT_WHEN_LOG_ERRORS:-2}"
  fi

  local host_ip="${LOCAL_IP_FOR_GPFDIST:-}"; [[ -z "$host_ip" ]] && host_ip="$(get_local_hosts_for_gpfdist)"

  # 为每个分片计算专属 gpfdist 端口（8800, 8802, 8804, ...）
  local base_port="${GPFDIST_PORT:-8800}" idx_str n use_port
  idx_str="$(basename "$file_path" .csv)"; n="${idx_str##*_}"; n="${n##0}"; [[ -z "$n" ]] && n=1
  use_port=$(( base_port + (n - 1) * 2 ))

  {
    echo "VERSION: 1.0.0.1"
    echo "DATABASE: $(yaml_escape "${DB_NAME:-}")"
    echo "USER: $(yaml_escape "${DB_USER:-}")"
    echo "HOST: $(yaml_escape "${DB_HOST:-}")"
    echo "PORT: ${DB_PORT:-5432}"
    echo "GPLOAD:"
    echo "  INPUT:"
    echo "    - SOURCE:"
    echo "        LOCAL_HOSTNAME:"
    echo "          - $(yaml_escape "$host_ip")"
    echo "        PORT: $use_port"
    echo "        FILE:"
    echo "          - $(yaml_escape "$file_path")"
    echo "    - FORMAT: csv"
    echo "    - DELIMITER: $(yaml_escape "$CSV_DELIM")"
    echo "    - QUOTE: $(yaml_escape "$CSV_QUOTE")"
    echo "    - ESCAPE: $(yaml_escape "$CSV_ESCAPE")"
    echo "    - NULL_AS: $(yaml_escape "$CSV_NULL")"
    echo "    - ENCODING: $(yaml_escape "$CSV_ENCODING")"
    if (( skip_lines > 0 )); then
      echo "    - HEADER: true"   # 单文件模式跳过表头
    fi
    echo "    - ERROR_LIMIT: $EFFECTIVE_ERROR_LIMIT"
    [[ "${ENABLE_LOG_ERRORS:-false}" == "true" ]] && echo "    - LOG_ERRORS: true"
    echo "    - COLUMNS:"
    local i; for ((i=0;i<${#INPUT_COLS[@]-0};i++)); do
      echo "        - $(yaml_escape "${INPUT_COLS[$i]}"): ${INPUT_TYPES[$i]:-text}"
    done
    echo "  OUTPUT:"
    local qtable="\"${schema}\".\"${table}\""
    echo "    - TABLE: $(yaml_escape "$qtable")"
    echo "    - MODE: INSERT"
    if ((${#MAPPING_KV[@]-0} > 0)); then
      echo "    - MAPPING:"
      local kv tgt src qcol
      for kv in "${MAPPING_KV[@]-}"; do
        tgt="${kv%%:*}"         # 目标列（表列）
        src="${kv#*: }"         # 输入列（CSV/INPUT 列）
        qcol="\"${tgt}\""       # 给目标列表达式加 SQL 双引号
        echo "        $(yaml_escape "$qcol"): $(yaml_escape "$src")"
      done
    fi
    echo "  PRELOAD:"
    echo "    - TRUNCATE: false"
    [[ "${ENABLE_REUSE_TABLES:-false}" == "true" ]] && echo "    - REUSE_TABLES: true"
  } > "$yml"
}

#################### 分片（剥离 + 开关） ####################
_split_csv_into_chunks(){ local has_header="$1" in_file="$2" out_dir="$3" chunk_lines="$4"
  mkdir -p "$out_dir"
  awk -v header="$has_header" -v dir="$out_dir" -v CL="$chunk_lines" '
  BEGIN{ idx=0; count=0; fname="" }
  {
    if (NR==1 && header==1) next
    if (count==0 || count>=CL){
      if (fname!="") close(fname)
      idx++; count=0
      fname=sprintf("%s/chunk_%06d.csv", dir, idx)
    }
    print $0 >> fname; count++
  }
  END{ if (fname!="") close(fname) }' "$in_file"
}

build_chunks(){
  local has_header="$1" in_file="$2" out_dir="$3"
  mkdir -p "$out_dir"
  if [[ "$ENABLE_CHUNKING" == "true" ]]; then
    _split_csv_into_chunks "$has_header" "$in_file" "$out_dir" "$CHUNK_LINES"
    CHUNK_YML_SKIP=0
    log_info "chunking" "mode=split, header_dropped=$has_header, lines_per_chunk=$CHUNK_LINES"
  else
    local one="$out_dir/chunk_000001.csv"
    if [[ "$SINGLE_FILE_LINK_MODE" == "copy" ]]; then
      cp -f -- "$(abs_path "$in_file")" "$one"
    else
      ln -sf "$(abs_path "$in_file")" "$one"
    fi
    CHUNK_YML_SKIP="$SKIP_LINES"
    log_info "chunking" "mode=single_file, yml_skip=$CHUNK_YML_SKIP -> $one"
  fi
}

#################### 工作目录/状态 ####################
init_state(){
  CSV_HASH="$(sha256 "$CSV_PATH")"
  WORKDIR="$ROOT_DIR/work_${SCHEMA}_${TABLE}_${CSV_HASH:0:12}"
  mkdir -p "$WORKDIR"/{chunks,yml,logs,status}
  STATE_FILE="$WORKDIR/state.meta"
  { echo "run_id=$RUN_ID"; echo "schema=$SCHEMA"; echo "table=$TABLE"; echo "csv=$CSV_PATH"; echo "hash=$CSV_HASH"; echo "created_at=$(_ts)"; } > "$STATE_FILE"
  log_info "state" "work=$WORKDIR"; log_info "state" "log=$LOG_DIR"
}

list_chunks(){
  # 兼容普通文件和软链接
  find "$WORKDIR/chunks" -maxdepth 1 \( -type f -o -type l \) -name 'chunk_*.csv' -print 2>/dev/null | sort
}

mark_status(){ local c="${1:-}"; local s="${2:-}"
  [[ -z "${c:-}" ]] && { log_error "mark_status" "empty chunk arg"; return; }
  echo "${s:-}" > "$WORKDIR/status/$(basename "$c").status"
}
get_status(){ local c="${1:-}"; [[ -z "${c:-}" ]] && { echo "NEW"; return; }
  local f="$WORKDIR/status/$(basename "$c").status"; [[ -f "$f" ]] && cat "$f" || echo "NEW"
}

#################### 准备阶段 ####################
prepare_all(){
  step_start "prepare"
  log_info "prepare" "DB=${DB_HOST}:${DB_PORT}/${DB_NAME}, USER=${DB_USER}"
  log_info "prepare" "Target=${SCHEMA}.${TABLE}"
  log_info "prepare" "CSV=${CSV_PATH}"
  log_info "prepare" "MODE=$([[ "$ENABLE_PARALLEL" == "true" ]] && echo "PARALLEL x$MAX_JOBS" || echo "SEQUENTIAL")"
  log_info "prepare" "CHUNKING=${ENABLE_CHUNKING} (CHUNK_LINES=$CHUNK_LINES when true)"
  log_info "prepare" "LOG_ERRORS=${ENABLE_LOG_ERRORS}, ERROR_LIMIT=${GPLOAD_ERROR_LIMIT} (min_if_log=${MIN_ERROR_LIMIT_WHEN_LOG_ERRORS})"

  local META=(); mapfile -t META < <(fetch_table_meta)
  (( ${#META[@]} > 0 )) || { log_error "prepare" "No columns for $SCHEMA.$TABLE"; exit 3; }

  TABLE_COLS=(); GENERATED_SET=(); NOTNULL_NO_DEFAULT=(); TARGET_TYPE=()
  local row name dtype ord isnull hasdef isid def isgen lc
  for row in "${META[@]}"; do
    IFS='|' read -r name dtype ord isnull hasdef isid def isgen <<<"$row"
    TABLE_COLS+=("$name"); lc="$(echo "$name" | tr 'A-Z' 'a-z')"; TARGET_TYPE["$lc"]="$dtype"
    [[ "$isgen" == "true" ]] && GENERATED_SET["$lc"]="1" || GENERATED_SET["$lc"]="0"
    if [[ "$isnull" == "NO" && "$hasdef" != "t" && "$isgen" != "true" ]]; then NOTNULL_NO_DEFAULT["$lc"]="1"; else NOTNULL_NO_DEFAULT["$lc"]="0"; fi
  done
  log_info "prepare" "Columns: ${TABLE_COLS[*]}"

  local first_line; first_line="$(read_first_line)"
  [[ -z "$first_line" ]] && { log_error "prepare" "CSV empty"; exit 4; }
  log_dbg "prepare" "CSV first line: ${first_line:0:200}"
  local CSV_HAS_HEADER; CSV_HAS_HEADER="$(detect_header "$first_line")"
  log_info "prepare" "Header detected? $([[ "$CSV_HAS_HEADER" == 1 ]] && echo yes || echo no)"
  if [[ "$CSV_HAS_HEADER" == 1 ]]; then build_mapping_with_header "$first_line"; else build_mapping_positional "$first_line"; fi
  log_info "prepare" "INPUT_COLS: ${INPUT_COLS[*]-}"
  log_info "prepare" "INPUT_TYPES: ${INPUT_TYPES[*]-}"
  log_info "prepare" "MAPPING_KV: ${MAPPING_KV[*]-}"

  build_chunks "$CSV_HAS_HEADER" "$CSV_PATH" "$WORKDIR/chunks"
  local cnt; cnt=$(list_chunks | wc -l | awk '{print $1}')
  log_info "prepare" "Chunk files: $cnt"
  step_end "prepare"
}

#################### 执行分片 ####################
run_chunk(){
  local chunk="$1" idx; idx="$(basename "$chunk" .csv)"
  local st; st="$(get_status "$chunk")"
  local yml="$WORKDIR/yml/${idx}.yml" logf="$WORKDIR/logs/${idx}.log"

  log_info "run_chunk" "[$idx] status=$st file=$chunk"
  [[ "$st" == "DONE" ]] && { log_info "run_chunk" "[$idx] skip (DONE)"; return 0; }

  write_yaml "$yml" "$CHUNK_YML_SKIP" "$SCHEMA" "$TABLE" "$chunk"
  log_dbg "run_chunk" "[$idx] yml=$yml"; sed -n '1,40p' "$yml" | sed 's/^/YML| /' >> "$MAIN_LOG" || true

  local tries=0 ok=0 rc=0
  while (( tries <= RETRY_TIMES )); do
    tries=$((tries+1)); log_info "run_chunk" "[$idx] try $tries/$((RETRY_TIMES+1))"
    if PGPASSWORD="$DB_PASSWORD" gpload -f "$yml" -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" >> "$logf" 2>&1; then ok=1; rc=0; else rc=$?; ok=0; fi
    if (( ok==1 )); then mark_status "$chunk" "DONE"; log_info "run_chunk" "[$idx] DONE rc=$rc log=$logf"; return 0
    else log_warn "run_chunk" "[$idx] gpload failed rc=$rc log=$logf"; sleep 1; fi
  done
  mark_status "$chunk" "FAIL"; log_error "run_chunk" "[$idx] FAIL after $((RETRY_TIMES+1)) tries log=$logf"; return 1
}

#################### 执行全部分片 ####################
run_all_chunks(){
  step_start "run_all_chunks"
  local -a chunks; mapfile -t chunks < <(list_chunks)
  (( ${#chunks[@]} > 0 )) || { log_info "run_all_chunks" "No chunks to import"; step_end "run_all_chunks"; return 0; }
  log_info "run_all_chunks" "Mode=$([[ "$ENABLE_PARALLEL" == "true" ]] && echo "PARALLEL x$MAX_JOBS" || echo "SEQUENTIAL"), chunks=${#chunks[@]}"

  local fail_cnt=0
  if [[ "$ENABLE_PARALLEL" == "true" ]]; then
    local running=0; local -a pids=(); local ch p
    for ch in "${chunks[@]}"; do
      [[ "$(get_status "$ch")" == "DONE" ]] && { log_info "run_all_chunks" "Skip $(basename "$ch") (DONE)"; continue; }
      run_chunk "$ch" & p=$!; pids+=($p); running=$((running+1))
      if (( running >= MAX_JOBS )); then wait "${pids[0]}" || fail_cnt=$((fail_cnt+1)); pids=("${pids[@]:1}"); running=$((running-1)); fi
    done
    local pid; for pid in "${pids[@]}"; do wait "$pid" || fail_cnt=$((fail_cnt+1)); done
  else
    local ch; for ch in "${chunks[@]}"; do
      [[ "$(get_status "$ch")" == "DONE" ]] && { log_info "run_all_chunks" "Skip $(basename "$ch") (DONE)"; continue; }
      run_chunk "$ch" || fail_cnt=$((fail_cnt+1))
    done
  fi

  if (( fail_cnt > 0 )); then log_error "run_all_chunks" "There are $fail_cnt failed chunks. Re-run to retry failed ones (resume)."; step_end "run_all_chunks"; return 1
  else log_info "run_all_chunks" "All chunks DONE."; step_end "run_all_chunks"; return 0; fi
}

#################### 总结 ####################
on_exit(){
  local rc=$?
  local total_ms=$(( $(_now_ms) - TOTAL_START ))
  local done=$(grep -l "^DONE$" "$WORKDIR/status/"*.status 2>/dev/null | wc -l | awk '{print $1}')
  local fail=$(grep -l "^FAIL$" "$WORKDIR/status/"*.status 2>/dev/null | wc -l | awk '{print $1}')
  log_info "summary" "DONE=$done FAIL=$fail elapsed=${total_ms}ms"
  if (( rc==0 )); then log_info "summary" "SUCCESS main log: $MAIN_LOG"; else log_error "summary" "FAILED rc=$rc main log: $MAIN_LOG"; fi
}
trap on_exit EXIT

#################### 主流程 ####################
main(){ step_start "main"; parse_args "$@"; require_env; ensure_gpload; LOCAL_IP_FOR_GPFDIST="${GPLOAD_LOCAL_HOSTS:-}"
  init_state; prepare_all; run_all_chunks; step_end "main"; }
main "$@"