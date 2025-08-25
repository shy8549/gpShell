#!/usr/bin/env bash
# gp_copy_csv.sh
# 从客户端用 psql \copy 将 CSV 导入到 Greenplum：参数仅 schema table csv_file
# 日志详细、函数化封装；连接参数走环境变量 PGHOST/PGPORT/PGUSER/PGPASSWORD/DB_NAME
# ====================
# 使用方法：
# export PGHOST=10.187.137.173
# export PGPORT=5432
# export PGUSER=tddbuser
# export PGPASSWORD=cdphadoop@2023
# export DB_NAME=SJGXPT

# 导入
# ./gp_copy_csv.sh "MySchema" "MyTable" /path/to/data.csv
# ====================

set -Eeuo pipefail

# =======================
# 日志配置（符合你偏好的格式）
# =======================
LOG_DIR="$(pwd)/logs/$(basename "$0" .sh)"  # Log directory based on the script name
mkdir -p "$LOG_DIR"

log_info() {
    local func_name="$1"
    local message="$2"
    local logfile="$LOG_DIR/$(basename "$0" .sh)_info_$(date +'%Y%m%d').log"
    echo "$(date +'%Y-%m-%d %H:%M:%S') INFO [$func_name]: $message" | tee -a "$logfile"
}

log_error() {
    local func_name="$1"
    local message="$2"
    local logfile="$LOG_DIR/$(basename "$0" .sh)_error_$(date +'%Y%m%d').log"
    echo "$(date +'%Y-%m-%d %H:%M:%S') ERROR [$func_name]: $message" | tee -a "$logfile"
}

die() {
    local func_name="$1"
    local message="$2"
    log_error "$func_name" "$message"
    exit 1
}

on_trap_err() {
    local exit_code=$?
    log_error "trap" "Script aborted at line $BASH_LINENO with exit code $exit_code."
    exit $exit_code
}
trap on_trap_err ERR

# =======================
# 默认参数（可用环境变量覆盖）
# =======================
CSV_HEADER="${CSV_HEADER:-true}"     # true/false
CSV_DELIM="${CSV_DELIM:-,}"
CSV_NULL="${CSV_NULL:-}"
CSV_ENCODING="${CSV_ENCODING:-UTF8}"

# 可选：设置 statement_timeout（毫秒，0 表示不限制）
STATEMENT_TIMEOUT_MS="${STATEMENT_TIMEOUT_MS:-0}"

# =======================
# 工具函数
# =======================
usage() {
    cat <<'EOF'
Usage:
  gp_copy_csv.sh <SCHEMA> <TABLE> <CSV_FILE>

Connection via environment variables:
  PGHOST, PGPORT, PGUSER, PGPASSWORD, DB_NAME  (required)

CSV options via environment variables (optional):
  CSV_HEADER=true|false     (default: true)
  CSV_DELIM=<delimiter>     (default: ,)
  CSV_NULL=<null-string>    (default: empty string)
  CSV_ENCODING=<encoding>   (default: UTF8)
  STATEMENT_TIMEOUT_MS=<ms> (default: 0)

Example:
  export PGHOST=gp-master; export PGPORT=5432; export PGUSER=gpadmin; export PGPASSWORD=***; export DB_NAME=demo
  ./gp_copy_csv.sh "Sales" "Orders" /data/orders.csv
EOF
}

check_env() {
    local f="check_env"
    [[ -n "${DB_NAME:-}" ]] || die "$f" "DB_NAME is not set."
    [[ -n "${PGHOST:-}" ]]   || die "$f" "PGHOST is not set."
    [[ -n "${PGPORT:-}" ]]   || die "$f" "PGPORT is not set."
    [[ -n "${PGUSER:-}" ]]   || die "$f" "PGUSER is not set."
    [[ -n "${PGPASSWORD:-}" ]] || die "$f" "PGPASSWORD is not set."
    log_info "$f" "Env OK: DB_NAME=$DB_NAME PGHOST=$PGHOST PGPORT=$PGPORT PGUSER=$PGUSER"
}

check_inputs() {
    local f="check_inputs"
    local schema="$1" table="$2" csv="$3"
    [[ -n "$schema" && -n "$table" && -n "$csv" ]] || { usage; die "$f" "Missing required arguments."; }
    [[ -f "$csv" ]] || die "$f" "CSV file not found: $csv"
    [[ -r "$csv" ]] || die "$f" "CSV file not readable: $csv"
    log_info "$f" "Args OK: schema=\"$schema\" table=\"$table\" csv=\"$csv\""
}

test_connection() {
    local f="test_connection"
    local out
    out=$(psql -d "$DB_NAME" -Atq -v ON_ERROR_STOP=1 -X -c "SELECT version();" 2>&1) || {
        die "$f" "Failed to connect to DB. Output: $out"
    }
    log_info "$f" "Connected to DB. Server: $(echo "$out" | head -n1)"
}

ensure_table_exists() {
    local f="ensure_table_exists"
    local schema="$1" table="$2"
    # 精准大小写匹配：用 to_regclass('"Schema"."Table"')
    local reg
    reg=$(psql -d "$DB_NAME" -Atq -v ON_ERROR_STOP=1 -X \
          -c "SELECT to_regclass('\"$schema\".\"$table\"');") || {
        die "$f" "Table check failed (psql error)."
    }
    [[ "$reg" != "" && "$reg" != "NULL" ]] || die "$f" "Target table not found: \"$schema\".\"$table\""
    log_info "$f" "Target table exists: \"$schema\".\"$table\""
}

print_csv_stats() {
    local f="print_csv_stats"
    local csv="$1"
    local lines
    lines=$(wc -l < "$csv" | tr -d ' ') || lines="unknown"
    if [[ "${CSV_HEADER,,}" == "true" ]]; then
        log_info "$f" "CSV lines: $lines (header enabled, expected data rows ~= lines-1)"
    else
        log_info "$f" "CSV lines: $lines (no header)"
    fi
}

copy_csv() {
    local f="copy_csv"
    local schema="$1" table="$2" csv="$3"

    log_info "$f" "COPY options: HEADER=$CSV_HEADER DELIM='$CSV_DELIM' NULL='$CSV_NULL' ENCODING=$CSV_ENCODING"
    log_info "$f" "Begin \copy \"$schema\".\"$table\" FROM '$csv' ..."

    local start_ts end_ts elapsed
    start_ts=$(date +%s)

    # 通过 psql 变量避免复杂转义；\copy 在客户端读取文件并流式传输
    local copy_sql
    copy_sql="\copy \"$schema\".\"$table\" \
FROM :'f' WITH (FORMAT csv, HEADER ${CSV_HEADER}, DELIMITER :'d', NULL :'n', QUOTE '\"', ESCAPE '\"', ENCODING :'e');"

    # 设置 statement_timeout（仅影响服务器端语句，\copy 会发送 INSERT 流）
    local pgopts="-c statement_timeout=${STATEMENT_TIMEOUT_MS}"
    export PGOPTIONS="${PGOPTIONS:-} ${pgopts}"

    # 实际执行
    set +e
    local out
    out=$(psql -d "$DB_NAME" -v ON_ERROR_STOP=1 -X -q \
        -v f="$csv" -v d="$CSV_DELIM" -v n="$CSV_NULL" -v e="$CSV_ENCODING" \
        -c "$copy_sql" 2>&1)
    local rc=$?
    set -e

    end_ts=$(date +%s)
    elapsed=$(( end_ts - start_ts ))

    if [[ $rc -ne 0 ]]; then
        log_error "$f" "psql \copy failed. Elapsed ${elapsed}s. Output:"
        # 将 psql 输出逐行写入 error 日志
        while IFS= read -r line; do log_error "$f" "$line"; done <<< "$out"
        exit $rc
    else
        log_info "$f" "psql \copy succeeded. Elapsed ${elapsed}s."
        # 将 psql 输出逐行写入 info 日志（通常会有 'COPY xxx' 或 '\copy: xxx'）
        while IFS= read -r line; do log_info "$f" "$line"; done <<< "$out"
    fi
}

main() {
    local schema="${1:-}"
    local table="${2:-}"
    local csv="${3:-}"

    log_info "main" "----- START $(basename "$0") -----"
    check_env
    check_inputs "$schema" "$table" "$csv"
    test_connection
    ensure_table_exists "$schema" "$table"
    print_csv_stats "$csv"
    copy_csv "$schema" "$table" "$csv"
    log_info "main" "----- END $(basename "$0") -----"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

main "$@"