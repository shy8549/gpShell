#!/bin/bash
# =========================================================
# remote_cmd.sh — 批量远程操作（免密优先 + 密码回退 + 可选分发公钥）
# 依赖：bash、ssh/scp、awk、sed、(expect 仅在回退或分发密钥时需要)
# 作者：Suhy
# 版本：v2.0
# hosts.txt 行格式：IP USER PASSWORD      # PASSWORD 可留空（仅走免密）
# 说明：本脚本已通过 `bash -n` 语法检查；所有 ssh 默认加 -n，避免吞掉 while 的 stdin
# =========================================================

# ---------- 基本安全项 ----------
# 不用 set -e（避免单机失败中断全局）；使用未定义变量时报错；管道错误往外传
set -u -o pipefail

# ---------- 用户配置（按需修改） ----------
SRC_FILE="/data1/packages/presto-server-0.257.tar.gz"                      # 需要上传的本地文件（留空表示不上传）
DEST_PATH="/data1/packages"     # 远端目录（上传时会自动 mkdir -p）
PORT=4567                        # Kafka 示例脚本的监听端口
HOSTS_FILE="/home/cdphadoop/scripts/hosts.txt"

# SSH/SCP 选项（关键：-n 避免 ssh 读取 while 的 stdin）
SSH_PORT=22
# SSH 连接超时秒
SSH_TIMEOUT=10
# SCP 传输超时秒
SCP_TIMEOUT=120
SSH_OPTS="-n -o StrictHostKeyChecking=no -o ConnectTimeout=${SSH_TIMEOUT} -p ${SSH_PORT}"
SCP_OPTS="-q -o StrictHostKeyChecking=no -o ConnectTimeout=${SCP_TIMEOUT} -P ${SSH_PORT}"

# 本机密钥（用于免密分发）
LOCAL_SSH_KEY="${HOME}/.ssh/id_rsa"
LOCAL_SSH_PUB="${HOME}/.ssh/id_rsa.pub"

# ---------- 开关（true/false） ----------
DO_CHECK_LOGIN=false          # 是否做登录检查
DO_SETUP_KEY=false            # 是否把本机公钥分发到远端（实现免密）
DO_GEN_SCRIPT=false           # 是否生成 Kafka 临时脚本
DO_SCP_UPLOAD_SCRIPT=false    # 是否上传 Kafka 临时脚本
DO_SSH_EXECUTE_SCRIPT=false   # 是否执行 Kafka 临时脚本
DO_SIMPLE_CMD=true            # 是否执行简单命令
DO_UPLOAD_FILE=false          # 是否上传 SRC_FILE -> DEST_PATH

# 要执行的简单命令（示例）
SIMPLE_CMD="source /etc/profile && java -version"

# ---------- 日志相关 ----------
LOG_SUCCESS="batch_success.log"               # 成功主机列表
LOG_FAILED="batch_failed.log"                 # 失败主机列表
LOG_DIR="./logs/remote_cmd"                   # 每台主机独立日志
mkdir -p "$LOG_DIR"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log_info()  { echo "$(ts) [INFO ] $*"; }
log_warn()  { echo "$(ts) [WARN ] $*"; }
log_error() { echo "$(ts) [ERROR] $*" 1>&2; }
host_log()  { local host="$1"; shift; echo "$(ts) $*" | tee -a "${LOG_DIR}/${host}.log"; }

clear_summary_logs() { : > "$LOG_SUCCESS"; : > "$LOG_FAILED"; }

# ---------- 依赖与准备 ----------
need_expect() {
  if ! command -v expect >/dev/null 2>&1; then
    log_error "Expect is not installed. Please install it first!"
    exit 1
  fi
}

ensure_local_key() {
  if [ ! -f "$LOCAL_SSH_KEY" ] || [ ! -f "$LOCAL_SSH_PUB" ]; then
    log_info "Local SSH key not found. Generating 4096-bit RSA keypair..."
    mkdir -p "$(dirname "$LOCAL_SSH_KEY")"
    ssh-keygen -t rsa -b 4096 -N "" -f "$LOCAL_SSH_KEY" >/dev/null 2>&1 || {
      log_error "Failed to generate SSH key."
      exit 1
    }
  fi
}

# ---------- 免密探测 & 分发公钥 ----------
# 仅用密钥试连：成功会输出 login_success（不涉及密码）
probe_key_login() {
  local host="$1" user="$2"
  ssh -o BatchMode=yes $SSH_OPTS "${user}@${host}" "echo login_success" 2>/dev/null
}

# 将本机公钥追加到远端 authorized_keys（需提供密码）
distribute_pubkey() {
  local host="$1" user="$2" passwd="${3-}"

  if [ -z "${passwd:-}" ]; then
    host_log "$host" "[KEY ] No password provided; cannot distribute public key."
    return 2
  fi

  need_expect
  ensure_local_key

  host_log "$host" "[KEY ] Prepare ~/.ssh and authorized_keys"
  expect <<EOF
    set timeout ${SSH_TIMEOUT}
    log_user 0
    spawn ssh $SSH_OPTS ${user}@${host} "mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
    expect {
      "(yes/no)?" { send "yes\n"; exp_continue }
      "*assword:" { send "$passwd\n" }
      timeout { exit 3 }
    }
    expect eof
EOF
  local rc1=$?
  if [ $rc1 -ne 0 ]; then
    host_log "$host" "[KEY ] Prepare ~/.ssh failed (rc=$rc1)"
    return $rc1
  fi

  host_log "$host" "[KEY ] Upload public key"
  expect <<EOF
    set timeout ${SCP_TIMEOUT}
    log_user 0
    spawn scp $SCP_OPTS "${LOCAL_SSH_PUB}" ${user}@${host}:/tmp/.tmp_id_rsa.pub
    expect {
      "(yes/no)?" { send "yes\n"; exp_continue }
      "*assword:" { send "$passwd\n" }
      timeout { exit 3 }
    }
    expect eof
EOF
  local rc2=$?
  if [ $rc2 -ne 0 ]; then
    host_log "$host" "[KEY ] Upload public key failed (rc=$rc2)"
    return $rc2
  fi

  host_log "$host" "[KEY ] Append public key to authorized_keys"
  expect <<EOF
    set timeout ${SSH_TIMEOUT}
    log_user 0
    spawn ssh $SSH_OPTS ${user}@${host} "cat /tmp/.tmp_id_rsa.pub >> ~/.ssh/authorized_keys && rm -f /tmp/.tmp_id_rsa.pub"
    expect {
      "(yes/no)?" { send "yes\n"; exp_continue }
      "*assword:" { send "$passwd\n" }
      timeout { exit 3 }
    }
    expect eof
EOF
  local rc3=$?
  if [ $rc3 -ne 0 ]; then
    host_log "$host" "[KEY ] Append public key failed (rc=$rc3)"
    return $rc3
  fi

  if probe_key_login "$host" "$user" | grep -q login_success; then
    host_log "$host" "[KEY ] Passwordless SSH OK."
    return 0
  else
    host_log "$host" "[KEY ] Passwordless SSH still not working."
    return 4
  fi
}

# ---------- SSH/SCP：免密优先，失败回退密码 ----------
ssh_run() {
  local host="$1" user="$2" passwd="${3-}" cmd="$4"

  # 先试密钥
  if probe_key_login "$host" "$user" | grep -q login_success; then
    host_log "$host" "[SSH ] KEY  exec: $cmd"
    ssh -o BatchMode=yes $SSH_OPTS "${user}@${host}" "$cmd"
    return $?
  fi

  # 回退口令
  if [ -n "${passwd:-}" ]; then
    need_expect
    host_log "$host" "[SSH ] PASS exec: $cmd"
    expect <<EOF
      set timeout ${SSH_TIMEOUT}
      log_user 1
      spawn ssh $SSH_OPTS ${user}@${host} "$cmd"
      expect {
        "(yes/no)?" { send "yes\n"; exp_continue }
        "*assword:" { send "$passwd\n" }
        timeout { exit 3 }
      }
      expect eof
EOF
    return $?
  fi

  host_log "$host" "[SSH ] No key login and no password provided."
  return 5
}

scp_upload() {
  local host="$1" user="$2" passwd="${3-}" src="$4" dest="$5"

  host_log "$host" "[SCP ] Ensure remote dir: $dest"
  ssh_run "$host" "$user" "$passwd" "mkdir -p '$dest'" >/dev/null 2>&1 || {
    host_log "$host" "[SCP ] mkdir -p '$dest' failed"
    return 6
  }

  host_log "$host" "[SCP ] KEY  put: $src -> ${host}:$dest"
  scp -o BatchMode=yes $SCP_OPTS "$src" "${user}@${host}:$dest" 2>/dev/null && return 0

  if [ -n "${passwd:-}" ]; then
    need_expect
    host_log "$host" "[SCP ] PASS put: $src -> ${host}:$dest"
    expect <<EOF
      set timeout ${SCP_TIMEOUT}
      log_user 1
      spawn scp $SCP_OPTS "$src" ${user}@${host}:"$dest"
      expect {
        "(yes/no)?" { send "yes\n"; exp_continue }
        "*assword:" { send "$passwd\n" }
        timeout { exit 3 }
      }
      expect eof
EOF
    return $?
  fi

  host_log "$host" "[SCP ] Failed (no password to fallback)."
  return 7
}

# ---------- Kafka 示例临时脚本 ----------
generate_tmp_script() {
  local port="$1"
  local tmp_script="/tmp/remote_update_kafka.sh"

  # 注意：用 'EOF' 保证此处变量不被本地展开；稍后用 sed 注入端口逻辑
  cat > "$tmp_script" <<'EOF'
#!/bin/bash
set -e

CONFIG_FILE="/usr/local/kafka/config/server.properties"
START_SCRIPT="/usr/local/kafka/bin/kafka-server-start.sh"

echo "Checking configuration file: $CONFIG_FILE"
if [ ! -f "$CONFIG_FILE" ]; then
  echo "ERROR: $CONFIG_FILE does not exist"
  exit 1
fi

echo "Checking startup script: $START_SCRIPT"
if [ ! -f "$START_SCRIPT" ]; then
  echo "ERROR: $START_SCRIPT does not exist"
  exit 2
fi

HOSTNAME=$(hostname)
echo "Current hostname: $HOSTNAME"

BROKER_ID=$(echo "$HOSTNAME" | awk -F'.' '{print $1}' | awk -F'-' '{print $3}')
if [[ -z "$BROKER_ID" ]]; then
  echo "ERROR: Could not extract broker.id from hostname"
  exit 3
fi

echo "Setting broker.id=$BROKER_ID"
sed -i "s|^broker.id=.*|broker.id=$BROKER_ID|" "$CONFIG_FILE"

__LISTENERS_LINE__

echo "Updating Kafka heap memory settings..."
sed -i 's|export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"|export KAFKA_HEAP_OPTS="-Xmx8G -Xms8G"|' "$START_SCRIPT"

echo "Update complete."
EOF

  # 注入监听设置（注意 \$ 转义，保持脚本中变量在远端才展开）
  sed -i "s|__LISTENERS_LINE__|echo \"Setting listeners=PLAINTEXT://\\\$HOSTNAME:${port}\"; sed -i \"s|^listeners=.*|listeners=PLAINTEXT://\\\$HOSTNAME:${port}|\" \"\\\$CONFIG_FILE\"|g" "$tmp_script"
  echo "$tmp_script"
}

# ---------- 单台主机处理 ----------
process_node() {
  local host="$1" user="$2" passwd="${3-}"

  host_log "$host" "==================================="
  host_log "$host" "[FLOW] Start process host: $host (user=$user)"

  # 0) 分发免密（可选）
  if [ "$DO_SETUP_KEY" = true ]; then
    distribute_pubkey "$host" "$user" "$passwd" || {
      host_log "$host" "[KEY ] Distribute SSH key failed (continue)."
      echo "$host" >> "$LOG_FAILED"
    }
  fi

  # 1) 登录检查（可选）
  if [ "$DO_CHECK_LOGIN" = true ]; then
    if probe_key_login "$host" "$user" | grep -q login_success; then
      host_log "$host" "[CHECK] Key-based login OK."
    else
      if [ -n "${passwd:-}" ]; then
        host_log "$host" "[CHECK] Key login not ready; try password..."
        if ssh_run "$host" "$user" "$passwd" "echo login_success" | grep -q login_success; then
          host_log "$host" "[CHECK] Password-based login OK."
        else
          host_log "$host" "[CHECK] Login failed."
          echo "$host" >> "$LOG_FAILED"
          return
        fi
      else
        host_log "$host" "[CHECK] No key login and no password provided."
        echo "$host" >> "$LOG_FAILED"
        return
      fi
    fi
  fi

  # 2) 生成临时脚本（可选）
  local TMP_SCRIPT=""
  if [ "$DO_GEN_SCRIPT" = true ]; then
    TMP_SCRIPT=$(generate_tmp_script "$PORT")
    host_log "$host" "[KAFKA] Generated local script: $TMP_SCRIPT"
  fi

  # 3) 上传临时脚本（可选）
  if [ "$DO_SCP_UPLOAD_SCRIPT" = true ] && [ -n "$TMP_SCRIPT" ]; then
    scp_upload "$host" "$user" "$passwd" "$TMP_SCRIPT" "/tmp" || {
      host_log "$host" "[KAFKA] Upload temp script failed."
      echo "$host" >> "$LOG_FAILED"
      rm -f "$TMP_SCRIPT"
      return
    }
  fi

  # 4) 执行临时脚本（可选）
  if [ "$DO_SSH_EXECUTE_SCRIPT" = true ]; then
    host_log "$host" "[KAFKA] Executing remote temp script..."
    ssh_run "$host" "$user" "$passwd" "bash /tmp/remote_update_kafka.sh && rm -f /tmp/remote_update_kafka.sh"
    if [ $? -eq 0 ]; then
      host_log "$host" "[KAFKA] Configuration updated successfully."
      echo "$host" >> "$LOG_SUCCESS"
    else
      host_log "$host" "[KAFKA] Failed to update configuration."
      echo "$host" >> "$LOG_FAILED"
    fi
  fi

  # 5) 上传普通文件（可选）
  if [ "$DO_UPLOAD_FILE" = true ] && [ -n "$SRC_FILE" ]; then
    if [ ! -f "$SRC_FILE" ]; then
      host_log "$host" "[FILE ] SRC_FILE not found: $SRC_FILE"
      echo "$host" >> "$LOG_FAILED"
    else
      host_log "$host" "[FILE ] Upload: $SRC_FILE -> ${DEST_PATH}"
      scp_upload "$host" "$user" "$passwd" "$SRC_FILE" "$DEST_PATH"
      if [ $? -eq 0 ]; then
        host_log "$host" "[FILE ] Upload success."
        echo "$host" >> "$LOG_SUCCESS"
      else
        host_log "$host" "[FILE ] Upload failed."
        echo "$host" >> "$LOG_FAILED"
      fi
    fi
  fi

  # 6) 执行简单命令（可选）
  if [ "$DO_SIMPLE_CMD" = true ]; then
    host_log "$host" "[CMD  ] $SIMPLE_CMD"
    ssh_run "$host" "$user" "$passwd" "$SIMPLE_CMD"
    if [ $? -ne 0 ]; then
      host_log "$host" "[CMD  ] Simple command failed."
      echo "$host" >> "$LOG_FAILED"
    else
      host_log "$host" "[CMD  ] Simple command finished."
      echo "$host" >> "$LOG_SUCCESS"
    fi
  fi

  # 7) 清理本地临时脚本
  if [ -n "${TMP_SCRIPT:-}" ]; then
    rm -f "$TMP_SCRIPT"
  fi

  host_log "$host" "[FLOW] Done."
}

# ---------- 主流程 ----------
main() {
  clear_summary_logs

  if [ ! -f "$HOSTS_FILE" ]; then
    log_error "Hosts file not found: $HOSTS_FILE"
    exit 1
  fi

  log_info "===== Batch start ====="
  log_info "Hosts file: $HOSTS_FILE"
  log_info "SSH opts  : $SSH_OPTS"
  log_info "Switches  : DO_CHECK_LOGIN=$DO_CHECK_LOGIN DO_SETUP_KEY=$DO_SETUP_KEY DO_GEN_SCRIPT=$DO_GEN_SCRIPT DO_SCP_UPLOAD_SCRIPT=$DO_SCP_UPLOAD_SCRIPT DO_SSH_EXECUTE_SCRIPT=$DO_SSH_EXECUTE_SCRIPT DO_SIMPLE_CMD=$DO_SIMPLE_CMD DO_UPLOAD_FILE=$DO_UPLOAD_FILE"
  log_info "Action    : SIMPLE_CMD='$SIMPLE_CMD' SRC_FILE='${SRC_FILE:-}' DEST_PATH='$DEST_PATH'"

  # 关键：用 FD 3 读取 hosts，避免 ssh 影响 while 的 stdin
  exec 3< "$HOSTS_FILE"
  while IFS=' ' read -r HOST USER PASS <&3; do
    # 跳过空行与注释
    [ -z "${HOST:-}" ] && continue
    [[ "${HOST:0:1}" == "#" ]] && continue
    process_node "$HOST" "$USER" "${PASS:-}"
  done
  exec 3<&-

  log_info "===== Batch end ====="
  log_info "Success list -> $LOG_SUCCESS"
  log_info "Failed  list -> $LOG_FAILED"
}

main
