#!/bin/sh

# gp_vacuum_sys_tables.sh
# 0 22 * * * sh /home/gpadmin/scripts/gp_vacuum_sys_tables.sh

# 将脚本名称定义为全局变量
script_name=$(basename "$0")

# 设置日志文件路径
LOG_DIR="/home/gpadmin/scripts/"
LOG_FILE="$LOG_DIR/backup_log_$(date +%Y%m%d).log"

# 创建日志目录（如果不存在）
if [ ! -d "$LOG_DIR" ]; then
  mkdir -p "$LOG_DIR"
fi

# 启用错误捕获
set -e

# 定义日志函数
log_message() {
  message="$1"
  if [ -z "$message" ]; then
    message="日志消息为空"
  fi
  timestamp=$(date +"%Y-%m-%d %H:%M:%S")
  echo "[$timestamp] - $script_name : $message" | tee -a "$LOG_FILE"
}

# 添加 GreenPlum 的环境变量，并检查是否成功
if ! source /usr/local/greenplum-db/greenplum_path.sh; then
  log_message "加载 Greenplum 环境变量失败"
  exit 1
fi

# 获取所有非系统数据库的函数
get_user_databases() {
  psql -d postgres -Atc "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres', 'template0', 'template1', 'gpadmin')"
}

# 对指定数据库中的系统表执行 vacuum analyze 的函数
vacuum_database() {
  dbname="$1"
  log_message "开始对数据库 <$dbname> 中的系统表进行 vacuum analyze ..."

  # 构建 vacuum analyze 系统表的命令
  vacuum_command="SELECT 'VACUUM ANALYZE ' || b.nspname || '.' || a.relname || ';' 
                  FROM pg_class a
                  JOIN pg_namespace b ON a.relnamespace = b.oid
                  WHERE b.nspname IN ('pg_toast', 'pg_catalog', 'pg_aoseg', 'information_schema') 
                  AND a.relkind = 'r';"

  # 执行 vacuum analyze 系统表操作
  psql -d "$dbname" -Atc "$vacuum_command" | psql -d "$dbname" -a 2>&1 | tee -a "$LOG_FILE"

  # 检查 vacuum analyze 操作是否成功
  if [ $? -ne 0 ]; then
      log_message "数据库 <$dbname> 的系统表 Vacuum 操作失败。"
  else
      log_message "数据库 <$dbname> 的系统表 Vacuum 操作成功完成。"
  fi
}

# 主流程函数
main() {
  log_message "获取所有非系统数据库列表并对其系统表进行 vacuum 操作。"
  
  # 获取数据库列表并执行 vacuum
  for db in $(get_user_databases); do
    vacuum_database "$db"
  done

  log_message "脚本执行结束。"
}

# 调用主流程函数
main