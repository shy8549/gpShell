#!/bin/bash

# 脚本名称：auto_partition_manager.sh
# 脚本说明：
#   该脚本用于在 Greenplum 数据库中管理分区表。脚本会查询 conf_part.part_auto_conf 表中定义的 schema.table，
#   并依次检查每个表是否为分区表。如果是分区表，则调用分区管理函数 conf_part.part_for_single_upper_table。
#   脚本支持自动记录日志，记录每次执行的操作结果和错误信息。
#
# 使用方法：
#   1. 配置数据库名称 DB_NAME，确保脚本在 Greenplum master 节点上执行。
#   2. 将脚本保存并赋予执行权限：chmod +x partition_management.sh
#   3. 运行脚本：./partition_management.sh
#
# 日志：
#   脚本在 /home/gpadmin/scripts/ 目录下生成每日日志文件，文件名格式为 partition_management_log_YYYYMMDD.log。
#   日志中包含了脚本执行过程中的所有操作记录、成功信息和错误信息。
#
# 注意事项：
#   - 该脚本仅适用于分区表，请确保 conf_part.part_auto_conf 中配置的表为分区表。
#   - 脚本需要在 master 节点执行，确保配置的数据库和表权限正确。
#   - 为避免意外操作，请在执行前备份相关数据。

# 数据库连接信息
DB_NAME="sjzt"

# 定义脚本名称和日志路径
script_name=$(basename "$0")
LOG_DIR="/home/gpadmin/scripts/"
LOG_FILE="$LOG_DIR/${script_name%.*}_log_$(date +%Y%m%d).log"

# 创建日志目录（如果不存在）
if [ ! -d "$LOG_DIR" ]; then
  mkdir -p "$LOG_DIR"
fi

# 定义日志函数
log_message() {
  local message="$1"
  local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
  echo "[$timestamp] - $script_name : $message" | tee -a "$LOG_FILE"
}

# 将所有输出重定向到日志文件
exec > >(tee -a "$LOG_FILE") 2>&1

# 定义检查查询结果的函数
check_query_result() {
  local result="$1"
  if [ -z "$result" ]; then
      log_message "No records found, or query failed."
      exit 1
  fi
}

# 定义校验函数，检查指定的表是否为分区表
check_if_partitioned_table() {
  local schema_name="$1"
  local table_name="$2"

  # 查询表是否为分区表
  local is_partitioned=$(psql -d $DB_NAME -t -c "SELECT EXISTS (
    SELECT 1
    FROM pg_partitions
    WHERE schemaname = '$schema_name'
      AND tablename = '$table_name'
  );" | xargs)

  # 返回校验结果
  if [ "$is_partitioned" = "t" ]; then
    return 0  # 是分区表
  else
    return 1  # 不是分区表
  fi
}

# 定义执行分区管理的函数
execute_partition_management() {
  local schema_name="$1"
  local table_name="$2"

  log_message "Processing table: $schema_name.$table_name"
  
  # 调用分区管理函数
  psql -d $DB_NAME -c "SELECT conf_part.part_for_single_upper_table('$schema_name', '$table_name');"
  
  # 检查函数执行结果
  if [ $? -eq 0 ]; then
      log_message "Successfully processed $schema_name.$table_name"
  else
      log_message "Error processing $schema_name.$table_name"
  fi
}

# 定义处理所有表的函数
process_all_tables() {
  local schema_tables="$1"

  # 循环遍历每个 schema 和 table
  while IFS="|" read -r schema_name table_name; do
      # 去除字符串中的空格
      schema_name=$(echo "$schema_name" | xargs)
      table_name=$(echo "$table_name" | xargs)
  
      # 确保 schema 和 table 名称非空
      if [ -n "$schema_name" ] && [ -n "$table_name" ]; then
          # 检查表是否为分区表
          if check_if_partitioned_table "$schema_name" "$table_name"; then
              execute_partition_management "$schema_name" "$table_name"
          else
              log_message "Skipping $schema_name.$table_name as it is not a partitioned table."
          fi
      fi
  done <<< "$schema_tables"
}

# 定义主流程函数
main() {
  log_message "Starting partition management script."

  # 查询 schema.table 的 SQL 语句
  SQL_QUERY="SELECT schema_name, table_name FROM conf_part.part_auto_conf;"
  
  # 使用 psql 查询 schema 和 table
  schema_tables=$(psql -d $DB_NAME -t -c "$SQL_QUERY")
  
  # 检查查询是否成功
  check_query_result "$schema_tables"
  
  log_message "Starting partition management task..."
  
  # 调用处理所有表的函数
  process_all_tables "$schema_tables"
  
  log_message "Partition management task completed."
  log_message "Partition management script finished."
}

# 调用主流程函数
main