#!/bin/bash

# database_metadata_backup.sh



# 设置备份目录
BACKUP_DIR="/home/gpadmin/scripts/meta_backup/files"
LOG_DIR="/home/gpadmin/scripts/meta_backup/logs"

# Script name defined as a global variable
script_name=$(basename "$0")

# 创建备份目录和日志目录（如果不存在）
mkdir -p $BACKUP_DIR
mkdir -p $LOG_DIR

# 设置日志文件路径
LOG_FILE="$LOG_DIR/backup_log_$(date +%Y%m%d).log"

# 定义日志函数
log_message() {
  message="$1"
  timestamp=$(date +"%Y-%m-%d %H:%M:%S")
  echo "[$timestamp] - $script_name : $message" | tee -a "$LOG_FILE"
}

# 定义数据库备份函数
backup_database() {
    local db_name=$1
    local timestamp=$(date "+%Y%m%d%H%M%S")
    local backup_file1="${BACKUP_DIR}/${db_name}_metadata_${timestamp}.dump"
    local backup_file2="${BACKUP_DIR}/${db_name}_metadata_${timestamp}.sql"

    # 备份为 dump 文件
    log_message "${db_name} dump file 备份开始"
    pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER -w -s -F c -f $backup_file1 $db_name >> $LOG_FILE 2>&1
    if [ $? -eq 0 ]; then
        log_message "${db_name} dump file 元数据备份成功 $backup_file1"
    else
        log_message "${db_name} dump file 元数据备份失败"
        return 1
    fi

    # 备份为 SQL 文件
    log_message "${db_name} sql file 备份开始"
    pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER -w -s -f $backup_file2 $db_name >> $LOG_FILE 2>&1
    if [ $? -eq 0 ]; then
        log_message "${db_name} sql file 元数据备份成功 $backup_file2"
    else
        log_message "${db_name} sql file 元数据备份失败"
        return 1
    fi

    return 0
}

# 定义清理过期文件函数
cleanup_files() {
    local directory=$1
    local days=$2
    find $directory -type f -mtime +$days -exec rm {} \;
    log_message "清理 $directory 中超过 $days 天的文件完成"
}

# 定义处理数据库备份的循环函数
process_databases() {
    # 获取所有非系统数据库列表，不包含gpperfmon数据库
    local databases=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -t -c "SELECT datname FROM pg_database WHERE datname NOT IN ('template0', 'template1', 'postgres', 'gpperfmon');")
    
    # 循环遍历每个数据库进行备份
    for db_name in $databases; do
        # 去除前后空格
        db_name=$(echo $db_name | xargs)

        # 调用备份函数
        backup_database $db_name
        if [ $? -ne 0 ]; then
            log_message "${db_name} 数据库备份出现错误，跳过此数据库"
        fi
    done
}

# 主执行逻辑
main() {
    # 记录备份开始信息到日志
    log_message "备份开始"

    # 执行数据库备份的循环逻辑
    process_databases

    # 记录备份结束信息到日志
    log_message "备份结束"

    # 清理过期备份文件（保留30天）
    cleanup_files $BACKUP_DIR 30

    # 清理过期日志文件（保留30天）
    cleanup_files $LOG_DIR 30
}

# 调用主函数
main