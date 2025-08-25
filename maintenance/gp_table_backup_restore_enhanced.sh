#!/bin/bash

# gp_table_backup_restore_enhanced.sh
# Greenplum 环境加载
source /usr/local/greenplum-db/greenplum_path.sh

##########################################
# 配置区（请按需修改）
##########################################

DB_NAME="SJGXPT"
TABLE_LIST_FILE="./tables_to_backup.txt"
BACKUP_DIR="/data/gp_backup"  ## 修改找个大空间的，别把home给写满了
MODE="backup"     # 可选值：backup 或 restore

ENABLE_GZIP=true             # 是否启用 gzip 压缩（仅在 backup 模式有效）
MAX_JOBS=4                   # 最大并发数（适用于 backup/restore）
RESTORE_STRUCTURE_ONLY=false  # 仅恢复结构（restore 模式）
RESTORE_DATA_ONLY=false       # 仅恢复数据（restore 模式）

##########################################
# 日志 & 成功失败记录
##########################################

DATE_STR=$(date +'%Y%m%d')
SCRIPT_NAME=$(basename "$0" .sh)
LOG_DIR="$(pwd)/logs/$SCRIPT_NAME"
LOG_INFO="$LOG_DIR/info_$DATE_STR.log"
LOG_ERROR="$LOG_DIR/error_$DATE_STR.log"
SUCCESS_LIST="$LOG_DIR/success_list_$DATE_STR.txt"
FAIL_LIST="$LOG_DIR/fail_list_$DATE_STR.txt"

mkdir -p "$LOG_DIR" "$BACKUP_DIR"

log_info() {
    local msg="$1"
    echo "$(date +'%F %T') [INFO] $msg" | tee -a "$LOG_INFO"
}

log_error() {
    local msg="$1"
    echo "$(date +'%F %T') [ERROR] $msg" | tee -a "$LOG_ERROR" >&2
}

##########################################
# 并发控制（限速）
##########################################
limit_jobs() {
    while [[ $(jobs -r | wc -l) -ge $MAX_JOBS ]]; do
        sleep 1
    done
}

##########################################
# 结构备份
##########################################
backup_structure() {
    local schema="$1" table="$2"
    local struct_file="$BACKUP_DIR/${schema}.${table}_schema.sql"
    local full_table="\"$schema\".\"$table\""

    if pg_dump -s -t "$full_table" -d "$DB_NAME" -f "$struct_file"; then
        [[ $ENABLE_GZIP == true ]] && gzip -f "$struct_file"
        log_info "Structure backup success: $full_table"
    else
        log_error "Structure backup failed: $full_table"
        return 1
    fi
}

##########################################
# 数据备份
##########################################
backup_data() {
    local schema="$1" table="$2"
    local data_file="$BACKUP_DIR/${schema}.${table}_data.csv"
    local full_table="\"$schema\".\"$table\""

    if psql -d "$DB_NAME" -c "\COPY $full_table TO '$data_file' WITH CSV HEADER"; then
        [[ $ENABLE_GZIP == true ]] && gzip -f "$data_file"
        log_info "Data backup success: $full_table"
    else
        log_error "Data backup failed: $full_table"
        return 1
    fi
}

##########################################
# 结构恢复
##########################################
restore_structure() {
    local schema="$1" table="$2"
    local struct_file="$BACKUP_DIR/${schema}.${table}_schema.sql"
    [[ -f "${struct_file}.gz" ]] && gunzip -f "${struct_file}.gz"

    if [[ ! -f "$struct_file" ]]; then
        log_error "Structure file not found: $struct_file"
        return 1
    fi

    if psql -d "$DB_NAME" -f "$struct_file"; then
        log_info "Structure restore success: $schema.$table"
    else
        log_error "Structure restore failed: $schema.$table"
        return 1
    fi
}

##########################################
# 数据恢复
##########################################
restore_data() {
    local schema="$1" table="$2"
    local full_table="\"$schema\".\"$table\""
    local data_file="$BACKUP_DIR/${schema}.${table}_data.csv"
    [[ -f "${data_file}.gz" ]] && gunzip -f "${data_file}.gz"

    if [[ ! -f "$data_file" ]]; then
        log_error "Data file not found: $data_file"
        return 1
    fi

    if psql -d "$DB_NAME" -c "\COPY $full_table FROM '$data_file' WITH CSV HEADER"; then
        log_info "Data restore success: $full_table"
    else
        log_error "Data restore failed: $full_table"
        return 1
    fi
}

##########################################
# 处理单张表
##########################################
process_table() {
    local schema="$1" table="$2"
    local status=0

    if [[ "$MODE" == "backup" ]]; then
        backup_structure "$schema" "$table" || status=1
        backup_data "$schema" "$table" || status=1
    elif [[ "$MODE" == "restore" ]]; then
        if [[ "$RESTORE_DATA_ONLY" == false ]]; then
            restore_structure "$schema" "$table" || status=1
        fi
        if [[ "$RESTORE_STRUCTURE_ONLY" == false ]]; then
            restore_data "$schema" "$table" || status=1
        fi
    fi

    if [[ $status -eq 0 ]]; then
        echo "$schema.$table" >> "$SUCCESS_LIST"
    else
        echo "$schema.$table" >> "$FAIL_LIST"
    fi
}

##########################################
# 主逻辑
##########################################
main() {
    if [[ "$MODE" != "backup" && "$MODE" != "restore" ]]; then
        log_error "Invalid MODE: $MODE. Should be backup or restore"
        exit 1
    fi

    if [[ ! -f "$TABLE_LIST_FILE" ]]; then
        log_error "Table list file not found: $TABLE_LIST_FILE"
        exit 1
    fi

    log_info "Starting $MODE with max $MAX_JOBS jobs..."
    > "$SUCCESS_LIST"
    > "$FAIL_LIST"

    while IFS=. read -r schema table; do
        [[ -z "$schema" || -z "$table" ]] && continue
        limit_jobs
        process_table "$schema" "$table" &
    done < "$TABLE_LIST_FILE"

    wait
    log_info "All tasks completed. Success: $(wc -l < "$SUCCESS_LIST"), Fail: $(wc -l < "$FAIL_LIST")"
}

main