#!/bin/bash

# gp_auto_maintenance.sh
# 描述：该脚本用于对 Greenplum 集群执行自动化维护任务，包括检查并恢复故障段、段重平衡操作，
#      以及自动清理日志文件，以确保集群的高可用性和稳定性。所有操作和结果都记录到按日轮换的日志文件中。
# 依赖：该脚本依赖公共函数脚本./common_functions.sh
# 使用方法：
# 设置一个 cron 任务每小时运行此脚本：
# 0 * * * * sh /home/gpadmin/scripts/gp_auto_maintenance.sh

# 设置脚本名称，用于日志记录和其他操作
script_name=$(basename "$0" .sh)

# 加载通用函数脚本，包括日志管理和环境初始化
source ./common_functions.sh

# 配置全局变量
initialize_log_dir                                  # 初始化日志目录
LOGFILE="$LOG_DIR/${script_name}_$(date +'%Y%m%d').log"   # 日志文件路径，按日期轮换
qRECOVERY_WAIT_TIME=120                              # 增量恢复后的等待时间（秒）
REBALANCE_WAIT_TIME=300                             # 重平衡前的等待时间（秒）
LOG_RETENTION_DAYS=30                               # 日志保留天数

# 定义用于检查段状态的 SQL 查询
SQL_CHECK_FAILED_SEGMENTS="SELECT count(1) FROM gp_segment_configuration WHERE status = 'd';"   # 检查故障段
SQL_CHECK_UNSYNCED_SEGMENTS="SELECT count(1) FROM gp_segment_configuration WHERE mode = 'n' AND status = 'u' AND content <> -1;"   # 检查未同步段
SQL_CHECK_NON_PREFERRED_SEGMENTS="SELECT count(1) FROM gp_segment_configuration WHERE preferred_role <> role AND status = 'u' AND mode = 's';"  # 检查不在首选角色的段

# 函数：检查脚本是否已有其他实例在运行，避免并发执行
check_if_running() {
    log_start "check_if_running"
    log INFO "Checking if another instance of $script_name is already running."
    
    running_instances=$(pgrep -fc "$script_name")
    if [[ "$running_instances" -gt 1 ]]; then
        log WARNING "Another instance of $script_name is already running. Exiting."
        exit 1  # 发现并发实例则退出
    fi
    log INFO "No other instance of $script_name found."
    log_end "check_if_running"
}

# 函数：检查所需命令和环境变量是否加载，确保脚本正常执行环境
check_environment() {
    log_start "environment check"    
    log INFO "Checking environment prerequisites: Ensuring required commands and Greenplum environment are loaded."
    source /home/gpadmin/.bashrc
    # 检查命令是否存在
    if ! command -v psql &> /dev/null || ! command -v gprecoverseg &> /dev/null; then
        log ERROR "Required commands (psql, gprecoverseg) not found. Exiting."
        exit 1
    fi
    # 加载 Greenplum 环境变量
    source /usr/local/greenplum-db/greenplum_path.sh || {
        log ERROR "Failed to load Greenplum environment variables. Exiting."
        exit 1
    }
    log_end "environment check"
}

# 函数：执行 SQL 查询并返回结果，不带日志输出
execute_sql() {
    local query="$1"
    psql -d postgres -Atc "$query" 2>/dev/null  # 执行 SQL 查询，静默模式，返回纯结果
}

# 函数：处理错误消息并退出
handle_error() {
    log_start "handle_error"
    log ERROR "$1. Exiting script."
    exit 1
}

# 函数：尝试恢复段
# 执行增量恢复，若恢复不完全，则执行全量恢复
attempt_recovery() {
    log_start "attempt_recovery"
    log INFO "Attempting incremental recovery to restore segment functionality."
    
    gprecoverseg -a | tee -a "$LOGFILE"  # 执行增量恢复
    [[ $? -ne 0 ]] && handle_error "Incremental recovery failed"  # 若失败则记录错误并退出
    log INFO "Waiting $RECOVERY_WAIT_TIME seconds before rechecking failed segments."
    sleep "$RECOVERY_WAIT_TIME"

    # 检查增量恢复后是否仍存在故障段，必要时进行全量恢复
    if check_segment_status "$SQL_CHECK_FAILED_SEGMENTS" "Segments still down." "No failed segments detected."; then
        log INFO "Performing full recovery as incremental recovery was insufficient."
        gprecoverseg -aF | tee -a "$LOGFILE"  # 执行全量恢复
        [[ $? -ne 0 ]] && handle_error "Full recovery failed"
    fi
    log_end "attempt_recovery"
}

# 函数：检查段状态，基于 SQL 查询结果输出日志信息
# 参数1：SQL 查询条件，参数2：发现问题时的日志消息，参数3：无问题时的日志消息
check_segment_status() {
    log_start "check_segment_status"
    log INFO "Checking segment status based on provided SQL condition."
    
    local condition="$1"
    local message_if_true="$2"
    local message_if_false="$3"
    local result
    result=$(execute_sql "$condition")  # 执行查询并获取结果

    if [[ "$result" -gt 0 ]]; then
        log WARNING "$message_if_true"  # 若存在问题，记录警告日志
        return 0
    else
        log INFO "$message_if_false"   # 无问题则记录正常日志
        return 1
    fi
    log_end "check_segment_status"
}

# 函数：检查并恢复故障段
check_and_recover_segments() {
    log_start "check_and_recover_segments"
    log INFO "Initiating check for failed segments and performing recovery if necessary."
    
    if check_segment_status "$SQL_CHECK_FAILED_SEGMENTS" "Segments are down." "All segments are operational."; then
        attempt_recovery  # 若存在故障段，调用恢复函数
    fi
    log_end "check_and_recover_segments"
}

# 函数：检查并重平衡段，保证所有段在其首选角色并同步
check_and_rebalance_segments() {
    log_start "check_and_rebalance_segments"
    log INFO "Checking segments for role consistency and synchronizing as needed."
    
    if check_segment_status "$SQL_CHECK_NON_PREFERRED_SEGMENTS" \
        "Non-preferred segments detected." \
        "All segments are in their preferred roles."; then

        # 若有非首选角色段，检查是否存在未同步的段
        if check_segment_status "$SQL_CHECK_UNSYNCED_SEGMENTS" \
            "Unsynchronized segments detected; waiting $REBALANCE_WAIT_TIME seconds." \
            "All segments are synchronized."; then
            sleep "$REBALANCE_WAIT_TIME"  # 等待指定时间以确保同步
        fi

        log INFO "Starting segment rebalancing."
        gprecoverseg -ar | tee -a "$LOGFILE"  # 执行重平衡操作
        [[ $? -ne 0 ]] && handle_error "Segment rebalancing failed"  # 若重平衡失败则记录错误并退出
    fi
    log_end "check_and_rebalance_segments"
}

# 主函数：调用检查、恢复、重平衡和日志清理操作
main() {
    log_start "gp_auto_maintenance"
    log INFO "Starting main maintenance function to handle segment recovery, rebalancing, and log cleanup."
    
    check_if_running            # 检查是否已有其他脚本实例在运行
    check_environment           # 检查环境和依赖
    check_and_recover_segments  # 检查并恢复故障段
    check_and_rebalance_segments  # 检查并重平衡段
    clean_old_logs "$LOG_RETENTION_DAYS"  # 清理旧日志
    
    log INFO "Completed all checks, recovery actions, and log cleanup."
    log_end "gp_auto_maintenance"
}

# 运行主函数
main
