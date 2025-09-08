remote_cmd.sh 使用说明（README）

批量远程操作脚本：免密优先、密码回退、可选分发公钥、可选上传文件/执行脚本/执行简单命令。
适用于一批服务器做一次性或重复性的分发与巡检。

1. 环境与依赖

Linux / Unix（bash）

必需：ssh、scp、awk、sed

可选：expect（当需要口令回退或分发公钥时才需要）

建议：dos2unix（若 hosts.txt 来自 Windows）

# 语法检查（建议先执行）
bash -n remote_cmd.sh

2. 目录与文件
/home/cdphadoop/scripts/
├── remote_cmd.sh        # 主脚本
├── hosts.txt            # 主机清单（IP USER PASSWORD）
├── logs/
│   └── remote_cmd/
│       └── <host>.log   # 每台主机的详细日志
├── batch_success.log    # 执行成功的主机列表
└── batch_failed.log     # 执行失败的主机列表

3. 主机清单（hosts.txt）

每行：IP USER PASSWORD

PASSWORD 可留空（表示只用密钥登录）

支持注释（行首 #）和空行

示例：

# IP                 USER         PASSWORD
192.168.1.10         root         S3cretPass!
192.168.1.11         cdphadoop
192.168.1.12         admin        Admin@123


提示：如文件来自 Windows，建议 dos2unix hosts.txt。

4. 核心特性

✅ 免密优先、密码回退：先尝试密钥登录，失败再用 expect 回退口令

✅ 一键分发公钥：可把本机公钥追加到远端 authorized_keys

✅ 避免“只执行第一行”：所有 ssh 默认加 -n，并用 FD3 读取 hosts.txt

✅ 详尽日志：控制台日志 + 每台主机独立日志 + 成功/失败清单

✅ 可选能力：上传任意文件、生成/上传/执行 Kafka 临时脚本、执行简单命令

5. 脚本内可配置项（摘录）

在 remote_cmd.sh 顶部配置区：

# 上传文件
SRC_FILE=""                      # 留空则不上传
DEST_PATH="/data1/cdphadoop"     # 远端目录（自动 mkdir -p）

# Kafka 示例脚本参数
PORT=4567

# 主机清单
HOSTS_FILE="/home/cdphadoop/scripts/hosts.txt"

# SSH/SCP 选项（已包含 -n）
SSH_PORT=22
SSH_TIMEOUT=10
SCP_TIMEOUT=60

# 开关（true/false）
DO_CHECK_LOGIN=false
DO_SETUP_KEY=false
DO_GEN_SCRIPT=false
DO_SCP_UPLOAD_SCRIPT=false
DO_SSH_EXECUTE_SCRIPT=false
DO_SIMPLE_CMD=true
DO_UPLOAD_FILE=false

# 简单命令
SIMPLE_CMD="source /etc/profile && java -version"

6. 快速开始
6.1 仅执行简单命令（免密优先，密码回退）
# remote_cmd.sh 内：
DO_SIMPLE_CMD=true
SIMPLE_CMD="source /etc/profile && java -version"

chmod +x remote_cmd.sh
bash remote_cmd.sh

6.2 分发免密并执行命令
DO_SETUP_KEY=true
DO_SIMPLE_CMD=true
bash remote_cmd.sh


要求 hosts.txt 中提供密码（用于首次分发公钥）。

6.3 上传文件到远端目录
SRC_FILE="/path/to/pkg.tar.gz"
DEST_PATH="/opt/pkg"
DO_UPLOAD_FILE=true
bash remote_cmd.sh

6.4 Kafka 示例脚本：生成 → 上传 → 执行
PORT=4567
DO_GEN_SCRIPT=true
DO_SCP_UPLOAD_SCRIPT=true
DO_SSH_EXECUTE_SCRIPT=true
bash remote_cmd.sh

7. 日志与结果

控制台打印带时间戳与级别

每台主机详细日志：logs/remote_cmd/<host>.log

成功主机清单：batch_success.log

失败主机清单：batch_failed.log

常见关键日志前缀：

[KEY ]：免密分发/验证

[SSH ]：执行远端命令（KEY/PASS）

[SCP ]：文件上传

[CHECK]：登录检查

[KAFKA]：Kafka 示例脚本相关

[CMD ]：简单命令执行

[FLOW]：流程节点

8. 故障排查（FAQ）
Q1：只在第一台主机执行？

已在脚本中通过 -n 和 FD3 读取修复。若仍遇到：

确认 SSH_OPTS 中包含 -n

确认 main() 中使用了 exec 3< "$HOSTS_FILE" 与 <&3

Q2：提示 expect: command not found

只有在口令回退或分发密钥时需要 expect。安装后再执行：

# CentOS / RHEL
sudo yum install -y expect
# Ubuntu / Debian
sudo apt-get install -y expect

Q3：Permission denied (publickey) 或无法免密

开启 DO_SETUP_KEY=true，并在 hosts.txt 提供密码进行免密分发；

或手工检查远端目录与权限：
~/.ssh 700，authorized_keys 600，文件属主为目标用户。

Q4：hosts.txt 读不到后续行

可能是 Windows 换行符：dos2unix hosts.txt；

确保每行有以空格分隔的 3 列（最后一列可空）。

Q5：如何并行执行？

目前为串行，简单、可控、日志清晰。需要并行可后续改为 xargs -P 或 GNU Parallel（我可以按你需求给出并行版）。

9. 安全建议

不在日志中打印明文密码（脚本已避免）。

建议尽早启用免密，停用口令方式。

控制 hosts.txt 权限（例如 chmod 600）。

使用专用低权限账号执行批量任务。

10. 版本自检
# 语法检查（不执行）
bash -n remote_cmd.sh && echo "Syntax OK"
