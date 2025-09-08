# remote\_cmd.sh 使用说明

> 批量远程操作脚本：**免密优先、密码回退、可选分发公钥、可选上传文件/执行临时脚本/执行简单命令**。\
> 已修复“只执行第一行”的典型问题：所有 `ssh` 带 `-n`，并用 **FD 3** 读取 `hosts.txt`。

***

## 目录结构

```
/home/cdphadoop/scripts/
├── remote_cmd.sh              # 主脚本
├── hosts.txt                  # 主机清单（IP USER PASSWORD）
├── logs/
│   └── remote_cmd/
│       └── <host>.log         # 每台主机的详细日志
├── batch_success.log          # 成功主机列表
└── batch_failed.log           # 失败主机列表

```

***

## 环境要求

*   Linux / Unix（`bash`）
*   必需：`ssh`、`scp`、`sed`、`awk`
*   可选：`expect`（当需要**口令回退**或**分发公钥**时才需要）
*   建议：`dos2unix`（若 `hosts.txt` 来自 Windows）

语法自检：

```
bash -n remote_cmd.sh

```

***

## hosts.txt 格式

*   每行一台机器：`IP USER PASSWORD`
*   `PASSWORD` 可留空（表示仅密钥登录）
*   支持注释（行首 `#`）与空行

示例：

```
# IP                USER        PASSWORD
10.215.223.32       cdphadoop   Passw0rd!
10.215.223.33       cdphadoop
10.215.223.34       root        Root@123

```

> 如果文件来自 Windows，请先：`dos2unix hosts.txt`

***

## 可配置项（脚本顶部）

```
# 文件分发
SRC_FILE=""                      # 要上传的本地文件（留空=不上传）
DEST_PATH="/data1/cdphadoop"     # 远端目录（上传时自动 mkdir -p）

# 示例业务参数（若用到 Kafka/Presto 的临时脚本）
PORT=4567

# 主机清单
HOSTS_FILE="/home/cdphadoop/scripts/hosts.txt"

# SSH/SCP（已含 -n，避免 ssh 吞 stdin）
SSH_PORT=22
SSH_TIMEOUT=10
SCP_TIMEOUT=60
SSH_OPTS="-n -o StrictHostKeyChecking=no -o ConnectTimeout=${SSH_TIMEOUT} -p ${SSH_PORT}"
SCP_OPTS="-q -o StrictHostKeyChecking=no -o ConnectTimeout=${SCP_TIMEOUT} -P ${SSH_PORT}"

# 功能开关
DO_CHECK_LOGIN=false
DO_SETUP_KEY=false
DO_GEN_SCRIPT=false
DO_SCP_UPLOAD_SCRIPT=false
DO_SSH_EXECUTE_SCRIPT=false
DO_SIMPLE_CMD=true
DO_UPLOAD_FILE=false

# 简单命令默认值
SIMPLE_CMD="source /etc/profile && java -version"

```

***

## 快速开始

### 1) 执行简单命令（免密优先，密码回退）

```
# 开关
DO_SIMPLE_CMD=true

# 命令示例：查看 Java 版本
SIMPLE_CMD="source /etc/profile && java -version"

bash remote_cmd.sh

```

### 2) 分发公钥以启用免密

```
DO_SETUP_KEY=true
DO_SIMPLE_CMD=false
bash remote_cmd.sh

```

> 需要在 `hosts.txt` 提供密码，脚本会将本机 `~/.ssh/id_rsa.pub` 追加到远端 `authorized_keys`，并验证免密可用。

### 3) 上传文件到远端目录

```
SRC_FILE="/path/to/pkg.tar.gz"
DEST_PATH="/opt/pkg"
DO_UPLOAD_FILE=true
DO_SIMPLE_CMD=false
bash remote_cmd.sh

```

### 4) 运行“临时脚本”（示例：配置 Presto 的 node.properties）

> 已提供一个生成并执行的临时脚本函数 `generate_tmp_script`（脚本名：`/tmp/presto_node_config.sh`），逻辑：
>
> *   设置：
>
>     *   `node.id=<hostname>`（优先使用 FQDN）
>     *   `node.data-dir=/data1/presto/data`
> *   不存在则 `sudo mkdir -p /data1/presto/data` 并 `sudo chown -R <SSH用户>:<组>`
> *   对 `node.properties` 先备份再原子覆盖，最后在日志中打印结果片段

使用方式：

```
DO_GEN_SCRIPT=true
DO_SCP_UPLOAD_SCRIPT=true
DO_SSH_EXECUTE_SCRIPT=true
DO_SIMPLE_CMD=false
bash remote_cmd.sh

```

> 注意：如果你之前的版本还在执行 `/tmp/remote_update_kafka.sh`，请确认主脚本中**执行路径**已改为 `/tmp/presto_node_config.sh`。

***

## 修改 Presto/PrestoSQL 配置的例子

### 修改 `config.properties` 中键值

*   把 `query.max-memory` 设为 `10240GB`：

    ```
    sudo sed -i 's/^[[:space:]]*\(#[[:space:]]*\)\?query\.max-memory[[:space:]]*=.*/query.max-memory=10240GB/' \
    /usr/local/presto-server-0.257/etc/config.properties

    ```
*   把 `discovery.uri` 改为 `http://<hostname>:9888`：

    ```
    H=$(hostname -f 2>/dev/null || hostname)
    sudo sed -i -E "s|^[[:space:]]*(#[[:space:]]*)?discovery\.uri[[:space:]]*=.*|discovery.uri=http://$H:9888|" \
    /usr/local/presto-server-0.257/etc/config.properties

    ```

> 也可以把上述命令作为 `SIMPLE_CMD` 批量执行。

***

## 日志与结果

*   控制台：带时间戳与级别的实时日志
*   每台主机独立日志：`logs/remote_cmd/<host>.log`
*   成功/失败清单：`batch_success.log` / `batch_failed.log`

常见前缀说明：

*   `[FLOW]`：流程节点
*   `[KEY ]`：免密分发/验证
*   `[SSH ]`：远程命令执行（KEY=密钥，PASS=口令）
*   `[SCP ]`：文件上传
*   `[CHECK]`：登录检查
*   `[KAFKA]` / `[CMD ]` / `[FILE ]`：对应的业务阶段

***

## 常见问题（FAQ）

**Q1：只执行了第一行？**\
A：脚本已通过 `SSH_OPTS` 的 `-n` 与 FD 3 读取 `hosts.txt` 解决。如果仍遇到，请检查：

*   顶部 `SSH_OPTS` 是否包含 `-n`
*   `main()` 中是否使用 `exec 3< "$HOSTS_FILE"` 与 `<&3` 读取

**Q2：`expect: command not found`？**\
A：只有在**口令回退**或**分发公钥**时需要 `expect`。安装后再执行：

```
# CentOS / RHEL
sudo yum install -y expect
# Debian / Ubuntu
sudo apt-get install -y expect

```

**Q3：免密仍不可用？**\
A：确认远端权限：

```
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
chown -R <user>:<group> ~/.ssh

```

**Q4：`hosts.txt` 格式异常**\
A：确保每行以空格分隔 3 列（最后一列可空）；如果来自 Windows，运行 `dos2unix hosts.txt`。

***

## 安全建议

*   保护 `hosts.txt`：`chmod 600 hosts.txt`
*   尽快切换到免密方式；避免在日志中输出明文口令（本脚本已避免）
*   对关键操作使用 `sudo`，并限制 sudo 规则（如 `NOPASSWD` 指定范围）

