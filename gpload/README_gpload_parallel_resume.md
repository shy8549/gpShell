# gpload_parallel_resume.sh — 使用说明（含大写 Schema/Table）

这是一个“可恢复、可并发、可切片”的 **CSV → Greenplum** 装载脚本。纯 Bash/AWK 实现，无 Python 依赖。自动读取目标表结构、识别入库生成列（`uuid`/`now()`/`identity` 等）、自动做字段映射、支持断点续传与详细日志。

---

## 1) 功能特性

- **自动获取表结构**：从 `information_schema.columns` 读取列名/类型、`NOT NULL`、默认值、`IDENTITY`。
- **识别生成列**（CSV 可不提供）：`IDENTITY`/`nextval(...)`、`gen_random_uuid()`/`uuid_generate_*`、`now()`/`current_timestamp` 等。
- **CSV 表头自动识别**：
  - 有表头：按表头→表列名匹配（不区分表头大小写）。
  - 无表头：自动生成 `col1/col2/...` 按位置映射到**非生成列**。
- **严格大小写兼容**：
  - YAML 中 `OUTPUT.TABLE` 写成 `"\"SCHEMA\".\"TABLE\""`，不被小写化。
  - `MAPPING` 左边为**目标列**（带 SQL 双引号），右边为**输入列**（表头或 `colX`）。
- **切片 & 并发**：默认开启；每分片独立 gpfdist 端口（`8800, 8802, 8804, ...`）。
- **断点续传**：分片状态记录在 `work_.../status/`；仅重试失败分片。
- **详细日志**：主日志 + 每分片 gpload 原始日志 + YAML 预览。

---

## 2) 前置条件

- 机器可执行 `gpload`（若不在 PATH，脚本会尝试 `source /usr/local/greenplum-db/greenplum_path.sh`）。
- 可连到 Greenplum（网络、端口、账号权限）。
- 目标表已存在；**大写 Schema/Table 在建表时需要双引号**。

---

## 3) 快速开始

### 3.1 设置数据库环境变量
```bash
export DB_HOST=10.10.84.251
export DB_PORT=5432
export DB_NAME=SJGXPT
export DB_USER=demo
export DB_PASSWORD='your_password'
```

> 也可将这些写入 `gpload_parallel_resume.env`，然后：`source gpload_parallel_resume.env`。

### 3.2 运行（默认：并发 + 切片）
```bash
bash gpload_parallel_resume.sh <SCHEMA> <TABLE> </path/to/file.csv>
# 例：
bash gpload_parallel_resume.sh GPTEST TEST_IMPORT /home/gpadmin/script/gpload/test_upper.csv
```

### 3.3 串行 + 单文件模式
```bash
export ENABLE_PARALLEL=false
export ENABLE_CHUNKING=false
bash gpload_parallel_resume.sh GPTEST TEST_IMPORT /home/gpadmin/script/gpload/test_upper.csv
```

> **大小写注意**：命令行参数不用加引号；脚本会在 YAML 中正确写成 `"\"SCHEMA\".\"TABLE\""`。

---

## 4) 环境变量（可选）

| 变量 | 默认 | 说明 |
|---|---:|---|
| `ENABLE_CHUNKING` | `true` | 是否切片（`true`=按 `CHUNK_LINES` 切分；`false`=单文件） |
| `CHUNK_LINES` | `200000` | 每片行数（有表头时切片会先丢弃表头） |
| `ENABLE_PARALLEL` | `true` | 是否并发执行分片 |
| `MAX_JOBS` | `4` | 并发分片数 |
| `RETRY_TIMES` | `2` | gpload 失败重试次数（总尝试 = `RETRY_TIMES + 1`） |
| `GPFDIST_PORT` | `8800` | 基础端口；每分片自动 `+2`（`8800,8802,…`）避免冲突 |
| `GPLOAD_LOCAL_HOSTS` | *(自动探测)* | 覆盖 `LOCAL_HOSTNAME`（多网卡时设为对 Segment 可达 IP） |
| `ENABLE_LOG_ERRORS` | `true` | 在 YAML 写入 `LOG_ERRORS: true` |
| `GPLOAD_ERROR_LIMIT` | `0` | gpload `ERROR_LIMIT`；当 `LOG_ERRORS=true` 时自动≥`MIN_ERROR_LIMIT_WHEN_LOG_ERRORS` |
| `MIN_ERROR_LIMIT_WHEN_LOG_ERRORS` | `2` | 启用日志时自动使用的最小 `ERROR_LIMIT` |
| `ENABLE_REUSE_TABLES` | `true` | YAML `PRELOAD` 中加入 `REUSE_TABLES: true` |
| `ALLOW_CSV_OVERRIDE_GENERATED` | `true` | CSV 表头包含生成列名（如 `UUID_COL/TS_COL`）时允许覆盖默认值 |
| `SINGLE_FILE_LINK_MODE` | `symlink` | 单文件模式将原 CSV → `chunks/chunk_000001.csv` 的方式：`symlink` 或 `copy` |
| `CSV_DELIM` | `,` | 分隔符 |
| `CSV_QUOTE` | `"` | 引号字符 |
| `CSV_ESCAPE` | `\\` | 转义字符 |
| `CSV_NULL` | `\\N` | 空值表示 |
| `CSV_ENCODING` | `UTF8` | 字符集 |
| `LOG_LEVEL` | `DEBUG` | 日志级别：`DEBUG` / `INFO` / `WARN` / `ERROR` |

---

## 5) 工作目录与日志

- **工作目录**：`./work_<SCHEMA>_<TABLE>_<csv_hash12>/`
  - `chunks/`：分片 CSV（或单文件软链接/副本）
  - `yml/`：每分片 gpload YAML
  - `logs/`：每分片 gpload 原始日志（`chunk_xxx.log`）
  - `status/`：分片状态（`DONE` / `FAIL`）
- **主日志**：`./logs/gpload_parallel_resume/<timestamp_pid>/main.log`（含 YAML 预览）

**断点续传**：再次执行同一 CSV，会跳过 `DONE` 分片，仅重试 `FAIL`。  
**完全重跑**：删除对应 `work_*` 或其中的 `status/`。

---

## 6) 字段映射

- **有表头**：`CSV 表头 → 表列` 名称匹配（表头名大小写不敏感）。
  - 生成列默认不写；若 `ALLOW_CSV_OVERRIDE_GENERATED=true` 且 CSV 有该列名，则允许覆盖。
  - YAML 示例：
    ```yaml
    GPLOAD:
      INPUT:
        - COLUMNS:
            - "name": text
            - "age": integer
      OUTPUT:
        - TABLE: "\"GPTEST\".\"TEST_IMPORT\""
        - MODE: INSERT
        - MAPPING:
            "\"NAME\"": "name"
            "\"AGE\"":  "age"
    ```
- **无表头**：生成 `col1/col2/...`，按出现顺序映射到**非生成列**；`MAPPING` 左列**带引号**，右列 `colX`。

- **必要列校验**：若表中存在 `NOT NULL` 且无默认值，但映射未覆盖 → 直接报错退出。

---

## 7) 常见用法

### 7.1 并发 + 切片（默认）
```bash
bash gpload_parallel_resume.sh public test_import /data/big.csv
```

### 7.2 串行 + 单文件
```bash
export ENABLE_PARALLEL=false
export ENABLE_CHUNKING=false
bash gpload_parallel_resume.sh GPTEST TEST_IMPORT /home/gpadmin/script/gpload/test_upper.csv
```

### 7.3 大写 Schema/Table 建表示例
```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS "GPTEST";
DROP TABLE IF EXISTS "GPTEST"."TEST_IMPORT";
CREATE TABLE "GPTEST"."TEST_IMPORT" (
  "UUID_COL" uuid DEFAULT gen_random_uuid() NOT NULL,
  "TS_COL"   timestamp without time zone DEFAULT now() NOT NULL,
  "NAME"     text,
  "AGE"      integer,
  PRIMARY KEY ("UUID_COL","TS_COL")
) DISTRIBUTED BY ("UUID_COL");
```

CSV（含表头）：
```csv
name,age
Alice,30
Bob,25
```

运行：
```bash
bash gpload_parallel_resume.sh GPTEST TEST_IMPORT /home/gpadmin/script/gpload/test_upper.csv
```

---

## 8) 故障排查

- **table `<schema>.<table>` does not exist**  
  确保 YAML 中 `OUTPUT.TABLE` 是 `"\"SCHEMA\".\"TABLE\""`（脚本已处理）。

- **`<COL> in mapping is not in table "SCHEMA"."TABLE"`**  
  `MAPPING` 左边必须是**目标列**（且带 SQL 引号），右边是**输入列**。脚本已按此生成。

- **`unrecognized key: "skip"`**  
  旧写法；本脚本使用 `FORMAT: csv` + 单文件模式 `HEADER: true`。

- **`failed to start gpfdist`**  
  端口冲突/权限/防火墙：降低 `MAX_JOBS`、调整 `GPFDIST_PORT`、检查网络策略、确保 `gpload` 可执行。

- **连接/权限问题**  
  校验 `DB_*` 变量；确保对目标表有 `INSERT` 权限。

---

## 9) 性能建议

- **切片大小**：增大 `CHUNK_LINES` 可减少 gpfdist 启停（更省 IO）；减小则失败重试粒度更细。
- **并发数**：结合集群资源与网络带宽调 `MAX_JOBS`。
- **分布键**：Greenplum 建表建议选高基数列（示例用 `"UUID_COL"`）。
- **网络**：必要时设置 `GPLOAD_LOCAL_HOSTS` 为 Segment 可达的业务网段 IP。

---

## 10) 返回码与清理

- 返回码：存在 `FAIL` 分片 → 非 0；全部成功 → 0。
- 清理工作目录：`rm -rf work_*`
- 清理 7 天前日志：
  ```bash
  find logs/gpload_parallel_resume -type d -mtime +7 -exec rm -rf {} +
  ```

---

## 11) 进阶：允许 CSV 覆盖生成列

- `ALLOW_CSV_OVERRIDE_GENERATED=true` 时，若 CSV 表头包含 `"UUID_COL"`/`"TS_COL"` 等生成列，将覆盖默认值。  
- 若不希望覆盖，设置：`export ALLOW_CSV_OVERRIDE_GENERATED=false`。

---

## 12) 一键最小示例（大写 Schema）

```bash
# 1) 建表（见上文 SQL）

# 2) 准备 CSV
cat > /home/gpadmin/script/gpload/test_upper.csv <<'EOF'
name,age
Alice,30
Bob,25
EOF

# 3) 导入（并发+切片）
bash gpload_parallel_resume.sh GPTEST TEST_IMPORT /home/gpadmin/script/gpload/test_upper.csv

# 4) 校验
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \
'SELECT COUNT(*) FROM "GPTEST"."TEST_IMPORT";'
```
