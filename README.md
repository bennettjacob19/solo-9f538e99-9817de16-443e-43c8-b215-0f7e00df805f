# 制品目录历史查询服务

发布值班工程师追查“误上线时制品目录实际指向哪个版本”的历史查询系统。
源 PostgreSQL 中的制品目录（制品编号 / 服务名 / 版本 / SHA-256）通过
**一致基线 + 逻辑复制**被完整捕获到本地 SQLite 仅增日志，可按任意已捕获
提交点回看整张目录，支持删除后编号重用、多行事务整体可见。

## 1. 架构与正确性设计

```
PostgreSQL 源库 (artifacts 固定四列)
   │  ① 建逻辑复制槽 test_decoding + pg_export_snapshot 导出一致性快照
   │  ② 另开连接 SET TRANSACTION SNAPSHOT 复用快照做整表基线
   │  ③ 从槽 LSN 持续消费逻辑变更流（walsender）
   ▼
捕获工作线程（FastAPI 进程内 daemon thread）
   │  每个源事务：先整体写 SQLite（行变更 + 提交点同一事务）
   │              落盘成功后才 send_feedback 推进源端槽位
   ▼
SQLite 历史库（WAL，仅增不改）  ←→  FastAPI 网页 / JSON API
```

### 关键不变量

| 需求 | 实现方式 |
| --- | --- |
| 一致基线，普通 SQL 不中断 | 建槽与 `pg_export_snapshot()` 在同一事务完成；基线用 `SET TRANSACTION SNAPSHOT` 读一致性快照，源库无需停机、普通读写照常提交 |
| 基线前开始、基线后提交的事务不漏 | 该事务对导出快照不可见（不进基线），但其提交记录的 LSN 晚于槽起始 LSN，必然出现在逻辑流中被补捕获（自测第 2 组确定性验证） |
| **每笔源事务可独立回看** | 每个提交点分配全库唯一、单调递增的**序号 seq**（基线=0，之后每笔源事务 +1），回看一律按 seq 定位；同时记录该事务真实的源提交 LSN（复制消息的 `data_start`，即提交记录结束位置）用于展示与旧链接兼容。绝不使用 `wal_end` 充当提交点——它不是事务标识，多笔事务可能对应同一记录值 |
| 多行事务整体可见 | 一个源事务的全部行变更与提交点在**同一个 SQLite 事务**落盘，不存在半条事务 |
| 删除后重用编号保留旧历史 | `artifact_history` 仅追加，每行带 `change_type` 与提交点序号；按编号取“最后一条 ≤ 查询点”的记录重建目录，DELETE 与后续 INSERT 各自留痕 |
| 只确认已完整落盘的事务 | 严格“先 SQLite commit，后 `send_feedback(flush_lsn=该事务自己的提交LSN, force=True)`”；keepalive 只回送已确认位置且钳制到源端当前 WAL 末端。自测断言槽 `confirmed_flush_lsn` 永不超前已落盘提交点、永不超出源端 WAL 末端 |
| 落盘后确认前断开/强杀 | 复制槽持久化在源库；重启后从槽位重放，SQLite 以源事务 xid 幂等去重——重放的事务跳过落盘、只补确认，不重复、不漏掉后续事务（自测第 8 组用 `TEST_SKIP_FEEDBACK` 钩子确定性验证） |
| 源断连自动续接 | 断线固定退避重连；连续瞬断未超阈值时保持 STREAMING（历史可查），并在 `/api/progress` 暴露连接状态、累计接通次数、最近断线/接通时间 |
| 初始化未完成不开放历史 | 状态机 `INITIALIZING → STREAMING / ERROR`；未到 STREAMING 时 `/api/points`、`/api/directory`、`/api/history/*` 一律 503 |
| 固定结构的增改删 | 网页/API 只接受 `insert/update/delete` 三类操作，pydantic 判别联合按 `op` 分流，仅四列，sha256 强制 64 位十六进制；无任何 DDL/任意 SQL 通道 |

### SQLite 表结构（`/data/history.db`，schema v2）

- `capture_state`：阶段、槽名、基线 LSN、最近提交点/确认位置/消息 LSN、
  连接状态、schema 版本、错误信息。
- `txns`：源事务 xid（主键）、提交 LSN、提交时间、**回看序号 seq**、
  **lsn_kind**（`commit`=真实源提交 LSN / `recorded`=旧版记录值）。
- `artifact_history`：仅增变更链（BASELINE/INSERT/UPDATE/DELETE），带 xid、
  提交 LSN 与**所属提交点序号 seq**，支持按提交点过滤与编号重建。

### 旧卷原地升级（v1 → v2）

旧版历史库（无 seq 列）启动时自动原地迁移，**不丢任何历史**：

- 旧事务按 `(commit_lsn_num, xid)` 补发回看序号；同一 LSN 下的多笔旧事务
  由此获得各自独立的回看位置；
- 旧事务的 LSN 原样保留并标记 `lsn_kind=recorded`——旧版记录的可能不是
  真实源提交 LSN，**不伪造、不改写**；
- 旧链接（`?point=<LSN>`）继续可用：该 LSN 只对应一笔提交点时直接回看；
  对应多笔事务时返回 **409 + 候选列表**，由用户按序号逐笔选择；
- 新旧记录混合时，回看严格按 seq 截断：只包含所选事务及此前状态。

## 2. 用 Docker Compose 启动（交付方式）

```bash
docker compose up --build
# 网页：http://localhost:8000/
# 源 PostgreSQL：localhost:5432（user/pass/db = postgres/postgres/postgres）
```

- `postgres` 服务：`postgres:16-bookworm`，已开启 `wal_level=logical`，
  命名卷 `pgdata` 持久化；首次启动自动执行 `init/01_schema.sql`
  幂等建表并播种 3 行示例数据。
- `app` 服务：本仓库构建的 Python 3.12 镜像，命名卷 `historydata`
  挂载到 `/data` 持久化 SQLite；`restart: unless-stopped`，源库健康检查
  通过后才启动。
- 两侧均为命名卷，`docker compose down` 不丢数据，`down -v` 才清空。

## 3. 网页功能（单页应用，无前端构建）

1. **捕获进度与续接状态**：阶段徽章、复制流连接状态、复制槽、基线/最近
   提交/已确认落盘 LSN、已捕获事务数、历史行数、累计接通次数、最近
   接通/断线时间、存储版本（旧卷升级时间）、最近错误，3 秒自动刷新。
2. **成组修改源目录**：动态增删 insert/update/delete 行，整组在一个源事务
   提交；任一操作失败整组回滚。
3. **提交点回看**：列出 BASELINE 与每个源事务提交点（序号、LSN、xid、
   时间、变更行数、旧记录/歧义标记），点击任一行即查看该时刻整张目录
   （只含该事务及此前状态）。
4. **旧链接 LSN 查询**：粘贴旧链接的 LSN 直接回看；若该 LSN 对应多笔
   事务（旧版本记录的共享提交点），列出候选事务供逐笔选择。
5. **单编号完整历史**：含 BASELINE/INSERT/UPDATE/DELETE 全链，
   直观验证“删除后重用编号，旧历史保留”。

## 4. HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/progress` | 捕获状态、进度与续接状态（初始化期也可用） |
| GET | `/api/source/artifacts` | 源库当前目录 |
| POST | `/api/source/operations` | 成组增改删，单源事务，失败整组回滚 |
| GET | `/api/points` | 全部可查询提交点（含 seq、lsn_kind、歧义标记；未就绪时 503） |
| GET | `/api/directory?seq=3` | 按提交点序号精确回看（推荐） |
| GET | `/api/directory?point=0/16B3748` | 兼容旧链接按 LSN 回看；对应多笔事务时 409 返回候选列表 |
| GET | `/api/history/{artifact_no}` | 单编号完整变更链 |

请求示例：

```bash
curl -X POST localhost:8000/api/source/operations -H 'Content-Type: application/json' -d '{
  "operations": [
    {"op":"insert","artifact_no":1004,"service_name":"cart-svc","version":"3.1.0",
     "sha256":"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"},
    {"op":"delete","artifact_no":1003}
  ]}'
curl 'localhost:8000/api/directory?seq=3'
curl 'localhost:8000/api/directory?point=0/16B3748'   # 歧义时 409 + 候选
```

## 5. 自测

### 5.1 单元测试

```bash
python3 -m unittest tests.test_decoder tests.test_storage -v   # 仅标准库
python3 -m unittest tests.test_capture -v                      # 需 psycopg2
```

- `test_decoder`：BEGIN/COMMIT、带空格与转义引号的值、DELETE 仅主键、
  其他表/DDL 忽略。
- `test_storage`：v1→v2 原地迁移（序号补发、旧 LSN 标记 recorded、历史
  保留）、按 seq 回看只含所选及此前、共享 LSN 候选解析、新旧混合、
  幂等重开、新库序号分配与 xid 去重。
- `test_capture`：keepalive 应答钳制到源端 WAL 末端、未确认不应答、
  应答节流。

### 5.2 端到端自测（60 项断言）

`tests/selftest.sh` 在本机自动拉起一个便携 PostgreSQL（不依赖 Docker）、
启停应用并完成全链路验证：

```bash
# 可选：指定本机 PostgreSQL 二进制位置
export PGBIN=/path/to/postgresql/bin PGCLIENT=/path/to/client/bin
bash tests/selftest.sh
```

覆盖场景：

1. 初始化窗口历史接口 503，完成后 STREAMING 与基线目录正确；
2. **跨基线事务**：通过应用内置测试钩子
   （`TEST_INFLIGHT_ARTIFACT_NO` 环境变量，默认关闭）确定性构造
   “基线快照点之后打开、基线落盘之后提交”的事务，断言不进基线、
   不被遗漏、在其提交点可见；
3. 多行事务只产生一个提交点、同事务插入/删除同时生效；
4. 删除后重用编号，旧历史链完整（BASELINE,DELETE,INSERT）；
5. 失败整组回滚、非法 sha256/非法 LSN 参数校验；
6. `kill -9` 强杀应用后重启：事务数不重复、最后更新仍在、
   槽 `confirmed_flush_lsn` 不超前已落盘 LSN、续接后新变更继续捕获；
7. 源 PostgreSQL 重启后应用自动重连恢复 STREAMING，恢复后变更不丢，
   续接状态可见；
8. **积压事务突发消费**：应用离线期间连发两笔版本修改，重连同批消费后
   两笔获得各自独立的提交点 LSN，选择第一笔只看到第一笔版本；
9. **落盘后确认前强杀**：`TEST_SKIP_FEEDBACK=1` 钩子让事务落盘后故意
   不确认，`kill -9` 重启后重放去重不重复、后续事务不遗漏、槽位不超前；
10. **旧卷原地升级**：v1 旧库（两笔事务共享同一记录 LSN）启动自动迁移，
    历史保留、共享 LSN 旧链接 409 给候选、逐笔选择互不漏串、无歧义旧
    链接照常、旧 LSN 不伪造、新旧混合回看只含所选及此前、旧卷记录值
    不污染真实槽位；
11. SQLite 与 PostgreSQL 数据目录均落盘持久化。

最近一次结果：**60 passed / 0 failed**（同一自测在修复前代码上运行：
15 项失败，覆盖共享 LSN 误报、无序号回看、无歧义提示、无续接状态等）。

## 6. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PG_DSN` | 本地开发 DSN | 源库 libpq 连接串，Compose 中指向 `postgres` 服务 |
| `SQLITE_PATH` | `/tmp/capture/history.db` | 历史库路径（Compose 中为 `/data/history.db`） |
| `REPLICATION_SLOT` | `artifacts_history_slot` | 逻辑复制槽名 |
| `SOURCE_TABLE` | `artifacts` | 捕获的源表（结构固定） |
| `REPL_STATUS_INTERVAL_S` | `5` | 复制协议 keepalive 间隔 |
| `REPL_RECONNECT_BACKOFF_S` | `2` | 断线重连退避秒数 |
| `CAPTURE_INIT_SLEEP_MS` | `0` | 测试钩子：拉长初始化窗口 |
| `TEST_INFLIGHT_ARTIFACT_NO` | `0`(关) | 测试钩子：跨基线事务编号 |
| `TEST_INFLIGHT_SLEEP_S` | `3` | 测试钩子：在途事务持有秒数 |
| `TEST_SKIP_FEEDBACK` | `0`(关) | 测试钩子：落盘后跳过确认，模拟落盘后确认前断开 |

## 7. 已知边界 / 未完成项

- 逻辑解码使用 PostgreSQL 内置 `test_decoding`（无需安装插件），解析器
  依赖本项目固定的四列结构；表结构变更不在支持范围内（需求明确禁止 DDL）。
- 历史库为单进程 SQLite，适合值班排查类读多写少场景；如需多副本水平扩展
  需更换存储层。
- 复制槽若被人工从源库删除，应用进入 ERROR 并在 `/api/progress` 暴露，
  不会自动重建（重建会使旧 LSN 失效，需人工确认后清空历史库重新基线）。
- 旧库迁移时，旧事务的回看顺序按 `(记录LSN, xid)` 补发——同一记录 LSN
  内多笔旧事务的相对先后只能按 xid 近似（真实提交顺序已不可考），
  新事务不受此限。
- 沙箱环境未提供 Docker，Compose 文件未经本机 `docker compose` 实跑，
  但应用本体已用同等配置在本机 PostgreSQL 15 + uvicorn 下完成全部端到端
  验证；Compose 镜像基于官方 `python:3.12-slim` 与 `postgres:16-bookworm`。
