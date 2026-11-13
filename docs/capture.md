# 异步导出任务采集客户端（capture）

`contract_warehouse.capture` 是与既有建仓/归因完全独立的采集客户端：它对接一个
原创的 **loopback 模拟门户**（协议夹具，非生产服务、不连接任何真实客户系统、无需
账号），把一次异步导出任务从创建、等待、下载、业务校验到目录交付的全过程做成
**单操作者可串行接续**的本地工作流。只使用 Python 标准库与 SQLite，不改变任何既有
入口、CLI、数据库结构或分析口径。

## 1. 调用方式

```sh
# 执行/接续一次采集（同一命令可在任意阶段退出后原样重跑）
python -m contract_warehouse.capture run \
    --endpoint http://127.0.0.1:8080 \
    --request-id 'REQ-2026-001/含 空格?和中文' \
    --state run/capture.sqlite \
    --output run/delivery

# 只读状态投影：不联网、不建库；state 缺失时明确失败
python -m contract_warehouse.capture status --state run/capture.sqlite
```

Python 调用：

```python
from contract_warehouse.capture import run, status
receipt = run(endpoint, request_id, state_path, output_dir)
snapshot = status(state_path)  # 只读，绝不创建文件
```

交付目录恰好包含五个文件，且可直接交给既有 v2 建仓入口：

```text
output/
├── customers.csv      # 门户冻结清单的原始字节，逐字节保留
├── hierarchy.csv
├── contracts.csv
├── tickets.csv
└── capture.json       # 身份、冻结清单原文/摘要、四角色来源与实际摘要
```

`python -m contract_warehouse.inherited --source <output> --database <db>` 可
直接消费该目录。

## 2. 门户协议（夹具语义）

所有 JSON 均为严格 UTF-8；解析拒绝重复对象键与额外字段；`schema_version` 只接受
整数 `1`（布尔 `true/false` 不是整数）。

| 请求 | 语义 |
| --- | --- |
| `GET <endpoint>/exports/by-request/<编码ID>` | `200` 返回 `schema_version/request_id/job_id`；明确 `404` 表示该请求尚无任务，响应体不作业务数据；其他状态码一律失败 |
| `POST <endpoint>/exports` | 仅当本地尚未绑定 `job_id` 且上一步是明确 `404` 时发送；正文精确为 `schema_version=1`、`request_id`、`mode="inherited_contracts_v2"`；`201` 返回三个身份字段 |
| `GET <endpoint>/exports/<编码job_id>` | 每次 run 至多一次；`pending`/`ready`/`failed` 三态，字段与空值约束严格 |
| `GET <manifest-url>` | 下载单份 CSV；实际地址由 endpoint 的 scheme/host/port 与同源绝对路径组成 |

- **endpoint 限制**：仅 `http`、主机只能是显式 `localhost`/`127.0.0.1`/`::1`、必须带
  有效端口；拒绝用户凭据、查询、片段、反斜杠、其他主机与任何重定向。
- **身份编码**：`request_id`/`job_id` 用 `urllib.parse.quote(safe="")` 百分号编码，
  `/`、空格、`?`、`#`、非 ASCII 全部编码，不可能拼出路径逃逸或查询注入；原文身份
  （区分大小写、无首尾空白、非空）保存在 JSON 正文与本地 state 中。
- **限制在读取时执行**：JSON 响应 ≤ 1 MiB、CSV ≤ 2 MiB，按实际读到的字节计数；
  有 `Content-Length` 时必须与完整读到的字节相符；只接受 `identity`
  Content-Encoding。超时 5 秒；状态码与结构不符、截断、超限、网络错误均明确失败，
  不换身份、不无限重试。
- **清单（ready.files）**：恰好覆盖 `customers/hierarchy/contracts/tickets` 四个
  不同角色，每项精确为 `role/url/sha256/byte_length`；`sha256` 为 64 位小写十六
  进制，`byte_length` 为 `[0, 2MiB]` 整数（布尔不算整数）；`url` 必须是以单个 `/`
  开头的同源绝对路径，可带查询，拒绝片段、反斜杠、凭据、`//` 前缀与其他 origin。
  数组顺序不影响角色绑定（客户端按 `role` 固定关联）。

## 3. 本地状态机（一个 state 文件只管一次导出）

SQLite state 在**首次联网之前**原子创建，并写入三个不可变绑定
（endpoint、request_id、output）与初始 `prepared` 阶段。缺失文件允许初始化；
结构无法识别的既有数据库一律拒绝复用/覆盖。后续 run 的三个绑定必须完全一致，
变更在联网前拒绝。阶段：

| 阶段 | 含义（失败时可区分的本地事实） |
| --- | --- |
| `prepared` | 绑定已落盘，尚无任何创建结果（未创建/创建结果未知的起点） |
| `create_submitted` | POST 已发出，创建回执本地未知（进程可能在收到 201 前退出） |
| `bound` | 已从 201 或 by-request 查询接受不可变 `job_id` |
| `waiting` | 门户明确回答 `pending` |
| `remote_failed` | 门户明确回答 `failed`；终态，重跑离线结束，不创建替代任务 |
| `frozen` | 已接受唯一冻结清单（snapshot_id + 四角色），此后不再查询任务状态 |
| `validation_failed` | 四文件齐全但未通过既有 v2 业务校验；原文与错误保留 |
| `published` | 目录已交付并通过离线复核 |

状态机禁止非法跳转：`frozen` 之后不能回到任何联网阶段，`job_id` 一经接受不可改变，
快照身份不可替换。

### 3.1 创建与恢复（不重复创建）

1. 先 `GET by-request`。只有“本地未绑定 job_id **且** 本次得到明确 404”才允许
   `POST /exports`。超时、500、坏 JSON、连接断开都不是 404，记录后失败退出。
2. 门户保证同一 request_id 幂等创建；客户端仍始终先查询。201 丢失或提交后退出，
   下一进程只做 by-request 查询并绑定已有 `job_id`，不重新选择 ID。
3. 任何身份响应里的 `request_id` 必须与本地一致，`job_id` 必须与已接受值一致，
   否则明确失败。`capture_http` 表保存每次真实请求的方法、URL、请求体、来源、
   状态码、响应原文与响应摘要——不伪造未收到的回执。

### 3.2 下载检查点（不混用快照、不重下已完成角色）

- 接受 ready 清单时，把 ready 原文、其 SHA256、snapshot_id 与四角色声明行写入
  state，随后只接续该冻结清单。
- 每个角色只有“完整接收且长度与 SHA256 同时符合清单”时，才在**一个本地事务**里
  写入原文字节并置完成标记；短读、摘要不符、写库失败不会留下完成标记，其他角色
  的既有检查点保留。
- 新进程对已完成角色先**重算已存 BLOB 的摘要与长度**再决定跳过，绝不只信完成
  布尔值；发现 BLOB 损坏就丢弃该检查点并按同一冻结清单重新下载。保存并提交后不
  再打开网络来源计算摘要。

### 3.3 业务校验（复用，不重写宽松规则）

四份原文齐备后，在私有临时目录复用 `hierarchy.load_inherited` 与
`hierarchy.build_bridge` 跑完整 v2 契约：空关系合法、时序森林/历史环、未知引用、
合同区间重叠、日期与金额边界、客户/合同/工单非空等。HTTP 成功不等于业务成功：
校验失败时四份原文与明确错误保留在 state，`output` 不出现完整目录，也不会删除坏
角色去另抓一个快照拼凑成功。

## 4. 目录交付与发布恢复

- 先在 `output` 同目录的私有暂存目录（`.capture-stage-*`）写完全部五个文件，关闭
  后逐个复核，再一次性 `os.rename` 发布。`output` 已存在且不属于本次请求时拒绝
  覆盖；output 本身或路径中的符号链接、state 位于 output/暂存目录内均拒绝。
- 正常异常路径只清理本次未发布暂存，**不删除** SQLite 下载检查点。
- 目录 rename 与 SQLite `published` 标记不是同一事务。若 rename 已完成而标记提交前
  进程终止，下一次 run **不发任何 HTTP**：先核对目录的精确文件集合、四份 CSV
  原文字节、确定性 `capture.json` 与本次 request_id/job_id/snapshot_id，全部一致才
  补记 `published`；缺文件、多余文件、字节被改、外来目录一律报错并保留现场。
- `published` 之后的 run 只做离线核对；目录损坏不会被静默忽略。

`capture.json` 为稳定排序、UTF-8、末尾换行的确定性 JSON，不含当前时间、耗时或
机器绝对路径；原样接续产出相同交付字节。其中摘要只证明获取内容与门户声明一致，
不证明客户事实真实性。

## 5. 演示门户与故障注入

```sh
python -m contract_warehouse.capture.portal --port 8080 --source fixtures_inherited
```

该服务只绑定 loopback，作业保存在内存中，新建任务默认 `pending`；
`POST /__demo__/ready?request_id=<ID>` 是仅用于演示的本地控制端点，把任务翻到
`ready`。`DemoPortal` 另提供确定性故障开关供测试使用：丢弃 201 响应、by-request
返回 500/断连/身份改写、文件短读、摘要不符、清单乱序/缺角色/跨源链接等。

## 6. 测试钩子（同步屏障，不靠 sleep 猜时序）

- 进程内：monkeypatch `contract_warehouse.capture.engine.barrier`。
- 真实子进程：设置环境变量 `CAPTURE_BARRIER=<名称>`，命中时 `os._exit(3)`
  硬终止（无 `finally` 清理，等价于在该指令被 SIGKILL）。

| 屏障 | 位置 |
| --- | --- |
| `create_sent` | 创建 POST 字节已发出、尚未读取 201 |
| `role_before_commit` | 角色 BLOB/标记事务提交之前（回滚验证） |
| `role_saved` | 角色事务提交返回之后（检查点保留验证） |
| `after_rename` | 目录 rename 完成、`published` 标记提交之前 |
| `before_publish_commit` | 发布标记事务内、提交之前 |
