# data-qd-107 · 维修服务合同数仓

将维修合同历史与工单接入 SQLite，建立 ODS 原始层、有效时间合同维度、
DWD 工单事实和 ADS 成本集市。解决“工单发生时适用哪个服务等级”的业务问题，
按半开区间关联，明确保留未覆盖工单。

演示含 3 个合同版本、5 张工单。basic 为 2 笔/13000 分，premium 为 1 笔/20000 分，
uncovered 为 2 笔/12000 分。2月1日恰归新合同，C2在1月20日已过合同终点。
输出数据库支持直接 SQL 查询 ads_tier_cost；全量重建不会累加历史工单。

## 两种模式（互不迁移、互不影响）

1. **基线模式（v1，两文件）**：`contracts.csv` + `tickets.csv`，入口
   `contract_warehouse.cli` / `contract_warehouse.build.build(source, target)`，
   保留原有 load、返回结构与数据库表，行为完全不变。
2. **集团继承模式（v2，四文件，schema 标识 `inherited_contracts_v2`）**：
   新增 `customers.csv`、`hierarchy.csv`，把客户历史归属真正建成可供 SQL 复用的
   祖先桥表，并按工单 `opened` 日期沿当日有效上级链就近继承合同。独立入口
   `contract_warehouse.inherited` / `contract_warehouse.inherited.build(source, target)`，
   不要求也不执行旧库迁移。

两种模式都只使用 Python 标准库和 SQLite。

3. **两期客户组合成本归因（compare，只读分析报告）**：独立入口
   `contract_warehouse.compare`，只读消费上述任一模式已建好的事实层，
   把共同支持客户的平均工单成本差额精确分解为成本变化项与组合变化项，
   区分“同一批客户服务变贵”与“高成本客户占比上升”，输出单个确定性
   JSON 报告。不修改旧模式、不重新匹配合同。

4. **异步导出任务采集客户端（capture，独立接续工作流）**：
   `contract_warehouse.capture` 对接原创 loopback 模拟门户（协议夹具，非生产
   服务、无需账号），先创建一次异步导出任务，再以同一请求身份跨进程接续，
   从同一冻结快照取得客户、关系、合同、工单四份 CSV，经完整 v2 业务校验后
   原子发布为可直接交给 `inherited.build` 的五文件输入目录（四份 CSV +
   `capture.json`）。一个 SQLite state 只管一次导出，支持创建回执丢失、部分
   下载、发布中断等恢复，不重复创建业务导出、不混用两个快照、不发布半成品。
   协议、状态机、来源与恢复边界见 `docs/capture.md`。

## 安装与运行

要求 Python 3.11 或更高；先确认 `python3 --version`。以下命令在仓库根目录执行：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
# 基线模式（两文件）
.venv/bin/python -m contract_warehouse.cli --source fixtures --database run/warehouse.sqlite
# 集团继承模式（四文件）
.venv/bin/python -m contract_warehouse.inherited \
  --source fixtures_inherited --database run/inherited.sqlite
# 两期归因报告（只读分析，任一模式的库）
.venv/bin/python -m contract_warehouse.cli --source fixtures_compare --database run/compare.sqlite
.venv/bin/python -m contract_warehouse.compare --database run/compare.sqlite \
  --a-start 2026-01-01 --a-end 2026-02-01 \
  --b-start 2026-03-01 --b-end 2026-04-01 \
  --output run/compare-report.json
.venv/bin/python -m unittest discover -s tests -v
```

不安装、直接从源码树运行时，在 `src/` 目录下执行
`python3 -m contract_warehouse.inherited --source <DIR> --database <FILE>`
即可；测试即按此方式调用真实 CLI 子进程。

v2 演示（`fixtures_inherited/`，全部为合成数据）：S 的上级 1 月挂靠 D1、
2 月 1 日起迁移到 D2，D1/D2 又挂靠 H。六张工单依次命中
K1/深度1、K3/深度0、K2/深度2、K2/深度2、K4/深度1、uncovered；
basic 3 笔 800 分、premium 2 笔 700 分、uncovered 1 笔 600 分，
按持有人 H 归集 2 笔 700 分，合计 6 笔 2100 分。

归因演示（`fixtures_compare/`，全部为合成数据）：A 期（1 月）共同客户
X 两单各 100 分、Y 一单 300 分，A 独有 L 一单 900 分；B 期（3 月）X 一单
200 分、Y 两单各 400 分，B 独有 N 两单各 50 分，全部 basic。共同支持两期
均值为 500/3 与 1000/3，差额 500/3 精确拆成成本变化项 100/1 与组合变化项
200/3（X 两项贡献 50/1 与 −50/1，Y 为 50/1 与 350/3）；全体均值反而从
350/1 降到 220/1，说明共同支持差额不能冒称全体差额。报告契约见
docs/contract.md 的 compare 章节。

运行依赖与测试仅使用标准库；首次安装构建工具通常需要联网。每个仓库有独立
 `.venv`。安装的是源码的构建副本；编辑源码后须再次执行安装命令。
所有演示输出在忽略的 `run/` 中；演示可重复执行。临时目录中的测试不改变 fixture。
CLI 参数错误或无效数据返回非零；失败原因打印到 stderr。

## v2 数据库内容（SQLite）

- `wh_meta`：schema 家族 `inherited_contracts_v2` 与版本 `2` 标识。
- ODS：`ods_source_file`（逻辑文件角色、文件名、SHA256、字节长度、原始字节 BLOB）、
  `ods_customers`、`ods_hierarchy`、`ods_contracts`、`ods_tickets`（行号从 1 计，
  表头不计）。四份输入各读取一次原始字节，解析、入库与摘要使用同一内容。
- 维度：`dim_customer`、`dim_hierarchy_edge`（关系事实，含来源行号）、
  `dim_contract`（有效时间合同维度）。
- 桥表：`bridge_hierarchy_ancestor`（后代、祖先、正整数 depth、有序 edge 路径
  JSON、半开有效期；不含自身）+ `bridge_hierarchy_path`（每个桥接行的有序边明细，
  可连接回关系事实与来源行）。同一完整路径仅保留最大连续区间；路径不同即使
  端点与深度相同也分开存储。
- 事实：`dwd_ticket` 一行一工单，保留原服务客户 `customer`，并保存
  `match_depth`、`contract_sk`、`contract_holder`；未覆盖时后三者为空，
  持有人与所选 `dim_contract` 由外键约束保证一致。
- 审计：`audit_ticket_match` 从自身（深度 0）逐层记录到选中合同；未覆盖则到
  当日根。每层含客户、深度、进入该层的关系边依据与
  `selected`/`no_active_contract` 状态，选中层附合同版本依据。
- ADS：`ads_tier_cost`（按最终服务等级）与 `ads_contract_owner_cost`
  （按合同持有人 × 服务等级，未覆盖为独立的空持有人组）。两者均从
  一行一工单的最终事实直接汇总，不展开桥表或审计。

## 目录

- `src/contract_warehouse/`：解析、业务转换及命令行入口。
  - `ingest.py`/`build.py`/`cli.py`：基线两文件模式（未改动）。
  - `hierarchy.py`：四文件解析、时序森林校验、区间桥表构建与就近继承选择。
  - `inherited.py`：v2 建仓入口与 CLI。
  - `compare.py`：两期客户组合成本归因只读分析与报告 CLI。
  - `capture/`：异步导出任务采集客户端（`engine.py` 工作流、`protocol.py`
    严格 loopback 协议、`state.py` 单导出 SQLite 状态、`portal.py` 原创演示
    门户协议夹具、`cli.py`/`__main__.py` 命令行）。
- `fixtures/`：基线模式 MIT 许可的合成输入。
- `fixtures_inherited/`：v2 模式的合成输入；来源声明见 DATA-SOURCES.md。
- `fixtures_compare/`：两期归因演示的合成输入（X/Y 共同客户 + L/N 独有客户）。
- `docs/contract.md`：字段和口径、边界及失败约定（含 v2 与 compare 章节）。
- `docs/capture.md`：采集客户端的协议、状态机、来源审计与恢复边界说明。
- `tests/`：业务结果、边界与失败测试（两套建仓模式 + 归因报告 + 采集客户端）。
- `requirements*.lock`、`pyproject.toml`：环境和构建声明。

本基线不承诺实时同步或生产调度。仅验证本机环境的事实见交付记录；本仓库不
 声称已经验证任何特定 Linux 容器。所有路径均由调用参数提供，无宿主机绝对路径。
