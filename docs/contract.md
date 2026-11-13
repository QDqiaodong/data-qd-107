# 字段与口径

## 基线模式（v1，两文件）

contracts.csv: version_id 唯一版本键；customer 客户键；valid_from 含起始日；
valid_to 不含结束日，空值表示无上限；tier 仅 basic/premium。
同一客户有效区间不允许重叠；相邻合法，空档允许。只支持完整快照，不合并增量。
tickets.csv: ticket_id 唯一；customer；opened 日期；cost_cents 非负整数，统一为合成单币种分。
日期严格 YYYY-MM-DD；CSV 列顺序见 fixture，空输入、缺列、重复键均失败。
工单以 opened 时点匹配合同，未匹配保留并归 uncovered，而不是丢失事实。

ODS 两张表保留源行顺序及 JSON 字段；dim_contract 为有效时间合同维度，
contract_sk 为该快照内代理键（不保证跨快照稳定）；dwd_ticket 外键可空，
ads_tier_cost 按历史服务级别汇总笔数与成本。不计算赔付资格或合同收入。
该建模反映历史版本；不实现双时态、自动 SCD 变更识别、迟到事实处理。

先校验整个输入，在临时 SQLite 建仓并启用外键，提交后替换目标单文件。
可重复执行全量重建；失败保留原目标，临时文件清理。不要对有活跃读写连接的
目标执行构建；并发发布及长连接协调不在基线承诺内。

## 集团继承模式（v2，四文件）

数据库带 `wh_meta` 标识：schema_family=`inherited_contracts_v2`、
schema_version=`2`，与 v1 物理分开，不迁移旧库。

### 输入契约（四份 UTF-8 CSV）

- contracts.csv 与 tickets.csv 沿用 v1 的精确有序表头、日期格式、重复键、
  服务等级和非负整数分契约，且仍要求至少一条数据记录。
- customers.csv：有序表头 `customer`，登记全部客户、至少一条，customer 唯一。
- hierarchy.csv：有序表头 `edge_id,customer,parent,valid_from,valid_to`，
  允许只有表头（空关系文件合法）。

客户与关系标识非空、无首尾空白、精确区分大小写，不做自动修正。edge_id 全局
唯一；所有关系端点、合同 customer、工单 customer 都必须存在于 customers.csv。
客户在某日没有有效上级时即当日根，不需要额外根标记。

关系日期严格 YYYY-MM-DD，区间为 `[valid_from, valid_to)`；空 valid_to 为无上限；
有限终点必须晚于起点。同一 customer 的上级区间不得重叠（即使 parent 相同也
拒绝），相邻与空档合法。任一日期的有效关系必须构成森林：禁止自指与环路。

### 时序森林校验口径

不能对全部历史边的并集做静态无环判断：A→B 与 B→A 若从未同时有效则合法，
只要在某个非空时间区间同时成立才拒绝。实现按所有关系的起止端点切分时间轴，
并在最后一个端点之后补一个无上限开段，逐段检查“当时有效”的边集合；不逐日
展开、不用任意截止日期截断无上限关系。成环报错给出节点环路、涉及的 edge_id
与来源行号，以及环路同时成立的非空交集区间（交集起点取各边起点最大值，
终点取有限终点最小值；无有限终点时报告 open）。校验覆盖全部关系历史和全部
分量，包括没有工单、没有合同的客户。

### 祖先桥表口径

历史层级被真正物化为可 SQL 复用的桥表，而非匹配工单时临时遍历：

- `bridge_hierarchy_ancestor`：descendant、ancestor、正整数 depth（不含自身，
  最小 depth=1）、`edge_path_json`（有序 edge_id JSON 数组）、valid_from、
  valid_to；端点、深度、区间都是可直接查询的列。
- `bridge_hierarchy_path`：每个桥接行按 step 展开的 edge_id 明细，外键回到
  `dim_hierarchy_edge`，可继续追到来源行号。
- 路径有效期是路径上全部边有效期的交集，只保存非空区间，绝不拼接不同时段
  的边。相同完整路径（有序 edge_id 完全相同）合并为最大连续区间；路径不同的
  记录即使后代、祖先、深度相同也必须分开（例如 S→H 在 2 月 1 日前后深度都是
  2，但路径由 [e1,e3] 变为 [e2,e4]）。

### 就近继承匹配口径

每张工单按其 opened 日期、使用同一日期同时解释关系与合同（终点均不含）：

1. 先看自身（depth=0，无进入边）；自身有当日有效合同即选中。
2. 自身无当日有效合同，才沿当日有效上级链逐层向上，选第一个拥有当日有效
   合同的客户。近层客户存在“其他日期”的合同不阻止继续向上；不按 tier 高低
   或合同起始日期排序，因此自身 basic 有效时不会借用更远的 premium。
3. 同一持有人合同区间不允许重叠，故不存在合同平局规则；到根仍无覆盖时保留
   工单，置为 uncovered。上级迁移后旧上级链立即失效，不能沿旧关系继续查找。
4. 工单 customer 始终是原服务对象；合同持有人单独保存于 contract_holder，
   并由复合外键保证与所选 dim_contract 的 customer 完全一致。

### 逐层匹配审计

`audit_ticket_match` 从自身开始逐层记录，到选中合同为止；未覆盖则到当日根。
每层保存客户、深度、进入该层所用的 entry_edge_id（自身层为空，其余层必填）、
状态 `selected`/`no_active_contract`；选中层保存合同 contract_sk、version_id、
tier 与来源行。任一 `no_active_contract` 层都能用 dim_contract 的半开区间
独立核实当日确无有效合同。

### 汇总、金额与来源

- `ads_tier_cost` 按最终选中服务等级统计工单数与成本，uncovered 独立保留。
- `ads_contract_owner_cost` 按合同持有人 × 服务等级统计，未覆盖为独立的空
  持有人组。两种汇总都只从一行一工单的 dwd_ticket 最终事实计算，绝不通过
  桥表或审计展开重复累加。
- 金额全程使用整数，不经过 float；单个分值或聚合和超出有符号 64 位整数范围
  时明确报错失败，不静默转浮点。
- 四份输入各捕获一次原始字节，解析、ODS 与 SHA256 使用同一内容。原始字节、
  逻辑文件角色、摘要、字节长度存入 `ods_source_file`；维度/事实/审计携带从
  1 开始的数据记录序号（表头不计）。移走输入后仍可在库内重算摘要并沿
  桥表明细核对回关系事实。
- 稳定业务键与排序保证相同输入重跑的全部逻辑内容一致（不要求 SQLite
  二进制一致）；不写入当前时间或宿主机绝对路径。

### 单写者全量重建与失败约定

先完成输入与历史层级校验，再在目标目录的临时 SQLite 中写完全部维度、桥表、
事实、审计与汇总；启用并显式执行外键检查、关闭连接后才 `os.replace` 替换
目标。读取、转换、写入、约束检查或替换任一失败均非零退出：旧目标字节不变、
不留下正式半成品、本次临时文件被清理，修正后可直接重跑。输出路径不得指向
任一输入文件。本模式不提供在线同步、增量修订、知识时间或并发发布；
不对有活跃读写连接的目标承诺安全替换。

## 两期客户组合成本归因（compare，独立分析报告）

`contract_warehouse.compare` 是只读分析入口，直接消费上述任一模式已保存的
一行一工单事实层与最终合同归属，不重新匹配合同、不重建祖先桥表、不修改输入。
回答“平均工单成本上升中，多少来自同一批客户自身成本变化、多少来自客户工单
占比变化”。

### 调用与取数口径

```sh
python3 -m contract_warehouse.compare --database FILE \
  --a-start DATE --a-end DATE --b-start DATE --b-end DATE --output FILE
```

- 两期均按 `opened` 自然日取半开区间 `[start, end)`；日期严格 YYYY-MM-DD，
  起点必须早于终点；两期不得重叠（允许空档、允许相邻）；B 可以早于 A，
  A/B 只是比较标签，不自动推断先后。一个工单至多进入一期。
- 支持 v1 两文件库（报告中记为 `baseline_v1`）与 `wh_meta` 明确标识
  `inherited_contracts_v2` / `2` 的 v2 库。以 `mode=ro` 只读打开已存在的库：
  缺失文件不会创建；非 SQLite 文件、缺表/列不符的不兼容结构、不支持的格式
  标记均整次明确失败；不建库、不升级、不修改输入。compare 只要求并校验它
  消费的表（`wh_meta`、`dwd_ticket`、`dim_contract`），其余表有无不影响结果。
- 全部读取（格式标记、结构、合同、工单）在同一个 SQLite 读取事务内完成，
  即同一逻辑快照；读取期间其他连接的提交不会混入本次取数。
- 非空 `contract_sk` 必须唯一连接到一个 tier 为 basic/premium 的
  `dim_contract` 行，否则是数据错误（悬空、歧义连接、非法等级均整次拒绝），
  绝不解释为 uncovered；`contract_sk` 为 NULL 才是 uncovered。
- 消费前逐行校验整个事实层：`ticket_id` 非空文本且唯一、`customer` 非空文本、
  `opened` 严格合法日期、`cost_cents` 为非负整数（按 Python 类型严格判定，
  不靠 SQLite 类型转换吞掉 TEXT/REAL/负值）；v2 还要求覆盖工单的
  `match_depth` 为非负整数、`contract_holder` 与合同维度一致，未覆盖工单
  不得携带继承信息。所有错误信息定位到工单业务键。
- 分析客户是 `dwd_ticket.customer`（实际接受服务的客户），不是
  `contract_holder`；v2 中同一持有人的不同子客户仍是不同分析客户。每张工单
  只计一次，只读事实与合同两表，绝不连接桥表或匹配审计放大数量。服务等级
  按该工单最终匹配的合同 tier 决定；客户跨期改变等级时进入对应等级分组。
- 标识沿用原文精确字符串语义：不改大小写、不去空白、不补零；客户与工单
  排序均按 Unicode 码位升序。

### 分组与总体指标

报告固定给出 `all`、`basic`、`premium`、`uncovered` 四组。每组先给两期
全部工单数、成本合计（JSON 整数，任意精度）与平均成本（有理数字符串）；
空期平均值为 `null`，不伪造为零。`all` 是原始工单总体直接汇总，不是三个
等级均值的再平均。

### 共同支持与对称归因公式

每组单独找出两期都至少有一张工单的客户集合 S（只有合同没有当期工单不算
观测；零成本工单是真实观测）。A 独有、B 独有客户分别报告客户数、工单数、
成本及逐客户明细，不给缺失期填零，也不并入共同支持。共同支持覆盖率报告
各期被覆盖的工单数与成本，以及占该期全体工单数、全体成本的比例；分母为
零（无工单或总成本为零）时比例为 `null`。

归因只针对 S 内工单的平均成本差。对客户 c：n_Ac、n_Bc 为两期工单数，
m_Ac、m_Bc 为对应平均成本，p_Ac = n_Ac/Σ_S n_Ac，p_Bc = n_Bc/Σ_S n_Bc：

- 成本变化贡献 = (p_Ac + p_Bc) · (m_Bc − m_Ac) / 2
- 组合变化贡献 = (p_Bc − p_Ac) · (m_Ac + m_Bc) / 2

分别合计所有共同客户的两项贡献，精确满足恒等式：
共同支持 B 均值 − A 均值 = 成本变化项 + 组合变化项。报告保留每个共同客户
的两期数量、成本、均值、占比及两项有符号贡献，按客户原文码位稳定排序。

S 为空时状态为 `insufficient_common_support`：归因均值、差额与两项贡献为
`null`，不生成貌似平衡的零归因；全体指标与独有客户诊断仍正常提供。S 非空
即 `comparable`，即使只有一个客户或贡献恰好为零也合法。

交换 A/B 后：全体与客户依据对应交换，共同支持集合不变，两项贡献与差额
各自精确变号。

### 数值表示

数量与金额合计为任意精度整数（JSON 整数，可超过 SQLite 有符号 64 位范围，
合计在 Python 中计算，不经 SQL SUM）。所有均值、占比、差额与贡献使用
`fractions.Fraction` 精确有理数，不经过 float、不用四舍五入满足恒等式。
JSON 中有理数统一为约分后的字符串 `分子/正分母`，零为 `0/1`。

### 报告结构、依据与摘要

报告为单个确定性 JSON（`sort_keys`、缩进 2、UTF-8、末尾换行）：

- `schema_version`：独立的 `compare_report_v1`，与数仓格式标记无关。
- `interpretation`：描述性边界声明（见下）。
- `input`：`database_schema`（输入库模式）与两期区间。
- `groups`：上述四组全部指标。
- `evidence`：实际进入两期的逐工单依据，按 `ticket_id` 码位升序，字段为
  `ticket_id`、`customer`（服务客户）、`opened`、`cost_cents`、`period`
  （`a`/`b`）、`tier`、`version_id`、`contract_holder`、`match_depth`。
  v1 未保存继承深度，`match_depth` 如实为 `null`；未覆盖工单后三者为
  `null`。只取已保存的最终归属，不重新解释组织链。
- `evidence.sha256`：对 `evidence.tickets` 数组的规范序列化
  `json.dumps(tickets, sort_keys=True, separators=(",", ":"), ensure_ascii=True)`
  的 ASCII 字节计算 SHA256 十六进制。这是读取事务中逻辑事实的摘要，不是
  SQLite 文件二进制摘要；移走输入库后仍可仅凭报告重算摘要、覆盖率与归因。

相同逻辑输入与参数重复运行，报告 JSON 字节一致；工单/合同插入顺序或无关
表变化不影响分析结果与依据摘要。报告不含当前时间、耗时或机器绝对路径。

### 输出原子性与失败约定

完整读取、校验、计算并序列化后，先在 output 同目录的临时文件
（`.compare-report-*.tmp`）写完并关闭，再 `os.replace` 安全替换目标。
读取、计算、写入或替换任一失败均非零退出：旧报告字节不变、本次临时文件
被清理，修正后可直接重跑。output 不得覆盖输入库，包括可识别的符号链接与
硬链接别名（按 realpath 与 `os.path.samefile` 双重判定）。替换成功后才打印
stdout 摘要；摘要打印失败不回滚已替换的报告。本报告为单写者离线产物，
不提供服务、调度器、因果模型或可视化页面。

### 描述性边界

该分解是描述性的：它刻画共同支持客户上平均成本变化的构成，不代表合同
等级或组织调整对成本的因果效应；共同支持未覆盖全体时，共同支持差额也不
等于全体均值差，报告将两者分列，读者不得以前者冒充后者。

## 受限查询网关（query，v2 库的按主体受限 SQL 出口）

`contract_warehouse.query` 让外包维修人员对着已有 v2 库自助执行 SQL 查询
工单，同时保证任何合同方之外的行都不可读。它只新增独立入口，不修改
既有任何模块的接口与行为。

### 信任模型

- **可信调用方**（网关的嵌入系统）指定四样输入：主体 `principal`、严格
  JSON 策略（主体 → `contract_holder` 白名单）、正整数指令步数上限
  `max_steps`、正整数结果行数上限 `max_rows`。
- **SQL 文本不可信**：可以是任意单条语句，网关不假设其善意。
- **身份认证不在本模块范围内**：调用方负责先认证操作者，再以对应
  `principal` 调用；网关只保证“以某主体身份执行的 SQL 只能看到该主体
  白名单授权的行”。

### 输入与校验顺序

- `--database`：已存在的 v2 库。以 `mode=ro` 只读打开（缺失文件不会被
  创建），在同一个读取事务内校验 `wh_meta` 标识
  （`inherited_contracts_v2` / `2`）与 `wh_meta`、`dwd_ticket` 的精确
  列集合；非 SQLite 文件、v1 库、标识或结构不符均整次拒绝
  （`WarehouseError`），源库绝不写入。
- `--policy`：严格 JSON 策略。顶层恰好含 `schema`（必须等于
  `contract_query_policy_v1`）与 `principals` 两键；`principals` 为对象，
  键是 1..128 字符的主体名（至多 1024 个），值恰好含 `contract_holders`
  数组（每主体至多 1024 项；每项为 1..128 字符字符串，不得重复）。
  重复 JSON 键、`NaN`/`Infinity`、缺键/多键、类型错误、过深嵌套、文本
  超过 1 MiB 均拒绝（`PolicyError`）。标识沿用原文精确语义：不改大小写、
  不去空白。
- `--principal`：必须存在于策略中，否则 `UnknownPrincipalError`。
- `--max-steps` / `--max-rows`：正整数；布尔、浮点、字符串、非正数均
  拒绝（`QueryError`）。
- `--sql` / `--sql-file`：互斥且必给其一；`--sql-file -` 读标准输入。
  SQL 文本非空、不超过 1,000,000 字符。
- 请求校验先于任何数据库访问：策略类型 → 预算 → 主体 → SQL 形状 →
  数据库，错误按此顺序确定性地产出。

### 暴露关系与逐行授权规则

只暴露一个关系：`tickets(ticket_id, customer, opened, cost_cents,
contract_holder)`。其行是 `dwd_ticket` 中**已持久化**的
`contract_holder` 落在该主体白名单内的事实行（`IN` 匹配，因此
`contract_holder` 为 NULL 的未覆盖工单永不授权）；不按服务客户
`customer` 扩权，不沿当前层级桥表扩权，也不重新计算继承归属。空白名单
得到空 `tickets` 关系，查询照常执行并返回空集。授权行在同一读取事务内
按 `ticket_id` 排序后复制出库，提取时逐行校验五列的存储类型。

### 允许的 SQL

单条 `SELECT`：可含 `WITH`/`WITH RECURSIVE`、子查询、
`UNION`/`INTERSECT`/`EXCEPT`、连接、聚合与窗口函数、`EXPLAIN`；只能读
`tickets` 的五列（`ROWID` 不可读）。函数调用限于契约内白名单：聚合
（`avg`/`count`/`sum`/`total`/`min`/`max`/`group_concat`/`string_agg`）、
窗口（`row_number`/`rank`/`dense_rank`/`ntile`/`lag`/`lead`/`first_value`/
`last_value`/`nth_value`）、纯标量与日期时间助手（`abs`/`coalesce`/
`ifnull`/`iif`/`nullif`/`length`/`lower`/`upper`/`substr`/`trim`/`ltrim`/
`rtrim`/`replace`/`instr`/`hex`/`quote`/`round`/`typeof`/`unicode`/`char`/
`sign`/`concat`/`concat_ws`/`like`/`glob`/`likelihood`/`likely`/`unlikely`/
`date`/`time`/`datetime`/`julianday`/`strftime`/`unixepoch`/`timediff`）
及数学函数（`floor`/`sqrt`/`power`/`log` 等，取决于 SQLite 构建是否
启用）。`random`、`randomblob`、`zeroblob`、`printf`/`format`、
`sqlite_version`、`load_extension` 等一律拒绝；白名单内但当前构建未
注册的函数（如 `string_agg`）按“无此函数”拒绝该条查询。

### 禁止项与执行机制（不是关键字过滤）

禁止：访问底表/ODS 原文/审计/桥表（它们在执行环境中物理不存在）、模式
表（`sqlite_master`、`sqlite_schema`、`pragma_*` 表值函数）、其他文件
（`ATTACH`/`DETACH`、`VACUUM INTO`）、写操作与 DDL、`PRAGMA`、事务与
保存点、扩展加载、多语句。执行机制层层独立、不依赖字符串匹配：

1. 授权行被复制进一个仅含 `tickets` 表的**内存沙箱**，不可信 SQL 只在
   沙箱上执行；源库连接在此之前已关闭。
2. SQLite 授权回调（由 SQLite 解析器驱动、默认拒绝）只放行
   `SELECT`/递归求值、五列读取与函数白名单；`SQLITE_LIMIT_ATTACHED=0`
   与 `PRAGMA query_only=ON` 作为纵深防御（`VACUUM INTO` 的内部附加
   同样被这两层挡下，不会写出任何文件）。
3. SQLite 资源限制：单值 64 MiB、结果列 256、表达式深度 1000、复合
   查询项 500、SQL 文本 1,000,000 字符。
4. 进度回调实施步数预算：每 `min(1000, max_steps)` 条 VM 指令设一个
   检查点，累计超过预算即中断（超调小于 1000 条）；递归 CTE 耗尽与
   笛卡尔积爆炸都被此终止，不产生部分结果。
5. 取数时实施行数预算：最终结果行数超过 `max_rows` 即整次失败。

字符串字面量或引号标识符里的 `ATTACH`、`PRAGMA`、`DROP` 等字样只是
数据，不影响合法查询——拦截发生在解析器/执行层，而非关键字层。

### 输出与失败约定

成功时 stdout 输出单个 JSON（`sort_keys`）：
`{"columns": [...], "principal": "...", "row_count": N, "rows": [[...]]}`；
结果值只能是 NULL/整数/有限浮点/文本，出现 BLOB 或非有限浮点整次拒绝
（`ResultError`）。任何拒绝或超预算：退出码 2、原因到 stderr、stdout
无任何部分结果（结果先完整物化并通过值校验才输出）。每次调用都新建
沙箱，被拒绝或中断的查询不影响后续合法查询；源库只读且字节不变，不
产生 `-wal`/`-shm` 等伴生文件；沙箱为纯内存，不留临时文件。错误分类
（均为 `QueryError` 子类）：`PolicyError`、`UnknownPrincipalError`、
`WarehouseError`、`UnsafeSqlError`、`BudgetExceededError`、`ResultError`。

API：`query.Policy.from_json(text)` 解析策略；
`query.run_query(database, policy, principal, sql, max_steps, max_rows)`
返回 `QueryResult(columns, rows)`（完全物化、JSON 安全）。CLI：

```sh
python3 -m contract_warehouse.query --database run/inherited.sqlite \
  --policy fixtures_query/policy.json --principal tech-h \
  --max-steps 1000000 --max-rows 1000 \
  --sql "SELECT contract_holder, count(*), sum(cost_cents) FROM tickets GROUP BY 1"
```

非目标：身份认证与传输安全、按查询配额的计费、定时/缓存侧信道防护
（预算只约束单次查询的粗粒度资源）、查询审计日志。

### 合成策略夹具

`fixtures_query/policy.json`（全合成）：`tech-h`→`[H]`（仅见 T3/T4，
合计 700 分）、`tech-s`→`[S]`（仅见 T2/200 分，证明不按服务客户扩权）、
`tech-d1d2`→`[D1, D2]`（T1/T5）、`tech-u`→`[U]`（空集：U 无合同；
T6 未覆盖、持有人为 NULL，对一切主体不可见）、`tech-none`→`[]`
（空授权返回空集）。
