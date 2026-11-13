# 字段与口径

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
