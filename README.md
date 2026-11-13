# data-qd-107 · 维修服务合同数仓

将维修合同历史与工单接入 SQLite，建立 ODS 原始层、有效时间合同维度、
DWD 工单事实和 ADS 成本集市。解决“工单发生时适用哪个服务等级”的业务问题，
按半开区间关联，明确保留未覆盖工单。

演示含 3 个合同版本、5 张工单。basic 为 2 笔/13000 分，premium 为 1 笔/20000 分，
uncovered 为 2 笔/12000 分。2月1日恰归新合同，C2在1月20日已过合同终点。
输出数据库支持直接 SQL 查询 ads_tier_cost；全量重建不会累加历史工单。

## 安装与运行

要求 Python 3.11 或更高；先确认 `python3 --version`。以下命令在仓库根目录执行：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m contract_warehouse.cli --source fixtures --database run/warehouse.sqlite
.venv/bin/python -m unittest discover -s tests -v
```

运行依赖与测试仅使用标准库；首次安装构建工具通常需要联网。每个仓库有独立
 `.venv`。安装的是源码的构建副本；编辑源码后须再次执行安装命令。
所有演示输出在忽略的 `run/` 中；演示可重复执行。临时目录中的测试不改变 fixture。
CLI 参数错误或无效数据返回非零；失败原因打印到 stderr。

## 目录

- `src/contract_warehouse/`：解析、业务转换及命令行入口。
- `fixtures/`：MIT 许可的合成输入；来源声明见 DATA-SOURCES.md。
- `docs/contract.md`：字段和口径、边界及失败约定。
- `tests/`：业务结果、边界与失败测试。
- `requirements*.lock`、`pyproject.toml`：环境和构建声明。

本基线不承诺实时同步或生产调度。仅验证本机环境的事实见交付记录；本仓库不
 声称已经验证任何特定 Linux 容器。所有路径均由调用参数提供，无宿主机绝对路径。
