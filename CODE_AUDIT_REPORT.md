# 代码体检报告：对照 agents.md 规则检查

> 检查日期：2025-02-13  
> 规则来源：`agents.md`（单一真相来源）  
> 检查范围：项目内所有 `*.py` 文件

---

## 一、规则抽取与分类

### 1.1 架构/线程模型类
- **R1**: 双线程架构：主线程负责 QMT 订阅/UI/溢价计算；IB 子线程负责 IB 连接与数据流
- **R2**: IB 子线程必须在 `run()` 开头执行 `asyncio.new_event_loop()` 并 `set_event_loop`
- **R3**: 系统以 `run_realtime.py` 为核心入口，`main.py` 为旧入口

### 1.2 业务逻辑类
- **R4**: 晨间校准：爬 NAV → reqHistoricalData 获取 NAV 日 MES/USDCNH 收盘 → 失败时降级到 config 静态值
- **R5**: CME 故障容错：价格优先级 **Last > Close > MarketPrice**
- **R6**: 净值日期必须与基准收盘价日期严格对齐（禁止 T-0 昨收匹配 T-2 净值）
- **R7**: 合约：使用 MES（而非 ES），按 `utils_contract` 计算主力月份

### 1.3 数据/时间处理类
- **R8**: `run_history.py` 中所有 IB 数据必须经 `normalize_ib_timezone` 清洗
- **R9**: QMT 为北京时间，IB 为 UTC/交易所时间，合并时需对齐

### 1.4 工具与文件职责类
- **R10**: `utils_nav.get_fund_nav` 用于实盘，`fetch_history_navs` 用于回测
- **R11**: `utils_contract` 实现主力合约与换月逻辑，被入口脚本调用
- **R12**: `notifier.py` 为钉钉封装，无业务逻辑强耦合
- **R13**: `config.py` 含 ETF 列表、`DATA_MODE`、钉钉 Webhook、灾备基准

### 1.5 待办项类
- **R14**: 日志持久化：引入 logging，DEBUG/DATA 写入 `logs/daily_YYYYMMDD.csv`
- **R15**: 钉钉报警：基于状态机，连续 N 次超阈值才报警 + 5 分钟静默期

---

## 二、问题清单（按严重程度）

### 🔴 严重（High）

| # | 问题 | 涉及文件 | 与规则 | 建议修改 |
|---|------|----------|--------|----------|
| H1 | ~~**main.py 导入不存在的函数**~~ | `main.py` | R10 | ✅ 已处理：main.py 已废弃并替换为最小桩，不再包含有问题的导入 |
| H2 | ~~**main.py 调用不存在的 IB/QMT 接口**~~ | `main.py` | R3 | ✅ 已处理：main.py 已废弃，改为最小桩，运行时会提示使用 run_realtime.py 并退出 |
| H3 | **run_realtime 无 IB 失败时的 config 降级**：当 `bars_mes` 或 `bars_fx` 为空时，`BASE_DATA['MES_CLOSE']` / `FX_CLOSE` 保持 `None`，程序无限等待数据 | `run_realtime.py` L83–112 | R4 | 在 `get_historical_baseline` 的 `else` 分支中，当 IB 完全无数据时，降级使用 `config.YESTERDAY_ES_CLOSE` / `YESTERDAY_FX_RATE`（config 需增加 MES 版本或与 ES 共用） |
| H4 | **ETF 列表与配置割裂**：`run_realtime.py` 硬编码 `ETF_LIST`，`run_history.py` 使用 `config.TARGET_ETFS`，两处列表可能不一致 | `run_realtime.py` L24, `config.py` | R13 | 统一从 `config.TARGET_ETFS` 读取 ETF 列表，`run_realtime.py` 移除 `ETF_LIST` 硬编码 |

### 🟠 中等（Medium）

| # | 问题 | 涉及文件 | 与规则 | 建议修改 |
|---|------|----------|--------|----------|
| M1 | **CME 价格优先级不完整**：文档要求 Last > Close > MarketPrice。`run_realtime.py` 中 MES 为 `Last > marketPrice()`，**未使用 Close 作为中间档**；FX 仅用 `marketPrice()` | `run_realtime.py` L137–149 | R5 | MES 段改为：`t.last if (t.last and t.last > 0) else (t.close if (t.close and t.close > 0) else t.marketPrice())`；FX 若 IB 提供 last/close 则同样加入优先级 |
| M2 | **run_realtime 未使用 config 的 IB/灾备配置**：`IB_PORT`、`IB_CLIENT_ID`、`DATA_MODE`、`YESTERDAY_ES_CLOSE` 等均在 `run_realtime.py` 内硬编码，未引用 `config` | `run_realtime.py` L19–22 | R13 | 改为 `from config import IB_PORT, IB_CLIENT_ID, DATA_MODE, YESTERDAY_ES_CLOSE, YESTERDAY_FX_RATE, TARGET_ETFS` 等，并删除局部重复定义 |
| M3 | **config 灾备使用 ES 名称**：`YESTERDAY_ES_CLOSE` 与 agents.md 中的 MES 表述不一致；业务上若使用 MES，建议命名/注释明确为 MES 或兼容 ES | `config.py` L18 | R7 | 将 `YESTERDAY_ES_CLOSE` 重命名为 `YESTERDAY_MES_CLOSE` 或增加注释说明 ES/MES 通用 |
| M4 | **run_history IB 端口与 config 不一致**：`run_history.py` 使用 `7497`，`config.py` 为 `4001`，易导致配置混乱 | `run_history.py` L95 | - | 改为 `config.IB_PORT` |
| M5 | **data_ib 与 utils_contract 换月逻辑重复且不一致**：`data_ib.get_es_expiry` 以每月 15 号为界；`utils_contract.get_es_expiry` 以交割日当周周一为界。agents.md 指定 `utils_contract` 为合约算法来源 | `data_ib.py` L34–50 | R11 | `data_ib` 应调用 `utils_contract.get_es_expiry`，删除内部 `get_es_expiry` 实现；若需支持 MES，则扩展 `utils_contract` |
| M6 | **data_ib 使用 ES 而非 MES**：`data_ib` 构建的是 ES 合约，agents.md 明确使用 MES；main.py 已废弃 | `data_ib.py` | R7 | 若 data_ib 仍被其他脚本使用，将合约构建改为 MES |

### 🟡 建议（Low）

| # | 问题 | 涉及文件 | 与规则 | 建议修改 |
|---|------|----------|--------|----------|
| L1 | **日志持久化未实现**：全项目无 `import logging`，无 `logs/daily_*.csv` 输出 | 全项目 | R14 | 在 `run_realtime.py` 引入 logging，INFO 输出到控制台，DEBUG/DATA 按 `logs/daily_YYYYMMDD.csv` 格式写入 |
| L2 | **钉钉报警需状态机**：`run_realtime.py` 尚未集成钉钉；若集成，需实现“连续 N 次超阈值 + 5 分钟静默期”，避免简单 sleep 冷却 | `run_realtime.py` | R15 | 实现状态变量（如 `consecutive_over_threshold`、`last_alert_time`），满足条件才调用 `send_dingtalk_msg` |
| L3 | **run_realtime 无钉钉报警**：生产入口 `run_realtime.py` 未集成钉钉通知，超阈值时仅控制台输出 | `run_realtime.py` | - | 若需生产报警，在溢价率超阈值时调用 `notifier.send_dingtalk_msg`，并配合 L2 的状态机 |
| L4 | **data_nav 与 utils_nav 职责重叠**：`data_nav.fetch_history_navs` 与 `utils_nav.fetch_history_navs` 功能相似，存在重复实现 | `data_nav.py`, `utils_nav.py` | R10 | 统一使用 `utils_nav` 作为 NAV 数据源；考虑废弃或合并 `data_nav.py`（main.py 已废弃，不再依赖 data_nav） |
| L5 | **agents.md 中 data_ib/data_qmt 未列入文件映射**：文档未明确这两者的职责与调用关系 | `agents.md` | - | 在 agents.md 文件职责表中补充 `data_ib.py`、`data_qmt.py` 的职责说明，或明确由 run_realtime 直接使用底层库、不经过 data_* |
| L6 | **run_history 使用 datetime.now() 无时区**：`END_DATE = datetime.now().strftime(...)` 依赖本地时区 | `run_history.py` L34 | R9 | 如需严格按北京时间，使用 `datetime.now(timezone(timedelta(hours=8)))` 或 `pandas.Timestamp.now(tz='Asia/Shanghai')` |

---

## 三、符合规则的部分（正面清单）

| 规则 | 实现情况 |
|------|----------|
| R2 子线程 new_event_loop | ✅ `run_realtime.py` L116–118 正确实现 |
| R4 晨间校准三步（爬 NAV → IB 历史 → 降级） | ✅ 前两步正确；降级缺失见 H3 |
| R5 CME Last 优先 | ⚠️ 部分实现，缺少 Close 中间档见 M1 |
| R6 日期对齐 | ✅ `run_realtime.py` 以 NAV 日期为锚点请求历史收盘 |
| R7 MES 合约 | ✅ `run_realtime.py` 使用 `utils_contract.get_es_expiry` 与 MES |
| R8 normalize_ib_timezone | ✅ `run_history.py` 在 IB 数据读写路径中统一调用 |
| R10 utils_nav 分工 | ✅ `run_realtime` 用 `get_fund_nav`，`run_history` 用 `fetch_history_navs` |
| R11 utils_contract 调用 | ✅ `run_realtime` 正确调用 |
| R12 notifier 职责 | ✅ `notifier.py` 仅为钉钉封装，无业务耦合 |
| R13 config 内容 | ⚠️ 有 ETF、Webhook、灾备；`DATA_MODE` 在 config 中存在，run_realtime 未引用 |

---

## 四、横向风险扫描摘要

- **线程/事件循环**：仅 `run_realtime.py` 在 IB 子线程中创建 event loop，符合双线程模型；`main.py` 为单线程 asyncio，与文档不符。
- **时区**：`run_history` 使用 `normalize_ib_timezone`；`data_ib`、`data_nav` 的 `datetime.now()` 未显式指定时区，对批量 ETL 影响有限，但建议统一。
- **数据源分散**：`main.py` 依赖 `data_ib`、`data_qmt`、`data_nav`，而这些模块的实时接口不完整，导致 main 无法运行；`run_realtime` 直接使用 `ib_insync`、`xtquant`、`utils_nav`，与“run_realtime 为核心入口”的设计一致。

---

## 五、修改优先级建议

1. **立即处理**：H3（run_realtime 灾备降级）、H4（ETF 列表统一）；H1、H2 已处理（main 已废弃）
2. **短期**：M1–M6（CME 优先级、config 统一、端口与合约逻辑）
3. **迭代**：L1–L6（日志、钉钉状态机、职责梳理）

---

## 六、下一步确认

请确认：

1. 是否希望我**按优先级依次直接修改代码**？
2. 是否希望**先处理某一模块**（例如仅双线程/晨间校准）？

注：`main.py` 已按“废弃”处理完成。
