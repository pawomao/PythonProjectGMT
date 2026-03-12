# 📘 Project Context: IB-QMT 跨境 ETF 套利监控系统

> **项目代号**: GMT-Arb (Gemini-QMT-Trader)
> **核心目标**: 实时监控 CME 美股期货 (MES) 与 A 股标普 500 ETF (如 513500) 的溢价率。
> **关键约束**: 必须保证“净值日期”与“基准收盘价日期”严格对齐，杜绝时间错配。

---

## 🏗️ 1. 系统架构与技术栈 (System Architecture)

### 1.1 核心技术栈
- **语言**: Python 3.10+ (Windows 环境)
- **美股接口 (IB)**: `ib_insync` (基于 `asyncio`)
- **A股接口 (QMT)**: `xtquant` (MiniQMT 模式)
- **数据源**: 天天基金 (NAV), Interactive Brokers (ES/MES/FX), QMT (ETF现价)

### 1.2 线程模型 (Thread Model) - ⚠️ 严禁修改
系统采用 **双线程架构** 以解决 IB 的 `asyncio` 与 QMT 的阻塞冲突：
1.  **Main Thread (主线程)**:
    - 负责 `xtdata` 的订阅与数据回调。
    - 负责 UI 输出 (Print/Logging)。
    - 负责核心溢价率计算 (IOPV Calculation)。
2.  **IB Thread (子线程 `ib_loop`)**:
    - 必须手动创建 `new_event_loop()`。
    - 负责维护 IB 的连接 (`ib.connect`) 和数据流 (`reqMktData`)。
    - 将数据写入全局变量 `REALTIME_DATA` (线程安全字典)。

### 1.3 核心公式 (The Formula)
$$\text{Fair Value} = \text{NAV}_{date} \times \left( \frac{\text{MES}_{realtime}}{\text{MES}_{close\_date}} \right) \times \left( \frac{\text{USDCNH}_{realtime}}{\text{USDCNH}_{close\_date}} \right)$$
* **NAV_{date}**: 基金公布的最新净值（通常是 T-1 或 T-2）。
* **Close_date**: **必须**是 `NAV_{date}` 当日的收盘价。严禁使用 T-0 的昨收去匹配 T-2 的净值。

---

## 📂 2. 文件职责映射 (File Map)

| 文件名 | 职责描述 | 关键注意点 |
| :--- | :--- | :--- |
| **`run_realtime.py`** | **[核心入口]** 生产环境主程序。 | 包含全自动日期对齐逻辑、双线程启动、CME 故障容错。 |
| `utils_nav.py` | [工具] 爬虫模块。 | `get_fund_nav` 用于实盘，`fetch_history_navs` 用于回测。 |
| `utils_contract.py` | [工具] 合约算法。 | 自动计算 ES/MES 主力合约代码 (如 `202603`)，处理换月逻辑。 |
| `config.py` | [配置] 全局参数。 | 包含 ETF 列表、钉钉 Webhook、备用基准数据。 |
| `notifier.py` | [工具] 消息推送。 | 钉钉报警封装，包含签名逻辑。 |
| `run_history.py` | [ETL] 历史数据处理。 | 独立的 ETL 流程，用于清洗数据生成 csv 供分析。 |
| `analyze_premium.py` | [分析] 可视化报表。 | 生成 Plotly HTML 图表，包含动态通道和价差矩阵。 |
| `main.py` | **[已废弃]** 不再使用。 | 请使用 `run_realtime.py` 作为唯一生产入口。 |
| `data_ib.py` | **[已废弃]** 不再使用。 | 实盘直接使用 `ib_insync`；回测在 `run_history.py` 内直接调用。 |
| `data_qmt.py` | **[已废弃]** 不再使用。 | 实盘直接使用 `xtquant.xtdata`；回测在 `run_history.py` 内直接调用。 |
| `data_nav.py` | **[已废弃]** 不再使用。 | NAV 数据统一使用 `utils_nav`（`get_fund_nav` 实盘，`fetch_history_navs` 回测）。 |

---

## ⚙️ 3. 关键业务规则 (Business Rules)

### 3.1 晨间校准 (Morning Calibration)
程序启动时必须执行以下步骤（见 `run_realtime.py`）：
1.  爬取 ETF 最新净值，获取净值日期 `D`。
2.  **强制回溯**：向 IB 发送 `reqHistoricalData`，查询日期 `D` 当天的 MES 和 USDCNH 收盘价。
3.  如果 `D` 当天 IB 查不到（如美股休市/权限/超时/时区错配），系统必须执行“共同交易日自动回退”：继续尝试更早的候选日期；若候选窗口内仍无法锁定同日 MES/FX 收盘价，则报“致命错误”，**进入错误告警模式（持续运行、定时发送错误原因），而不是直接退出进程**，以避免产生时间错配结果。

### 3.1.1 共同交易日自动回退 (Common Trading Day Fallback) - ✅ 必须执行
当 `D` 当天 **MES 或 USDCNH 任一历史收盘价无法获取**（休市/权限/超时/时区错配等）时，系统必须按以下规则处理，以保证“净值日期”与“基准收盘价日期”严格对齐：

1. **候选日期集**：从所有监控 ETF 的 NAV 历史中，计算出“所有 ETF 都存在官方净值”的日期交集，按日期从新到旧排序，得到候选列表 `CANDIDATE_DATES`。
2. **自动回退**：IB 线程必须按 `CANDIDATE_DATES` 从新到旧逐日尝试，直到找到某一天 `D*` 同时满足：
   - IB 可查到 `MES_close(D*)`；
   - IB 可查到 `USDCNH_close(D*)`；
   - 且两者日期都精确匹配 `D*`（禁止用“最近一根/最后一根 bar”冒充同日数据）。
3. **锚点锁定**：一旦找到 `D*`，系统必须将最终锚点日期设置为 `D*`，并使用该日期的 NAV 与该日期的 MES/FX 收盘价作为基准进行后续实时溢价率计算。
4. **禁止策略**：严禁在找不到 `D` 精确匹配 bar 时，直接使用 `bars[-1]` 或“最近交易日”的收盘价来配对 `D` 的 NAV（这会造成时间错配）。

### 3.2 故障容错 (Fault Tolerance)
* **CME 数据异常处理**：
    * **背景**: 见上传的 `1000026010.jpg`，CME 官方通告 Bid/Ask 数据可能丢失。
    * **规则**: 在 `ib_loop` 中，获取价格的优先级为：**Last (最新成交) > Close (昨收) > MarketPrice (中间价)**。
    * **代码片段**:
        ```python
        price = t.last if (t.last and t.last > 0) else t.marketPrice()
        ```
* **周末/休市模式**:
    * 配置项 `DATA_MODE`:
        * `1`: 实时模式 (Real-time) -> 实盘必选。
        * `3`: 延迟模式 (Delayed)。
        * `4`: 冻结模式 (Frozen) -> **周末调试必选**，否则 IB 返回 `None`。

### 3.3 数据对齐
* **QMT**: 返回的是北京时间。
* **IB**: 返回的是 UTC 或 Exchange Time。
* **处理**: 在 `run_history.py` 中，所有 IB 数据必须经过 `normalize_ib_timezone` 清洗，统一转换为无时区的 datetime 或北京时间。

### 3.4 统一错误处理与告警 (Error Handling & Alerting)
* **错误判定范围**：来自以下任一数据源的“关键数据缺失或不可用”，并会直接影响溢价率计算结果时，必须视为“阻断性错误”：
    * 天天基金爬虫 (`utils_nav`)：NAV 历史/锚点日 NAV 获取失败。
    * QMT (`xtquant.xtdata`)：ETF 实时价格或昨收价格长时间为空。
    * IBKR (`ib_insync`)：MES/USDCNH 历史基准价无法锁定，或实时行情长时间为空。
* **处理策略**：
    1. 主线程必须**停止本轮溢价率计算与表格输出**，转为“错误状态”，但**程序整体不得退出**。
    2. 在错误状态下，系统需在控制台持续输出当前错误原因，并通过 `notifier.send_dingtalk_msg` 以固定时间间隔（如 5 分钟）向钉钉发送错误告警。
    3. 一旦关键数据恢复（如 MES/FX 实时报价恢复、QMT 返回有效 ETF 价格、NAV 数据重新可用），主线程应**自动清除错误状态并恢复正常溢价率计算**，无需人工重启。

### 3.5 估值摘要定时推送 (Periodic Premium Snapshot)
在系统处于“正常计算状态”（非错误状态）且溢价率可被正确计算时，程序必须按固定间隔向钉钉推送“估值摘要”，用于远程观察运行情况。

* **推送内容（建议）**：
    * 锚点日期 `NAV_DATE`
    * MES/USDCNH 当前值与相对基准日涨跌幅
    * 4 只 ETF 的：现价、IOPV(估)、溢价率
* **推送节流**：
    * 推送间隔必须可配置（例如 `PREMIUM_NOTIFY_INTERVAL`，单位秒）。
    * 推送节流必须与错误告警节流独立，避免互相影响。
* **禁止条件**：
    * 在错误状态下禁止发送估值摘要，只允许发送错误告警。

---

## 📝 4. 待办需求 (Current Tasks & Todo)

**当前开发阶段**: v2.1 (生产环境优化)

**AI Agent 请注意，生成代码时需关注以下待办事项：**

1.  **日志持久化 (Logging)**:
    * 目前 `run_realtime.py` 使用 `print` 输出，刷屏过快且无法回溯。
    * **需求**: 引入 `logging` 模块，将 INFO 级别日志打印到屏幕，将 DEBUG/DATA 日志写入 `logs/daily_YYYYMMDD.csv`。
    * *CSV 格式*: `Timestamp, ETF_Code, Price, Fair_Value, Premium_Rate`。

2.  **钉钉报警去重**:
    * 需在 `run_realtime.py` 中实现，避免简单 `sleep` 冷却。
    * **需求**: 实现基于状态机的报警。只有当“溢价率连续 N 次超过阈值”才报警，且报警后进入 5 分钟静默期，除非溢价率反向突破。

3.  **UI 界面 (远期规划)**:
    * 计划将控制台输出改为 PyQt6 或 Tkinter 的轻量级 Dashboard，显示 4 个 ETF 的实时红绿状态。

---

## ⚠️ 5. 常见报错与解决方案 (Knowledge Base)

* **Error**: `RuntimeError: There is no current event loop in thread 'Thread-1'.`
    * **Fix**: 在子线程 `run()` 方法的最开始，必须执行 `loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)`。
* **Error**: `IB connection timed out` / `Error 10275`
    * **Fix**: 检查 TWS/Gateway 是否开启，端口是否为 `4001` (Live) 或 `4002` (Paper)。确认 `ClientId` 不冲突。
* **Data**: `MES` 显示为 `None`
    * **Check**: 检查是否为周末？如果是，请将 `DATA_MODE` 改为 `4`。检查是否购买了 "CME Real-Time" 数据包。

---

**End of Instructions**