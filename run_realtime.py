# -*- coding: utf-8 -*-
"""
run_realtime.py (最终版：强制日期对齐)
功能：
1. 【智能对齐】先获取 ETF 净值日期，再向 IB 请求该日期的收盘价。
2. 【精确计算】避免因净值滞后（如只有周五净值）导致的计算错误。
3. 【全自动】无需人工干预日期。
"""
import time
import threading
import asyncio
import datetime
import pandas as pd
from ib_insync import *
from xtquant import xtdata
import utils_nav
import utils_contract
import notifier

# ================= ⚙️ 配置区域 =================
IB_PORT = 4002
IB_CLIENT_ID = 100
DATA_MODE = 1  # 1=实时, 4=周末测试

# 监控 ETF 列表
ETF_LIST = ['513500.SH', '513650.SH', '159612.SZ', '159655.SZ']

# 合约定义
CURRENT_MONTH = utils_contract.get_es_expiry()
MES_CONTRACT = Future('MES', CURRENT_MONTH, 'CME')
FOREX_CONTRACT = Forex('USDCNH')

# ===============================================

BASE_DATA = {
    'NAV_DATE': None,        # 净值日期 (锚点)
    'MES_CLOSE': None,       # 该日期的 MES 收盘价
    'FX_CLOSE': None,        # 该日期的 汇率 收盘价
    'CANDIDATE_DATES': None, # 共同交易日候选列表 (从新到旧，YYYY-MM-DD)
    'BASELINE_READY': False  # IB 线程基准数据是否就绪
}

REALTIME_DATA = {
    'MES': None,
    'MES_TS': 0.0,  # MES 最近更新时间戳
    'FX': None,
    'FX_TS': 0.0,   # FX 最近更新时间戳
    'ETFS': {}
}

ERROR_STATE = {
    'active': False,
    'message': '',
    'last_notify_ts': 0.0,
    'first_seen_ts': 0.0  # 首次发现当前错误的时间，用于“持续超过 N 秒”判定
}

# 估值摘要推送状态（独立于错误告警）
PREMIUM_STATE = {
    'last_notify_ts': 0.0
}

# 错误告警发送间隔（秒）
ERROR_NOTIFY_INTERVAL = 300

# 估值摘要发送间隔（秒，可调）
PREMIUM_NOTIFY_INTERVAL = 300

# 行情心跳超时阈值（秒）
MES_FX_STALE_TIMEOUT = 30     # MES / FX 超过该秒数无更新 => 认为 IB 通讯异常
ETF_STALE_TIMEOUT = 60        # 单只 ETF 超过该秒数无 tick => 认为 QMT 该标的行情异常

# 启动宽限期（秒）：主循环进入后，前 N 秒内若仅 MES/FX 实时为空，不报错只等待（IB 常先推 nan 再推有效价）
REALTIME_GRACE_SEC = 20


def set_error(message: str):
    """
    设置阻断性错误状态。
    - 首次进入错误状态或错误信息变化时，重置 first_seen_ts，用于后续“持续超过 N 秒”的告警触发。
    - 进入错误状态时，同时重置估值摘要节流计时器，保证故障恢复后能第一时间推送最新正常数据。
    """
    now = time.time()
    new_msg = message or ""
    if not ERROR_STATE['active']:
        ERROR_STATE['active'] = True
        ERROR_STATE['message'] = new_msg
        ERROR_STATE['first_seen_ts'] = now
        ERROR_STATE['last_notify_ts'] = 0.0
        PREMIUM_STATE['last_notify_ts'] = 0.0
    else:
        # 如果错误类型/文案发生变化，视为新的错误事件，重新计时
        if ERROR_STATE['message'] != new_msg:
            ERROR_STATE['message'] = new_msg
            ERROR_STATE['first_seen_ts'] = now
            ERROR_STATE['last_notify_ts'] = 0.0
            PREMIUM_STATE['last_notify_ts'] = 0.0
    print(f"❌ [ERROR] {ERROR_STATE['message']}")


def clear_error():
    if ERROR_STATE['active']:
        # 恢复时立即推送一次“恢复正常”通知
        recover_msg = f"故障已恢复，系统恢复正常计算。上一次错误：{ERROR_STATE['message']}"
        print("✅ [ERROR] 关键数据已恢复，退出错误状态。")
        notifier.send_dingtalk_msg(recover_msg)
        notifier.send_ntfy_msg(recover_msg)
    ERROR_STATE['active'] = False
    ERROR_STATE['message'] = ""
    ERROR_STATE['last_notify_ts'] = 0.0
    ERROR_STATE['first_seen_ts'] = 0.0


def maybe_notify_error():
    """
    按策略将当前错误发送到钉钉/手机（若已配置）：
    - 错误首次出现后先观察 30s，若在此期间恢复则不推送（过滤瞬时抖动）；
    - 持续超过 30s 仍未恢复，则立即推送一次；
    - 之后按 ERROR_NOTIFY_INTERVAL 节流重复推送，直到错误解除。
    """
    if not ERROR_STATE['active'] or not ERROR_STATE['message']:
        return
    now = time.time()
    first_ts = ERROR_STATE.get('first_seen_ts') or 0.0
    # 错误持续时间不足 30 秒，不推送（视为待确认期）
    if first_ts and (now - first_ts < 30):
        return
    if now - ERROR_STATE['last_notify_ts'] < ERROR_NOTIFY_INTERVAL:
        return
    ERROR_STATE['last_notify_ts'] = now
    msg = f"运行异常：{ERROR_STATE['message']}"
    notifier.send_dingtalk_msg(msg)
    notifier.send_ntfy_msg(msg)


def maybe_notify_premium_snapshot(baseline_date: str, mes_curr: float, fx_curr: float, mes_base: float, fx_base: float,
                                 etf_rows: list):
    """
    etf_rows: [(code, price, iopv, premium), ...]
    """
    if ERROR_STATE['active']:
        return
    now = time.time()
    if now - PREMIUM_STATE['last_notify_ts'] < PREMIUM_NOTIFY_INTERVAL:
        return
    PREMIUM_STATE['last_notify_ts'] = now

    mes_ratio = (mes_curr / mes_base) if (mes_curr and mes_base) else None
    fx_ratio = (fx_curr / fx_base) if (fx_curr and fx_base) else None

    lines = []
    lines.append(f"估值摘要 | 锚点: {baseline_date} | {time.strftime('%Y-%m-%d %H:%M:%S')}")
    if mes_ratio is not None:
        lines.append(f"MES {mes_curr:.2f} / {mes_base:.2f} ({(mes_ratio - 1) * 100:+.2f}%)")
    else:
        lines.append(f"MES {mes_curr} / {mes_base}")
    if fx_ratio is not None:
        lines.append(f"USDCNH {fx_curr:.4f} / {fx_base:.4f} ({(fx_ratio - 1) * 100:+.2f}%)")
    else:
        lines.append(f"USDCNH {fx_curr} / {fx_base}")
    lines.append("-" * 24)
    for code, price, iopv, premium in etf_rows:
        lines.append(f"{code} 价{price:.3f} IOPV{iopv:.3f} 溢价{premium * 100:+.2f}%")

    content = "\n".join(lines)
    notifier.send_dingtalk_msg(content)
    notifier.send_ntfy_msg(content)

for code in ETF_LIST:
    REALTIME_DATA['ETFS'][code] = {
        'price': None,
        'nav': None,
        'last_update_ts': 0.0  # QMT 该标的最近更新时间戳
    }


def get_historical_baseline(ib: IB, target_date_str):
    """
    🔥 核心算法：根据【净值日期】去查那一天的收盘价
    target_date_str: 格式 '2026-02-06'
    """
    print(f"🔍 正在锁定锚点日期: {target_date_str}...")

    dt = datetime.datetime.strptime(target_date_str, "%Y-%m-%d")
    end_dt = dt + datetime.timedelta(days=1)
    # 注意：IB endDateTime 字符串建议使用空格分隔（YYYYMMDD HH:MM:SS）
    end_str = end_dt.strftime("%Y%m%d %H:%M:%S")

    def _get_daily_close_exact(contract, what_to_show: str, use_rth: bool):
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_str,
            durationStr='10 D',
            barSizeSetting='1 day',
            whatToShow=what_to_show,
            useRTH=use_rth
        )
        for bar in bars or []:
            if bar.date.strftime("%Y-%m-%d") == target_date_str:
                return bar.close
        return None

    # 1. 获取 MES 当日收盘
    print(f"   (1/2) 正在回溯 {MES_CONTRACT.localSymbol} 在 {target_date_str} 的收盘价...")
    ib.qualifyContracts(MES_CONTRACT)
    found_mes = _get_daily_close_exact(MES_CONTRACT, what_to_show='TRADES', use_rth=False)
    if found_mes is None:
        print("      ❌ MES 历史数据未精确匹配该日期")

    # 2. 获取 汇率 当日收盘
    print(f"   (2/2) 正在回溯 汇率 在 {target_date_str} 的收盘价...")
    ib.qualifyContracts(FOREX_CONTRACT)
    found_fx = _get_daily_close_exact(FOREX_CONTRACT, what_to_show='MIDPOINT', use_rth=True)
    if found_fx is None:
        print("      ❌ 汇率 历史数据未精确匹配该日期")

    return found_mes, found_fx


def ib_loop():
    """IB 专用子线程"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    ib = IB()
    try:
        host = '127.0.0.1'
        ib.RequestTimeout = 30

        # IB API 连接可能在启动阶段短暂不可用（Gateway 尚未完全就绪/重启中）。
        # 这里必须重试，避免一次 ConnectionRefused 就让 IB 线程退出，导致主程序“永远拿不到 MES/FX”。
        while True:
            try:
                print(f"🔌 [IB线程] 尝试连接 IB API: {host}:{IB_PORT} (clientId={IB_CLIENT_ID}) ...")
                ib.connect(host, IB_PORT, clientId=IB_CLIENT_ID, timeout=15)
                break
            except Exception as e:
                msg = f"IB API 连接失败（{host}:{IB_PORT}）：{e}"
                print(f"❌ [IB线程] {msg}")
                set_error(msg)
                maybe_notify_error()
                time.sleep(5)

        # A. 等待主线程计算出共同交易日候选列表
        print("⏳ [IB线程] 等待共同交易日候选列表就绪...")
        while not BASE_DATA.get('CANDIDATE_DATES'):
            time.sleep(1)

        # B. 按候选日期从新到旧逐日回退，直到同时锁定 MES/FX 同日收盘价
        locked = False
        for d in BASE_DATA['CANDIDATE_DATES']:
            mes_close, fx_close = get_historical_baseline(ib, d)
            if mes_close is not None and fx_close is not None:
                BASE_DATA['NAV_DATE'] = d
                BASE_DATA['MES_CLOSE'] = mes_close
                BASE_DATA['FX_CLOSE'] = fx_close
                BASE_DATA['BASELINE_READY'] = True
                print(f"🎯 [IB线程] 锚点日期锁定为: {d} | MES_CLOSE={mes_close} | FX_CLOSE={fx_close}")
                locked = True
                break

        if not locked:
            msg = "IB 历史基准价获取失败：候选日期内无法锁定同日 MES/FX 收盘价。"
            print(f"❌ [IB线程] 致命错误: {msg}")
            set_error(msg)

        # C. 订阅实时
        ib.reqMarketDataType(DATA_MODE)
        ib.reqMktData(MES_CONTRACT, '', False, False)
        ib.reqMktData(FOREX_CONTRACT, '', False, False)

        def on_pending_tickers(tickers):
            for t in tickers:
                # 若 MES / FX 仍为空，打印有限的调试信息，帮助确认 IB 是否有推行情
                if REALTIME_DATA['MES'] is None or REALTIME_DATA['FX'] is None:
                    print(f"[IB线程][TICK] contract={t.contract}, last={t.last}, marketPrice={t.marketPrice()}")

                if t.contract.conId == MES_CONTRACT.conId:
                    # 优先取 Last (防CME故障)
                    price = t.last if (t.last and t.last > 0) else t.marketPrice()
                    if price and price > 0:
                        REALTIME_DATA['MES'] = price
                        REALTIME_DATA['MES_TS'] = time.time()

                # 使用 conId 精确匹配 USDCNH 合约，避免仅按 symbol 判断带来的错配
                elif t.contract.conId == FOREX_CONTRACT.conId:
                    price = t.marketPrice()
                    if price and price > 0:
                        REALTIME_DATA['FX'] = price
                        REALTIME_DATA['FX_TS'] = time.time()

        ib.pendingTickersEvent += on_pending_tickers
        # 等待至少收到一次有效 MES+FX 再视为「已建立」（IB 常先推 nan，几秒后才推有效价）
        for _ in range(20):
            ib.waitOnUpdate(timeout=1)
            if REALTIME_DATA.get('MES') and REALTIME_DATA.get('FX'):
                break
        print(f"✅ [IB线程] 实时数据流已建立")
        ib.run()
    except Exception as e:
        print(f"❌ [IB线程] 错误: {e}")
    finally:
        if ib.isConnected(): ib.disconnect()
        loop.close()


def main_monitor():
    print("=" * 60)
    print(f"🚀 启动高精度监控 | MES: {MES_CONTRACT.localSymbol} | FX: USDCNH")
    print("=" * 60)

    # 1. 拉取 NAV 历史并计算“所有 ETF 都存在官方净值”的共同日期交集
    print("1️⃣ 获取官方净值历史并计算共同交易日候选集...")
    nav_hist = utils_nav.fetch_history_navs(ETF_LIST, days=60)
    if len(nav_hist) != len(ETF_LIST):
        msg = "NAV 历史不完整，无法计算共同交易日候选集。"
        print(f"❌ 致命错误: {msg}")
        set_error(msg)
        while True:
            maybe_notify_error()
            time.sleep(60)

    date_sets = []
    for code in ETF_LIST:
        df = nav_hist.get(code)
        if df is None or df.empty:
            msg = f"{code} 无净值历史，无法继续。"
            print(f"❌ 致命错误: {msg}")
            set_error(msg)
            while True:
                maybe_notify_error()
                time.sleep(60)
        date_sets.append(set(d.strftime("%Y-%m-%d") for d in df.index))

    common_dates = sorted(set.intersection(*date_sets), reverse=True)
    if not common_dates:
        msg = "未找到所有 ETF 共同的净值日期交集。"
        print(f"❌ 致命错误: {msg}")
        set_error(msg)
        while True:
            maybe_notify_error()
            time.sleep(60)

    # 控制回退窗口，避免过深回溯
    BASE_DATA['CANDIDATE_DATES'] = common_dates[:30]
    print(f"📅 共同交易日候选数: {len(BASE_DATA['CANDIDATE_DATES'])} | 最新候选: {BASE_DATA['CANDIDATE_DATES'][0]}")

    # 2. 启动 IB（它会按候选日期回退并锁定最终锚点日）
    t = threading.Thread(target=ib_loop, daemon=True)
    t.start()

    # 3. 启动 QMT
    print("2️⃣ 启动 QMT 订阅...")
    try:
        for code in ETF_LIST:
            xtdata.subscribe_quote(code, period='1m', count=1)
    except:
        pass

    # 4. 等待基准数据锁定
    print("3️⃣ 等待 IB 历史数据同步...")
    for _ in range(60):
        if BASE_DATA.get('BASELINE_READY') and BASE_DATA.get('MES_CLOSE') and BASE_DATA.get('FX_CLOSE'):
            break
        time.sleep(1)

    if not (BASE_DATA.get('BASELINE_READY') and BASE_DATA.get('MES_CLOSE') and BASE_DATA.get('FX_CLOSE') and BASE_DATA.get('NAV_DATE')):
        msg = "无法锁定同日基准数据（MES/FX/NAV_DATE），请检查 IB 历史行情权限或网络。"
        print(f"❌ 致命错误: {msg}")
        set_error(msg)
        while True:
            maybe_notify_error()
            time.sleep(60)

    # 5. 使用最终锚点日期，回填每只 ETF 当日 NAV（保证严格同日对齐）
    baseline_date = BASE_DATA['NAV_DATE']
    print(f"🎯 最终锚点日期: 【{baseline_date}】| 正在回填当日 NAV...")
    ts_baseline = pd.Timestamp(baseline_date)
    for code in ETF_LIST:
        df = nav_hist[code]
        try:
            nav_val = float(df.loc[ts_baseline, 'nav'])
        except Exception:
            # 保险：按日期字符串匹配
            matches = df[df.index.strftime("%Y-%m-%d") == baseline_date]
            if matches.empty:
                msg = f"{code} 无法获取锚点日 NAV={baseline_date}"
                print(f"❌ 致命错误: {msg}")
                set_error(msg)
                while True:
                    maybe_notify_error()
                    time.sleep(60)
            nav_val = float(matches.iloc[-1]['nav'])
        REALTIME_DATA['ETFS'][code]['nav'] = nav_val
        print(f"   ✅ {code}: NAV={nav_val:.4f} (日期: {baseline_date})")

    print("\n✅ 系统运行中... (每3秒刷新)\n")
    main_loop_start_ts = time.time()  # 用于启动宽限期，避免刚启动就因 MES/FX 未到而报错

    while True:
        try:
            # 更新 QMT
            tick = xtdata.get_full_tick(ETF_LIST)
            if tick:
                for code in ETF_LIST:
                    if code in tick:
                        price = tick[code].get('lastPrice')
                        if not price or price == 0: price = tick[code].get('lastClose')
                        if price:
                            REALTIME_DATA['ETFS'][code]['price'] = price
                            REALTIME_DATA['ETFS'][code]['last_update_ts'] = time.time()

            # 计算前检查关键数据是否齐全
            mes_curr = REALTIME_DATA['MES']
            fx_curr = REALTIME_DATA['FX']
            mes_base = BASE_DATA['MES_CLOSE']
            fx_base = BASE_DATA['FX_CLOSE']

            # 1) 基础空值检查（包括启动阶段）
            if not (mes_curr and fx_curr and mes_base and fx_base):
                # 启动宽限期：基准已有、仅实时 MES/FX 未到时不报错（IB 常先推 nan 再推有效价）
                in_grace = (mes_base and fx_base) and (time.time() - main_loop_start_ts < REALTIME_GRACE_SEC)
                if in_grace:
                    print(f"⏳ 等待 IB 实时数据... (MES:{mes_curr} FX:{fx_curr})")
                else:
                    set_error(f"基准/实时数据缺失：MES={mes_curr}, FX={fx_curr}, MES_CLOSE={mes_base}, FX_CLOSE={fx_base}")
                    maybe_notify_error()
                    print(f"⏳ 等待数据... (MES:{mes_curr} FX:{fx_curr})")
                time.sleep(3)
                continue

            # 2) MES / FX 心跳超时检查（通讯层面故障）
            now_ts = time.time()
            mes_ts = REALTIME_DATA.get('MES_TS') or 0.0
            fx_ts = REALTIME_DATA.get('FX_TS') or 0.0
            stale_reasons = []
            if now_ts - mes_ts > MES_FX_STALE_TIMEOUT:
                stale_reasons.append(f"MES 超过 {MES_FX_STALE_TIMEOUT}s 未更新 (最后: {time.strftime('%H:%M:%S', time.localtime(mes_ts)) if mes_ts else '从未更新'})")
            if now_ts - fx_ts > MES_FX_STALE_TIMEOUT:
                stale_reasons.append(f"FX 超过 {MES_FX_STALE_TIMEOUT}s 未更新 (最后: {time.strftime('%H:%M:%S', time.localtime(fx_ts)) if fx_ts else '从未更新'})")
            if stale_reasons:
                msg = "IB 行情心跳超时：" + "；".join(stale_reasons)
                set_error(msg)
                maybe_notify_error()
                print(f"⏳ 等待 IB 行情恢复... {msg}")
                time.sleep(3)
                continue

            # 3) 检查每只 ETF 的价格和 NAV 是否齐全 & 心跳是否超时
            missing_etfs = []
            stale_etfs = []
            etf_snapshot = []
            for code in ETF_LIST:
                data = REALTIME_DATA['ETFS'][code]
                price = data['price']
                nav = data['nav']
                last_ts = data.get('last_update_ts') or 0.0

                if not (price and nav):
                    missing_etfs.append(code)
                    continue

                # 心跳超时：有价格 & NAV，但长时间无新 tick，判定为行情通讯异常
                if now_ts - last_ts > ETF_STALE_TIMEOUT:
                    stale_etfs.append(code)
                    continue

                etf_snapshot.append((code, price, nav))

            if missing_etfs or stale_etfs:
                reasons = []
                if missing_etfs:
                    reasons.append(f"缺少价格或净值: {', '.join(missing_etfs)}")
                if stale_etfs:
                    reasons.append(f"行情心跳超时(>{ETF_STALE_TIMEOUT}s 未更新): {', '.join(stale_etfs)}")
                msg = "以下 ETF 数据异常，无法计算溢价率：" + "；".join(reasons)
                set_error(msg)
                maybe_notify_error()
                print(f"⏳ 等待 ETF 数据恢复... {msg}")
                time.sleep(3)
                continue

            # 关键数据齐全，清除错误状态并计算溢价
            clear_error()

            mes_ratio = mes_curr / mes_base
            fx_ratio = fx_curr / fx_base
            total_ratio = mes_ratio * fx_ratio

            ts = time.strftime('%H:%M:%S')
            print(f"\n⏰ {ts} | 锚点日期: {BASE_DATA['NAV_DATE']}")
            print(f"   MES: {mes_curr:.2f} (基准:{mes_base:.2f} | 涨幅:{(mes_ratio - 1) * 100:+.2f}%)")
            print(f"   汇率: {fx_curr:.4f} (基准:{fx_base:.4f} | 涨幅:{(fx_ratio - 1) * 100:+.2f}%)")
            print("-" * 65)
            print(f"{'代码':<10} | {'现价':<8} | {'IOPV(估)':<8} | {'溢价率':<8} | {'状态'}")
            print("-" * 65)

            premium_rows = []
            for code, price, nav in etf_snapshot:
                iopv = nav * total_ratio
                premium = (price / iopv) - 1
                premium_rows.append((code, price, iopv, premium))

                flag = ""
                if abs(premium) > 0.015: flag = "⚡关注"
                if abs(premium) > 0.03: flag = "🔥极值"

                print(f"{code:<10} | {price:<8.3f} | {iopv:<8.3f} | {premium * 100:>+6.2f}% | {flag}")
            print("-" * 65)

            maybe_notify_premium_snapshot(
                baseline_date=BASE_DATA['NAV_DATE'],
                mes_curr=mes_curr,
                fx_curr=fx_curr,
                mes_base=mes_base,
                fx_base=fx_base,
                etf_rows=premium_rows
            )

            time.sleep(3)

        except KeyboardInterrupt:
            break
        except Exception as e:
            # 运行时异常统一纳入错误状态与告警通道，避免只在控制台打印
            err_msg = f"主循环异常: {e}"
            print(err_msg)
            set_error(err_msg)
            maybe_notify_error()
            time.sleep(3)


if __name__ == "__main__":
    main_monitor()