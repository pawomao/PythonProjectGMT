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
IB_PORT = 4001
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
    'NAV_DATE': None,  # 净值日期 (锚点)
    'MES_CLOSE': None,  # 该日期的 MES 收盘价
    'FX_CLOSE': None,  # 该日期的 汇率 收盘价
    'CANDIDATE_DATES': None,  # 共同交易日候选列表 (从新到旧，YYYY-MM-DD)
    'BASELINE_READY': False  # IB 线程基准数据是否就绪
}

REALTIME_DATA = {
    'MES': None,
    'FX': None,
    'ETFS': {}
}

ERROR_STATE = {
    'active': False,
    'message': '',
    'last_notify_ts': 0.0
}

# 估值摘要推送状态（独立于错误告警）
PREMIUM_STATE = {
    'last_notify_ts': 0.0
}

# 错误告警发送间隔（秒）
ERROR_NOTIFY_INTERVAL = 300

 # 估值摘要发送间隔（秒，可调）
PREMIUM_NOTIFY_INTERVAL = 300


def set_error(message: str):
    ERROR_STATE['active'] = True
    ERROR_STATE['message'] = message or ""
    print(f"❌ [ERROR] {ERROR_STATE['message']}")


def clear_error():
    if ERROR_STATE['active']:
        print("✅ [ERROR] 关键数据已恢复，退出错误状态。")
    ERROR_STATE['active'] = False
    ERROR_STATE['message'] = ""


def maybe_notify_error():
    """按固定间隔将当前错误发送到钉钉（若已配置）"""
    if not ERROR_STATE['active'] or not ERROR_STATE['message']:
        return
    now = time.time()
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
    REALTIME_DATA['ETFS'][code] = {'price': None, 'nav': None}


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
        print(f"🔌 [IB线程] 连接端口 {IB_PORT}...")
        ib.connect('127.0.0.1', IB_PORT, clientId=IB_CLIENT_ID)
        ib.RequestTimeout = 30

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
        print(f"✅ [IB线程] 实时数据流已建立")

        def on_pending_tickers(tickers):
            for t in tickers:
                if t.contract.conId == MES_CONTRACT.conId:
                    # 优先取 Last (防CME故障)
                    price = t.last if (t.last and t.last > 0) else t.marketPrice()
                    if price and price > 0:
                        REALTIME_DATA['MES'] = price

                elif t.contract.symbol == 'USD':
                    price = t.marketPrice()
                    if price and price > 0:
                        REALTIME_DATA['FX'] = price

        ib.pendingTickersEvent += on_pending_tickers
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

    while True:
        try:
            # 更新 QMT
            tick = xtdata.get_full_tick(ETF_LIST)
            if tick:
                for code in ETF_LIST:
                    if code in tick:
                        price = tick[code].get('lastPrice')
                        if not price or price == 0: price = tick[code].get('lastClose')
                        if price: REALTIME_DATA['ETFS'][code]['price'] = price

            # 计算前检查关键数据是否齐全
            mes_curr = REALTIME_DATA['MES']
            fx_curr = REALTIME_DATA['FX']
            mes_base = BASE_DATA['MES_CLOSE']
            fx_base = BASE_DATA['FX_CLOSE']

            if not (mes_curr and fx_curr and mes_base and fx_base):
                set_error(f"基准/实时数据缺失：MES={mes_curr}, FX={fx_curr}, MES_CLOSE={mes_base}, FX_CLOSE={fx_base}")
                maybe_notify_error()
                print(f"⏳ 等待数据... (MES:{mes_curr} FX:{fx_curr})")
                time.sleep(3)
                continue

            # 检查每只 ETF 的价格和 NAV 是否齐全
            missing_etfs = []
            etf_snapshot = []
            for code in ETF_LIST:
                data = REALTIME_DATA['ETFS'][code]
                price = data['price']
                nav = data['nav']
                if price and nav:
                    etf_snapshot.append((code, price, nav))
                else:
                    missing_etfs.append(code)

            if missing_etfs:
                set_error(f"以下 ETF 缺少价格或净值，无法计算溢价率: {', '.join(missing_etfs)}")
                maybe_notify_error()
                print(f"⏳ 等待 ETF 数据补全... 缺失标的: {', '.join(missing_etfs)}")
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
            print(f"错误: {e}")
            time.sleep(3)


if __name__ == "__main__":
    main_monitor()