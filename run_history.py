# -*- coding: utf-8 -*-
"""
run_history.py (架构重构版)
功能：历史数据 ETL 批处理中心
特点：
1. 保持 STEP_SWITCH 开关设计。
2. 移除对 data_ib/data_qmt 的依赖，直接调用底层库，更轻量。
3. 引入 utils_nav 公共模块。
"""
import asyncio
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime, timedelta
from ib_insync import *  # 直接使用 ib_insync
from xtquant import xtdata  # 直接使用 xtquant

# 引入公共配置和模块
import config
import utils_nav

# ==========================================
# 🎛️ 总控制台
# ==========================================
# 1 = IB 数据下载 (增量更新)
# 2 = 基金净值下载 (调用 utils_nav)
# 3 = QMT 数据下载 (本地读取)
# 4 = 合并计算 (生成 Analysis 所需的 CSV)
STEP_SWITCH = 4

# 📅 设定数据范围
START_DATE = "2026-01-01"  # 建议设早一点，保证有足够的历史数据算比率
END_DATE = datetime.now().strftime("%Y-%m-%d")

# 临时文件目录
TEMP_DIR = "temp_data"
if not os.path.exists(TEMP_DIR): os.makedirs(TEMP_DIR)


# ==========================================
# 🛠️ 核心工具函数
# ==========================================
def normalize_ib_timezone(df):
    """强制清洗 IB 数据的时间索引 (解决 tz 报错的核心)"""
    if df is None or df.empty: return df

    # 1. 确保索引是 DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True, errors='coerce')

    df = df[~df.index.isna()]  # 去空

    # 2. 统一转为北京时间 (无时区信息)
    if df.index.tz is not None:
        # 先转为上海时间，再移除时区信息 (naive datetime)
        df.index = df.index.tz_convert('Asia/Shanghai').tz_localize(None)

    return df


# ==========================================
# 🟢 步骤 1: IB 数据下载 (直接集成版)
# ==========================================
async def download_ib_contract(ib, symbol, local_symbol, exchange, duration_str, file_name):
    """通用的 IB 下载函数"""
    contract = Future(symbol, '202503', exchange) if symbol in ['MES', 'ES'] else Forex(symbol)
    if symbol == 'USDCNH': contract = Forex('USDCNH')

    # 简单处理：这里演示下载最近一段时间的数据
    # 实战中保留你原来的 increment 逻辑会更好，这里为了代码简洁展示核心逻辑
    print(f"   🚀 请求 IB 数据: {symbol}...")
    bars = await ib.reqHistoricalDataAsync(
        contract, endDateTime='', durationStr=duration_str,
        barSizeSetting='1 min', whatToShow='MIDPOINT', useRTH=False
    )

    if bars:
        df = util.df(bars)
        df.set_index('date', inplace=True)
        df = normalize_ib_timezone(df)
        df.to_csv(f"{TEMP_DIR}/{file_name}")
        print(f"   💾 已保存 {symbol}: {len(df)} 条")
    else:
        print(f"   ⚠️ 未获取到 {symbol} 数据")


async def step_1_ib():
    print("=" * 60)
    print(f">>> [步骤 1] IB 数据下载 (连接 TWS/Gateway)")

    ib = IB()
    try:
        # 请确保 config.py 里有 IB_PORT
        await ib.connectAsync('127.0.0.1', 7497, clientId=99)

        # 1. 下载分钟线 (用于计算溢价) - 示例下载 30 天，可根据需求调大
        await download_ib_contract(ib, 'MES', 'MES', 'CME', '30 D', 'ib_ES_full.csv')
        await download_ib_contract(ib, 'USDCNH', 'USDCNH', 'IDEALPRO', '30 D', 'ib_FX_full.csv')

        # 2. 下载日线 (用于计算基准 Fair Value Ratio)
        print("   -> 更新日线基准 (3年)...")
        bars_es_d = await ib.reqHistoricalDataAsync(Future('MES', '202503', 'CME'), '', '2 Y', '1 day', 'MIDPOINT',
                                                    False)
        bars_fx_d = await ib.reqHistoricalDataAsync(Forex('USDCNH'), '', '2 Y', '1 day', 'MIDPOINT', False)

        normalize_ib_timezone(util.df(bars_es_d).set_index('date')).to_csv(f"{TEMP_DIR}/ib_es_daily.csv")
        normalize_ib_timezone(util.df(bars_fx_d).set_index('date')).to_csv(f"{TEMP_DIR}/ib_fx_daily.csv")

        print("   ✅ IB 数据更新完毕")
    except Exception as e:
        print(f"   ❌ IB 连接或下载失败: {e}")
    finally:
        ib.disconnect()


# ==========================================
# 🟢 步骤 2: NAV 下载 (调用 utils_nav)
# ==========================================
def step_2_nav():
    print("=" * 60)
    print(f">>> [步骤 2] 基金净值下载 (From 天天基金)")

    # 从 config 读取 ETF 列表
    codes = list(config.TARGET_ETFS.keys())

    # 调用公共模块
    nav_data = utils_nav.fetch_history_navs(codes, days=365)

    for code, df in nav_data.items():
        df.to_csv(f"{TEMP_DIR}/nav_{code}.csv")
        print(f"   💾 存档: {TEMP_DIR}/nav_{code}.csv")


# ==========================================
# 🟢 步骤 3: QMT 下载 (直接集成版)
# ==========================================
def step_3_qmt():
    print("=" * 60)
    print(f">>> [步骤 3] QMT 数据处理 (读取本地数据)")

    codes = list(config.TARGET_ETFS.keys())

    # 1. 尝试下载 (如果 QMT 客户端开启)
    try:
        xtdata.download_history_data2(codes, period='1m', start_time=START_DATE.replace('-', ''))
        print("   ✅ QMT 下载指令已发送")
    except:
        print("   ⚠️ QMT 客户端未连接，尝试直接读取本地数据...")

    # 2. 读取数据
    for code in codes:
        # xtdata 返回的数据是 dict {code: dataframe}
        data = xtdata.get_market_data(field_list=['close'], stock_list=[code], period='1m',
                                      start_time=START_DATE.replace('-', ''))
        if code in data and not data[code].empty:
            df = data[code]
            # QMT index 格式通常是 YYYYMMDDHHMMSS 字符串，需转换
            df.index = pd.to_datetime(df.index, format='%Y%m%d%H%M%S')
            df.rename(columns={'close': 'etf_price'}, inplace=True)
            df.to_csv(f"{TEMP_DIR}/qmt_{code}.csv")
            print(f"   💾 {code}: {len(df)} 条数据已就绪")
        else:
            print(f"   ⚠️ {code}: 未读取到数据")


# ==========================================
# 🟢 步骤 4: 合并计算 (核心逻辑优化)
# ==========================================
def step_4_merge():
    print("=" * 60)
    print(f">>> [步骤 4] 数据对齐与溢价率计算")

    # 1. 加载基准数据
    try:
        es_min = normalize_ib_timezone(pd.read_csv(f"{TEMP_DIR}/ib_ES_full.csv", index_col=0))
        fx_min = normalize_ib_timezone(pd.read_csv(f"{TEMP_DIR}/ib_FX_full.csv", index_col=0))
        es_daily = normalize_ib_timezone(pd.read_csv(f"{TEMP_DIR}/ib_es_daily.csv", index_col=0))
        fx_daily = normalize_ib_timezone(pd.read_csv(f"{TEMP_DIR}/ib_fx_daily.csv", index_col=0))

        # 重命名方便 merge
        es_min.rename(columns={'close': 'es_price'}, inplace=True)
        fx_min.rename(columns={'close': 'fx_rate'}, inplace=True)

        print(f"   ✅ 基准数据加载: ES_Min({len(es_min)}), FX_Min({len(fx_min)})")
    except FileNotFoundError:
        print("   ❌ 缺少 IB 数据文件，请先运行 Step 1")
        return

    # 2. 遍历每个 ETF 进行计算
    for code in config.TARGET_ETFS:
        qmt_file = f"{TEMP_DIR}/qmt_{code}.csv"
        nav_file = f"{TEMP_DIR}/nav_{code}.csv"

        if not os.path.exists(qmt_file) or not os.path.exists(nav_file):
            print(f"   ⚠️ 跳过 {code}: 缺少 QMT 或 NAV 数据")
            continue

        # 读取 ETF 和 NAV
        df_etf = pd.read_csv(qmt_file, index_col=0, parse_dates=True)
        df_nav = pd.read_csv(nav_file, index_col=0, parse_dates=True)

        # 3. 数据对齐 (Merge Asof)
        # 将 ETF 分钟线与 ES/FX 分钟线对齐
        # direction='backward': ETF 10:30 的价格 匹配 ES 10:30 (或最近的之前时刻) 的价格
        df_merge = pd.merge_asof(df_etf.sort_index(), es_min.sort_index(), left_index=True, right_index=True,
                                 direction='backward', tolerance=pd.Timedelta('5min'))
        df_merge = pd.merge_asof(df_merge, fx_min.sort_index(), left_index=True, right_index=True, direction='backward',
                                 tolerance=pd.Timedelta('60min'))

        df_merge.dropna(subset=['es_price', 'fx_rate'], inplace=True)
        if df_merge.empty:
            print(f"   ⚠️ {code}: 时间对齐后无数据 (可能是时区问题)")
            continue

        # 4. 计算公允价值比率 (Fair Value Ratio)
        # 逻辑: 每天根据 T-1 日的 NAV 计算一个固定的 Ratio
        # Ratio = NAV_T-1 / (ES_Daily_Close_T-1 * FX_Daily_Close_T-1)

        df_merge['ratio'] = np.nan
        df_merge['ref_nav'] = np.nan
        df_merge['date_str'] = df_merge.index.strftime('%Y-%m-%d')

        # 优化：不使用循环，使用 map 映射加快速度
        # 构建一个 {date: ratio} 的字典
        ratio_dict = {}
        nav_dict = {}

        for date_ts in df_nav.index:
            date_str = date_ts.strftime('%Y-%m-%d')
            try:
                # 找到这一天对应的 ES 和 FX 日线收盘价
                # 注意：这里我们用 NAV 的日期去查 IB 的日线
                if date_ts in es_daily.index and date_ts in fx_daily.index:
                    base_es = es_daily.loc[date_ts]['close']
                    base_fx = fx_daily.loc[date_ts]['close']
                    nav_val = df_nav.loc[date_ts]['nav']

                    if base_es > 0:
                        # 计算出的 Ratio 代表：每 1 单位 (ES*FX) 对应多少 ETF 净值
                        # 这个 Ratio 应该在第二天(T+1)使用
                        next_day = date_ts + timedelta(days=1)
                        # 简单处理：未来 1-3 天都用这个 Ratio (直到有新的 NAV)
                        for d in range(1, 5):
                            target_day = (date_ts + timedelta(days=d)).strftime('%Y-%m-%d')
                            if target_day not in ratio_dict:  # 只填补还没填的，保证用最近的 NAV
                                ratio_dict[target_day] = nav_val / (base_es * base_fx)
                                nav_dict[target_day] = nav_val
            except Exception as e:
                pass

        # 映射 Ratio 到分钟线
        df_merge['ratio'] = df_merge['date_str'].map(ratio_dict)
        df_merge['ref_nav'] = df_merge['date_str'].map(nav_dict)

        # 5. 最终计算
        df_merge.dropna(subset=['ratio'], inplace=True)
        df_merge['fair_value'] = df_merge['es_price'] * df_merge['fx_rate'] * df_merge['ratio']
        df_merge['premium'] = (df_merge['etf_price'] / df_merge['fair_value']) - 1

        # 6. 保存
        out_file = f"History_Premium_{code}.csv"
        cols = ['etf_price', 'es_price', 'fx_rate', 'ref_nav', 'fair_value', 'premium']
        df_merge[cols].to_csv(out_file)

        avg_prem = df_merge['premium'].mean() * 100
        print(f"   ✅ {code} 分析完成: {len(df_merge)} 条 | 平均溢价: {avg_prem:.2f}%")


# ==========================================
# 主入口
# ==========================================
async def main():
    if STEP_SWITCH == 1:
        await step_1_ib()
    elif STEP_SWITCH == 2:
        step_2_nav()
    elif STEP_SWITCH == 3:
        step_3_qmt()
    elif STEP_SWITCH == 4:
        step_4_merge()
    else:
        print("❌ 无效的 STEP_SWITCH")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass