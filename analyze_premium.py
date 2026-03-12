# -*- coding: utf-8 -*-
"""
交互式分析脚本：动态趋势通道 + 价差矩阵分析 (5日均线增强版)
顺序: 513500 -> 159612 -> 513650 -> 159655
功能：
1. 主图：显示4只ETF的溢价率及动态通道。
2. 附图：指标A及4组价差分析。
3. 增强：十字光标联动。
4. 【新增】差值图中增加 5日均值滤波虚线 (5-Day Moving Average)。
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import webbrowser

# 输出目录
OUTPUT_DIR = "analysis_report"
if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)


def plot_premium_analysis():
    # ---------------------------------------------------------
    # 1. 定义配置与顺序
    # ---------------------------------------------------------
    ORDERED_CODES = ['513500.SH', '159612.SZ', '513650.SH', '159655.SZ']

    etf_map = {
        '513500.SH': {'name': '513500 (博时)', 'color': '#d62728'},  # 红
        '159612.SZ': {'name': '159612 (国泰)', 'color': '#2ca02c'},  # 绿
        '513650.SH': {'name': '513650 (南方)', 'color': '#ff7f0e'},  # 橙
        '159655.SZ': {'name': '159655 (华夏)', 'color': '#1f77b4'}  # 蓝
    }

    TARGET_CODE = '159612.SZ'
    PARAM_B = 0.05
    PARAM_C = 0
    PARAM_D = 0.05

    print(">>> [1/4] 正在读取并合并数据...")

    # ---------------------------------------------------------
    # 2. 读取数据并合并
    # ---------------------------------------------------------
    data_dict = {}
    for code in ORDERED_CODES:
        filename = f"History_Premium_{code}.csv"
        if os.path.exists(filename):
            try:
                df = pd.read_csv(filename, index_col=0, parse_dates=True)
                if not df.empty:
                    data_dict[code] = df['premium'] * 100
            except Exception:
                pass

    if not data_dict:
        print("❌ 没有找到 History_Premium_*.csv 数据文件。")
        return

    df_all = pd.DataFrame(data_dict)
    df_all.dropna(inplace=True)
    df_all.sort_index(inplace=True)

    if df_all.empty:
        print("❌ 合并后无有效数据。")
        return

    print(">>> [2/4] 正在计算全量指标 (Q值/通道/价差/均线)...")

    # ---------------------------------------------------------
    # 3. 全局计算核心指标
    # ---------------------------------------------------------
    # 3.1 基础指标
    s_current_max = df_all.max(axis=1)
    s_current_min = df_all.min(axis=1)
    s_range = s_current_max - s_current_min
    s_range = s_range.replace(0, 1e-6)

    if TARGET_CODE in df_all.columns:
        s_Q = (df_all[TARGET_CODE] - s_current_min) / s_range
    else:
        s_Q = (df_all[ORDERED_CODES[0]] - s_current_min) / s_range

    s_A = s_Q.rolling(window='60min', min_periods=1).mean()

    # 3.2 动态通道
    df_all['Upper_Dynamic'] = s_current_min + s_range * (s_A + PARAM_B)
    df_all['Lower_Dynamic'] = s_current_min + s_range * (s_A - PARAM_B)
    df_all['Indicator_A'] = s_A

    # 3.3 计算价差 (500->612->650->655)
    # 差值1: 513500 - 159612
    if '513500.SH' in df_all and '159612.SZ' in df_all:
        df_all['Diff_1'] = df_all['513500.SH'] - df_all['159612.SZ']
    else:
        df_all['Diff_1'] = 0

    # 差值2: 159612 - 513650
    if '159612.SZ' in df_all and '513650.SH' in df_all:
        df_all['Diff_2'] = df_all['159612.SZ'] - df_all['513650.SH']
    else:
        df_all['Diff_2'] = 0

    # 差值3: 513650 - 159655
    if '513650.SH' in df_all and '159655.SZ' in df_all:
        df_all['Diff_3'] = df_all['513650.SH'] - df_all['159655.SZ']
    else:
        df_all['Diff_3'] = 0

    # 差值4: 全场最大 - 全场最小
    df_all['Diff_Max_Min'] = s_current_max - s_current_min

    # --- 核心新增：计算 5日均值滤波 (Rolling Mean) ---
    # 使用 '5D' (5 calendar days) 作为窗口，适合时间索引
    diff_columns = ['Diff_1', 'Diff_2', 'Diff_3', 'Diff_Max_Min']
    for col in diff_columns:
        # min_periods=1 保证数据刚开始也有值
        df_all[f'{col}_MA5'] = df_all[col].rolling('1D', min_periods=1).mean()

    print(f">>> [3/4] 开始分段生成报表 (按季度切分)...")

    # ---------------------------------------------------------
    # 4. 按季度切分并绘图
    # ---------------------------------------------------------
    try:
        grouper = df_all.groupby(pd.Grouper(freq='3ME'))
    except ValueError:
        grouper = df_all.groupby(pd.Grouper(freq='3M'))

    file_count = 0
    last_file_path = ""

    for date_key, df_chunk in grouper:
        if df_chunk.empty: continue

        start_str = df_chunk.index[0].strftime('%Y%m%d')
        end_str = df_chunk.index[-1].strftime('%Y%m%d')
        title_period = f"{start_str}-{end_str}"
        print(f"   -> 处理分段: {title_period}...")

        # =========================================================
        # 5. 构建分段无缝时间轴
        # =========================================================
        x_indices = np.arange(len(df_chunk))
        tick_vals = []
        tick_text = []
        last_date = None
        step = 30 if len(df_chunk) < 5000 else 60

        timestamps = df_chunk.index
        for i, ts in enumerate(timestamps):
            current_date = ts.date()
            if last_date is None or current_date != last_date:
                tick_vals.append(i)
                tick_text.append(ts.strftime('%m-%d %H:%M'))
                last_date = current_date
            elif i % step == 0:
                tick_vals.append(i)
                tick_text.append(ts.strftime('%H:%M'))

        hover_dates = [t.strftime('%Y-%m-%d %H:%M:%S') for t in timestamps]

        # =========================================================
        # 6. 绘图 (6 行子图)
        # =========================================================
        fig = make_subplots(
            rows=6, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.02,
            row_heights=[0.35, 0.13, 0.13, 0.13, 0.13, 0.13],
            subplot_titles=(
                f"溢价率动态通道 ({title_period})",
                "相对强度 (A值)",
                "差值1: 513500 - 159612",
                "差值2: 159612 - 513650",
                "差值3: 513650 - 159655",
                "全场最大价差 (Max - Min)"
            )
        )

        # --- Row 1: 溢价率 ---
        for code in ORDERED_CODES:
            if code not in df_chunk.columns: continue
            info = etf_map[code]
            fig.add_trace(go.Scatter(
                x=x_indices, y=df_chunk[code],
                mode='lines', name=info['name'],
                line=dict(color=info['color'], width=1.5 if code != TARGET_CODE else 2.5),
                customdata=hover_dates, hovertemplate='<b>%{customdata}</b><br>溢价: %{y:.2f}%'
            ), row=1, col=1)

        # 动态通道线
        lines_cfg = [
            {'col': 'Upper_Dynamic', 'color': 'red', 'dash': 'dot', 'name': '动态阻力'},
            {'col': 'Lower_Dynamic', 'color': 'salmon', 'dash': 'dash', 'name': '动态警戒'},
        ]
        for l in lines_cfg:
            fig.add_trace(go.Scatter(
                x=x_indices, y=df_chunk[l['col']],
                mode='lines', name=l['name'],
                line=dict(color=l['color'], width=1, dash=l['dash']),
                opacity=0.6, hoverinfo='skip'
            ), row=1, col=1)

        # --- Row 2: 指标 A ---
        fig.add_trace(go.Scatter(
            x=x_indices, y=df_chunk['Indicator_A'],
            mode='lines', name='指标 A', line=dict(color='#2ca02c', width=1.5),
            fill='tozeroy', fillcolor='rgba(44, 160, 44, 0.1)',
            customdata=hover_dates, hovertemplate='A值: %{y:.3f}'
        ), row=2, col=1)
        fig.add_hline(y=0.8, line_dash="dot", line_color="red", row=2, col=1)
        fig.add_hline(y=0.2, line_dash="dot", line_color="green", row=2, col=1)

        # --- Row 3-6: 价差分析 (带5日均线) ---
        diff_configs = [
            {'col': 'Diff_1', 'row': 3, 'color': '#9467bd', 'name': '500-612'},
            {'col': 'Diff_2', 'row': 4, 'color': '#8c564b', 'name': '612-650'},
            {'col': 'Diff_3', 'row': 5, 'color': '#e377c2', 'name': '650-655'},
            {'col': 'Diff_Max_Min', 'row': 6, 'color': '#d62728', 'name': '全场极差'}
        ]

        for cfg in diff_configs:
            # 1. 绘制差值面积图
            fig.add_trace(go.Scatter(
                x=x_indices, y=df_chunk[cfg['col']],
                mode='lines', name=cfg['name'],
                line=dict(color=cfg['color'], width=1.5),
                fill='tozeroy',
                fillcolor=f"rgba{tuple(int(cfg['color'].lstrip('#')[i:i + 2], 16) for i in (0, 2, 4)) + (0.1,)}",
                customdata=hover_dates, hovertemplate=f'{cfg["name"]}: %{{y:.2f}}%'
            ), row=cfg['row'], col=1)

            # 2. 绘制 5日均线 (虚线) - 新增
            ma_col = f"{cfg['col']}_MA5"
            fig.add_trace(go.Scatter(
                x=x_indices, y=df_chunk[ma_col],
                mode='lines', name=f"{cfg['name']} MA5",
                line=dict(color='black', width=1.2, dash='dash'),  # 黑色虚线
                opacity=0.7,
                hoverinfo='skip'  # 鼠标放上去不显示均线数值，避免遮挡
            ), row=cfg['row'], col=1)

            # 0轴参考线
            fig.add_hline(y=0, line_color="black", line_width=1, opacity=0.3, row=cfg['row'], col=1)

        # =========================================================
        # 7. 布局优化 (包含十字光标设置)
        # =========================================================
        fig.update_layout(
            height=1400,
            title_text=f"<b>标普ETF 价差矩阵分析 ({title_period})</b>",
            hovermode="x unified",
            template="plotly_white",
            legend=dict(orientation="h", y=1.005, x=0.5, xanchor='center'),
            dragmode='pan',
            margin=dict(t=80, b=50, l=50, r=50)
        )

        # --- 十字光标配置 ---
        fig.update_xaxes(
            tickmode='array', tickvals=tick_vals, ticktext=tick_text,
            showgrid=True, gridcolor='rgba(0,0,0,0.1)',
            showspikes=True, spikemode='across', spikesnap='cursor',
            showline=False, spikedash='dash', spikecolor='grey', spikethickness=1
        )

        fig.update_yaxes(
            showspikes=True, spikemode='across', spikesnap='cursor',
            showline=False, spikedash='dash', spikecolor='grey', spikethickness=1
        )

        # 单独设置标题和范围
        fig.update_yaxes(title_text="溢价(%)", row=1, col=1)
        fig.update_yaxes(title_text="A值", range=[-0.1, 1.1], row=2, col=1)
        fig.update_yaxes(title_text="差值(%)", row=3, col=1)
        fig.update_yaxes(title_text="差值(%)", row=4, col=1)
        fig.update_yaxes(title_text="差值(%)", row=5, col=1)
        fig.update_yaxes(title_text="最大价差", row=6, col=1)

        # 导出
        out_filename = f"{OUTPUT_DIR}/Spread_Matrix_{title_period}.html"
        fig.write_html(out_filename)
        last_file_path = os.path.abspath(out_filename)
        file_count += 1

    print(f"\n✅ 全部完成！共生成 {file_count} 个矩阵分析报表。")
    if last_file_path:
        print(f"👉 正在打开最新报表: {last_file_path}")
        webbrowser.open(f"file://{last_file_path}")


if __name__ == "__main__":
    plot_premium_analysis()