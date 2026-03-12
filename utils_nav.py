# -*- coding: utf-8 -*-
"""
utils_nav.py (全能版)
功能：
1. get_fund_nav: 获取单个基金的最新官方净值 (用于 run_realtime.py)
2. fetch_history_navs: 批量获取基金的历史净值 (用于 run_history.py)
"""
import requests
import re
import pandas as pd
import time
import random
import json


# ==========================================
# 🟢 功能 1: 获取单个最新净值 (实盘用)
# ==========================================
def get_fund_nav(fund_code):
    """
    从天天基金获取指定基金的最新净值 (T-1 或 T-2)
    返回: (净值日期字符串, 单位净值浮点数)
    """
    # 纯数字代码处理 (513500.SH -> 513500)
    clean_code = fund_code.split('.')[0]

    # 接口：返回 jsonp 格式
    url = f"http://fundgz.1234567.com.cn/js/{clean_code}.js"

    try:
        # 添加随机时间戳防止缓存
        timestamp = int(time.time() * 1000)
        full_url = f"{url}?rt={timestamp}"

        response = requests.get(full_url, timeout=3)
        text = response.text

        # 返回格式示例: jsonpgz({"fundcode":"513500","name":"...","dwjz":"1.2345","gsz":"...","jzrq":"2024-02-07",...});

        # 正则提取 dwjz (单位净值) 和 jzrq (净值日期)
        # 注意：不要取 gsz (估算值)，那是盘中不准的，我们要官方结算价
        pattern_val = r'"dwjz":"([^"]+)"'
        pattern_date = r'"jzrq":"([^"]+)"'

        val_match = re.search(pattern_val, text)
        date_match = re.search(pattern_date, text)

        if val_match and date_match:
            nav = float(val_match.group(1))
            nav_date = date_match.group(1)
            # print(f"   [NAV] {fund_code} 锚点获取成功: {nav} ({nav_date})")
            return nav_date, nav
        else:
            # 有时候新发基金或者停牌可能取不到
            print(f"⚠️ 解析 {fund_code} 净值失败，返回内容: {text[:50]}...")
            return None, None

    except Exception as e:
        print(f"❌ 连接天天基金失败 ({fund_code}): {e}")
        return None, None


# ==========================================
# 🟢 功能 2: 获取历史净值 (回测用)
# ==========================================
def get_fund_nav_history_single(fund_code, days=365):
    """(内部调用) 获取单个基金历史"""
    clean_code = fund_code.split('.')[0]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "http://fundf10.eastmoney.com/"
    }
    url = f"http://api.fund.eastmoney.com/f10/lsjz?fundCode={clean_code}&pageIndex=1&pageSize={days}&startDate=&endDate="

    try:
        response = requests.get(url, headers=headers, timeout=5)
        data = response.json()
        if data['Data']['LSJZList']:
            df = pd.DataFrame(data['Data']['LSJZList'])
            df['FSRQ'] = pd.to_datetime(df['FSRQ'])
            df['DWJZ'] = pd.to_numeric(df['DWJZ'], errors='coerce')
            df = df[['FSRQ', 'DWJZ']].rename(columns={'FSRQ': 'date', 'DWJZ': 'nav'})
            df.set_index('date', inplace=True)
            df.sort_index(inplace=True)
            return df
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def fetch_history_navs(codes_list, days=365):
    """
    批量获取 (供 run_history.py 调用)
    """
    res = {}
    print(f"📡 开始下载 {len(codes_list)} 只ETF的净值数据...")
    for code in codes_list:
        df = get_fund_nav_history_single(code, days)
        if not df.empty:
            res[code] = df
            print(f"   ✅ {code}: 获取到 {len(df)} 条净值")
        else:
            print(f"   ⚠️ {code}: 无数据")
        time.sleep(random.uniform(0.3, 1.0))
    return res


# ==========================================
# 自测代码
# ==========================================
if __name__ == "__main__":
    print(">>> 测试单点获取 (实盘模式):")
    d, n = get_fund_nav("513500.SH")
    print(f"日期: {d}, 净值: {n}")

    print("\n>>> 测试批量获取 (历史模式):")
    # history = fetch_history_navs(["513500.SH"], days=10)
    # print(history)