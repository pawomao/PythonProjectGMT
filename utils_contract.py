# -*- coding: utf-8 -*-
"""
utils_contract.py
功能：自动计算 ES/MES 的主力合约月份
"""
import datetime
import calendar


def get_es_expiry(target_date=None):
    """
    自动计算 ES (E-mini S&P 500) 的主力合约月份代码 (格式: YYYYMM)

    规则:
    1. ES 交割月为 3, 6, 9, 12 月。
    2. 交割日为当月第3个周五。
    3. 我们设定【切换日 (Rollover)】为交割日所在的周一。
       (注：实际上机构往往提前一周切换，但周一切换对散户监控来说足够安全)
    4. 如果今天 >= 切换日，就用下一个季度的合约；否则用当季合约。
    """
    if target_date is None:
        target_date = datetime.date.today()
    elif isinstance(target_date, str):
        target_date = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
    elif isinstance(target_date, datetime.datetime):
        target_date = target_date.date()

    year = target_date.year
    month = target_date.month

    # ES 的四个交割月
    expiry_months = [3, 6, 9, 12]

    # 1. 找到当前所处的"本季度"交割月
    # 例如：现在是4月，本季度交割月就是6月
    candidate_month = -1
    for m in expiry_months:
        if month <= m:
            candidate_month = m
            break

    # 如果是12月之后（虽然逻辑上不会发生，防个万一），归到明年3月
    if candidate_month == -1:
        return f"{year + 1}03"

    # 2. 计算候选月份的"切换日"
    # 获取该月日历 (每周一行)
    cal = calendar.monthcalendar(year, candidate_month)
    # 提取所有周五 (索引4)，排除0
    fridays = [week[4] for week in cal if week[4] != 0]

    # 第3个周五是交割日
    if len(fridays) < 3:
        # 极罕见情况，防错
        third_friday_day = fridays[-1]
    else:
        third_friday_day = fridays[2]

    third_friday_date = datetime.date(year, candidate_month, third_friday_day)

    # 设定切换日 = 交割日 - 4天 (即同一个礼拜的周一)
    rollover_date = third_friday_date - datetime.timedelta(days=4)

    # 3. 比较：今天是否已经过了切换日？
    if target_date >= rollover_date:
        # 已过切换日，换到下个季度
        idx = expiry_months.index(candidate_month)
        if idx == 3:  # 如果当前是12月，换到明年3月
            return f"{year + 1}03"
        else:
            next_month = expiry_months[idx + 1]
            return f"{year}{next_month:02d}"
    else:
        # 还没到，继续用当季合约
        return f"{year}{candidate_month:02d}"


if __name__ == "__main__":
    print(f"📅 今天 ({datetime.date.today()}) 的主力合约是: {get_es_expiry()}")