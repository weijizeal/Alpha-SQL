# from datetime import date, datetime, datetime.timedelta
# 新增类时需要修改的地方包括
# 当前文件：
# single_date_views,
# generate_sql_from_orm,
# new_stock_selector_rag
# import,
# safe_locals,

## __tablename__后跟:str的为大模型暂不支持表，包括：MarketHistory，BoardFinancialSingleInfo,BoardFinancialSingleProfitView,StockFinancialPropertyBalanceSingleView,AStockFinancialSingle
## 从NetbuyZhuLevel0以后的表都还没重构，只是粘贴了过来
## wzholdingpools重构完成
# 遗留问题：
# 季度表需要参考current表和history表的处理方式加一个类似current的参数，否则无法区分单季度表和财报表
# 席位龙虎榜近三日上榜次数、日线入选get函数看起来需要和外部secucode联表，与现有组合架构冲突，尚未有好的解决方案
# 首次入池时间和最后出池时间的datafields处理，它数据库里存的是时分秒的一个组合int
# query_field_in_range函数自定义字段进行大小比较时的正确性尚未测试

# 特殊修改记录
# AiForecastZdLevel20的type属性修改为zdt_type
# 为避免混淆且满足诊股等指标需求，通过删除python doc来确保其不会进入rag，但是可以调用,现包括[get_latest_hot_money_name,get_latest_reason]
# 席位龙虎榜里有两个上榜游资，目前太容易冲突，暂时先去掉了第一个
# AStockMarketCur里的tq字段python名重命名为tis（total issued shares），避免和wzholdingpool里的tq冲突
# AStockMarketCur的trade_seat_times_1month改成trade_seat_times_3d，3天才是正确语义，数据表字段名就不改了
# AStockMarketCur里的三把锁、波段决策、长线决策每次重新生成get函数时需要特殊处理，解决和高级字段重名冲突的问题。同时将选股模块所有对n日多空、敢死队等资金统一使用计算方式获取，删除所有通过字段直接获取的get函数，为避免与7*5模块冲突，原始字段依然保留，但不提供get函数
# 注释了20日涨跌幅和240日涨幅get函数
# 二级市场180天相关指标统一调整为20日涨跌幅同样逻辑（由于不支持任意日期查询，只是将日期统一调整为0d，0d）
# 减:库存库去掉减:
# f10_profit_view的中文注释去掉了很多冗余字段，如其中：、减：、加：
# allotment_process字段的alias从方案进度更名为配股情况方案进度，dividend_schedule字段的alias从方案进度更名为分红情况方案进度
# 每个_get函数的返回cte现在统一添加unique_id,避免在多次调用时出现重名（多次调用的需求来源于query_columns和query_field_in_range同时存在时，既要比较字段又要列出字段，字段在当前设计下如果是一个通过_get函数返回的cte就会重名，因此统一提供uuid）
# 去掉席位总买/总卖的(去重)

import datetime
import uuid
import json
import re
from typing import Optional, List, Any, Union
from openai import AsyncOpenAI
import asyncio
from sqlmodel import (
    SQLModel,
    Field,
    select,
    create_engine,
    col,
    and_,
    true,
    func,
    intersect,
    union,
)
from sqlalchemy import (
    BinaryExpression,
    Column,
    String,
    Float,
    Boolean,
    Date,
    Integer,
    case,
    func,
    literal,
    text,
    asc,
    desc,
    union_all,
    or_,
    Numeric,
    cast,
    case,
    null,
)
from sqlalchemy.orm import aliased
import ast
from dateutil.relativedelta import relativedelta
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
name2concept_file_path = os.path.join(BASE_DIR, "stock_detail_mapper", "name2concept.json")
with open(name2concept_file_path, "r", encoding="utf-8") as f:
    name2concept = json.load(f)
name2industry_file_path = os.path.join(BASE_DIR, "stock_detail_mapper", "name2trade.json")
with open(name2industry_file_path, "r", encoding="utf-8") as f:
    name2trade = json.load(f)
name2region_file_path = os.path.join(BASE_DIR, "stock_detail_mapper", "name2region.json")
with open(name2region_file_path, "r", encoding="utf-8") as f:
    name2region = json.load(f)

BENCHMARK_CODE = "SHHQ000001"


def get_days_ago_date(days_ago):
    """
    获取第N个交易日前的日期（如days_ago=3表示3个交易日前）
    等效于SQL: LIMIT (days_ago-1), 1
    """
    return (
        select(AStockMarket.date)
        .where(AStockMarket.secucode == BENCHMARK_CODE)
        .order_by(AStockMarket.date.desc())
        .offset(days_ago - 1)  # 调整为days_ago-1
        .limit(1)
        .scalar_subquery()
    )


def get_date(date_str: str, is_start: bool = False, lookback_days: int = 0):
    """
    :功能:
        根据日期字符串获取日期对象，支持绝对日期格式（yyyy-mm-dd）和相对日期格式（nX格式，n表示数值，X表示单位(w,m,y,d)）
        新增支持财报季度格式 (q)，例如 '2025y1q', '-1q'。
    :参数:
        date_str(str): 起始时间点
        时间点简写,单位:年y、月m、周w、日d、时h、分mm、秒s、交易日t、周几wd（周一  1wd）.格式如'nm','nd', 'ny','nw',加正负号表示从当前开始的时间偏移量,否则就是准确率时间点 n=0时,0d表示今天,0m表示本月1号,0y表示今年1月一日开始,0w表示本周一,1wd表示周一, n加-号时代表过去时间,如n=-1, -1d表示昨天, -1m表示近一个月, -1y表示近一年, -1w表示近一周, 当n加+号时 代表未来时间,+1d表示明天,+1m表示下个月,+1y表示明年,+1w表示下周,不加时表示当前时间点.
        举例:近20日/20天内：('-20d', '0d')  半年内： ('-6m', '0d') 上半年：('1m', '6m') 连续三天: ('-3d', '0d') 5月19日至5月23日: ('5m19d', '5m23d') 下周: ('+1wd', '+5wd')
    :量化解释:
        根据时间范围简写,计算出开始日期和结束日期,日期范围条件股票
        格式如'nm','nd', 'ny', 'nw'
        n > 0, 1m 表示下个月, 1d 表示明天, 1y 表示明年, 1w 表示下周
        n = 0, 0m 表示本月, 0d 表示今天, 0y 表示今年, 0w 表示本周
        n < 0, -1m 表示上个月, -1d 表示昨天, -1y 表示去年, -1w 表示上周
    """
    if "q" in date_str:
        return get_report_date(date_str, is_start)
    if "m" in date_str and "d" in date_str:
        date_str = date_str.replace("-", "")
    if re.fullmatch(r"\d{8}", date_str):  # ✅ 匹配 yyyymmdd 格式
        year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
        date_str = f"{year}y{int(month)}m{int(day)}d"
    if not isinstance(date_str, str) or len(date_str) < 2:
        raise ValueError("日期参数格式错误，应为nX格式（如1d、-2m等）")

    if date_str == "近期" and is_start:
        date_str = "-10d"
    if date_str == "近期" and not is_start:
        date_str = "0d"

    def _apply_lookback(date_obj: datetime.date, is_start: bool, lookback_days: int) -> datetime.date:
        """应用回溯天数（内部辅助函数）"""
        if is_start and lookback_days > 0:
            return (
                select(AStockMarket.date)
                .where(
                    AStockMarket.secucode == BENCHMARK_CODE,
                    AStockMarket.date < date_obj,
                )
                .order_by(AStockMarket.date.desc())
                .offset(lookback_days)
                .limit(1)
                .scalar_subquery()
            )
        else:
            return date_obj

    def _parse_composite_date(date_str: str, is_start: bool, lookback_days: int) -> datetime.date:
        """解析复合日期格式（2025y6m15d 或 6m15d）"""
        # 解析年份（如果有）
        year_match = re.match(r"^(\d{4})y", date_str)
        if year_match:
            year = int(year_match.group(1))
            remaining_str = date_str[5:]  # 去掉年份部分
        else:
            year = datetime.datetime.today().year
            remaining_str = date_str

        # 解析月份和日
        month_day_match = re.match(r"^(\d{1,2})m(\d{1,2})d$", remaining_str)
        if not month_day_match:
            raise ValueError("复合日期格式错误，应为2025y6m15d或6m15d格式")

        month = int(month_day_match.group(1))
        day = int(month_day_match.group(2))

        # 创建日期对象
        try:
            result_date = datetime.date(year, month, day)
        except ValueError as e:
            raise ValueError(f"无效的日期: {e}")

        # 应用回溯天数
        return _apply_lookback(result_date, is_start, lookback_days)

    # 处理绝对日期格式（yyyy-mm-dd）
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        result_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        return _apply_lookback(result_date, is_start, lookback_days)

    # 处理复合日期格式（2025y6m15d 或 6m15d）
    if re.match(r"^(\d{4}y)?\d{1,2}m\d{1,2}d$", date_str):
        return _parse_composite_date(date_str, is_start, lookback_days)

    # 解析数值部分和单位部分
    try:
        value = int(date_str[:-1])
        unit = date_str[-1].lower()
    except (ValueError, IndexError):
        raise ValueError("日期参数格式错误，应为nX格式（如1d、-2m等）")

    today = datetime.datetime.today().date()

    # 处理特殊周期（本周/本月/本年）
    if value == 0:
        if unit == "d":  # 0d = 今天
            back_days = 1
            back_days += lookback_days if is_start and lookback_days > 0 else 0
            return get_days_ago_date(back_days)  # 返回SQL表达式
        elif unit == "w":  # 0w = 本周
            if is_start:
                result_date = today - datetime.timedelta(days=today.weekday())  # 本周一
            else:
                result_date = today + datetime.timedelta(days=(6 - today.weekday()))  # 本周日
        elif unit == "m":  # 0m = 本月
            if is_start:
                result_date = today.replace(day=1)  # 本月第一天
            else:
                next_month = today.replace(day=28) + datetime.timedelta(days=4)
                result_date = next_month.replace(day=1) - datetime.timedelta(days=1)  # 本月最后一天
        elif unit == "y":  # 0y = 今年
            if is_start:
                result_date = today.replace(month=1, day=1)  # 年初
            else:
                result_date = today.replace(month=12, day=31)  # 年末
        else:
            raise ValueError(f"无效的时间单位: {unit}")

        return _apply_lookback(result_date, is_start, lookback_days)

    # 处理相对日期（非d单位）
    if unit != "d":
        if unit == "w":  # 周
            base_date = today + datetime.timedelta(weeks=value)
            if is_start:
                result_date = base_date - datetime.timedelta(days=base_date.weekday())  # 当周周一
            else:
                result_date = base_date + datetime.timedelta(days=(6 - base_date.weekday()))  # 当周周日
        elif unit == "m":  # 月
            result_date = today + relativedelta(months=value)
            # 上个月和近1个月语义不一致
            # base_date = today + relativedelta(months=value)
            # if is_start:
            #     result_date = base_date.replace(day=1)  # 当月第一天
            # else:
            #     next_month = base_date.replace(day=28) + datetime.timedelta(days=4)
            #     result_date = next_month.replace(day=1) - datetime.timedelta(days=1)  # 当月最后一天
        elif unit == "y":  # 年
            result_date = today + relativedelta(years=value)
            # 去年和近1年语义不一致
            # base_date = today + relativedelta(years=value)
            # if is_start:
            #     result_date = base_date.replace(month=1, day=1)  # 年初
            # else:
            #     result_date = base_date.replace(month=12, day=31)  # 年末

        else:
            raise ValueError(f"无效的时间单位: {unit}")

        return _apply_lookback(result_date, is_start, lookback_days)

    # 单独处理d单位
    if value > 0:
        # 未来日期直接计算自然日
        result_date = today + datetime.timedelta(days=value)
        return _apply_lookback(result_date, is_start, lookback_days)
    else:
        # 过去日期使用交易日查询
        back_days = abs(value) + 1
        back_days += lookback_days if is_start and lookback_days > 0 else 0

        return get_days_ago_date(back_days)


def get_report_ago_date(days_ago):
    """
    获取第N个报告期前的日期（如days_ago=3表示3个报告期前）
    等效于SQL: LIMIT (days_ago-1), 1
    """
    return (
        select(AStockFinancial.date)
        .group_by(AStockFinancial.date)
        .order_by(AStockFinancial.date.desc())
        .offset(days_ago - 1)
        .limit(1)
        .scalar_subquery()
    )


def get_report_date(date_str: str, is_start: bool = False):
    """
    根据日期字符串获取财报报告期日期对象。
    支持格式:
    - 绝对年份季度: '2025y1q' (2025年第一季度)
    - 相对年份季度: '-1y4q' (去年第四季度)
    - 相对季度: '0q' (最新季度), '-1q' (上一个季度), '-2q' (上上个季度)
    """
    date_str = date_str.strip()  # 增加strip()以提高健壮性
    today = datetime.datetime.today().date()
    QUARTER_ENDS = [(3, 31), (6, 30), (9, 30), (12, 31)]

    def get_quarter_end_date(year: int, quarter: int) -> datetime.date:
        """给定年份和季度，返回该季度末日期"""
        if not 1 <= quarter <= 4:
            raise ValueError("季度必须在1到4之间")
        month, day = QUARTER_ENDS[quarter - 1]
        return datetime.date(year, month, day)

    # 模式1：匹配绝对年份+季度, e.g., '2024y1q'
    match_abs_year = re.match(r"^(\d{4})y([1-4])q$", date_str)
    if match_abs_year:
        year = int(match_abs_year.group(1))
        quarter = int(match_abs_year.group(2))
        result_date = get_quarter_end_date(year, quarter)
        if result_date > today:
            return get_report_ago_date(1)
        return result_date

    # 模式2：匹配相对年份+季度, e.g., '-1y2q'
    match_rel_year = re.match(r"^([+-]\d+)y([1-4])q$", date_str)
    if match_rel_year:
        year_offset = int(match_rel_year.group(1))
        quarter = int(match_rel_year.group(2))
        target_year = today.year + year_offset
        result_date = get_quarter_end_date(target_year, quarter)
        if result_date > today:
            return get_report_ago_date(1)
        return result_date

    # 模式3：匹配仅相对季度, e.g., '-1q', '0q'
    match_quarter_only = re.match(r"^([+-]?\d+)q$", date_str)
    if match_quarter_only:
        offset_str = match_quarter_only.group(1)
        offset = int(offset_str)

        if not offset_str.startswith(("+", "-")) and 1 <= offset <= 4:
            # 直接拼接成当前年份形式，如 "2025y1q"
            year = today.year
            new_date_str = f"{year}y{offset}q"
            return get_report_date(new_date_str, is_start=is_start)

        if offset <= 0:
            report_periods_ago = abs(offset) + 1
            return get_report_ago_date(report_periods_ago)
        else:
            # 未来季度 (+1q, etc.)
            month = today.month
            current_quarter = (month - 1) // 3 + 1
            total_quarters = current_quarter + offset

            new_year = today.year + (total_quarters - 1) // 4
            new_quarter = ((total_quarters - 1) % 4) + 1

            result_date = get_quarter_end_date(new_year, new_quarter)

            if result_date > today:
                return get_report_ago_date(1)
            return result_date

    # 如果所有模式都未匹配，则抛出错误
    raise ValueError(f"无效的报告期日期格式: {date_str}")


# ──────────── A股行情 ────────────
class AStockMarket(SQLModel, table=True):
    __tablename__ = "hq_history_agg_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    close: Optional[float] = Field(default=None, sa_column=Column("close", Float), alias="收盘价(股价)")
    open: Optional[float] = Field(default=None, sa_column=Column("open", Float), alias="开盘价")
    low: Optional[float] = Field(default=None, sa_column=Column("low", Float), alias="最低价")
    high: Optional[float] = Field(default=None, sa_column=Column("high", Float), alias="最高价")
    amount: Optional[float] = Field(default=None, sa_column=Column("amount", Float), alias="成交额")
    volume: Optional[int] = Field(default=None, sa_column=Column("volume", Integer), alias="成交量")
    marketvalue: Optional[float] = Field(default=0.0, description="总市值")
    zt: Optional[bool] = Field(default=None, sa_column=Column("zt", Boolean), alias="涨停")
    dt: Optional[bool] = Field(default=None, sa_column=Column("dt", Boolean), alias="跌停")
    zhu: Optional[float] = Field(default=None, sa_column=Column("zhu", Float), alias="主力资金占比")
    toratio: Optional[float] = Field(default=None, sa_column=Column("toratio", Float), alias="换手率")
    zdf: Optional[float] = Field(default=0.0, description="涨跌幅")
    pe_dynamic: Optional[float] = Field(default=0.0, description="市盈率（动）")
    pe_static: Optional[float] = Field(default=0.0, description="市盈率")
    pe_ttm: Optional[float] = Field(default=0.0, description="市盈率（TTM）")
    ma_dtpl: Optional[bool] = Field(default=None, sa_column=Column("ma_dtpl", Boolean), alias="MA均线-多头排列")
    ma_ktpl: Optional[bool] = Field(default=None, sa_column=Column("ma_ktpl", Boolean), alias="MA均线-空头排列")
    ma_jincha: Optional[bool] = Field(default=None, sa_column=Column("ma_jincha", Boolean), alias="MA均线-金叉")
    ma_sicha: Optional[bool] = Field(default=None, sa_column=Column("ma_sicha", Boolean), alias="MA均线-死叉")


# ──────────── A股市场数据 ────────────
class AStockMarketCur(SQLModel, table=True):
    __tablename__ = "hq_current_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    zhu_1: float = Field(default=0.0, description="1日主力资金流入")
    zhu_3: float = Field(default=0.0, description="3日主力资金流入")
    zhu_13: float = Field(default=0.0, description="13日主力资金流入")
    duo_1: float = Field(default=0.0, description="1日多空资金流入")
    duo_3: float = Field(default=0.0, description="3日多空资金流入")
    gan_1: float = Field(default=0.0, description="1日敢死队资金流入")
    gan_3: float = Field(default=0.0, description="3日敢死队资金流入")
    pe_ratio_static: Optional[float] = Field(default=None, sa_column=Column("pe_ratio_static", Float), alias="市盈率")
    three_lock: str = Field(default="", description="三把锁")
    long_policy: Optional[str] = Field(default=None, description="长线决策")
    bd_policy: Optional[str] = Field(default=None, description="波段决策")
    ma: Optional[str] = Field(default=None, sa_column=Column("ma", String), alias="MA均线")
    chip: Optional[str] = Field(default=None, sa_column=Column("chip", String), alias="筹码分布")
    r_price: Optional[float] = Field(default=None, sa_column=Column("r_price", Float), alias="压力位价格")
    r_rate: Optional[float] = Field(default=None, sa_column=Column("r_rate", Float), alias="压力位幅度")
    s_rate: Optional[str] = Field(default=None, sa_column=Column("s_rate", String), alias="支撑位幅度")
    s_price: Optional[str] = Field(default=None, sa_column=Column("s_price", String), alias="支撑位价格")
    hot_value: float = Field(default=0, description="股票热度值")
    hot_industry_trade: Optional[str] = Field(
        default=None, sa_column=Column("hot_industry_trade", String), alias="热门行业"
    )
    hot_industry_concept: Optional[str] = Field(
        default=None, sa_column=Column("hot_industry_concept", String), alias="热门概念"
    )
    hot_word: Optional[str] = Field(default=None, description="热词")
    open: Optional[float] = Field(default=None, sa_column=Column("open", Float), alias="开盘价")
    low: Optional[float] = Field(default=None, sa_column=Column("low", Float), alias="最低价")
    high: Optional[float] = Field(default=None, sa_column=Column("high", Float), alias="最高价")
    close: Optional[float] = Field(default=None, sa_column=Column("close", Float), alias="收盘价(股价)")
    zdf: Optional[float] = Field(default=0.0, description="涨跌幅")
    volume: Optional[int] = Field(default=None, sa_column=Column("volume", Integer), alias="成交量")
    amount: Optional[int] = Field(default=None, sa_column=Column("amount", Integer), alias="成交额")
    toratio: Optional[float] = Field(default=None, sa_column=Column("toratio", Float), alias="换手率")
    amplitude: Optional[float] = Field(default=None, sa_column=Column("amplitude", Float), alias="振幅")
    macd_jincha: bool = Field(sa_column=Column("macd_jincha", Boolean), alias="MACD-金叉")
    macd_sicha: bool = Field(sa_column=Column("macd_sicha", Boolean), alias="MACD-死叉")
    macd_dingbeili: bool = Field(sa_column=Column("macd_dingbeili", Boolean), alias="MACD-顶背离")
    kdj_dibeili: bool = Field(sa_column=Column("kdj_dibeili", Boolean), alias="KDJ-底背离")
    kdj_dingbeili: bool = Field(sa_column=Column("kdj_dingbeili", Boolean), alias="KDJ-顶背离")
    kdj_jincha: bool = Field(sa_column=Column("kdj_jincha", Boolean), alias="KDJ-金叉")
    kdj_sicha: bool = Field(sa_column=Column("kdj_sicha", Boolean), alias="KDJ-死叉")
    rsi6_80: bool = Field(sa_column=Column("rsi6_80", Boolean), alias="RSI6上穿80")
    rsi6_20: bool = Field(sa_column=Column("rsi6_20", Boolean), alias="RSI6下穿20")
    rsi12_80: bool = Field(sa_column=Column("rsi12_80", Boolean), alias="RSI12上穿80")
    rsi12_20: bool = Field(sa_column=Column("rsi12_20", Boolean), alias="RSI12下穿20")
    cci_100: bool = Field(sa_column=Column("cci_100", Boolean), alias="CCI上穿100")
    cci_ne100: bool = Field(sa_column=Column("cci_ne100", Boolean), alias="CCI下穿100")
    cci_over_bought: bool = Field(sa_column=Column("cci_over_bought", Boolean), alias="CCI超买")
    cci_over_sold: bool = Field(sa_column=Column("cci_over_sold", Boolean), alias="CCI超卖")
    wr_80: bool = Field(sa_column=Column("wr_80", Boolean), alias="WR上穿80")
    wr_20: bool = Field(sa_column=Column("wr_20", Boolean), alias="WR下穿20")
    wr_over_bought: bool = Field(sa_column=Column("wr_over_bought", Boolean), alias="WR超买")
    wr_over_sold: bool = Field(sa_column=Column("wr_over_sold", Boolean), alias="WR超卖")
    ma_dtpl: bool = Field(sa_column=Column("ma_dtpl", Boolean), alias="MA均线-多头排列")
    ma_ktpl: bool = Field(sa_column=Column("ma_ktpl", Boolean), alias="MA均线-空头排列")
    ma_jincha: bool = Field(sa_column=Column("ma_jincha", Boolean), alias="MA均线-金叉")
    ma_sicha: bool = Field(sa_column=Column("ma_sicha", Boolean), alias="MA均线-死叉")
    cyc_dtpl: bool = Field(sa_column=Column("cyc_dtpl", Boolean), alias="成本均线CYC-多头排列")
    cyc_ktpl: bool = Field(sa_column=Column("cyc_ktpl", Boolean), alias="成本均线CYC-空头排列")
    cyc_jincha: bool = Field(sa_column=Column("cyc_jincha", Boolean), alias="成本均线CYC-金叉")
    cyc_sicha: bool = Field(sa_column=Column("cyc_sicha", Boolean), alias="成本均线CYC-死叉")
    cd_90: float = Field(sa_column=Column("cd_90", Float), alias="筹码90％集中度")
    cd_70: float = Field(sa_column=Column("cd_70", Float), alias="筹码70％集中度")
    kline_cxyx: bool = Field(sa_column=Column("kline_cxyx", Boolean), alias="K线形态-长下影线")
    kline_lyt: bool = Field(sa_column=Column("kline_lyt", Boolean), alias="K线形态-老鸭头")
    kline_jjyyt: bool = Field(sa_column=Column("kline_jjyyt", Boolean), alias="K线形态-九九艳阳天")
    kline_sstd: bool = Field(sa_column=Column("kline_sstd", Boolean), alias="K线形态-上升通道")
    kline_cyzp: bool = Field(sa_column=Column("kline_cyzp", Boolean), alias="K线形态-长阳重炮")
    kline_jztd: bool = Field(sa_column=Column("kline_jztd", Boolean), alias="K线形态-金针探底")
    kline_hsb: bool = Field(sa_column=Column("kline_hsb", Boolean), alias="K线形态-红三兵")
    kline_wygd: bool = Field(sa_column=Column("kline_wygd", Boolean), alias="K线形态-乌云盖顶")
    kline_zczx: bool = Field(sa_column=Column("kline_zczx", Boolean), alias="K线形态-早晨之星")
    kline_xrds: bool = Field(sa_column=Column("kline_xrds", Boolean), alias="K线形态-旭日东升")
    kline_vxfz: bool = Field(sa_column=Column("kline_vxfz", Boolean), alias="K线形态-v型反转")
    kline_mrj: bool = Field(sa_column=Column("kline_mrj", Boolean), alias="K线形态-美人肩")
    kline_zthmq: bool = Field(sa_column=Column("kline_zthmq", Boolean), alias="K线形态-涨停回马枪")
    kline_dfp: bool = Field(sa_column=Column("kline_dfp", Boolean), alias="K线形态-多方炮")
    kline_yycsx: bool = Field(sa_column=Column("kline_yycsx", Boolean), alias="K线形态-一阳穿三线")
    opinions_month3: int = Field(sa_column=Column("opinions_month3", Integer), alias="近3月机构调研次数")
    organ_grade_month3_buy: bool = Field(
        sa_column=Column("organ_grade_month3_buy", Boolean), alias="近3月机构评级-买入"
    )
    organ_grade_month3_overweight: bool = Field(
        sa_column=Column("organ_grade_month3_overweight", Boolean), alias="近3月机构评级-增持"
    )
    organ_grade_month3_neutral: bool = Field(
        sa_column=Column("organ_grade_month3_neutral", Boolean), alias="近3月机构评级-中性"
    )
    organ_grade_month3_reduction: bool = Field(
        sa_column=Column("organ_grade_month3_reduction", Boolean), alias="近3月机构评级-减持"
    )
    organ_grade_month3_sell: bool = Field(
        sa_column=Column("organ_grade_month3_sell", Boolean), alias="近3月机构评级-卖出"
    )
    violation_month3: int = Field(sa_column=Column("violation_month3", Integer), alias="近3月违规处理次数")
    exchange_record_month3_overweight: bool = Field(
        sa_column=Column("exchange_record_month3_overweight", Boolean), alias="近3月高管增减持-增持"
    )
    exchange_record_month3_reduction: bool = Field(
        sa_column=Column("exchange_record_month3_reduction", Boolean), alias="近3月高管增减持-减持"
    )
    sales_restrictions_lifted_future: str = Field(
        sa_column=Column("sales_restrictions_lifted_future", String), alias="限售解禁-未来日期"
    )
    sales_restrictions_lifted_over: str = Field(
        sa_column=Column("sales_restrictions_lifted_over", String), alias="限售解禁-过去日期"
    )
    private_placement: str = Field(sa_column=Column("private_placement", String), alias="定向增发")
    assets_reorganization: str = Field(sa_column=Column("assets_reorganization", String), alias="资产重组")
    # 0：无 1：盈利大增 2：盈利略增 3：盈利大减 4：盈利略降 5：扭亏为盈 6:由盈转亏 7:亏损增加 8:亏损减少
    performance_forecast: int = Field(sa_column=Column("performance_forecast", Integer), alias="业绩预告")
    organ_hold_total: int = Field(sa_column=Column("organ_hold_total", Integer), alias="机构持股家数合计")
    fund_hold_num: int = Field(sa_column=Column("fund_hold_num", Integer), alias="基金持股家数")
    qs_hold_num: int = Field(sa_column=Column("qs_hold_num", Integer), alias="券商持股家数")
    qfll_hold_num: int = Field(sa_column=Column("qfll_hold_num", Integer), alias="QFII持股家数")
    insure_hold_num: int = Field(sa_column=Column("insure_hold_num", Integer), alias="保险持股家数")
    social_security_hold_num: int = Field(sa_column=Column("social_security_hold_num", Integer), alias="社保持股家数")
    trust_hold_num: int = Field(sa_column=Column("trust_hold_num", Integer), alias="信托持股家数")
    organ_hold_ratio_total: float = Field(sa_column=Column("organ_hold_ratio_total", Float), alias="机构持股比例合计")
    fund_hold_ratio: float = Field(sa_column=Column("fund_hold_ratio", Float), alias="基金持股比例")
    qs_hold_ratio: float = Field(sa_column=Column("qs_hold_ratio", Float), alias="券商持股比例")
    qfll_hold_ratio: float = Field(sa_column=Column("qfll_hold_ratio", Float), alias="QFII持股比例")
    insure_hold_ratio: float = Field(sa_column=Column("insure_hold_ratio", Float), alias="保险持股比例")
    social_security_hold_ratio: float = Field(
        sa_column=Column("social_security_hold_ratio", Float), alias="社保持股比例"
    )
    trust_hold_ratio: float = Field(sa_column=Column("trust_hold_ratio", Float), alias="信托持股比例")
    sb_hold_2quarter: str = Field(sa_column=Column("sb_hold_2quarter", String), alias="社保增持股票")
    trade_seat_times_3d: int = Field(sa_column=Column("trade_seat_times_1month", Integer), alias="最近3天上龙虎榜次数")
    zt: Optional[bool] = Field(default=None, sa_column=Column("zt", Boolean), alias="涨停")
    dt: Optional[bool] = Field(default=None, sa_column=Column("dt", Boolean), alias="跌停")
    rise: Optional[float] = Field(default=None, sa_column=Column("rise", Float), alias="涨速")
    all_time_high: Optional[bool] = Field(
        default=None, sa_column=Column("all_time_high", Boolean), alias="今日创历史新高"
    )
    all_time_low: Optional[bool] = Field(
        default=None, sa_column=Column("all_time_low", Boolean), alias="今日创历史新低"
    )
    period_high: Optional[int] = Field(
        default=None, sa_column=Column("period_high", Integer), alias="今日创阶段新高，多少日新高"
    )
    period_low: Optional[int] = Field(
        default=None, sa_column=Column("period_low", Integer), alias="今日创阶段新低，多少日新低"
    )
    near_all_time_high: Optional[int] = Field(
        default=None, sa_column=Column("near_all_time_high", Integer), alias="近期创历史新高，近几日创历史新高"
    )
    near_all_time_low: Optional[int] = Field(
        default=None, sa_column=Column("near_all_time_low", Integer), alias="近期创历史新低，近几日创历史新低"
    )
    qrr: Optional[float] = Field(default=None, sa_column=Column("qrr", Float), alias="量比")
    chg_day20: Optional[float] = Field(default=0.0, description="20日涨跌幅")
    zf_month: Optional[float] = Field(default=None, sa_column=Column("zf_month", Float), alias="本月涨跌幅")
    pe_ratio: Optional[float] = Field(default=None, sa_column=Column("pe_ratio", Float), alias="市盈率（动）")
    pe_ratio_ttm: Optional[float] = Field(default=None, sa_column=Column("pe_ratio_ttm", Float), alias="市盈率（TTM）")
    pb_ratio: Optional[float] = Field(default=None, sa_column=Column("pb_ratio", Float), alias="市净率")
    market_cap: Optional[int] = Field(default=0, description="总市值")
    market_value: Optional[int] = Field(default=None, sa_column=Column("market_value", Integer), alias="流通市值")
    tis: Optional[int] = Field(default=None, sa_column=Column("tq", Integer), alias="总股本")
    cir_equity: Optional[int] = Field(default=None, sa_column=Column("cir_equity", Integer), alias="流通股本")
    chg_day240: Optional[float] = Field(default=0.0, description="240日涨跌幅")
    lz_day_count: Optional[int] = Field(default=None, sa_column=Column("lz_day_count", Integer), alias="连涨天数")
    zhu: Optional[float] = Field(default=None, sa_column=Column("zhu", Float), alias="主力资金占比")
    dividend_yield: float = Field(sa_column=Column("dividend_yield", Float), alias="股息率")


# ──────────── A股基本情况 ────────────
class AStockBasic(SQLModel, table=True):
    __tablename__ = "hq_basic_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    secuname: Optional[str] = Field(default=None, sa_column=Column("secuname", String), alias="股票名称")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="数据日期")
    type: Optional[str] = Field(default=None, sa_column=Column("type", String), alias="所属大分类")
    market: Optional[str] = Field(default=None, sa_column=Column("market", String), alias="所属市场")
    trade: Optional[str] = Field(default=None, sa_column=Column("trade", String), alias="所属行业")
    concept: Optional[str] = Field(default=None, sa_column=Column("concept", String), alias="所属概念")
    area: Optional[str] = Field(default=None, sa_column=Column("area", String), alias="所属地域")
    is_hszb: Optional[bool] = Field(default=None, sa_column=Column("is_hszb", Boolean), alias="沪深主板")
    is_kcb: Optional[bool] = Field(default=None, sa_column=Column("is_kcb", Boolean), alias="科创板")
    is_cyb: Optional[bool] = Field(default=None, sa_column=Column("is_cyb", Boolean), alias="创业板")
    is_tp: Optional[bool] = Field(default=None, sa_column=Column("is_tp", Boolean), alias="停牌")
    is_ts: Optional[bool] = Field(default=None, sa_column=Column("is_ts", Boolean), alias="退市")
    is_xingu: Optional[bool] = Field(default=None, sa_column=Column("is_xingu", Boolean), alias="新股")
    is_cixingu: Optional[bool] = Field(default=None, sa_column=Column("is_cixingu", Boolean), alias="次新股")
    is_cwfx: Optional[bool] = Field(default=None, sa_column=Column("is_cwfx", Boolean), alias="财务风险")
    is_qzts: Optional[bool] = Field(default=None, sa_column=Column("is_qzts", Boolean), alias="潜在退市")
    is_tsfx: Optional[bool] = Field(default=None, sa_column=Column("is_tsfx", Boolean), alias="退市风险")
    detail_company_name: Optional[str] = Field(
        default=None, sa_column=Column("detail_company_name", String), alias="公司名称"
    )
    detail_english_name: Optional[str] = Field(
        default=None, sa_column=Column("detail_english_name", String), alias="英文名"
    )
    detail_before_name: Optional[str] = Field(
        default=None, sa_column=Column("detail_before_name", String), alias="曾用名"
    )
    detail_region: Optional[str] = Field(default=None, sa_column=Column("detail_region", String), alias="所属地域")
    detail_industry: Optional[str] = Field(
        default=None, sa_column=Column("detail_industry", String), alias="所属证监会行业"
    )
    detail_board_name: Optional[str] = Field(
        default=None, sa_column=Column("detail_board_name", String), alias="所属指南针行业名称"
    )
    detail_website: Optional[str] = Field(default=None, sa_column=Column("detail_website", String), alias="公司网址")
    detail_main_business: Optional[str] = Field(
        default=None, sa_column=Column("detail_main_business", String), alias="主营业务"
    )
    detail_product_name: Optional[str] = Field(
        default=None, sa_column=Column("detail_product_name", String), alias="产品名称"
    )
    detail_ctrl_shareholder: Optional[str] = Field(
        default=None, sa_column=Column("detail_ctrl_shareholder", String), alias="控制股东"
    )
    detail_actual: Optional[str] = Field(default=None, sa_column=Column("detail_actual", String), alias="实际控制人")
    detail_ultimate: Optional[str] = Field(
        default=None, sa_column=Column("detail_ultimate", String), alias="最终控制人"
    )
    detail_chairman: Optional[str] = Field(default=None, sa_column=Column("detail_chairman", String), alias="董事长")
    detail_secretary: Optional[str] = Field(default=None, sa_column=Column("detail_secretary", String), alias="董秘")
    detail_legal: Optional[str] = Field(default=None, sa_column=Column("detail_legal", String), alias="法人")
    detail_general_manager: Optional[str] = Field(
        default=None, sa_column=Column("detail_general_manager", String), alias="总经理"
    )
    detail_regist_capital: Optional[int] = Field(
        default=None, sa_column=Column("detail_regist_capital", Integer), alias="注册资金"
    )
    detail_staff_num: Optional[int] = Field(
        default=None, sa_column=Column("detail_staff_num", Integer), alias="员工人数"
    )
    detail_telephone: Optional[str] = Field(default=None, sa_column=Column("detail_telephone", String), alias="电话")
    detail_fax: Optional[str] = Field(default=None, sa_column=Column("detail_fax", String), alias="传真")
    detail_zip_code: Optional[str] = Field(default=None, sa_column=Column("detail_zip_code", String), alias="邮编")
    detail_office_address: Optional[str] = Field(
        default=None, sa_column=Column("detail_office_address", String), alias="办公地址"
    )
    detail_company_profile: Optional[str] = Field(
        default=None, sa_column=Column("detail_company_profile", String), alias="公司简介"
    )
    detail_regist_address: Optional[str] = Field(
        default=None, sa_column=Column("detail_regist_address", String), alias="注册地址"
    )
    issue_establish_date: Optional[str] = Field(
        default=None, sa_column=Column("issue_establish_date", String), alias="成立日期"
    )
    issue_price: Optional[float] = Field(default=None, sa_column=Column("issue_price", Float), alias="发行价格")
    issue_list_date: Optional[str] = Field(default=None, sa_column=Column("issue_list_date", String), alias="上市日期")
    issue_pe: Optional[float] = Field(default=None, sa_column=Column("issue_pe", Float), alias="发行市盈率")
    issue_estimate: Optional[int] = Field(default=None, sa_column=Column("issue_estimate", Integer), alias="预计募资")
    issue_open_price: Optional[float] = Field(
        default=None, sa_column=Column("issue_open_price", Float), alias="首日开盘价"
    )
    issue_winning_rate: Optional[float] = Field(
        default=None, sa_column=Column("issue_winning_rate", Float), alias="发行中签率"
    )
    issue_actual: Optional[int] = Field(default=None, sa_column=Column("issue_actual", Integer), alias="实际募资")
    issue_lead_underwriter: Optional[str] = Field(
        default=None, sa_column=Column("issue_lead_underwriter", String), alias="主承销商"
    )
    issue_listing_sponsor: Optional[str] = Field(
        default=None, sa_column=Column("issue_listing_sponsor", String), alias="上市保荐人"
    )
    issue_nums: Optional[int] = Field(default=None, sa_column=Column("issue_nums", Integer), alias="发行数量")
    roa: Optional[float] = Field(default=None, sa_column=Column("roa", Float), alias="总资产净利率")
    roic: Optional[float] = Field(default=None, sa_column=Column("roic", Float), alias="资本回报率")


# ──────────── A股财务数据 ────────────
class AStockFinancial(SQLModel, table=True):
    __tablename__ = "f10_main_indicator_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    parent_net_profit: int = Field(sa_column=Column("parent_net_profit", Integer), alias="归母净利润")
    parent_net_profit_yoy: float = Field(sa_column=Column("parent_net_profit_yoy", Float), alias="归母净利润同比增长率")
    non_net_profit: int = Field(sa_column=Column("non_net_profit", Integer), alias="扣非净利润")
    non_net_profit_yoy: float = Field(sa_column=Column("non_net_profit_yoy", Float), alias="扣非净利润同比增长率")
    income_total: int = Field(sa_column=Column("income_total", Integer), alias="营业总收入")
    income_total_yoy: float = Field(sa_column=Column("income_total_yoy", Float), alias="营业总收入同比增长率")
    basic_eps: float = Field(sa_column=Column("basic_eps", Float), alias="基本每股收益")
    share_net_asset: float = Field(sa_column=Column("share_net_asset", Float), alias="每股净资产")
    common_fund: float = Field(sa_column=Column("common_fund", Float), alias="每股资本公积金")
    un_profit: float = Field(sa_column=Column("un_profit", Float), alias="每股未分配利润")
    share_opera_cash: float = Field(sa_column=Column("share_opera_cash", Float), alias="每股经营现金流")
    gross_margin: float = Field(sa_column=Column("gross_margin", Float), alias="销售毛利率")
    sales_margin: float = Field(sa_column=Column("sales_margin", Float), alias="销售净利率")
    roe: float = Field(sa_column=Column("roe", Float), alias="净资产收益率")
    roe_diluted: float = Field(sa_column=Column("roe_diluted", Float), alias="净资产收益率-摊薄")
    business_cycle: float = Field(sa_column=Column("business_cycle", Float), alias="营业周期")
    inventory_turn_rate: float = Field(sa_column=Column("inventory_turn_rate", Float), alias="存货周转率")
    inventory_turn_days: float = Field(sa_column=Column("inventory_turn_days", Float), alias="存货周转天数")
    account_turn_days: float = Field(sa_column=Column("account_turn_days", Float), alias="应收账款周转天数")
    current_ratio: float = Field(sa_column=Column("current_ratio", Float), alias="流动比率")
    quick_ratio: float = Field(sa_column=Column("quick_ratio", Float), alias="速动比率")
    con_quick_ratio: float = Field(sa_column=Column("con_quick_ratio", Float), alias="保守速动比率")
    equity_ratio: float = Field(sa_column=Column("equity_ratio", Float), alias="产权比率")
    assets_and_liability: float = Field(sa_column=Column("assets_and_liability", Float), alias="资产负债率")
    total_assets_turnover_rate: float = Field(
        sa_column=Column("total_assets_turnover_rate", Float), alias="总资产周转率"
    )


# ──────────── A股单季度财务数据 ────────────
class AStockFinancialSingle(SQLModel, table=True):
    __tablename__: str = "f10_main_indicator_single_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    parent_net_profit: int = Field(sa_column=Column("parent_net_profit", Integer), alias="归母净利润")
    non_net_profit: int = Field(sa_column=Column("non_net_profit", Integer), alias="扣非净利润")
    income_total: int = Field(sa_column=Column("income_total", Integer), alias="营业总收入")
    basic_eps: float = Field(sa_column=Column("basic_eps", Float), alias="基本每股收益")


# ──────────── 现金流量表 ────────────
class AStockCashFlow(SQLModel, table=True):
    __tablename__ = "f10_cash_flow_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    goodssell_labourservice_cash: int = Field(
        sa_column=Column("goodssell_labourservice_cash", Integer), alias="销售商品、提供劳务收到的现金"
    )
    taxlevy_refund: int = Field(sa_column=Column("taxlevy_refund", Integer), alias="收到的税费返还")
    operating_cash: int = Field(sa_column=Column("operating_cash", Integer), alias="收到其他与经营活动有关的现金")
    operating_inflow_cash: int = Field(sa_column=Column("operating_inflow_cash", Integer), alias="经营活动现金流入小计")
    goodsbuy_service_cash: int = Field(
        sa_column=Column("goodsbuy_service_cash", Integer), alias="购买商品、接受劳务支付的现金"
    )
    staff_cash: int = Field(sa_column=Column("staff_cash", Integer), alias="支付给职工以及为职工支付的现金")
    alltaxes_paid: int = Field(sa_column=Column("alltaxes_paid", Integer), alias="支付的各项税费")
    other_operate_cash: int = Field(
        sa_column=Column("other_operate_cash", Integer), alias="支付其他与经营活动有关的现金"
    )
    operating_outflow_cash: int = Field(
        sa_column=Column("operating_outflow_cash", Integer), alias="经营活动现金流出小计"
    )
    net_operate_cash: int = Field(sa_column=Column("net_operate_cash", Integer), alias="经营活动产生的现金流量净额")
    invest_return_cash: int = Field(sa_column=Column("invest_return_cash", Integer), alias="收回投资收到的现金")
    invest_proceeds: int = Field(sa_column=Column("invest_proceeds", Integer), alias="取得投资收益收到的现金")
    fixintan_otherassetdispo_cash: int = Field(
        sa_column=Column("fixintan_otherassetdispo_cash", Integer),
        alias="处置固定资产、无形资产和其他长期资产收回的现金净额",
    )
    subcompany_necash: int = Field(
        sa_column=Column("subcompany_necash", Integer), alias="处置子公司及其他营业单位收到的现金净额"
    )
    invest_other_cash: int = Field(sa_column=Column("invest_other_cash", Integer), alias="收到其他与投资活动有关的现金")
    invest_inflow_cash: int = Field(sa_column=Column("invest_inflow_cash", Integer), alias="投资活动现金流入小计")
    fixintan_otherasset_acqui_cash: int = Field(
        sa_column=Column("fixintan_otherasset_acqui_cash", Integer),
        alias="购建固定资产、无形资产和其他长期资产支付的现金",
    )
    invest_paid_cash: int = Field(sa_column=Column("invest_paid_cash", Integer), alias="投资支付的现金")
    suncompany_net_cash: int = Field(
        sa_column=Column("suncompany_net_cash", Integer), alias="取得子公司及其他营业单位支付的现金净额"
    )
    otherpaid_invest_cash: int = Field(
        sa_column=Column("otherpaid_invest_cash", Integer), alias="支付其他与投资活动有关的现金"
    )
    invest_outflow_cash: int = Field(sa_column=Column("invest_outflow_cash", Integer), alias="投资活动现金流出小计")
    invest_flow_cash: int = Field(sa_column=Column("invest_flow_cash", Integer), alias="投资活动产生的现金流量净额")
    receive_invest_cash: int = Field(sa_column=Column("receive_invest_cash", Integer), alias="吸收投资收到的现金")
    mino_holders_invest_cash: int = Field(
        sa_column=Column("mino_holders_invest_cash", Integer), alias="子公司吸收少数股东投资收到的现金"
    )
    borrow_cash: int = Field(sa_column=Column("borrow_cash", Integer), alias="取得借款收到的现金")
    bonds_issue_cash: int = Field(sa_column=Column("bonds_issue_cash", Integer), alias="发行债券收到的现金")
    other_finance_cash: int = Field(
        sa_column=Column("other_finance_cash", Integer), alias="收到其他与筹资活动有关的现金"
    )
    finance_inflow_cash: int = Field(sa_column=Column("finance_inflow_cash", Integer), alias="筹资活动现金流入小计")
    borrowed_repay_cash: int = Field(sa_column=Column("borrowed_repay_cash", Integer), alias="偿还债务支付的现金")
    dividend_porfit_interest_cash: int = Field(
        sa_column=Column("dividend_porfit_interest_cash", Integer), alias="分配股利、利润或偿付利息支付的现金"
    )
    sub_pay_minoholders_proceeds: int = Field(
        sa_column=Column("sub_pay_minoholders_proceeds", Integer), alias="子公司支付给少数股东的股利、利润或偿付的利息"
    )
    otherpaid_finance_cash: int = Field(
        sa_column=Column("otherpaid_finance_cash", Integer), alias="支付其他与筹资活动有关的现金"
    )
    finance_outflow_cash: int = Field(sa_column=Column("finance_outflow_cash", Integer), alias="筹资活动现金流出小计")
    finance_flow_netcash: int = Field(
        sa_column=Column("finance_flow_netcash", Integer), alias="筹资活动产生的现金流量净额"
    )
    ratechange_effect: int = Field(
        sa_column=Column("ratechange_effect", Integer), alias="汇率变动对现金及现金等价物的影响"
    )
    net_increase_cash: int = Field(sa_column=Column("net_increase_cash", Integer), alias="现金及现金等价物净增加额")
    begin_remain_cash: int = Field(sa_column=Column("begin_remain_cash", Integer), alias="期初现金及现金等价物余额")
    end_remain_cash: int = Field(sa_column=Column("end_remain_cash", Integer), alias="期末现金及现金等价物余额")
    assets_decrease_reserve: int = Field(sa_column=Column("assets_decrease_reserve", Integer), alias="资产减值准备")
    fixedasset_depreciation: int = Field(sa_column=Column("fixedasset_depreciation", Integer), alias="固定资产折旧")
    immaterialasset_amortization: int = Field(
        sa_column=Column("immaterialasset_amortization", Integer), alias="无形资产摊销"
    )
    deferred_expense: int = Field(sa_column=Column("deferred_expense", Integer), alias="长期待摊费用摊销")
    deal_assets_loss: int = Field(
        sa_column=Column("deal_assets_loss", Integer), alias="处置固定资产、无形资产和其他长期资产的损失"
    )
    fixedassets_scrap_loss: int = Field(sa_column=Column("fixedassets_scrap_loss", Integer), alias="固定资产报废损失")
    firevalue_change_loss: int = Field(sa_column=Column("firevalue_change_loss", Integer), alias="公允价值变动损失")
    finance_expense: int = Field(sa_column=Column("finance_expense", Integer), alias="现金流量财务费用")
    invest_loss: int = Field(sa_column=Column("invest_loss", Integer), alias="投资损失")
    decrease_defered_tax_asset: int = Field(
        sa_column=Column("decrease_defered_tax_asset", Integer), alias="递延所得税资产减少"
    )
    increase_defered_taxasset_debt: int = Field(
        sa_column=Column("increase_defered_taxasset_debt", Integer), alias="递延所得税负债增加"
    )
    decrease_inventory: int = Field(sa_column=Column("decrease_inventory", Integer), alias="存货的减少")
    decrease_operate_receivable: int = Field(
        sa_column=Column("decrease_operate_receivable", Integer), alias="经营性应收项目的减少"
    )
    increase_operate_receivable: int = Field(
        sa_column=Column("increase_operate_receivable", Integer), alias="经营性应付项目的增加"
    )
    other_cashflow: int = Field(sa_column=Column("other_cashflow", Integer), alias="其他现金流量")
    net_operate_cashflow_notes: int = Field(
        sa_column=Column("net_operate_cashflow_notes", Integer), alias="间接法经营活动产生的现金流量净额"
    )
    end_cash: int = Field(sa_column=Column("end_cash", Integer), alias="现金的期末余额")
    begin_cash: int = Field(sa_column=Column("begin_cash", Integer), alias="现金的期初余额")
    begin_cash_equivalents: int = Field(
        sa_column=Column("begin_cash_equivalents", Integer), alias="现金等价物的期初余额"
    )
    netincr_cash_and_equivalents: int = Field(
        sa_column=Column("netincr_cash_and_equivalents", Integer), alias="间接法现金及现金等价物净增加额"
    )


# ──────────── 股本结构表 ────────────
class AStockStructure(SQLModel, table=True):
    __tablename__ = "f10_stock_structure_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: int = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    total_a_marketvalue: Optional[int] = Field(sa_column=Column("total_a_marketvalue", Integer), alias="A股总股本")
    cir_a_marketvalue: Optional[int] = Field(sa_column=Column("cir_a_marketvalue", Integer), alias="流通A股")
    limit_a_marketvalue: Optional[int] = Field(sa_column=Column("limit_a_marketvalue", Integer), alias="限售A股")
    change_reason: Optional[str] = Field(sa_column=Column("change_reason", String), alias="变动原因")
    total_b_marketvalue: Optional[int] = Field(sa_column=Column("total_b_marketvalue", Integer), alias="B股总股本")
    cir_b_marketvalue: Optional[int] = Field(sa_column=Column("cir_b_marketvalue", Integer), alias="流通B股")
    limit_b_marketvalue: Optional[int] = Field(sa_column=Column("limit_b_marketvalue", Integer), alias="限售B股")
    total_h_marketvalue: Optional[int] = Field(sa_column=Column("total_h_marketvalue", Integer), alias="H股总股本")
    # 重复字段，总股本
    tq: int = Field()
    cir_h_marketvalue: Optional[int] = Field(sa_column=Column("cir_h_marketvalue", Integer), alias="流通H股")
    limit_h_marketvalue: Optional[int] = Field(sa_column=Column("limit_h_marketvalue", Integer), alias="限售H股")
    total_cir_marketvalue: Optional[int] = Field(sa_column=Column("total_cir_marketvalue", Integer), alias="流通总股本")
    total_limit_marketvalue: Optional[int] = Field(
        sa_column=Column("total_limit_marketvalue", Integer), alias="限售总股本"
    )


# ──────────── 股东人数表 ────────────
class AStockShareHolder(SQLModel, table=True):
    __tablename__ = "f10_shareholder_nums_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: int = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    holders_a_total: Optional[int] = Field(sa_column=Column("holders_a_total", Integer), alias="A股股东总人数")
    holders_b_total: Optional[int] = Field(sa_column=Column("holders_b_total", Integer), alias="B股股东总人数")
    holders_h_total: Optional[int] = Field(sa_column=Column("holders_h_total", Integer), alias="H股股东总人数")
    total_people_num: Optional[int] = Field(sa_column=Column("total_people_num", Integer), alias="股东总人数")
    tradable_share_avg: Optional[float] = Field(sa_column=Column("tradable_share_avg", Float), alias="人均流通股")
    industry_share_avg: Optional[float] = Field(sa_column=Column("industry_share_avg", Float), alias="行业平均")
    cir_a_avg_share: Optional[float] = Field(sa_column=Column("cir_a_avg_share", Float), alias="人均流通A股")
    cir_a_avg_share_change: Optional[float] = Field(
        sa_column=Column("cir_a_avg_share_change", Float), alias="人均流通A股变化"
    )


# ──────────── 分红情况表 ────────────
class AStockDividenedDetail(SQLModel, table=True):
    __tablename__ = "f10_dividend_detail_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    date_int: int = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    dividend_director_date: str = Field(sa_column=Column("dividend_director_date", String), alias="董事会日期")
    dividend_plan_date: str = Field(sa_column=Column("dividend_plan_date", String), alias="股东大会预案公告日期")
    dividend_notice_date: str = Field(sa_column=Column("dividend_notice_date", String), alias="实施公告日期")
    dividend_scheme: str = Field(sa_column=Column("dividend_scheme", String), alias="分红方案")
    dividend_register_date: str = Field(sa_column=Column("dividend_register_date", String), alias="股权登记")
    dividend_ex_date: str = Field(sa_column=Column("dividend_ex_date", String), alias="除息除权登记日期")
    dividend_amount: int = Field(sa_column=Column("dividend_amount", Integer), alias="分红总额")
    dividend_schedule: str = Field(sa_column=Column("dividend_schedule", String), alias="分红情况方案进度")
    dividend_pay_ratio: float = Field(sa_column=Column("dividend_pay_ratio", Float), alias="股息支付率")
    dividend_ratio: float = Field(sa_column=Column("dividend_ratio", Float), alias="税前分红率")


# ──────────── 增发情况表 ────────────
class AStockDividenedAddition(SQLModel, table=True):
    __tablename__ = "f10_dividend_addition_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: int = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    addition_issue_price: float = Field(sa_column=Column("addition_issue_price", Float), alias="实际发行价格")
    addition_issue_nums: int = Field(sa_column=Column("addition_issue_nums", Integer), alias="实际发行数量")
    addition_amount: int = Field(sa_column=Column("addition_amount", Integer), alias="实际募资净额")
    addition_price_method: str = Field(sa_column=Column("addition_price_method", String), alias="发行定价方式")
    addition_date: str = Field(sa_column=Column("addition_date", String), alias="增发时间")
    addition_process: str = Field(sa_column=Column("addition_process", String), alias="增发进度")


# ──────────── 配股情况表 ────────────
class AStockDividenedAllotment(SQLModel, table=True):
    __tablename__ = "f10_dividend_allotment_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    allotment_net_amount: int = Field(sa_column=Column("allotment_net_amount", Integer), alias="实际募资净额")
    allotment_code: str = Field(sa_column=Column("allotment_code", String), alias="配股代码")
    allotment_brief_name: str = Field(sa_column=Column("allotment_brief_name", String), alias="配股简称")
    allotment_actual_ratio: float = Field(sa_column=Column("allotment_actual_ratio", Float), alias="实际配股比例")
    allotment_list_date: str = Field(sa_column=Column("allotment_list_date", String), alias="配股上市日")
    allotment_approval_date: str = Field(sa_column=Column("allotment_approval_date", String), alias="证监会核准公告日")
    allotment_per_price: float = Field(sa_column=Column("allotment_per_price", Float), alias="每股配股价格")
    allotment_begin_date: str = Field(sa_column=Column("allotment_begin_date", String), alias="缴款起始日")
    allotment_end_date: str = Field(sa_column=Column("allotment_end_date", String), alias="缴款截止日")
    allotment_regist_date: str = Field(sa_column=Column("allotment_regist_date", String), alias="股权登记日")
    allotment_notice_date: str = Field(sa_column=Column("allotment_notice_date", String), alias="董事会公告日")
    allotment_process: str = Field(sa_column=Column("allotment_process", String), alias="配股情况方案进度")


# ──────────── 机构持仓汇总表 ────────────
class AStockPositionSum(SQLModel, table=True):
    __tablename__ = "f10_position_sum_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    institution_total_nums: int = Field(sa_column=Column("institution_total_nums", Integer), alias="机构数量")
    institution_hold_nums: int = Field(sa_column=Column("institution_hold_nums", Integer), alias="累计持有数量")
    institution_hold_value: int = Field(sa_column=Column("institution_hold_value", Integer), alias="累计市值")
    institution_hold_ratio: float = Field(sa_column=Column("institution_hold_ratio", Float), alias="持仓比例")


# ──────────── 资金统计数据 ────────────
class Fund3cjLevel0(SQLModel, table=True):
    __tablename__: str = "fund_3cj_level0_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    gan_1: float = Field()
    duo_1: float = Field()
    gan_3: float = Field()
    duo_3: float = Field()
    gan_13: float = Field()
    duo_13: float = Field()


# ──────────── 一日多空资金榜 ────────────
class RanklistDuo1Level0(SQLModel, table=True):
    __tablename__: str = "ranklist_duo1_level0_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    duo_1: int = Field()
    duor_1: float = Field()


# ──────────── 三日多空资金榜 ────────────
class RanklistDuo3Level0(SQLModel, table=True):
    __tablename__: str = "ranklist_duo3_level0_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    duo_3: int = Field()
    duor_3: float = Field()


# ──────────── 十三日敢死队资金榜 ────────────
class RanklistGan13Level0(SQLModel, table=True):
    __tablename__: str = "ranklist_gan13_level0_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    gan_13: int = Field()
    ganr_13: float = Field()


# ──────────── 三把锁指标 ────────────
class DailyThreeMethodLevel0(SQLModel, table=True):
    __tablename__ = "daily_threemethod_level0_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    optype: Optional[int] = Field(default=None, sa_column=Column("optype", Integer), alias="出击类型")
    opprice: Optional[float] = Field(default=None, sa_column=Column("opprice", Float), alias="出击价格")


# ──────────── 波段决策，价值决策，长线决策，总资金仓位 ────────────
class DailyCwDataLevel0(SQLModel, table=True):
    __tablename__ = "daily_cwdata_level0_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    value_policy: Optional[int] = Field(default=None, sa_column=Column("value_policy", Integer), alias="价值决策")
    long_policy: Optional[int] = Field(default=None, sa_column=Column("long_policy", Integer), alias="长线决策")
    long_marketopsatus: Optional[int] = Field(
        default=None, sa_column=Column("long_marketopsatus", Integer), alias="六大市场长线决策"
    )
    short_policy: Optional[int] = Field(default=None, sa_column=Column("short_policy", Integer), alias="短线决策")
    op_policy: Optional[int] = Field(default=None, sa_column=Column("op_policy", Integer), alias="波段决策")


# ──────────── 估值空间 ────────────
class DailyGzkjLevel0(SQLModel, table=True):
    __tablename__ = "daily_gzkj_level0_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    gzhigh: Optional[float] = Field(default=None, sa_column=Column("gzhigh", Float), alias="估值空间风险线")
    gzmid: Optional[float] = Field(default=None, sa_column=Column("gzmid", Float), alias="估值空间中线")
    gzlow: Optional[float] = Field(default=None, sa_column=Column("gzlow", Float), alias="估值空间安全线")


# ──────────── 市场融资融券表 ────────────
class MarketHistory(SQLModel, table=True):
    __tablename__: str = "market_history_view"

    market: str = Field(sa_column=Column("market", String, primary_key=True), alias="市场名称")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    rzrq_amt: Optional[float] = Field(default=None, sa_column=Column("rzrq_amt", Float), alias="融资融券余额")
    rz_amt: Optional[float] = Field(default=None, sa_column=Column("rz_amt", Float), alias="融资余额")
    rq_amt: Optional[float] = Field(default=None, sa_column=Column("rq_amt", Float), alias="融券余额")
    rzrq_trade_amt: Optional[float] = Field(
        default=None, sa_column=Column("rzrq_trade_amt", Float), alias="融资融券交易额"
    )
    rz_buy_amt: Optional[float] = Field(default=None, sa_column=Column("rz_buy_amt", Float), alias="融资买入额")
    rq_sell_amt: Optional[float] = Field(default=None, sa_column=Column("rq_sell_amt", Float), alias="融券卖出额")


# ──────────── 板块单季度财务数据 ────────────
class BoardFinancialSingleInfo(SQLModel, table=True):
    __tablename__: str = "board_f10_single_main_indicator_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="报告期（整型，格式YYYYMMDD）")
    parent_net_profit: Optional[int] = Field(
        default=None, sa_column=Column("parent_net_profit", Integer), alias="归母净利润"
    )
    non_net_profit: Optional[int] = Field(default=None, sa_column=Column("non_net_profit", Integer), alias="扣非净利润")
    income_total: Optional[int] = Field(default=None, sa_column=Column("income_total", Integer), alias="营业总收入")
    basic_eps: Optional[float] = Field(default=None, sa_column=Column("basic_eps", Float), alias="基本每股收益")


# ──────────── 板块单季度利润表 ────────────
class BoardFinancialSingleProfitView(SQLModel, table=True):
    __tablename__: str = "board_f10_single_profit_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="报告期（整型，格式YYYYMMDD）")
    income: Optional[int] = Field(default=None, sa_column=Column("income", Integer), alias="营业收入")
    cost_total: Optional[int] = Field(default=None, sa_column=Column("cost_total", Integer), alias="营业总成本")
    cost: Optional[int] = Field(default=None, sa_column=Column("cost", Integer), alias="营业成本")
    tax_surcharges: Optional[int] = Field(
        default=None, sa_column=Column("tax_surcharges", Integer), alias="营业税金及附加"
    )
    sell_expense: Optional[int] = Field(default=None, sa_column=Column("sell_expense", Integer), alias="销售费用")
    manage_expense: Optional[int] = Field(default=None, sa_column=Column("manage_expense", Integer), alias="管理费用")
    rd_expense: Optional[int] = Field(default=None, sa_column=Column("rd_expense", Integer), alias="研发费用")
    finance_expense_total: Optional[int] = Field(
        default=None, sa_column=Column("finance_expense_total", Integer), alias="财务费用"
    )
    interest_finexp: Optional[int] = Field(
        default=None, sa_column=Column("interest_finexp", Integer), alias="财务费用_利息费用"
    )
    income_finexp: Optional[int] = Field(
        default=None, sa_column=Column("income_finexp", Integer), alias="财务费用_利息收入"
    )
    asset_impairment_loss: Optional[int] = Field(
        default=None, sa_column=Column("asset_impairment_loss", Integer), alias="资产减值损失"
    )
    credit_impairment_loss: Optional[int] = Field(
        default=None, sa_column=Column("credit_impairment_loss", Integer), alias="信用减值损失"
    )
    income_firevalue_change: Optional[int] = Field(
        default=None, sa_column=Column("income_firevalue_change", Integer), alias="公允价值变动收益"
    )
    income_invest: Optional[int] = Field(default=None, sa_column=Column("income_invest", Integer), alias="投资收益")
    income_invest_associates: Optional[int] = Field(
        default=None, sa_column=Column("income_invest_associates", Integer), alias="投资收益_联营和合营企业"
    )
    income_assetdeal: Optional[int] = Field(
        default=None, sa_column=Column("income_assetdeal", Integer), alias="资产处置收益"
    )
    operating_profit: Optional[int] = Field(
        default=None, sa_column=Column("operating_profit", Integer), alias="营业利润"
    )
    income_nonoperating: Optional[int] = Field(
        default=None, sa_column=Column("income_nonoperating", Integer), alias="营业外收入"
    )
    earn_noncurrent_assetss: Optional[int] = Field(
        default=None, sa_column=Column("earn_noncurrent_assetss", Integer), alias="非流动资产处置利得"
    )
    expense_nonoperating: Optional[int] = Field(
        default=None, sa_column=Column("expense_nonoperating", Integer), alias="营业外支出"
    )
    loss_nocurrent_assets: Optional[int] = Field(
        default=None, sa_column=Column("loss_nocurrent_assets", Integer), alias="非流动资产处置净损失"
    )
    profit_total: Optional[int] = Field(default=None, sa_column=Column("profit_total", Integer), alias="利润总额")
    cost_incometax: Optional[int] = Field(default=None, sa_column=Column("cost_incometax", Integer), alias="所得税费用")
    net_profit: Optional[int] = Field(default=None, sa_column=Column("net_profit", Integer), alias="净利润")
    operate_profit: Optional[int] = Field(
        default=None, sa_column=Column("operate_profit", Integer), alias="持续经营净利润"
    )
    minority_profit: Optional[int] = Field(
        default=None, sa_column=Column("minority_profit", Integer), alias="少数股东损益"
    )
    comprehensive_total_income: Optional[int] = Field(
        default=None, sa_column=Column("comprehensive_total_income", Integer), alias="综合收益总额"
    )
    parent_owners_income: Optional[int] = Field(
        default=None, sa_column=Column("parent_owners_income", Integer), alias="归属于母公司所有者的其他综合收益总额"
    )
    minority_shareholders_income: Optional[int] = Field(
        default=None, sa_column=Column("minority_shareholders_income", Integer), alias="归属于少数股东的综合收益总额"
    )
    diluted_eps: Optional[int] = Field(default=None, sa_column=Column("diluted_eps", Integer), alias="稀释每股收益")


# ──────────── A股票资产负债表 ────────────
class StockFinancialPropertyBalanceView(SQLModel, table=True):
    __tablename__ = "f10_property_balance_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="报告期（整型，格式YYYYMMDD）")
    cash: Optional[int] = Field(sa_column=Column("cash", Integer), alias="货币资金")
    bill_accounts_receivable: Optional[int] = Field(
        sa_column=Column("bill_accounts_receivable", Integer), alias="应收票据及应收账款"
    )
    trading_assets: Optional[int] = Field(
        sa_column=Column("trading_assets", Integer), alias="以公允价值计量且其变动计入当期损益的金融资产"
    )
    bill_receivable: Optional[int] = Field(sa_column=Column("bill_receivable", Integer), alias="应收票据")
    accounts_receivable: Optional[int] = Field(sa_column=Column("accounts_receivable", Integer), alias="应收账款")
    advance_payment: Optional[int] = Field(sa_column=Column("advance_payment", Integer), alias="预付款项")
    other_receivable_ed: Optional[int] = Field(
        sa_column=Column("other_receivable_ed", Integer), alias="其他应收款(含利息和股利)"
    )
    interest_receive: Optional[int] = Field(sa_column=Column("interest_receive", Integer), alias="应收利息")
    other_receive: Optional[int] = Field(sa_column=Column("other_receive", Integer), alias="其他应收款")
    inventory: Optional[int] = Field(sa_column=Column("inventory", Integer), alias="存货")
    contrac_assets: Optional[int] = Field(sa_column=Column("contrac_assets", Integer), alias="合同资产")
    nocurrent_assets_1year: Optional[int] = Field(
        sa_column=Column("nocurrent_assets_1year", Integer), alias="一年内到期的非流动资产"
    )
    other_current_assets: Optional[int] = Field(sa_column=Column("other_current_assets", Integer), alias="其他流动资产")
    total_current_assets: Optional[int] = Field(sa_column=Column("total_current_assets", Integer), alias="流动资产合计")
    hold_forsale_assets: Optional[int] = Field(
        sa_column=Column("hold_forsale_assets", Integer), alias="可供出售金融资产"
    )
    hold_maturity_investments: Optional[int] = Field(
        sa_column=Column("hold_maturity_investments", Integer), alias="持有至到期投资"
    )
    longterm_equity_invest: Optional[int] = Field(
        sa_column=Column("longterm_equity_invest", Integer), alias="长期股权投资"
    )
    invest_property: Optional[int] = Field(sa_column=Column("invest_property", Integer), alias="投资性房地产")
    fixed_assets: Optional[int] = Field(sa_column=Column("fixed_assets", Integer), alias="固定资产合计")
    fixed_assets_checkup: Optional[int] = Field(sa_column=Column("fixed_assets_checkup", Integer), alias="固定资产清理")
    construct_project: Optional[int] = Field(sa_column=Column("construct_project", Integer), alias="在建工程合计")
    intangible_assets: Optional[int] = Field(sa_column=Column("intangible_assets", Integer), alias="无形资产")
    good_will: Optional[int] = Field(sa_column=Column("good_will", Integer), alias="商誉")
    long_deferred_expense: Optional[int] = Field(
        sa_column=Column("long_deferred_expense", Integer), alias="长期待摊费用"
    )
    other_noncurrent_assets: Optional[int] = Field(
        sa_column=Column("other_noncurrent_assets", Integer), alias="其他非流动资产"
    )
    total_noncurrent_assets: Optional[int] = Field(
        sa_column=Column("total_noncurrent_assets", Integer), alias="非流动资产合计"
    )
    total_assets: Optional[int] = Field(sa_column=Column("total_assets", Integer), alias="资产总计")
    shortterm_loan: Optional[int] = Field(sa_column=Column("shortterm_loan", Integer), alias="短期借款")
    trading_liability: Optional[int] = Field(
        sa_column=Column("trading_liability", Integer), alias="以公允价值计量且变动计入当期损益的金融负债"
    )
    derivative_liability: Optional[int] = Field(sa_column=Column("derivative_liability", Integer), alias="衍生金融负债")
    notaccounts_payment: Optional[int] = Field(
        sa_column=Column("notaccounts_payment", Integer), alias="应付票据及应付账款"
    )
    notes_payment: Optional[int] = Field(sa_column=Column("notes_payment", Integer), alias="应付票据")
    accounts_payment: Optional[int] = Field(sa_column=Column("accounts_payment", Integer), alias="应付账款")
    contract_liability: Optional[int] = Field(sa_column=Column("contract_liability", Integer), alias="合同负债")
    advance_receipts: Optional[int] = Field(sa_column=Column("advance_receipts", Integer), alias="预收款项")
    nocurrent_liability_1year: Optional[int] = Field(
        sa_column=Column("nocurrent_liability_1year", Integer), alias="一年内到期的非流动负债"
    )
    othercurrent_liability: Optional[int] = Field(
        sa_column=Column("othercurrent_liability", Integer), alias="其他流动负债"
    )
    totalcurrent_liability: Optional[int] = Field(
        sa_column=Column("totalcurrent_liability", Integer), alias="流动负债合计"
    )
    paid_capital: Optional[int] = Field(sa_column=Column("paid_capital", Integer), alias="实收资本(或股本)")
    capital_reserve_fund: Optional[int] = Field(sa_column=Column("capital_reserve_fund", Integer), alias="资本公积")
    treasury_stock: Optional[int] = Field(sa_column=Column("treasury_stock", Integer), alias="库存股")
    other_composite_income: Optional[int] = Field(
        sa_column=Column("other_composite_income", Integer), alias="其他综合收益"
    )
    surplus_reserve_fund: Optional[int] = Field(sa_column=Column("surplus_reserve_fund", Integer), alias="盈余公积")
    retained_profit: Optional[int] = Field(sa_column=Column("retained_profit", Integer), alias="未分配利润")
    parent_shareholder_equity: Optional[int] = Field(
        sa_column=Column("parent_shareholder_equity", Integer), alias="归属母公司股东权益合计"
    )
    minority_interests: Optional[int] = Field(sa_column=Column("minority_interests", Integer), alias="少数股东权益")
    total_shareholder_equity: Optional[int] = Field(
        sa_column=Column("total_shareholder_equity", Integer), alias="所有者权益(或股东权益)合计"
    )
    total_liability_equity: Optional[int] = Field(
        sa_column=Column("total_liability_equity", Integer), alias="负债和所有者权益(或股东权益)总计"
    )
    deferred_tax_assets: Optional[int] = Field(sa_column=Column("deferred_tax_assets", Integer), alias="递延所得税资产")
    total_liability: Optional[int] = Field(sa_column=Column("total_liability", Integer), alias="总负债合计")


# ──────────── A股票资产负债单季度表 ────────────
class StockFinancialPropertyBalanceSingleView(SQLModel, table=True):
    __tablename__: str = "f10_property_balance_single_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="报告期（整型，格式YYYYMMDD）")
    total_assets: Optional[int] = Field(sa_column=Column("total_assets", Integer), alias="资产总计")
    total_liability: Optional[int] = Field(sa_column=Column("total_liability", Integer), alias="总负债合计")


# ──────────── 利润表 ────────────
class AStockProfitView(SQLModel, table=True):
    __tablename__ = "f10_profit_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    income: int = Field(sa_column=Column("income", Integer), alias="营业收入")
    cost_total: int = Field(sa_column=Column("cost_total", Integer), alias="营业总成本")
    cost: int = Field(sa_column=Column("cost", Integer), alias="营业成本")
    tax_surcharges: int = Field(sa_column=Column("tax_surcharges", Integer), alias="营业税金及附加")
    sell_expense: int = Field(sa_column=Column("sell_expense", Integer), alias="销售费用")
    manage_expense: int = Field(sa_column=Column("manage_expense", Integer), alias="管理费用")
    rd_expense: int = Field(sa_column=Column("rd_expense", Integer), alias="研发费用")
    finance_expense_total: int = Field(sa_column=Column("finance_expense_total", Integer), alias="财务费用")
    interest_finexp: int = Field(sa_column=Column("interest_finexp", Integer), alias="利息费用(财务费用)")
    income_finexp: int = Field(sa_column=Column("income_finexp", Integer), alias="利息收入(财务费用)")
    asset_impairment_loss: int = Field(sa_column=Column("asset_impairment_loss", Integer), alias="资产减值损失")
    credit_impairment_loss: int = Field(sa_column=Column("credit_impairment_loss", Integer), alias="信用减值损失")
    income_firevalue_change: int = Field(sa_column=Column("income_firevalue_change", Integer), alias="公允价值变动收益")
    income_invest: int = Field(sa_column=Column("income_invest", Integer), alias="投资收益")
    income_invest_associates: int = Field(
        sa_column=Column("income_invest_associates", Integer), alias="联营和合营企业的投资收益"
    )
    income_assetdeal: int = Field(sa_column=Column("income_assetdeal", Integer), alias="资产处置收益")
    operating_profit: int = Field(sa_column=Column("operating_profit", Integer), alias="营业利润")
    income_nonoperating: int = Field(sa_column=Column("income_nonoperating", Integer), alias="营业外收入")
    earn_noncurrent_assetss: int = Field(
        sa_column=Column("earn_noncurrent_assetss", Integer), alias="非流动资产处置利得"
    )
    expense_nonoperating: int = Field(sa_column=Column("expense_nonoperating", Integer), alias="营业外支出")
    loss_nocurrent_assets: int = Field(sa_column=Column("loss_nocurrent_assets", Integer), alias="非流动资产处置净损失")
    profit_total: int = Field(sa_column=Column("profit_total", Integer), alias="利润总额")
    cost_incometax: int = Field(sa_column=Column("cost_incometax", Integer), alias="所得税费用")
    net_profit: int = Field(sa_column=Column("net_profit", Integer), alias="净利润")
    operate_profit: int = Field(sa_column=Column("operate_profit", Integer), alias="持续经营净利润")
    minority_profit: int = Field(sa_column=Column("minority_profit", Integer), alias="少数股东损益")
    comprehensive_total_income: int = Field(
        sa_column=Column("comprehensive_total_income", Integer), alias="综合收益总额"
    )
    parent_owners_income: int = Field(
        sa_column=Column("parent_owners_income", Integer), alias="归属于母公司所有者的其他综合收益总额"
    )
    minority_shareholders_income: int = Field(
        sa_column=Column("minority_shareholders_income", Integer), alias="归属于少数股东的综合收益总额"
    )
    diluted_eps: int = Field(sa_column=Column("diluted_eps", Integer), alias="稀释每股收益")


class NetbuyZhuLevel0(SQLModel, table=True):
    __tablename__: str = "netbuy_zhu_level0_view"
    secucode: str = Field(primary_key=True)
    date: datetime.date = Field(default_factory=datetime.date.today)
    date_int: int = Field(primary_key=True)
    zhu: float


class NetbuyGanLevel0(SQLModel, table=True):
    __tablename__: str = "netbuy_gan_level0_view"
    secucode: str = Field(primary_key=True)
    date: datetime.date = Field(default_factory=datetime.date.today)
    date_int: int = Field(primary_key=True)
    gan: float


class NetbuyDuoLevel0(SQLModel, table=True):
    __tablename__: str = "netbuy_duo_level0_view"
    secucode: str = Field(primary_key=True)
    date: datetime.date = Field(default_factory=datetime.date.today)
    date_int: int = Field(primary_key=True)
    duo: float


# ──────────── 资金动向分级表 ────────────
class DailyZjdxStatLevel5(SQLModel, table=True):
    __tablename__: str = "daily_zjdx_stat_level5_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(
        sa_column=Column("date_int", Integer, primary_key=True), alias="交易日期（整型，格式YYYYMMDD）"
    )
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    duo: float = Field(sa_column=Column("duo", Float, default=0.0), alias="多空资金")
    duor: float = Field(sa_column=Column("duor", Float, default=0.0), alias="多空增减仓")
    zhu: float = Field(sa_column=Column("zhu", Float, default=0.0), alias="主力资金")
    zhur: float = Field(sa_column=Column("zhur", Float, default=0.0), alias="主力增减仓")
    gan: float = Field(sa_column=Column("gan", Float, default=0.0), alias="敢死队资金")
    ganr: float = Field(sa_column=Column("ganr", Float, default=0.0), alias="敢死队增减仓")


# 注释的意思是还没接入
class DailyQlstockBasicLevel5(SQLModel, table=True):
    __tablename__ = "daily_qlstock_basic_level5_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="报告期（整型，格式YYYYMMDD）")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    lanchou: Optional[bool] = Field(sa_column=Column("lanchou", Boolean, default=None), alias="蓝筹股")
    # state: int = Field(sa_column=Column("state", Integer, default=0), alias="状态")
    ismultild: Optional[bool] = Field(sa_column=Column("ismultild", Boolean, default=None), alias="为多轮共振")
    # ma_90_480: float = Field(sa_column=Column("ma_90_480", Float, default=0.0), alias="MA乖离(90,480) * 1000MA")
    zldd: int = Field(sa_column=Column("zldd", Integer, default=0), alias="主力大单")
    warzone: str = Field(sa_column=Column("market", String, default="0"), alias="所属战区")
    tradecore: Optional[bool] = Field(sa_column=Column("tradecore", Boolean, default=None), alias="为行业核心")
    prepare: Optional[bool] = Field(sa_column=Column("prepare", Boolean, default=None), alias="为准备股票")
    multild: int = Field(sa_column=Column("multild", Integer, default=0), alias="多轮共振（轮动共振）")
    bd_ykcsl: int = Field(sa_column=Column("bd_ykcsl", Integer, default=0), alias="波段策略已开仓数量")
    stock_attribute: int = Field(
        sa_column=Column("stock_attribute", Integer, default=0),
        default=0,
        alias="属性掩码",
        schema_extra={"render_type": "bitmask", "options": {256: "成长白马股", 512: "红利优选股"}},
    )
    is_new: Optional[bool] = Field(default=None, description="新股")


class DailyLdPoolsLevel5(SQLModel, table=True):
    __tablename__: str = "daily_ldpools_level5_view"

    # 视图分组键：filter_conditions + date
    filter_conditions: str = Field(
        primary_key=True,
        description="""筛选条件
        首位:0-长线,1-小时,
        二位:1-名校-个股对行业,2-名校-个股对战区,3-名校-所属行业,4-名校-所属战区,5-海选-个股对大盘,6-战区优化旗，7-多空资金
        三位:0-稳健型,1-非稳健型,
        末位:0-普通版,1-私享家版""",
    )
    date: datetime.date = Field(default_factory=datetime.date.today, description="日期")
    # 聚合字段：argMaxMerge 取最新
    stock_pools: str = Field(default="", description="股票池（逗号分隔的证券代码）")
    date_int: int = Field(primary_key=True, description="整数日期 YYYYMMDD（聚合后最新）")


# ──────────── 轮动战法 ────────────
class DailyQlldLevel5(SQLModel, table=True):
    __tablename__ = "daily_qlld_level5_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(
        sa_column=Column("date_int", Integer, primary_key=True), alias="交易日期（整型，格式YYYYMMDD）"
    )
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    # （0:轮空 1:开始轮动 2:开始强势轮动 3:开始强势轮动 4:轮动中 5:强势轮动中）
    ldmarket: int = Field(sa_column=Column("ldmarket", Integer), alias="战区轮动")
    ldtrade: int = Field(sa_column=Column("ldtrade", Integer), alias="行业轮动")
    ld0z: int = Field(sa_column=Column("ld0z", Integer), alias="0Z轮动")
    state: int = Field(default=0, description="状态")
    ldindex: float = Field(sa_column=Column("ldindex", Float), alias="轮动指数")
    ld_20: int = Field(sa_column=Column("ld_20", Integer), alias="强势轮动_20")
    ld_60: int = Field(sa_column=Column("ld_60", Integer), alias="强势轮动_60")
    ld_120: int = Field(sa_column=Column("ld_120", Integer), alias="强势轮动_120")
    ld_240: int = Field(sa_column=Column("ld_240", Integer), alias="强势轮动_240")


class DailyHjkpoolsLevel5(SQLModel, table=True):
    __tablename__: str = "daily_hjkpools_level5_view"
    date_int: int = Field(primary_key=True)
    date: datetime.date = Field(default_factory=datetime.date.today)
    filter_conditions: str = Field(
        default="",
        description="""
        首位:0-长线,1-小时,
        二位:1-超跌选股,2-震荡选股,3-突破选股,
        三位:0-不含准备股票,1-包含准备股票,
        末位:0-普通版,1-私享家版
        """,
    )
    stock_pools: str = Field(default="", description="股票池，多个逗号分隔")


class DailyHjkLevel5(SQLModel, table=True):
    __tablename__ = "daily_hjk_level5_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="报告期（整型，格式YYYYMMDD）")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    hjk_13: float = Field(sa_column=Column("hjk_13", Float, default=0.0), alias="13日黄金坑")
    hjk_34: float = Field(sa_column=Column("hjk_34", Float, default=0.0), alias="34日黄金坑")
    hjk_60: float = Field(sa_column=Column("hjk_60", Float, default=0.0), alias="60日黄金坑")


# ──────────── 外资机构榜表 ────────────
class WzHoldingPoolsLevel15(SQLModel, table=True):
    __tablename__ = "wz_holding_pools_level15_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="报告期（整型，格式YYYYMMDD）")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    pool_type: int = Field(
        sa_column=Column("pool_type", Integer, default=0), alias="入选外资池类型"
    )  # 外资加仓池=5/外资持股池=6/外资入场池=7
    pp_count: int = Field(sa_column=Column("pp_count", Integer, default=0), alias="外资持股个数")
    pp_ratio: float = Field(sa_column=Column("pp_ratio", Float, default=0.0), alias="外资持股占比")
    pp_funds: int = Field(sa_column=Column("pp_funds", Integer, default=0), alias="外资投入总资金")
    last_quarter_price: float = Field(sa_column=Column("last_quarter_price", Float, default=0.0), alias="上季平均股价")
    last_in_date: int = Field(sa_column=Column("last_in_date", Integer, default=0), alias="最新进场时间")
    hold_volume: int = Field(sa_column=Column("hold_volume", Integer, default=0), alias="QFII持股总量")
    tq: int = Field(sa_column=Column("tq", Integer, default=0), alias="流通盘")


# ──────────── 融资融券日统计表 ────────────
class DailyRzrqStatLevel15(SQLModel, table=True):
    __tablename__ = "daily_rzrq_stat_level15_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="日期")
    rzrq_net_amt: int = Field(default=0, description="融资融券净额")
    rz_net_amt: int = Field(default=0, description="融资净额")
    rq_net_amt: int = Field(default=0, description="融券净额")
    rzrq_intensity: int = Field(default=0, description="融资融券力度")


# ──────────── 大宗交易统计表 ────────────
class DailyDzjmStockStatLevel15(SQLModel, table=True):
    __tablename__ = "dzjm_stock_stat_level15_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="日期整数")
    date: datetime.date = Field(sa_column=Column("date", Date, default=datetime.date.today), alias="日期")
    amount: int = Field(default=0, description="近一季度大宗成交额")
    trade_profit_rate: float = Field(
        sa_column=Column("trade_profit_rate", Float, default=0.0), alias="近一季度大宗收益率"
    )
    trade_count: int = Field(sa_column=Column("trade_count", Integer, default=0), alias="近一季度交易次数")


# ──────────── 大宗交易明细表 ────────────
class DailyDzjmStockDetailLevel15(SQLModel, table=True):
    __tablename__ = "dzjm_stock_detail_level15_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="日期整数")
    date: datetime.date = Field(sa_column=Column("date", Date, default=datetime.date.today), alias="大宗交易最新日期")
    trade_type: int = Field(
        sa_column=Column("trade_type", Integer, default=0),
        default=0,
        alias="大宗交易类型",
        schema_extra={
            "render_type": "bitmask",
            "options": {
                0: "其他",
                1: "机构交易(机构)",
                2: "焦点交易(焦点)",
                4: "对倒交易(对倒)",
                8: "股东席位(股东)",
                16: "溢价涨停(溢停)",
                32: "折价跌停(折停)",
            },
        },
    )
    trade_act: int = Field(sa_column=Column("trade_act", Integer, default=0), alias="大宗交易行为")
    trade_price: float = Field(sa_column=Column("trade_price", Float, default=0.0), alias="大宗交易成交价")
    trade_vol: int = Field(sa_column=Column("trade_vol", Integer, default=0), alias="大宗交易成交量")
    trade_amt: int = Field(sa_column=Column("trade_amt", Integer, default=0), alias="大宗交易成交额")
    zhanpan_rate: float = Field(sa_column=Column("zhanpan_rate", Float, default=0.0), alias="大宗交易占盘比")
    trade_rate: float = Field(sa_column=Column("trade_rate", Float, default=0.0), alias="大宗交易成交倍率")


# ──────────── 席位-披露上榜 ────────────
class XwlhSeatStatLevel15(SQLModel, table=True):
    __tablename__ = "xwlh_seat_stat_level15_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    hot_money_name: Optional[str] = Field(sa_column=Column("hot_money_name", String), alias="席位龙虎榜上榜游资")
    reasonid: Optional[int] = Field(default=0, description="异动原因ID")
    all_buying: Optional[int] = Field(sa_column=Column("all_buying", Integer), alias="席位总买")
    all_selling: Optional[int] = Field(sa_column=Column("all_selling", Integer), alias="席位总卖")
    continous_days: Optional[int] = Field(sa_column=Column("continous_days", Integer), alias="席位龙虎榜连续上榜天数")
    reason: Optional[str] = Field(sa_column=Column("reason", String), alias="异动原因")


# ──────────── 席位前五买榜/卖榜(明细) ────────────
class XwlhSeatdetailLevel15(SQLModel, table=True):
    __tablename__ = "xwlh_seat_detail_level15_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    seat_id: Optional[str] = Field(sa_column=Column("seat_id", String), alias="席位ID")
    seat_name: Optional[str] = Field(sa_column=Column("seat_name", String), alias="席位名称")
    buying_amt: Optional[int] = Field(sa_column=Column("buying_amt", Integer), alias="总买")
    selling_amt: Optional[int] = Field(sa_column=Column("selling_amt", Integer), alias="总卖")
    net_amt: Optional[int] = Field(sa_column=Column("net_amt", Integer), alias="净买额")
    seattype: Optional[int] = Field(
        sa_column=Column("type", Integer), alias="席位类型"
    )  # 基础席位类型，区分不同类型的交易席位
    starseattype: Optional[int] = Field(
        sa_column=Column("starseattype", Integer),
        alias="明星席位类型",
        schema_extra={
            "render_type": "bitmask",
            "options": {
                1: "上涨明星席位",
                2: "下跌明星席位",
                4: "活跃明星席位",
            },
        },
    )
    hot_money_id: Optional[str] = Field(sa_column=Column("hot_money_id", String), alias="游资ID")
    hot_money_name: Optional[str] = Field(sa_column=Column("hot_money_name", String), alias="游资名称")
    reasonid: Optional[int] = Field(default=0, description="异动原因ID")


# ──────────── 私募英雄榜 ────────────
class SmxyHoldingStatLevel15(SQLModel, table=True):
    __tablename__ = "smxy_holding_stat_level15_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(
        sa_column=Column("date_int", Integer, primary_key=True), alias="交易日期（整型，格式YYYYMMDD）"
    )
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    smyx_num: Optional[int] = Field(sa_column=Column("smyx_num", Integer), alias="私募英雄榜持有本股私募数")
    smyx_zc_num: Optional[int] = Field(sa_column=Column("smyx_zc_num", Integer), alias="私募英雄榜增仓私募数")
    buy_amount: Optional[int] = Field(sa_column=Column("buy_amount", Integer), alias="私募英雄榜私募增仓总额")
    buy_volume: Optional[int] = Field(sa_column=Column("buy_volume", Integer), alias="私募英雄榜增仓股数")
    sell_amount: int = Field(default=0, description="私募英雄榜私募减仓总额")
    sell_volume: int = Field(default=0, description="私募英雄榜减仓股数")


# ──────────── 高管交易榜 ────────────
class GgjyHoldStatLevel15(SQLModel, table=True):
    __tablename__ = "ggjy_hold_stat_level15_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="日期整数")
    date: datetime.date = Field(sa_column=Column("date", Date, default=datetime.date.today), alias="日期")
    zc_amt: int = Field(sa_column=Column("zc_amt", Integer, default=0), alias="二级市场180天增仓额")
    zc_vol: int = Field(sa_column=Column("zc_vol", Integer, default=0), alias="二级市场180天增仓数量")
    jc_amt: int = Field(default=0, description="二级市场180天减仓额")
    jc_vol: int = Field(default=0, description="二级市场180天减仓数量")


# ──────────── 研报监控 ────────────
class ReportHotstocksLevel15(SQLModel, table=True):
    __tablename__ = "report_organs_level15_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="日期整数")
    date: datetime.date = Field(default=datetime.date.today, description="发布时间")
    analystcode: str = Field(sa_column=Column("analystcode", String, default=""), alias="分析师ID")
    analystname: str = Field(sa_column=Column("analystname", String, default=""), alias="分析师名称")
    analystnum: int = Field(sa_column=Column("analystnum", Integer, default=0), alias="关注分析师数量")
    organs: int = Field(sa_column=Column("organs", Integer, default=0), alias="接待机构数")
    researchfreq: int = Field(sa_column=Column("researchfreq", Integer, default=0), alias="调研频率")
    title: str = Field(sa_column=Column("title", String, default=""), alias="研报标题")


class F10PositionTrend(SQLModel, table=True):
    __tablename__: str = "f10_position_trend_view"
    secucode: str = Field(primary_key=True)
    date: datetime.date = Field(default_factory=datetime.date.today)
    type: int = Field(default=0)
    date_int: int = Field(default=0)
    num: int = Field(default=0)


# ──────────── 智能alpha ────────────
class AiStockStrategyLevel20(SQLModel, table=True):
    __tablename__ = "ai_stock_strategy_level20_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="交易日期")
    date_int: Optional[int] = Field(sa_column=Column("date_int", Integer), alias="交易日期（整型，格式YYYYMMDD）")
    strategies: Optional[str] = Field(description="智能阿尔法策略评级")
    synthetic: Optional[float] = Field(sa_column=Column("synthetic", Float), alias="综合值")
    succ_rate: Optional[float] = Field(sa_column=Column("succ_rate", Float), alias="成功率")
    profit_rate: Optional[float] = Field(sa_column=Column("profit_rate", Float), alias="次均收益")


# ──────────── 供求资金表 ────────────
class DailyGqzjLevel20(SQLModel, table=True):
    __tablename__ = "daily_gqzj_level20_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="日期整数")
    date: datetime.date = Field(sa_column=Column("date", Date, default=datetime.date.today), alias="日期")
    gqzj: float = Field(sa_column=Column("gqzj", Float, default=0.0), alias="供求资金")
    flzj_lsdy: float = Field(sa_column=Column("flzj_lsdy", Float, default=0.0), alias="拉升/打压")
    flzj_gfsd: float = Field(sa_column=Column("flzj_gfsd", Float, default=0.0), alias="跟风/杀跌")
    flzj_cdpy: float = Field(sa_column=Column("flzj_cdpy", Float, default=0.0), alias="抄底/抛压")


# ──────────── 停板风向标 ────────────
class AiForecastZdLevel20(SQLModel, table=True):
    __tablename__ = "ai_forecast_zd_level20_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(
        sa_column=Column("date_int", Integer, primary_key=True), alias="交易日期（整型，格式YYYYMMDD）"
    )
    date: datetime.date = Field(sa_column=Column("date", Date, default=datetime.date.today), alias="交易日期")
    zdt_type: int = Field(sa_column=Column("type", Integer, default=0), alias="停板风向标涨跌停类型")
    fc_zdt_pro: float = Field(default=0.0, description="停板风向标15分钟涨/跌停概率")
    zd_in_time: int = Field(default=0, description="停板风向标首次入池时间")
    zd_out_time: int = Field(default=0, description="停板风向标最后出池时间")


# ──────────── 资金异动标识 ────────────
class FundMoveFlagLevel0(SQLModel, table=True):
    __tablename__ = "fund_move_flag_level0_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="整数日期")
    date: datetime.date = Field(sa_column=Column("date", Date, default=datetime.date.today), alias="日期")

    zhu_flag: int = Field(sa_column=Column("zhu_flag", Integer, default=0), alias="主力资金异动流入流出状态")
    duo_flag: int = Field(sa_column=Column("duo_flag", Integer, default=0), alias="多空资金异动流入流出状态")
    gan_flag: int = Field(sa_column=Column("gan_flag", Integer, default=0), alias="敢死队资金异动流入流出状态")


class HwListLevel0(SQLModel, table=True):
    __tablename__: str = "xxb_hwlist_level0_view"

    hw_id: int = Field(sa_column=Column("id", Integer, primary_key=True), alias="热词ID")
    hw_name: str = Field(sa_column=Column("hw_name", String, primary_key=True), alias="热词名称")
    hw_state: int = Field(sa_column=Column("state", Integer), alias="热词状态")
    hw_days: int = Field(sa_column=Column("days", Integer), alias="热词天数")
    hw_category: str = Field(sa_column=Column("category", Integer), alias="类别（0-行业板块，1-概念板块，2-网络热词）")


class HwStocksLevel0(SQLModel, table=True):
    __tablename__ = "xxb_hw_stocks_level0_view"

    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    hot_value: float = Field(sa_column=Column("hot_value", Float), alias="热度值")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="整数日期")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="日期")
    state: int = Field(sa_column=Column("state", Integer), alias="股票状态")
    category: str = Field(sa_column=Column("category", String), alias="类别")
    hw_name_to_stock: str = Field(sa_column=Column("hw_name", String), alias="股票所属消息榜热词")


class BoardFinancialInfo(SQLModel, table=True):
    __tablename__: str = "board_f10_main_indicator_view"
    secucode: str = Field(primary_key=True)
    date: datetime.date = Field(primary_key=True)
    date_int: int = Field(default=0)
    income_total: float
    parent_net_profit: float


class BoardFinancialPropertyBalanceView(SQLModel, table=True):
    __tablename__: str = "board_f10_property_balance_view"
    secucode: str = Field(primary_key=True)
    date: datetime.date = Field(primary_key=True)
    total_assets: int = Field(default=0, description="总资产")
    total_liability: int = Field(default=0, description="总负债")


# ──────────── 量能活跃度 ────────────
class DailyDcyfLevel5(SQLModel, table=True):
    __tablename__ = "daily_dcyf_level5_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="整数日期")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="日期")
    dcyf: int = Field(sa_column=Column("dcyf", Float), alias="量能活跃度")


class F10ForecastIndexView(SQLModel, table=True):
    __tablename__ = "f10_forecast_index_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date_int: int = Field(sa_column=Column("date_int", Integer, primary_key=True), alias="报告期（整型，格式YYYYMMDD）")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    forecast_income: int = Field(sa_column=Column("forecast_income", Integer, default=0), alias="预测营业收入")
    forecast_income_rate: float = Field(
        sa_column=Column("forecast_income_rate", Float, default=0.0), alias="预测营业收入增长率"
    )
    forecast_profit: int = Field(sa_column=Column("forecast_profit", Integer, default=0), alias="预测利润总额")
    forecast_net_profit: int = Field(sa_column=Column("forecast_net_profit", Integer, default=0), alias="预测净利润")
    forecast_net_profit_rate: float = Field(
        sa_column=Column("forecast_net_profit_rate", Float, default=0.0), alias="预测净利润增长率"
    )
    forecast_per_cashflow: float = Field(
        sa_column=Column("forecast_per_cashflow", Float, default=0.0), alias="预测每股现金流"
    )
    forecast_per_net_assets: float = Field(
        sa_column=Column("forecast_per_net_assets", Float, default=0.0), alias="预测每股净资产"
    )
    forecast_net_assets_rate: float = Field(
        sa_column=Column("forecast_net_assets_rate", Float, default=0.0), alias="预测净资产收益率"
    )
    forecast_dynamic_pe: float = Field(
        sa_column=Column("forecast_dynamic_pe", Float, default=0.0), alias="预测市盈率（动态）"
    )


class XxbShareholdersLevel0View(SQLModel, table=True):
    __tablename__ = "xxb_shareholders_level0_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="股票代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    shareholder: str = Field(sa_column=Column("shareholder", String, default=""), alias="高管名字")
    share_holders_job: str = Field(sa_column=Column("job", String, default=""), alias="高管职位")
    share_holders_change_volume: int = Field(sa_column=Column("volume", Integer, default=0), alias="高管增减持数量")
    share_holders_change_detail: str = Field(sa_column=Column("detail", String, default=""), alias="高管增减持原因")


class DailyDhydLevel5(SQLModel, table=True):
    __tablename__ = "daily_dhyd_level5_view"
    secucode: str = Field(sa_column=Column("secucode", String, primary_key=True), alias="代码")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="报告期")
    date_int: int = Field(sa_column=Column("date_int", Integer), alias="报告期（整型，格式YYYYMMDD）")
    dhyd13: int = Field(sa_column=Column("dhyd13", Float), alias="股价活跃度13")
    dhyd34: int = Field(sa_column=Column("dhyd34", Float), alias="股价活跃度34")


class HotInfoView(SQLModel, table=True):
    __tablename__: str = "hot_info_view"
    type: int = Field(sa_column=Column("type", Integer, nullable=False), alias="类型", description="数据类型标识")
    date: datetime.date = Field(sa_column=Column("date", Date), alias="日期")
    secucode: str = Field(sa_column=Column("data_code", String, primary_key=True), alias="代码")
    date_int: int = Field(
        sa_column=Column("date_int", Integer, nullable=False), alias="日期整型", description="整数日期格式 YYYYMMDD"
    )
    sortid: int = Field(sa_column=Column("sortid", Integer, default=0), alias="排序ID", description="排序标识")


single_date_views = [AStockBasic, AStockMarketCur, GgjyHoldStatLevel15]

multidate_views = [
    AStockMarket,
    RanklistDuo1Level0,
    RanklistDuo3Level0,
    RanklistGan13Level0,
    DailyCwDataLevel0,
    Fund3cjLevel0,
    DailyZjdxStatLevel5,
    DailyRzrqStatLevel15,
    AiForecastZdLevel20,
    AiStockStrategyLevel20,
    DailyQlldLevel5,
    DailyGqzjLevel20,
    XwlhSeatdetailLevel15,
    XwlhSeatStatLevel15,
    FundMoveFlagLevel0,
    DailyHjkpoolsLevel5,
    DailyDzjmStockDetailLevel15,
    DailyHjkLevel5,
    DailyDcyfLevel5,
    DailyQlstockBasicLevel5,
    DailyDhydLevel5,
    HotInfoView,
]
sparsedate_views = [
    AStockFinancial,
    AStockCashFlow,
    AStockStructure,
    AStockShareHolder,
    AStockDividenedDetail,
    AStockDividenedAddition,
    AStockDividenedAllotment,
    AStockPositionSum,
    AStockProfitView,
    DailyGzkjLevel0,
    DailyThreeMethodLevel0,
    StockFinancialPropertyBalanceView,
    WzHoldingPoolsLevel15,
    SmxyHoldingStatLevel15,
    DailyDzjmStockStatLevel15,
    ReportHotstocksLevel15,
    HwStocksLevel0,
    F10ForecastIndexView,
    XxbShareholdersLevel0View,
]

# 多主键表,表名+额外主键（即除secucode和date以外的主键）
extra_primary_key_views = {
    WzHoldingPoolsLevel15: [WzHoldingPoolsLevel15.pool_type],
    XwlhSeatdetailLevel15: [XwlhSeatdetailLevel15.seattype, XwlhSeatdetailLevel15.reasonid],
    XwlhSeatStatLevel15: [XwlhSeatStatLevel15.reasonid],
    AiForecastZdLevel20: [AiForecastZdLevel20.zdt_type],
}

# 盘后更新表列表（稠密表，稀疏表不需要加）
AFTER_MARKET_TABLES = [
    DailyRzrqStatLevel15,
    AiStockStrategyLevel20,
    XwlhSeatStatLevel15,
    XwlhSeatdetailLevel15,
    DailyDzjmStockDetailLevel15,
]

# 使用表内日期作为时间的视图(季度)
special_views = [
    # 季度
    AStockFinancial,
    AStockCashFlow,
    AStockStructure,
    AStockShareHolder,
    AStockDividenedDetail,
    AStockDividenedAddition,
    AStockDividenedAllotment,
    AStockPositionSum,
    AStockProfitView,
    StockFinancialPropertyBalanceView,
    WzHoldingPoolsLevel15,
    SmxyHoldingStatLevel15,
    DailyDzjmStockDetailLevel15,
]


class AStockGlobalQuery:
    """
    全局查询类，用于查询股票相关数据
    """

    # 工具方法

    # _union和_intersect待完善，交付日期不足以改好，先按老逻辑特殊处理多主键表
    # 状态：_intersect基本已经改好，_union还需要大幅调整
    # _intersect遗留问题：多日期条件难以支持，比如9月1日总卖大于1e且9月2日总卖小于2e，如果按日期联查询直接为空，如果只按股票代码联表又会笛卡尔乘积生成脏数据
    @staticmethod
    def _union(*queries):
        if not queries:
            raise ValueError("union操作至少需要一个查询")

        # 生成唯一 ID，避免别名冲突
        unique_id = uuid.uuid4().hex[:6]

        # 分类查询：单列 vs 多列
        single_col_queries = []
        multi_col_queries = []
        for q in queries:
            if len(list(q.selected_columns)) == 1:
                single_col_queries.append(q)
            else:
                multi_col_queries.append(q)

        # 处理单列查询（假设单列为 secucode）
        single_union = None
        if single_col_queries:
            if len(single_col_queries) == 1:
                single_union = single_col_queries[0]
            else:
                single_union = union(*single_col_queries)

        # 处理多列查询：secucode 交集过滤 + UNION ALL (NULL 填充) + GROUP BY 公共键 + MAX(聚合合并)
        multi_join = None
        if multi_col_queries:
            # 检查所有 q 都有 secucode
            for q in multi_col_queries:
                if "secucode" not in q.c:
                    raise ValueError(f"Query {q} missing 'secucode' column")

            # 收集所有唯一列名（按 q 顺序逐个判断 append，避免重复）
            all_col_names = []
            seen = set()
            for q in multi_col_queries:
                for col_name in q.c.keys():
                    if col_name not in seen:
                        all_col_names.append(col_name)
                        seen.add(col_name)

            # 计算公共键：所有查询共有的列（至少 secucode，用于 GROUP BY）
            all_col_sets = [set(q.c.keys()) for q in multi_col_queries]
            common_keys = sorted(set.intersection(*all_col_sets))

            if not common_keys:
                raise ValueError("No common columns across all multi-column queries")

            # 构建 UNION ALL selects（每个 q 过滤 secucode + NULL 填充）
            union_selects = []
            for q in multi_col_queries:
                # 构建 select_list
                select_list = []
                for col_name in all_col_names:
                    if col_name in q.c:
                        select_list.append(q.c[col_name])
                    else:
                        select_list.append(null().label(col_name))
                # 直接构建过滤 + 选择（避免嵌套子查询）
                this_select = select(*select_list).select_from(q)
                union_selects.append(this_select)

            # UNION ALL
            if len(union_selects) == 1:
                union_all_sub = union_selects[0]
            else:
                union_all_sub = union_all(*union_selects)

            # 转子查询，为 GROUP BY 准备
            union_all_subq = union_all_sub.subquery(f"union_all_{unique_id}")

            multi_join = union_all_subq

        # 合并单列和多列结果：过滤 multi_join 到 single secucode（无 UNION，因为 single 只用于过滤）
        if single_union is not None and multi_join is not None:
            # 过滤 multi_join 到 single_intersect 中的 secucode

            all_col_names = list(multi_join.c.keys())
            common_keys = ["secucode"]
            union_selects = []
            # 步骤1: 扩展 single 为“伪多列”（照搬 select_list 构建，但单次）
            single_subq = single_union.subquery(f"single_union_{unique_id}")
            single_select_list = []
            for col_name in all_col_names:
                if col_name in single_subq.c:  # single 只会有 'secucode'
                    single_select_list.append(single_subq.c[col_name])
                else:
                    single_select_list.append(null().label(col_name))
            single_extended = select(*single_select_list).select_from(single_subq)
            union_selects.append(single_extended)  # 添加到 union_selects，像一个“q”

            # 步骤2: 添加 multi_join 的 select（照搬 this_select 风格，无需 NULL 填充，因为已匹配列宽）
            multi_select = select(multi_join)  # 直接 select(multi_join)，列已对齐
            union_selects.append(multi_select)

            union_all_sub = union_all(*union_selects)

            # 步骤4: 转子查询，为 GROUP BY 准备（照搬）
            union_all_subq = union_all_sub.subquery(f"multi_single_union_all_{unique_id}")

            return select(union_all_subq)

        elif single_union is not None:
            return single_union
        elif multi_join is not None:
            # 返回显式 SELECT（保持列顺序）
            all_col_names = [col.key for col in multi_join.c]  # 或者直接用 list(multi_join.c.keys())
            return select(*[multi_join.c[col] for col in all_col_names]).select_from(multi_join)
        else:
            raise NotImplementedError("未能成功union任何给定对象")

    @staticmethod
    def _intersect(*queries):
        if not queries:
            raise ValueError("intersect操作至少需要一个查询")

        # 生成唯一 ID，避免别名冲突
        unique_id = uuid.uuid4().hex[:6]

        # 分类查询：单列 vs 多列
        single_col_queries = []
        multi_col_queries = []
        for q in queries:
            if len(list(q.selected_columns)) == 1:
                single_col_queries.append(q)
            else:
                multi_col_queries.append(q)

        # 处理单列查询（假设单列为 secucode）
        single_intersect = None
        if single_col_queries:
            if len(single_col_queries) == 1:
                single_intersect = single_col_queries[0]
            else:
                single_intersect = intersect(*single_col_queries)

        # ============================================
        # ========== 新版 multi_col_queries ==========
        # ============================================

        multi_join = None
        if multi_col_queries:
            # 检查所有 q 都有 secucode（你原本的逻辑）
            for q in multi_col_queries:
                if "secucode" not in q.c:
                    raise ValueError(f"Query {q} missing 'secucode' column")

            # ----------- 新增 merge_two_queries(A, B) --------------
            def merge_two_queries(A, B):
                uid = uuid.uuid4().hex[:4]

                # --- 强制转换为 FROM ---
                A = A if not hasattr(A, "selected_columns") else A.subquery(f"a_{uid}")
                B = B if not hasattr(B, "selected_columns") else B.subquery(f"b_{uid}")

                # 取公共列（至少 secucode）
                common_cols = [c for c in A.c.keys() if c in B.c.keys()]
                if not common_cols:
                    raise ValueError("Multi-col intersect requires at least one shared column")

                # ---- 构建全列合集（按 A → B 顺序，避免重复） ----
                all_cols = []
                seen = set()
                for q in (A, B):
                    for col in q.c.keys():
                        if col not in seen:
                            all_cols.append(col)
                            seen.add(col)

                # ======== 1) 只在 A 的 key ========
                only_A = (
                    select(*[A.c[col] if col in A.c else null().label(col) for col in all_cols])
                    .where(A.c["date"].notin_(select(B.c["date"])))
                    .select_from(A)
                )

                # ======== 2) 只在 B 的 key ========
                only_B = (
                    select(*[B.c[col] if col in B.c else null().label(col) for col in all_cols])
                    .where(B.c["date"].notin_(select(A.c["date"])))
                    .select_from(B)
                )

                # ======== 3) AB 都有：公共列 join → 笛卡尔组合 ========
                join_cond = and_(*[A.c[c] == B.c[c] for c in common_cols])

                both = select(
                    *[(A.c[col] if col in A.c else B.c[col] if col in B.c else null()).label(col) for col in all_cols]
                ).select_from(A.join(B, join_cond))

                # 合并三个部分
                merged = union_all(only_A, only_B, both).subquery(f"merge_{uid}")
                return merged

            # ========== 按 pairwise reduce 合并 ==========
            merged = multi_col_queries[0]
            for q in multi_col_queries[1:]:
                merged = merge_two_queries(merged, q)

            multi_join = merged

        # ============================================
        # ========== 原逻辑：过滤单列 + 最终输出 ==========
        # ============================================

        if single_intersect is not None and multi_join is not None:
            # 过滤 multi_join 到 single_intersect 中的 secucode
            single_intersect_sub = single_intersect.subquery(f"single_intersect_{unique_id}")
            filtered_multi = (
                select(*[multi_join.c[col] for col in multi_join.c.keys()])
                .where(multi_join.c.secucode.in_(select(single_intersect_sub.c.secucode)))
                .select_from(multi_join)
                .subquery(f"filtered_multi_{unique_id}")
            )

            all_col_names = list(multi_join.c.keys())
            return select(*[filtered_multi.c[col] for col in all_col_names]).select_from(filtered_multi)

        elif single_intersect is not None:
            return single_intersect

        elif multi_join is not None:
            all_col_names = list(multi_join.c.keys())
            return select(*[multi_join.c[col] for col in all_col_names]).select_from(multi_join)

        else:
            raise NotImplementedError("未能成功intersect任何给定对象")

    @staticmethod
    def _get_aftermarket_date_range(table, start_date: str, end_date: str, lookback_days: int = 0):
        """
        统一处理盘后表的日期范围逻辑

        对于盘后表的0d0d查询，返回该表的最新数据日期；
        对于其他情况，返回正常的日期范围。

        :参数:
            table: 表对象
            start_date: 开始日期字符串
            end_date: 结束日期字符串
            lookback_days: 需要向前查找的天数（用于窗口计算）

        :返回:
            (trade_start_original, trade_start_extended, trade_end, is_aftermarket_0d0d)
            - trade_start_original: 原始查询开始日期
            - trade_start_extended: 扩展的查询开始日期（用于窗口计算）
            - trade_end: 查询结束日期
            - is_aftermarket_0d0d: 是否为盘后表0d0d查询
        """
        if start_date == "0d" and end_date == "0d" and table in AFTER_MARKET_TABLES:
            # 对于盘后表的0d0d查询，获取该表全局最新数据的日期（不带股票筛选）
            latest_date_subquery = select(table.date).order_by(table.date.desc()).limit(1).scalar_subquery()
            # 扩展日期范围确保有足够的历史数据用于计算
            if lookback_days > 0:
                trade_start_extended = get_days_ago_date(lookback_days + 5)
            else:
                trade_start_extended = latest_date_subquery

            trade_start_original = latest_date_subquery  # 只返回最新日期的数据
            trade_end = latest_date_subquery  # 使用表的实际最新数据日期
            return trade_start_original, trade_start_extended, trade_end, True
        else:
            # 正常日期范围处理
            if lookback_days > 0:
                trade_start_extended = get_date(start_date, is_start=True, lookback_days=lookback_days)
            else:
                trade_start_extended = get_date(start_date, is_start=True)

            trade_start_original = get_date(start_date, is_start=True)
            trade_end = get_date(end_date)
            return trade_start_original, trade_start_extended, trade_end, False

    @staticmethod
    def _flatten_columns(columns):
        """展平嵌套的列列表，保持顺序"""

        def _flatten(items):
            for item in items:
                if isinstance(item, list):
                    yield from _flatten(item)
                else:
                    yield item

        return list(_flatten(columns))

    @staticmethod
    def _categorize_columns(columns):
        """对列进行分类：普通列、元组列、涉及的表"""
        ordered_columns = []
        seen_keys = []
        involved_tables = []
        tuple_columns = []
        seen_tuples = []

        for item in columns:
            # 生成唯一key用于去重
            if isinstance(item, tuple):
                # 元组格式: (function_name, field_name, params)
                # 对于元组，还要记录对应的字段名，用于与直接列进行去重
                field_name = item[1]
            elif hasattr(item, "key"):
                field_name = item.key
            else:
                field_name = str(item)

            # 检查是否已经存在相同字段名的列
            # 如果存在，优先保留非元组形式的直接列引用，因为它更简洁
            should_add = False
            if field_name not in seen_keys:
                # 字段名未出现过，直接添加
                should_add = True
            else:
                # 字段名已出现过，检查是否可以替换
                # 现有列是元组，新列是直接列 → 用直接列替换元组
                existing_item = None
                existing_index = -1
                for i, existing_key in enumerate(seen_keys):
                    if existing_key == field_name:
                        existing_item = ordered_columns[i]
                        existing_index = i
                        break

                if existing_item is not None:
                    # 如果现有的是直接列，新的是元组，则用元组替换直接列
                    if not isinstance(existing_item, tuple) and isinstance(item, tuple):
                        ordered_columns[existing_index] = item
                        seen_keys[existing_index] = field_name
                    # 其他情况保持现有列不变

            if should_add:
                seen_keys.append(field_name)
                ordered_columns.append(item)

            # 分类 (不管是否重复都要检查表)
            if isinstance(item, tuple):
                tup_key = (item[0], item[1])
                if tup_key not in seen_tuples:
                    seen_tuples.append(tup_key)
                    tuple_columns.append(item)
            elif hasattr(item, "class_") and item.class_ not in involved_tables:
                involved_tables.append(item.class_)

        return ordered_columns, involved_tables, tuple_columns

    @staticmethod
    def _build_table_cte(
        table, table_columns, required_cols, unique_id, trade_start, trade_end, subquery, start_date=None, end_date=None
    ):
        """为单个表构建CTE"""
        labeled_cols = [col.label(col.key) for col in required_cols + table_columns]
        cte_name = f"cte_{table.__name__}_{unique_id}"

        if table in sparsedate_views:
            # 稀疏表处理：forward fill
            return AStockGlobalQuery._build_sparse_table_cte(
                table, table_columns, required_cols, cte_name, unique_id, trade_start, trade_end, subquery
            )
        elif table in multidate_views:
            # 多日期表处理，使用传入的日期范围
            # 对于盘后表0d0d查询，trade_start和trade_end都是latest_date_subquery
            # between(same, same) 等价于 == same
            cte = (
                select(*labeled_cols)
                .where(table.date.between(trade_start, trade_end), table.secucode.in_(select(subquery.c.secucode)))
                .cte(cte_name)
            )
        else:
            # 单日期表
            cte = select(*labeled_cols).where(table.secucode.in_(select(subquery.c.secucode))).cte(cte_name)

        return cte

    @staticmethod
    def _build_sparse_table_cte(
        table, table_columns, required_cols, cte_name, unique_id, trade_start, trade_end, subquery
    ):
        """构建稀疏表的CTE（forward fill逻辑）"""
        if trade_start == trade_end:  # 简化判断，对于单日查询
            # 对于0d0d的情况，直接获取每个股票的最新数据
            latest_data_cte = (
                select(
                    table.secucode,
                    table.date,
                    *[col.label(col.key) for col in table_columns if col.key not in ["secucode", "date"]],
                    func.row_number().over(partition_by=table.secucode, order_by=desc(table.date)).label("rn"),
                )
                .where(table.secucode.in_(select(subquery.c.secucode)))
                .cte(f"latest_data_{table.__name__}_{unique_id}")
            )
            cte = (
                select(
                    latest_data_cte.c.secucode,
                    latest_data_cte.c.date,
                    *[latest_data_cte.c[col.key] for col in table_columns if col.key not in ["secucode", "date"]],
                )
                .where(latest_data_cte.c.rn == 1)
                .cte(cte_name)
            )
        else:
            # 标准稀疏表处理：生成日期网格并forward fill
            dates_cte = (
                select(AStockMarket.date.label("date"))
                .where(AStockMarket.date.between(trade_start, trade_end))
                .distinct()
                .cte(f"dates_{table.__name__}_{unique_id}")
            )

            trade_secucode_cte = select(dates_cte.c.date, subquery.c.secucode).cte(
                f"dates_{table.__name__}_secucode_{unique_id}"
            )

            joined = (
                select(
                    trade_secucode_cte.c.date.label("current_date"),
                    trade_secucode_cte.c.secucode.label("secucode"),
                    *[col.label(col.key) for col in table_columns if col.key not in ["secucode", "date"]],
                    table.date,
                    func.max(table.date)
                    .over(partition_by=[trade_secucode_cte.c.date, trade_secucode_cte.c.secucode])
                    .label("max_date"),
                )
                .select_from(
                    trade_secucode_cte.outerjoin(
                        table,
                        and_(table.secucode == trade_secucode_cte.c.secucode, table.date <= trade_secucode_cte.c.date),
                    )
                )
                .cte(f"cte_{table.__name__}_ranked_{unique_id}")
            )

            cte = (
                select(
                    joined.c.current_date.label("date"),
                    joined.c.secucode,
                    *[joined.c[col.key] for col in table_columns if col.key not in ["secucode", "date"]],
                )
                .where(joined.c.date == joined.c.max_date)
                .cte(cte_name)
            )

        return cte

    # 带中间计算过程的示例demo
    # @staticmethod
    # def get_d():
    #     """
    #     :术语名称:
    #         股票代码
    #     :术语解释:
    #         股票代码
    #     :功能:
    #         获取股票代码字段
    #     """
    #     expr = ((AStockBasic.issue_estimate + AStockBasic.issue_winning_rate)/2).label("haha")
    #     return expr

    # 老版本全表联方法
    # @staticmethod
    # def query_columns(columns, start_date: str , end_date: str , secucode_query=None):
    #     if secucode_query is None:
    #         raise ValueError("secucode_query must be provided")

    #     subquery = secucode_query.subquery()

    #     single_date_views = [AStockBasic, AStockMarketCur]
    #     multidate_views = [AStockMarket,AStockFinancial,DailyLongShortFlowRanking,
    #                        ThreeDayLongShortFlowRanking,ThirteenDayDaredevilFundsRanking,
    #                        ThreeLocks,InvestmentDecision,ValuationSpace]

    #     # 确定 columns 中涉及的唯一表
    #     involved_tables = set(col.class_ for col in columns)  # 假设 columns 是 Table.column 格式，col.class_ 获取表/模型

    #     # 开始构建查询
    #     query = select(*columns)

    #     # 动态确定基础表
    #     base_table = None
    #     needs_date_filter = False

    #     # 检查是否涉及多日期表，并优先选择 base_table
    #     for table in involved_tables:
    #         if table in multidate_views:
    #             needs_date_filter = True
    #             if base_table is None:
    #                 base_table = table  # 优先选多日期表作为 base_table
    #         elif table in single_date_views:
    #             if base_table is None:
    #                 base_table = table  # 否则选单日期表

    #     if base_table is None:
    #         raise ValueError("No valid base table found from columns")

    #     query = query.select_from(base_table)

    #     # 动态连接其他表，根据表类型决定 join 条件
    #     for table in involved_tables:
    #         if table != base_table:
    #             # 默认 join 条件：基于 code
    #             join_condition = base_table.secucode == table.secucode

    #             # 如果 base_table 和 table 都是 multidate_views，添加日期相等条件
    #             if base_table in multidate_views and table in multidate_views:
    #                 join_condition &= base_table.date == table.date

    #             # 执行 join
    #             query = query.join(table, join_condition)

    #     # 添加 code 子查询过滤
    #     query = query.join(subquery, base_table.secucode == subquery.c.secucode)

    #     # 如果需要，添加日期范围过滤（针对 multidate_views 表）
    #     if needs_date_filter:
    #         # 假设多日期表有 date 字段；如需泛化可调整
    #         for table in multidate_views:
    #             if table in involved_tables:
    #                 start_date = get_date(start_date, is_start=True)
    #                 end_date = get_date(end_date)
    #                 query = query.where(table.date.between(start_date, end_date))

    #     if AStockMarket.date in columns:
    #         query = query.order_by(desc(AStockMarket.date))

    #     return query

    @staticmethod
    def get_secucode(mode: int = 0):
        """
        :术语名称:
            股票代码
        :术语解释:
            股票代码
        :功能:
            获取股票代码字段
        """
        if mode == 1:
            return AStockMarketCur.secucode
        elif mode == 2:
            return AStockFinancial.secucode
        else:
            return AStockMarket.secucode

    @staticmethod
    def get_date(mode: int = 0):
        """
        :术语名称:
            交易日期
        :术语解释:
            交易日期
        :功能:
            获取交易日期字段
        """
        if mode == 1:
            return AStockMarketCur.date
        elif mode == 2:
            return AStockFinancial.date
        else:
            return AStockMarket.date

    @staticmethod
    def get_close(mode: int = 0):
        """
        :术语名称:
            收盘价(股价)
        :术语解释:
            收盘价(股价)
        :功能:
            获取收盘价(股价)字段，可以回答股价
        """
        if mode == 1:
            return AStockMarketCur.close
        else:
            return AStockMarket.close

    @staticmethod
    def get_open(mode: int = 0):
        """
        :术语名称:
            开盘价
        :术语解释:
            开盘价
        :功能:
            获取开盘价字段
        """
        if mode == 1:
            return AStockMarketCur.open
        else:
            return AStockMarket.open

    @staticmethod
    def get_low(mode: int = 0):
        """
        :术语名称:
            最低价
        :术语解释:
            最低价
        :功能:
            获取最低价字段
        """
        if mode == 1:
            return AStockMarketCur.low
        else:
            return AStockMarket.low

    @staticmethod
    def get_high(mode: int = 0):
        """
        :术语名称:
            最高价
        :术语解释:
            最高价
        :功能:
            获取最高价字段
        """
        if mode == 1:
            return AStockMarketCur.high
        else:
            return AStockMarket.high

    @staticmethod
    def get_amount(mode: int = 0):
        """
        :术语名称:
            成交额
        :术语解释:
            成交额
        :功能:
            获取成交额字段
        """
        if mode == 1:
            return AStockMarketCur.amount
        else:
            return AStockMarket.amount

    @staticmethod
    def get_volume(mode: int = 0):
        """
        :术语名称:
            成交量
        :术语解释:
            成交量
        :功能:
            获取成交量字段
        """
        if mode == 1:
            return AStockMarketCur.volume
        else:
            return AStockMarket.volume

    @staticmethod
    def get_zt(mode: int = 0):
        """
        :术语名称:
            涨停
        :术语解释:
            涨停
        :功能:
            获取是否涨停字段
        """
        if mode == 1:
            return AStockMarketCur.zt
        else:
            return AStockMarket.zt

    @staticmethod
    def query_zt(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            涨停
        :术语解释:
            涨停
        :功能:
            筛选出或剔除指定日期范围内标记为涨停的股票。与get_zt()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选涨停，0=剔除涨停
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarket.zt == flag) & AStockMarket.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarket.secucode).where(condition)

    @staticmethod
    def get_dt(mode: int = 0):
        """
        :术语名称:
            跌停
        :术语解释:
            跌停
        :功能:
            获取是否跌停字段，仅用于跌停标识，严禁用于直接回答关于跌停数据的问题
        """
        if mode == 1:
            return AStockMarketCur.dt
        else:
            return AStockMarket.dt

    @staticmethod
    def query_dt(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            跌停
        :术语解释:
            跌停
        :功能:
            筛选出或剔除指定日期范围内标记为跌停的股票。与get_dt()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选跌停，0=剔除跌停
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarket.dt == flag) & AStockMarket.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarket.secucode).where(condition)

    @staticmethod
    def get_zhu(mode: int = 0):
        """
        :术语名称:
            主力资金占比
        :术语解释:
            主力资金占比
        :功能:
            获取主力资金占比字段
        """
        if mode == 1:
            return AStockMarketCur.zhu
        else:
            return AStockMarket.zhu

    @staticmethod
    def get_toratio(mode: int = 0):
        """
        :术语名称:
            换手率
        :术语解释:
            换手率
        :功能:
            获取换手率字段
        """
        if mode == 1:
            return AStockMarketCur.toratio
        else:
            return AStockMarket.toratio

    @staticmethod
    def get_ma_dtpl(mode: int = 0):
        """
        :术语名称:
            MA均线-多头排列
        :术语解释:
            MA均线-多头排列
        :功能:
            获取是否MA均线-多头排列字段
        """
        if mode == 1:
            return AStockMarketCur.ma_dtpl
        else:
            return AStockMarket.ma_dtpl

    @staticmethod
    def query_ma_dtpl(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            MA均线-多头排列(均线向上/上升通道/上升趋势)
        :术语解释:
            MA均线-多头排列(均线向上/上升通道/上升趋势)
        :功能:
            筛选出或剔除指定日期范围内标记为MA均线-多头排列的股票，多头排列、均线向上、上升通道、上升趋势语义一致都是用该函数筛选。与get_ma_dtpl()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选MA均线-多头排列，0=剔除MA均线-多头排列
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarket.ma_dtpl == flag) & AStockMarket.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarket.secucode).where(condition)

    @staticmethod
    def get_ma_ktpl(mode: int = 0):
        """
        :术语名称:
            MA均线-空头排列
        :术语解释:
            MA均线-空头排列
        :功能:
            获取是否MA均线-空头排列字段
        """
        if mode == 1:
            return AStockMarketCur.ma_ktpl
        else:
            return AStockMarket.ma_ktpl

    @staticmethod
    def query_ma_ktpl(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            MA均线-空头排列
        :术语解释:
            MA均线-空头排列
        :功能:
            筛选出或剔除指定日期范围内标记为MA均线-空头排列的股票。与get_ma_ktpl()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选MA均线-空头排列，0=剔除MA均线-空头排列
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarket.ma_ktpl == flag) & AStockMarket.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarket.secucode).where(condition)

    @staticmethod
    def get_ma_jincha(mode: int = 0):
        """
        :术语名称:
            MA均线-金叉
        :术语解释:
            MA均线-金叉
        :功能:
            获取是否MA均线-金叉字段
        """
        if mode == 1:
            return AStockMarketCur.ma_jincha
        else:
            return AStockMarket.ma_jincha

    @staticmethod
    def query_ma_jincha(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            MA均线-金叉
        :术语解释:
            MA均线-金叉
        :功能:
            筛选出或剔除指定日期范围内标记为MA均线-金叉的股票。与get_ma_jincha()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选MA均线-金叉，0=剔除MA均线-金叉
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarket.ma_jincha == flag) & AStockMarket.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarket.secucode).where(condition)

    @staticmethod
    def get_ma_sicha(mode: int = 0):
        """
        :术语名称:
            MA均线-死叉
        :术语解释:
            MA均线-死叉
        :功能:
            获取是否MA均线-死叉字段
        """
        if mode == 1:
            return AStockMarketCur.ma_sicha
        else:
            return AStockMarket.ma_sicha

    @staticmethod
    def query_ma_sicha(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            MA均线-死叉
        :术语解释:
            MA均线-死叉
        :功能:
            筛选出或剔除指定日期范围内标记为MA均线-死叉的股票。与get_ma_sicha()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选MA均线-死叉，0=剔除MA均线-死叉
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarket.ma_sicha == flag) & AStockMarket.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarket.secucode).where(condition)

    @staticmethod
    def get_pe_ratio_static(mode: int = 0):
        """
        :术语名称:
            市盈率
        :术语解释:
            市盈率
        :功能:
            获取市盈率字段
        """
        if mode == 1:
            return AStockMarketCur.pe_ratio_static
        else:
            return AStockMarket.pe_static

    @staticmethod
    def get_ma():
        """
        :术语名称:
            MA均线
        :术语解释:
            MA均线
        :功能:
            获取MA均线字段
        """
        return AStockMarketCur.ma

    @staticmethod
    def get_chip():
        """
        :术语名称:
            筹码分布
        :术语解释:
            筹码分布
        :功能:
            获取筹码分布字段
        """
        return AStockMarketCur.chip

    @staticmethod
    def get_r_price():
        """
        :术语名称:
            压力位价格
        :术语解释:
            压力位价格
        :功能:
            获取压力位价格字段
        """
        return AStockMarketCur.r_price

    @staticmethod
    def get_r_rate():
        """
        :术语名称:
            压力位幅度
        :术语解释:
            压力位幅度
        :功能:
            获取压力位幅度字段
        """
        return AStockMarketCur.r_rate

    @staticmethod
    def get_s_rate():
        """
        :术语名称:
            支撑位幅度
        :术语解释:
            支撑位幅度
        :功能:
            获取支撑位幅度字段
        """
        return AStockMarketCur.s_rate

    @staticmethod
    def get_s_price():
        """
        :术语名称:
            支撑位价格
        :术语解释:
            支撑位价格
        :功能:
            获取支撑位价格字段
        """
        return AStockMarketCur.s_price

    @staticmethod
    def get_hot_industry_trade():
        """
        :术语名称:
            热门行业
        :术语解释:
            热门行业
        :功能:
            获取热门行业字段
        """
        return AStockMarketCur.hot_industry_trade

    @staticmethod
    def get_hot_industry_concept():
        """
        :术语名称:
            热门概念
        :术语解释:
            热门概念
        :功能:
            获取热门概念字段
        """
        return AStockMarketCur.hot_industry_concept

    @staticmethod
    def get_amplitude():
        """
        :术语名称:
            振幅
        :术语解释:
            振幅
        :功能:
            获取振幅字段
        """
        return AStockMarketCur.amplitude

    @staticmethod
    def get_macd_jincha():
        """
        :术语名称:
            MACD-金叉
        :术语解释:
            MACD-金叉
        :功能:
            获取是否MACD-金叉字段
        """
        return AStockMarketCur.macd_jincha

    @staticmethod
    def query_macd_jincha(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            MACD-金叉
        :术语解释:
            MACD-金叉
        :功能:
            筛选出或剔除指定日期范围内标记为MACD-金叉的股票。与get_macd_jincha()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选MACD-金叉，0=剔除MACD-金叉
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.macd_jincha == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_macd_sicha():
        """
        :术语名称:
            MACD-死叉
        :术语解释:
            MACD-死叉
        :功能:
            获取是否MACD-死叉字段
        """
        return AStockMarketCur.macd_sicha

    @staticmethod
    def query_macd_sicha(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            MACD-死叉
        :术语解释:
            MACD-死叉
        :功能:
            筛选出或剔除指定日期范围内标记为MACD-死叉的股票。与get_macd_sicha()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选MACD-死叉，0=剔除MACD-死叉
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.macd_sicha == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_macd_dingbeili():
        """
        :术语名称:
            MACD-顶背离
        :术语解释:
            MACD-顶背离
        :功能:
            获取是否MACD-顶背离字段
        """
        return AStockMarketCur.macd_dingbeili

    @staticmethod
    def query_macd_dingbeili(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            MACD-顶背离
        :术语解释:
            MACD-顶背离
        :功能:
            筛选出或剔除指定日期范围内标记为MACD-顶背离的股票。与get_macd_dingbeili()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选MACD-顶背离，0=剔除MACD-顶背离
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.macd_dingbeili == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kdj_dibeili():
        """
        :术语名称:
            KDJ-底背离
        :术语解释:
            KDJ-底背离
        :功能:
            获取是否KDJ-底背离字段
        """
        return AStockMarketCur.kdj_dibeili

    @staticmethod
    def query_kdj_dibeili(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            KDJ-底背离
        :术语解释:
            KDJ-底背离
        :功能:
            筛选出或剔除指定日期范围内标记为KDJ-底背离的股票。与get_kdj_dibeili()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选KDJ-底背离，0=剔除KDJ-底背离
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kdj_dibeili == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kdj_dingbeili():
        """
        :术语名称:
            KDJ-顶背离
        :术语解释:
            KDJ-顶背离
        :功能:
            获取是否KDJ-顶背离字段
        """
        return AStockMarketCur.kdj_dingbeili

    @staticmethod
    def query_kdj_dingbeili(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            KDJ-顶背离
        :术语解释:
            KDJ-顶背离
        :功能:
            筛选出或剔除指定日期范围内标记为KDJ-顶背离的股票。与get_kdj_dingbeili()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选KDJ-顶背离，0=剔除KDJ-顶背离
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kdj_dingbeili == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kdj_jincha():
        """
        :术语名称:
            KDJ-金叉
        :术语解释:
            KDJ-金叉
        :功能:
            获取是否KDJ-金叉字段
        """
        return AStockMarketCur.kdj_jincha

    @staticmethod
    def query_kdj_jincha(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            KDJ-金叉
        :术语解释:
            KDJ-金叉
        :功能:
            筛选出或剔除指定日期范围内标记为KDJ-金叉的股票。与get_kdj_jincha()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选KDJ-金叉，0=剔除KDJ-金叉
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kdj_jincha == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kdj_sicha():
        """
        :术语名称:
            KDJ-死叉
        :术语解释:
            KDJ-死叉
        :功能:
            获取是否KDJ-死叉字段
        """
        return AStockMarketCur.kdj_sicha

    @staticmethod
    def query_kdj_sicha(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            KDJ-死叉
        :术语解释:
            KDJ-死叉
        :功能:
            筛选出或剔除指定日期范围内标记为KDJ-死叉的股票。与get_kdj_sicha()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选KDJ-死叉，0=剔除KDJ-死叉
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kdj_sicha == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_rsi6_80():
        """
        :术语名称:
            RSI6上穿80
        :术语解释:
            RSI6上穿80
        :功能:
            获取是否RSI6上穿80字段
        """
        return AStockMarketCur.rsi6_80

    @staticmethod
    def query_rsi6_80(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            RSI6上穿80
        :术语解释:
            RSI6上穿80
        :功能:
            筛选出或剔除指定日期范围内标记为RSI6上穿80的股票。与get_rsi6_80()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选RSI6上穿80，0=剔除RSI6上穿80
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.rsi6_80 == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_rsi6_20():
        """
        :术语名称:
            RSI6下穿20
        :术语解释:
            RSI6下穿20
        :功能:
            获取是否RSI6下穿20字段
        """
        return AStockMarketCur.rsi6_20

    @staticmethod
    def query_rsi6_20(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            RSI6下穿20
        :术语解释:
            RSI6下穿20
        :功能:
            筛选出或剔除指定日期范围内标记为RSI6下穿20的股票。与get_rsi6_20()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选RSI6下穿20，0=剔除RSI6下穿20
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.rsi6_20 == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_rsi12_80():
        """
        :术语名称:
            RSI12上穿80
        :术语解释:
            RSI12上穿80
        :功能:
            获取是否RSI12上穿80字段
        """
        return AStockMarketCur.rsi12_80

    @staticmethod
    def query_rsi12_80(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            RSI12上穿80
        :术语解释:
            RSI12上穿80
        :功能:
            筛选出或剔除指定日期范围内标记为RSI12上穿80的股票。与get_rsi12_80()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选RSI12上穿80，0=剔除RSI12上穿80
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.rsi12_80 == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_rsi12_20():
        """
        :术语名称:
            RSI12下穿20
        :术语解释:
            RSI12下穿20
        :功能:
            获取是否RSI12下穿20字段
        """
        return AStockMarketCur.rsi12_20

    @staticmethod
    def query_rsi12_20(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            RSI12下穿20
        :术语解释:
            RSI12下穿20
        :功能:
            筛选出或剔除指定日期范围内标记为RSI12下穿20的股票。与get_rsi12_20()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选RSI12下穿20，0=剔除RSI12下穿20
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.rsi12_20 == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_cci_100():
        """
        :术语名称:
            CCI上穿100
        :术语解释:
            CCI上穿100
        :功能:
            获取是否CCI上穿100字段
        """
        return AStockMarketCur.cci_100

    @staticmethod
    def query_cci_100(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            CCI上穿100
        :术语解释:
            CCI上穿100
        :功能:
            筛选出或剔除指定日期范围内标记为CCI上穿100的股票。与get_cci_100()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选CCI上穿100，0=剔除CCI上穿100
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.cci_100 == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_cci_ne100():
        """
        :术语名称:
            CCI下穿100
        :术语解释:
            CCI下穿100
        :功能:
            获取是否CCI下穿100字段
        """
        return AStockMarketCur.cci_ne100

    @staticmethod
    def query_cci_ne100(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            CCI下穿100
        :术语解释:
            CCI下穿100
        :功能:
            筛选出或剔除指定日期范围内标记为CCI下穿100的股票。与get_cci_ne100()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选CCI下穿100，0=剔除CCI下穿100
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.cci_ne100 == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_cci_over_bought():
        """
        :术语名称:
            CCI超买
        :术语解释:
            CCI超买
        :功能:
            获取是否CCI超买字段
        """
        return AStockMarketCur.cci_over_bought

    @staticmethod
    def query_cci_over_bought(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            CCI超买
        :术语解释:
            CCI超买
        :功能:
            筛选出或剔除指定日期范围内标记为CCI超买的股票。与get_cci_over_bought()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选CCI超买，0=剔除CCI超买
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.cci_over_bought == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_cci_over_sold():
        """
        :术语名称:
            CCI超卖
        :术语解释:
            CCI超卖
        :功能:
            获取是否CCI超卖字段
        """
        return AStockMarketCur.cci_over_sold

    @staticmethod
    def query_cci_over_sold(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            CCI超卖
        :术语解释:
            CCI超卖
        :功能:
            筛选出或剔除指定日期范围内标记为CCI超卖的股票。与get_cci_over_sold()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选CCI超卖，0=剔除CCI超卖
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.cci_over_sold == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_wr_80():
        """
        :术语名称:
            WR上穿80
        :术语解释:
            WR上穿80
        :功能:
            获取是否WR上穿80字段
        """
        return AStockMarketCur.wr_80

    @staticmethod
    def query_wr_80(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            WR上穿80
        :术语解释:
            WR上穿80
        :功能:
            筛选出或剔除指定日期范围内标记为WR上穿80的股票。与get_wr_80()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选WR上穿80，0=剔除WR上穿80
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.wr_80 == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_wr_20():
        """
        :术语名称:
            WR下穿20
        :术语解释:
            WR下穿20
        :功能:
            获取是否WR下穿20字段
        """
        return AStockMarketCur.wr_20

    @staticmethod
    def query_wr_20(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            WR下穿20
        :术语解释:
            WR下穿20
        :功能:
            筛选出或剔除指定日期范围内标记为WR下穿20的股票。与get_wr_20()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选WR下穿20，0=剔除WR下穿20
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.wr_20 == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_wr_over_bought():
        """
        :术语名称:
            WR超买
        :术语解释:
            WR超买
        :功能:
            获取是否WR超买字段
        """
        return AStockMarketCur.wr_over_bought

    @staticmethod
    def query_wr_over_bought(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            WR超买
        :术语解释:
            WR超买
        :功能:
            筛选出或剔除指定日期范围内标记为WR超买的股票。与get_wr_over_bought()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选WR超买，0=剔除WR超买
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.wr_over_bought == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_wr_over_sold():
        """
        :术语名称:
            WR超卖
        :术语解释:
            WR超卖
        :功能:
            获取是否WR超卖字段
        """
        return AStockMarketCur.wr_over_sold

    @staticmethod
    def query_wr_over_sold(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            WR超卖
        :术语解释:
            WR超卖
        :功能:
            筛选出或剔除指定日期范围内标记为WR超卖的股票。与get_wr_over_sold()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选WR超卖，0=剔除WR超卖
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.wr_over_sold == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_cyc_dtpl():
        """
        :术语名称:
            成本均线CYC-多头排列
        :术语解释:
            成本均线CYC-多头排列
        :功能:
            获取是否成本均线CYC-多头排列字段
        """
        return AStockMarketCur.cyc_dtpl

    @staticmethod
    def query_cyc_dtpl(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            成本均线CYC-多头排列
        :术语解释:
            成本均线CYC-多头排列
        :功能:
            筛选出或剔除指定日期范围内标记为成本均线CYC-多头排列的股票。与get_cyc_dtpl()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选成本均线CYC-多头排列，0=剔除成本均线CYC-多头排列
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.cyc_dtpl == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_cyc_ktpl():
        """
        :术语名称:
            成本均线CYC-空头排列
        :术语解释:
            成本均线CYC-空头排列
        :功能:
            获取是否成本均线CYC-空头排列字段
        """
        return AStockMarketCur.cyc_ktpl

    @staticmethod
    def query_cyc_ktpl(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            成本均线CYC-空头排列
        :术语解释:
            成本均线CYC-空头排列
        :功能:
            筛选出或剔除指定日期范围内标记为成本均线CYC-空头排列的股票。与get_cyc_ktpl()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选成本均线CYC-空头排列，0=剔除成本均线CYC-空头排列
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.cyc_ktpl == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_cyc_jincha():
        """
        :术语名称:
            成本均线CYC-金叉
        :术语解释:
            成本均线CYC-金叉
        :功能:
            获取是否成本均线CYC-金叉字段
        """
        return AStockMarketCur.cyc_jincha

    @staticmethod
    def query_cyc_jincha(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            成本均线CYC-金叉
        :术语解释:
            成本均线CYC-金叉
        :功能:
            筛选出或剔除指定日期范围内标记为成本均线CYC-金叉的股票。与get_cyc_jincha()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选成本均线CYC-金叉，0=剔除成本均线CYC-金叉
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.cyc_jincha == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_cyc_sicha():
        """
        :术语名称:
            成本均线CYC-死叉
        :术语解释:
            成本均线CYC-死叉
        :功能:
            获取是否成本均线CYC-死叉字段
        """
        return AStockMarketCur.cyc_sicha

    @staticmethod
    def query_cyc_sicha(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            成本均线CYC-死叉
        :术语解释:
            成本均线CYC-死叉
        :功能:
            筛选出或剔除指定日期范围内标记为成本均线CYC-死叉的股票。与get_cyc_sicha()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选成本均线CYC-死叉，0=剔除成本均线CYC-死叉
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.cyc_sicha == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_cd_90():
        """
        :术语名称:
            筹码90％集中度
        :术语解释:
            筹码90％集中度
        :功能:
            获取筹码90％集中度字段
        """
        return AStockMarketCur.cd_90

    @staticmethod
    def get_cd_70():
        """
        :术语名称:
            筹码70％集中度
        :术语解释:
            筹码70％集中度
        :功能:
            获取筹码70％集中度字段
        """
        return AStockMarketCur.cd_70

    @staticmethod
    def get_kline_cxyx():
        """
        :术语名称:
            K线形态-长下影线
        :术语解释:
            K线形态-长下影线
        :功能:
            获取是否K线形态-长下影线字段
        """
        return AStockMarketCur.kline_cxyx

    @staticmethod
    def query_kline_cxyx(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-长下影线
        :术语解释:
            K线形态-长下影线
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-长下影线的股票。与get_kline_cxyx()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-长下影线，0=剔除K线形态-长下影线
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_cxyx == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_lyt():
        """
        :术语名称:
            K线形态-老鸭头
        :术语解释:
            K线形态-老鸭头
        :功能:
            获取是否K线形态-老鸭头字段
        """
        return AStockMarketCur.kline_lyt

    @staticmethod
    def query_kline_lyt(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-老鸭头
        :术语解释:
            K线形态-老鸭头
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-老鸭头的股票。与get_kline_lyt()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-老鸭头，0=剔除K线形态-老鸭头
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_lyt == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_jjyyt():
        """
        :术语名称:
            K线形态-九九艳阳天
        :术语解释:
            K线形态-九九艳阳天
        :功能:
            获取是否K线形态-九九艳阳天字段
        """
        return AStockMarketCur.kline_jjyyt

    @staticmethod
    def query_kline_jjyyt(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-九九艳阳天
        :术语解释:
            K线形态-九九艳阳天
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-九九艳阳天的股票。与get_kline_jjyyt()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-九九艳阳天，0=剔除K线形态-九九艳阳天
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_jjyyt == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_sstd():
        """
        :术语名称:
            K线形态-上升通道
        :术语解释:
            K线形态-上升通道
        :功能:
            获取是否K线形态-上升通道字段
        """
        return AStockMarketCur.kline_sstd

    @staticmethod
    def query_kline_sstd(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-上升通道
        :术语解释:
            K线形态-上升通道
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-上升通道的股票。与get_kline_sstd()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-上升通道，0=剔除K线形态-上升通道
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_sstd == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_cyzp():
        """
        :术语名称:
            K线形态-长阳重炮
        :术语解释:
            K线形态-长阳重炮
        :功能:
            获取是否K线形态-长阳重炮字段
        """
        return AStockMarketCur.kline_cyzp

    @staticmethod
    def query_kline_cyzp(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-长阳重炮
        :术语解释:
            K线形态-长阳重炮
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-长阳重炮的股票。与get_kline_cyzp()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-长阳重炮，0=剔除K线形态-长阳重炮
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_cyzp == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_jztd():
        """
        :术语名称:
            K线形态-金针探底
        :术语解释:
            K线形态-金针探底
        :功能:
            获取是否K线形态-金针探底字段
        """
        return AStockMarketCur.kline_jztd

    @staticmethod
    def query_kline_jztd(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-金针探底
        :术语解释:
            K线形态-金针探底
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-金针探底的股票。与get_kline_jztd()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-金针探底，0=剔除K线形态-金针探底
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_jztd == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_hsb():
        """
        :术语名称:
            K线形态-红三兵
        :术语解释:
            K线形态-红三兵
        :功能:
            获取是否K线形态-红三兵字段
        """
        return AStockMarketCur.kline_hsb

    @staticmethod
    def query_kline_hsb(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-红三兵
        :术语解释:
            K线形态-红三兵
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-红三兵的股票。与get_kline_hsb()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-红三兵，0=剔除K线形态-红三兵
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_hsb == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_wygd():
        """
        :术语名称:
            K线形态-乌云盖顶
        :术语解释:
            K线形态-乌云盖顶
        :功能:
            获取是否K线形态-乌云盖顶字段
        """
        return AStockMarketCur.kline_wygd

    @staticmethod
    def query_kline_wygd(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-乌云盖顶
        :术语解释:
            K线形态-乌云盖顶
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-乌云盖顶的股票。与get_kline_wygd()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-乌云盖顶，0=剔除K线形态-乌云盖顶
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_wygd == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_zczx():
        """
        :术语名称:
            K线形态-早晨之星
        :术语解释:
            K线形态-早晨之星
        :功能:
            获取是否K线形态-早晨之星字段
        """
        return AStockMarketCur.kline_zczx

    @staticmethod
    def query_kline_zczx(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-早晨之星
        :术语解释:
            K线形态-早晨之星
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-早晨之星的股票。与get_kline_zczx()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-早晨之星，0=剔除K线形态-早晨之星
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_zczx == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_xrds():
        """
        :术语名称:
            K线形态-旭日东升
        :术语解释:
            K线形态-旭日东升
        :功能:
            获取是否K线形态-旭日东升字段
        """
        return AStockMarketCur.kline_xrds

    @staticmethod
    def query_kline_xrds(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-旭日东升
        :术语解释:
            K线形态-旭日东升
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-旭日东升的股票。与get_kline_xrds()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-旭日东升，0=剔除K线形态-旭日东升
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_xrds == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_vxfz():
        """
        :术语名称:
            K线形态-v型反转
        :术语解释:
            K线形态-v型反转
        :功能:
            获取是否K线形态-v型反转字段
        """
        return AStockMarketCur.kline_vxfz

    @staticmethod
    def query_kline_vxfz(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-v型反转
        :术语解释:
            K线形态-v型反转
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-v型反转的股票。与get_kline_vxfz()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-v型反转，0=剔除K线形态-v型反转
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_vxfz == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_mrj():
        """
        :术语名称:
            K线形态-美人肩
        :术语解释:
            K线形态-美人肩
        :功能:
            获取是否K线形态-美人肩字段
        """
        return AStockMarketCur.kline_mrj

    @staticmethod
    def query_kline_mrj(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-美人肩
        :术语解释:
            K线形态-美人肩
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-美人肩的股票。与get_kline_mrj()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-美人肩，0=剔除K线形态-美人肩
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_mrj == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_zthmq():
        """
        :术语名称:
            K线形态-涨停回马枪
        :术语解释:
            K线形态-涨停回马枪
        :功能:
            获取是否K线形态-涨停回马枪字段
        """
        return AStockMarketCur.kline_zthmq

    @staticmethod
    def query_kline_zthmq(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-涨停回马枪
        :术语解释:
            K线形态-涨停回马枪
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-涨停回马枪的股票。与get_kline_zthmq()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-涨停回马枪，0=剔除K线形态-涨停回马枪
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_zthmq == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_dfp():
        """
        :术语名称:
            K线形态-多方炮
        :术语解释:
            K线形态-多方炮
        :功能:
            获取是否K线形态-多方炮字段
        """
        return AStockMarketCur.kline_dfp

    @staticmethod
    def query_kline_dfp(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-多方炮
        :术语解释:
            K线形态-多方炮
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-多方炮的股票。与get_kline_dfp()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-多方炮，0=剔除K线形态-多方炮
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_dfp == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_kline_yycsx():
        """
        :术语名称:
            K线形态-一阳穿三线
        :术语解释:
            K线形态-一阳穿三线
        :功能:
            获取是否K线形态-一阳穿三线字段
        """
        return AStockMarketCur.kline_yycsx

    @staticmethod
    def query_kline_yycsx(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            K线形态-一阳穿三线字段
        :术语解释:
            K线形态-一阳穿三线字段
        :功能:
            筛选出或剔除指定日期范围内标记为K线形态-一阳穿三线字段的股票。与get_kline_yycsx()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选K线形态-一阳穿三线字段，0=剔除K线形态-一阳穿三线字段
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.kline_yycsx == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_opinions_month3():
        """
        :术语名称:
            近3月机构调研次数
        :术语解释:
            近3月机构调研次数
        :功能:
            获取近3月机构调研次数字段
        """
        return AStockMarketCur.opinions_month3

    @staticmethod
    def get_organ_grade_month3_buy():
        """
        :术语名称:
            近3月机构评级-买入
        :术语解释:
            近3月机构评级-买入
        :功能:
            获取是否近3月机构评级-买入字段
        """
        return AStockMarketCur.organ_grade_month3_buy

    @staticmethod
    def query_organ_grade_month3_buy(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            近3月机构评级-买入
        :术语解释:
            近3月机构评级-买入
        :功能:
            筛选出或剔除指定日期范围内标记为近3月机构评级-买入的股票。与get_organ_grade_month3_buy()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选近3月机构评级-买入，0=剔除近3月机构评级-买入
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.organ_grade_month3_buy == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_organ_grade_month3_overweight():
        """
        :术语名称:
            近3月机构评级-增持
        :术语解释:
            近3月机构评级-增持
        :功能:
            获取是否近3月机构评级-增持字段
        """
        return AStockMarketCur.organ_grade_month3_overweight

    @staticmethod
    def query_organ_grade_month3_overweight(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            近3月机构评级-增持
        :术语解释:
            近3月机构评级-增持
        :功能:
            筛选出或剔除指定日期范围内标记为近3月机构评级-增持的股票。与get_organ_grade_month3_overweight()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选近3月机构评级-增持，0=剔除近3月机构评级-增持
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.organ_grade_month3_overweight == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_organ_grade_month3_neutral():
        """
        :术语名称:
            近3月机构评级-中性
        :术语解释:
            近3月机构评级-中性
        :功能:
            获取是否近3月机构评级-中性字段
        """
        return AStockMarketCur.organ_grade_month3_neutral

    @staticmethod
    def query_organ_grade_month3_neutral(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            近3月机构评级-中性
        :术语解释:
            近3月机构评级-中性
        :功能:
            筛选出或剔除指定日期范围内标记为近3月机构评级-中性的股票。与get_organ_grade_month3_neutral()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选近3月机构评级-中性，0=剔除近3月机构评级-中性
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.organ_grade_month3_neutral == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_organ_grade_month3_reduction():
        """
        :术语名称:
            近3月机构评级-减持
        :术语解释:
            近3月机构评级-减持
        :功能:
            获取是否近3月机构评级-减持字段
        """
        return AStockMarketCur.organ_grade_month3_reduction

    @staticmethod
    def query_organ_grade_month3_reduction(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            近3月机构评级-减持
        :术语解释:
            近3月机构评级-减持
        :功能:
            筛选出或剔除指定日期范围内标记为近3月机构评级-减持的股票。与get_organ_grade_month3_reduction()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选近3月机构评级-减持，0=剔除近3月机构评级-减持
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.organ_grade_month3_reduction == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_organ_grade_month3_sell():
        """
        :术语名称:
            近3月机构评级-卖出
        :术语解释:
            近3月机构评级-卖出
        :功能:
            获取是否近3月机构评级-卖出字段
        """
        return AStockMarketCur.organ_grade_month3_sell

    @staticmethod
    def query_organ_grade_month3_sell(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            近3月机构评级-卖出
        :术语解释:
            近3月机构评级-卖出
        :功能:
            筛选出或剔除指定日期范围内标记为近3月机构评级-卖出的股票。与get_organ_grade_month3_sell()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选近3月机构评级-卖出，0=剔除近3月机构评级-卖出
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.organ_grade_month3_sell == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_violation_month3():
        """
        :术语名称:
            近3月违规处理次数
        :术语解释:
            近3月违规处理次数
        :功能:
            获取近3月违规处理次数字段
        """
        return AStockMarketCur.violation_month3

    @staticmethod
    def get_exchange_record_month3_overweight():
        """
        :术语名称:
            近3月高管增减持-增持
        :术语解释:
            近3月高管增减持-增持
        :功能:
            获取是否近3月高管增减持-增持字段，仅当用户明确提及“近3月高管是否增持”时调用。
        """
        return AStockMarketCur.exchange_record_month3_overweight

    @staticmethod
    def query_exchange_record_month3_overweight(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            近3月高管增减持-增持
        :术语解释:
            近3月高管增减持-增持
        :功能:
            筛选出或剔除指定日期范围内标记为近3月高管增减持-增持的股票。与get_exchange_record_month3_overweight()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选近3月高管增减持-增持，0=剔除近3月高管增减持-增持
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.exchange_record_month3_overweight == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_exchange_record_month3_reduction():
        """
        :术语名称:
            近3月高管增减持-减持
        :术语解释:
            近3月高管增减持-减持
        :功能:
            获取是否近3月高管增减持-减持字段，仅当用户明确提及“近3月高管是否减持”时调用。
        """
        return AStockMarketCur.exchange_record_month3_reduction

    @staticmethod
    def query_exchange_record_month3_reduction(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            近3月高管增减持-减持
        :术语解释:
            近3月高管增减持-减持
        :功能:
            筛选出或剔除指定日期范围内标记为近3月高管增减持-减持的股票。与get_exchange_record_month3_reduction()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选近3月高管增减持-减持，0=剔除近3月高管增减持-减持
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.exchange_record_month3_reduction == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_sales_restrictions_lifted_future():
        """
        :术语名称:
            限售解禁-未来日期
        :术语解释:
            限售解禁-未来日期
        :功能:
            获取限售解禁-未来日期字段
        """
        return AStockMarketCur.sales_restrictions_lifted_future

    @staticmethod
    def get_sales_restrictions_lifted_over():
        """
        :术语名称:
            限售解禁-过去日期
        :术语解释:
            限售解禁-过去日期
        :功能:
            获取限售解禁-过去日期字段
        """
        return AStockMarketCur.sales_restrictions_lifted_over

    @staticmethod
    def get_private_placement():
        """
        :术语名称:
            定向增发
        :术语解释:
            定向增发
        :功能:
            获取定向增发字段
        """
        return AStockMarketCur.private_placement

    @staticmethod
    def get_assets_reorganization():
        """
        :术语名称:
            资产重组
        :术语解释:
            资产重组
        :功能:
            获取资产重组字段
        """
        return AStockMarketCur.assets_reorganization

    @staticmethod
    def get_performance_forecast():
        """
        :术语名称:
            业绩预告
        :术语解释:
            业绩预告
        :功能:
            获取业绩预告字段
        """
        return AStockMarketCur.performance_forecast

    @staticmethod
    def get_organ_hold_total():
        """
        :术语名称:
            机构持股家数合计
        :术语解释:
            机构持股家数合计
        :功能:
            获取机构持股家数合计字段
        """
        return AStockMarketCur.organ_hold_total

    @staticmethod
    def get_fund_hold_num():
        """
        :术语名称:
            基金持股家数
        :术语解释:
            基金持股家数
        :功能:
            获取基金持股家数字段
        """
        return AStockMarketCur.fund_hold_num

    @staticmethod
    def get_qs_hold_num():
        """
        :术语名称:
            券商持股家数
        :术语解释:
            券商持股家数
        :功能:
            获取券商持股家数字段
        """
        return AStockMarketCur.qs_hold_num

    @staticmethod
    def get_qfll_hold_num():
        """
        :术语名称:
            QFII持股家数
        :术语解释:
            QFII持股家数
        :功能:
            获取QFII持股家数字段
        """
        return AStockMarketCur.qfll_hold_num

    @staticmethod
    def get_insure_hold_num():
        """
        :术语名称:
            保险持股家数
        :术语解释:
            保险持股家数
        :功能:
            获取保险持股家数字段
        """
        return AStockMarketCur.insure_hold_num

    @staticmethod
    def get_social_security_hold_num():
        """
        :术语名称:
            社保持股家数
        :术语解释:
            社保持股家数
        :功能:
            获取社保持股家数字段
        """
        return AStockMarketCur.social_security_hold_num

    @staticmethod
    def get_trust_hold_num():
        """
        :术语名称:
            信托持股家数
        :术语解释:
            信托持股家数
        :功能:
            获取信托持股家数字段
        """
        return AStockMarketCur.trust_hold_num

    @staticmethod
    def get_organ_hold_ratio_total():
        """
        :术语名称:
            机构持股比例合计
        :术语解释:
            机构持股比例合计
        :功能:
            获取机构持股比例合计字段
        """
        return AStockMarketCur.organ_hold_ratio_total

    @staticmethod
    def get_fund_hold_ratio():
        """
        :术语名称:
            基金持股比例
        :术语解释:
            基金持股比例
        :功能:
            获取基金持股比例字段
        """
        return AStockMarketCur.fund_hold_ratio

    @staticmethod
    def get_qs_hold_ratio():
        """
        :术语名称:
            券商持股比例
        :术语解释:
            券商持股比例
        :功能:
            获取券商持股比例字段
        """
        return AStockMarketCur.qs_hold_ratio

    @staticmethod
    def get_qfll_hold_ratio():
        """
        :术语名称:
            QFII持股比例
        :术语解释:
            QFII持股比例
        :功能:
            获取QFII持股比例字段
        """
        return AStockMarketCur.qfll_hold_ratio

    @staticmethod
    def get_insure_hold_ratio():
        """
        :术语名称:
            保险持股比例
        :术语解释:
            保险持股比例
        :功能:
            获取保险持股比例字段
        """
        return AStockMarketCur.insure_hold_ratio

    @staticmethod
    def get_social_security_hold_ratio():
        """
        :术语名称:
            社保持股比例
        :术语解释:
            社保持股比例
        :功能:
            获取社保持股比例字段
        """
        return AStockMarketCur.social_security_hold_ratio

    @staticmethod
    def get_trust_hold_ratio():
        """
        :术语名称:
            信托持股比例
        :术语解释:
            信托持股比例
        :功能:
            获取信托持股比例字段
        """
        return AStockMarketCur.trust_hold_ratio

    @staticmethod
    def get_sb_hold_2quarter():
        """
        :术语名称:
            社保增持股票
        :术语解释:
            社保增持股票
        :功能:
            获取社保增持股票字段
        """
        return AStockMarketCur.sb_hold_2quarter

    @staticmethod
    def get_trade_seat_times_3d():
        """
        :术语名称:
            最近3天上龙虎榜次数
        :术语解释:
            最近3天上龙虎榜次数
        :功能:
            获取最近3天上龙虎榜次数字段。仅当用户明确询问“3天”内的“累计次数”或“上榜几次”时调用。这是一个固定3天的统计，如果用户询问其他天数（如5天、10天）则不适用。严禁在问题涉及“连续上榜天数”时调用此函数，因为它统计的是总次数而非连续天数。
        """
        return AStockMarketCur.trade_seat_times_3d

    @staticmethod
    def get_rise():
        """
        :术语名称:
            涨速
        :术语解释:
            涨速
        :功能:
            获取涨速字段
        """
        return AStockMarketCur.rise

    @staticmethod
    def get_all_time_high():
        """
        :术语名称:
            今日创历史新高
        :术语解释:
            今日创历史新高
        :功能:
            获取是否今日创历史新高字段
        """
        return AStockMarketCur.all_time_high

    @staticmethod
    def query_all_time_high(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            今日创历史新高
        :术语解释:
            今日创历史新高
        :功能:
            筛选出或剔除指定日期范围内标记为今日创历史新高的股票。与get_all_time_high()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选今日创历史新高，0=剔除今日创历史新高
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.all_time_high == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_all_time_low():
        """
        :术语名称:
            今日创历史新低
        :术语解释:
            今日创历史新低
        :功能:
            获取是否今日创历史新低字段
        """
        return AStockMarketCur.all_time_low

    @staticmethod
    def query_all_time_low(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            今日创历史新低
        :术语解释:
            今日创历史新低
        :功能:
            筛选出或剔除指定日期范围内标记为今日创历史新低的股票。与get_all_time_low()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选今日创历史新低，0=剔除今日创历史新低
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockMarketCur.all_time_low == flag) & AStockMarketCur.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockMarketCur.secucode).where(condition)

    @staticmethod
    def get_period_high():
        """
        :术语名称:
            今日创阶段新高，多少日新高
        :术语解释:
            今日创阶段新高，多少日新高
        :功能:
            获取今日创阶段新高，多少日新高字段
        """
        return AStockMarketCur.period_high

    @staticmethod
    def get_period_low():
        """
        :术语名称:
            今日创阶段新低，多少日新低
        :术语解释:
            今日创阶段新低，多少日新低
        :功能:
            获取今日创阶段新低，多少日新低字段
        """
        return AStockMarketCur.period_low

    @staticmethod
    def get_near_all_time_high():
        """
        :术语名称:
            近期创历史新高，近几日创历史新高
        :术语解释:
            近期创历史新高，近几日创历史新高
        :功能:
            获取近期创历史新高，近几日创历史新高字段
        """
        return AStockMarketCur.near_all_time_high

    @staticmethod
    def get_near_all_time_low():
        """
        :术语名称:
            近期创历史新低，近几日创历史新低
        :术语解释:
            近期创历史新低，近几日创历史新低
        :功能:
            获取近期创历史新低，近几日创历史新低字段
        """
        return AStockMarketCur.near_all_time_low

    @staticmethod
    def get_qrr():
        """
        :术语名称:
            量比
        :术语解释:
            量比
        :功能:
            获取量比字段
        """
        return AStockMarketCur.qrr

    @staticmethod
    def get_zf_month():
        """
        :术语名称:
            本月涨跌幅
        :术语解释:
            本月涨跌幅
        :功能:
            获取本月涨跌幅字段
        """
        return AStockMarketCur.zf_month

    @staticmethod
    def get_pe_ratio(mode: int = 0):
        """
        :术语名称:
            市盈率（动）
        :术语解释:
            市盈率（动）
        :功能:
            获取市盈率（动）字段
        """
        if mode == 1:
            return AStockMarketCur.pe_ratio
        else:
            return AStockMarket.pe_dynamic

    @staticmethod
    def get_pe_ratio_ttm(mode: int = 0):
        """
        :术语名称:
            市盈率（TTM）
        :术语解释:
            市盈率（TTM）
        :功能:
            获取市盈率（TTM）字段
        """
        if mode == 1:
            return AStockMarketCur.pe_ratio_ttm
        else:
            return AStockMarket.pe_ttm

    @staticmethod
    def get_pb_ratio():
        """
        :术语名称:
            市净率
        :术语解释:
            市净率
        :功能:
            获取市净率字段
        """
        return AStockMarketCur.pb_ratio

    @staticmethod
    def get_market_value():
        """
        :术语名称:
            流通市值(估值)
        :术语解释:
            流通市值(估值)
        :功能:
            获取流通市值字段
        """
        return AStockMarketCur.market_value

    @staticmethod
    def get_tis():
        """
        :术语名称:
            总股本
        :术语解释:
            总股本
        :功能:
            获取总股本字段
        """
        return AStockMarketCur.tis

    @staticmethod
    def get_cir_equity():
        """
        :术语名称:
            流通股本
        :术语解释:
            流通股本
        :功能:
            获取流通股本字段
        """
        return AStockMarketCur.cir_equity

    @staticmethod
    def get_lz_day_count():
        """
        :术语名称:
            连涨天数
        :术语解释:
            连涨天数
        :功能:
            获取连涨天数字段
        """
        return AStockMarketCur.lz_day_count

    @staticmethod
    def get_dividend_yield():
        """
        :术语名称:
            股息率
        :术语解释:
            股息率
        :功能:
            获取股息率字段
        """
        return AStockMarketCur.dividend_yield

    @staticmethod
    def get_secuname():
        """
        :术语名称:
            股票名称
        :术语解释:
            股票名称
        :功能:
            获取股票名称字段
        """
        return AStockBasic.secuname

    @staticmethod
    def get_type():
        """
        :术语名称:
            所属大分类
        :术语解释:
            所属大分类
        :功能:
            获取所属大分类字段
        """
        return AStockBasic.type

    @staticmethod
    def get_market():
        """
        :术语名称:
            所属市场
        :术语解释:
            所属市场
        :功能:
            获取股票所属市场字段
        """
        return AStockBasic.market

    @staticmethod
    def get_trade():
        """
        :术语名称:
            所属行业
        :术语解释:
            所属行业
        :功能:
            获取股票所属行业/板块字段，问什么板块要调用此函数
        """
        return AStockBasic.trade

    @staticmethod
    def get_concept():
        """
        :术语名称:
            所属概念
        :术语解释:
            所属概念
        :功能:
            获取股票所属概念字段，问什么概念要调用此函数
        """
        return AStockBasic.concept

    @staticmethod
    def get_area():
        """
        :术语名称:
            所属地域
        :术语解释:
            所属地域
        :功能:
            获取股票所属地域字段
        """
        return AStockBasic.area

    @staticmethod
    def get_is_hszb():
        """
        :术语名称:
            沪深主板
        :术语解释:
            沪深主板
        :功能:
            获取是否沪深主板字段
        """
        return AStockBasic.is_hszb

    @staticmethod
    def query_is_hszb(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            沪深主板
        :术语解释:
            沪深主板
        :功能:
            筛选出或剔除指定日期范围内标记为沪深主板的股票。与get_is_hszb()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选沪深主板，0=剔除沪深主板
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockBasic.is_hszb == flag) & AStockBasic.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockBasic.secucode).where(condition)

    @staticmethod
    def get_is_kcb():
        """
        :术语名称:
            科创板
        :术语解释:
            科创板
        :功能:
            获取是否科创板字段
        """
        return AStockBasic.is_kcb

    @staticmethod
    def query_is_kcb(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            科创板
        :术语解释:
            科创板
        :功能:
            筛选出或剔除指定日期范围内标记为科创板的股票。与get_is_kcb()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选科创板，0=剔除科创板
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockBasic.is_kcb == flag) & AStockBasic.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockBasic.secucode).where(condition)

    @staticmethod
    def get_is_cyb():
        """
        :术语名称:
            创业板
        :术语解释:
            创业板
        :功能:
            获取是否创业板字段
        """
        return AStockBasic.is_cyb

    @staticmethod
    def query_is_cyb(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            创业板
        :术语解释:
            创业板
        :功能:
            筛选出或剔除指定日期范围内标记为创业板的股票。与get_is_cyb()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选创业板，0=剔除创业板
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockBasic.is_cyb == flag) & AStockBasic.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockBasic.secucode).where(condition)

    @staticmethod
    def get_is_tp():
        """
        :术语名称:
            停牌
        :术语解释:
            停牌
        :功能:
            获取是否停牌字段
        """
        return AStockBasic.is_tp

    @staticmethod
    def query_is_tp(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            停牌
        :术语解释:
            停牌
        :功能:
            筛选出或剔除指定日期范围内标记为停牌的股票。与get_is_tp()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选停牌，0=剔除停牌
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockBasic.is_tp == flag) & AStockBasic.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockBasic.secucode).where(condition)

    @staticmethod
    def get_is_ts():
        """
        :术语名称:
            退市
        :术语解释:
            退市
        :功能:
            获取是否退市字段
        """
        return AStockBasic.is_ts

    @staticmethod
    def query_is_ts(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            退市
        :术语解释:
            退市
        :功能:
            筛选出或剔除指定日期范围内标记为退市的股票。与get_is_ts()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选退市，0=剔除退市
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockBasic.is_ts == flag) & AStockBasic.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockBasic.secucode).where(condition)

    @staticmethod
    def get_is_xingu():
        """
        :术语名称:
            新股
        :术语解释:
            新股
        :功能:
            获取是否新股字段
        """
        return AStockBasic.is_xingu

    @staticmethod
    def query_is_xingu(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            新股
        :术语解释:
            新股
        :功能:
            筛选出或剔除指定日期范围内标记为新股的股票。与get_is_xingu()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选新股，0=剔除新股
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockBasic.is_xingu == flag) & AStockBasic.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockBasic.secucode).where(condition)

    @staticmethod
    def get_is_cixingu():
        """
        :术语名称:
            次新股
        :术语解释:
            次新股
        :功能:
            获取是否次新股字段
        """
        return AStockBasic.is_cixingu

    @staticmethod
    def query_is_cixingu(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            次新股
        :术语解释:
            次新股
        :功能:
            筛选出或剔除指定日期范围内标记为次新股的股票。与get_is_cixingu()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选次新股，0=剔除次新股
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockBasic.is_cixingu == flag) & AStockBasic.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockBasic.secucode).where(condition)

    @staticmethod
    def get_is_cwfx():
        """
        :术语名称:
            财务风险
        :术语解释:
            财务风险
        :功能:
            获取是否财务风险字段
        """
        return AStockBasic.is_cwfx

    @staticmethod
    def query_is_cwfx(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            财务风险
        :术语解释:
            财务风险
        :功能:
            筛选出或剔除指定日期范围内标记为财务风险的股票。与get_is_cwfx()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选财务风险，0=剔除财务风险
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockBasic.is_cwfx == flag) & AStockBasic.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockBasic.secucode).where(condition)

    @staticmethod
    def get_is_qzts():
        """
        :术语名称:
            潜在退市
        :术语解释:
            潜在退市
        :功能:
            获取是否潜在退市字段
        """
        return AStockBasic.is_qzts

    @staticmethod
    def query_is_qzts(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            潜在退市
        :术语解释:
            潜在退市
        :功能:
            筛选出或剔除指定日期范围内标记为潜在退市的股票。与get_is_qzts()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选潜在退市，0=剔除潜在退市
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockBasic.is_qzts == flag) & AStockBasic.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockBasic.secucode).where(condition)

    @staticmethod
    def get_is_tsfx():
        """
        :术语名称:
            退市风险
        :术语解释:
            退市风险
        :功能:
            获取是否退市风险字段
        """
        return AStockBasic.is_tsfx

    @staticmethod
    def query_is_tsfx(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            退市风险
        :术语解释:
            退市风险
        :功能:
            筛选出或剔除指定日期范围内标记为退市风险的股票。与get_is_tsfx()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选退市风险，0=剔除退市风险
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (AStockBasic.is_tsfx == flag) & AStockBasic.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(AStockBasic.secucode).where(condition)

    @staticmethod
    def get_detail_company_name():
        """
        :术语名称:
            公司名称
        :术语解释:
            公司名称
        :功能:
            获取公司名称字段
        """
        return AStockBasic.detail_company_name

    @staticmethod
    def get_detail_english_name():
        """
        :术语名称:
            英文名
        :术语解释:
            英文名
        :功能:
            获取英文名字段
        """
        return AStockBasic.detail_english_name

    @staticmethod
    def get_detail_before_name():
        """
        :术语名称:
            曾用名
        :术语解释:
            曾用名
        :功能:
            获取曾用名字段
        """
        return AStockBasic.detail_before_name

    @staticmethod
    def get_detail_region():
        """
        :术语名称:
            所属地域
        :术语解释:
            所属地域
        :功能:
            获取所属地域字段
        """
        return AStockBasic.detail_region

    @staticmethod
    def get_detail_industry():
        """
        :术语名称:
            所属证监会行业
        :术语解释:
            所属证监会行业
        :功能:
            获取所属证监会行业字段
        """
        return AStockBasic.detail_industry

    @staticmethod
    def get_detail_board_name():
        """
        :术语名称:
            所属指南针行业名称
        :术语解释:
            所属指南针行业名称
        :功能:
            获取所属指南针行业名称字段
        """
        return AStockBasic.detail_board_name

    @staticmethod
    def get_detail_website():
        """
        :术语名称:
            公司网址
        :术语解释:
            公司网址
        :功能:
            获取公司网址字段
        """
        return AStockBasic.detail_website

    @staticmethod
    def get_detail_main_business():
        """
        :术语名称:
            主营业务
        :术语解释:
            主营业务
        :功能:
            获取主营业务字段
        """
        return AStockBasic.detail_main_business

    @staticmethod
    def get_detail_product_name():
        """
        :术语名称:
            产品名称
        :术语解释:
            产品名称
        :功能:
            获取产品名称字段
        """
        return AStockBasic.detail_product_name

    @staticmethod
    def get_detail_ctrl_shareholder():
        """
        :术语名称:
            控制股东
        :术语解释:
            控制股东
        :功能:
            获取控制股东字段
        """
        return AStockBasic.detail_ctrl_shareholder

    @staticmethod
    def get_detail_actual():
        """
        :术语名称:
            实际控制人
        :术语解释:
            实际控制人
        :功能:
            获取实际控制人字段
        """
        return AStockBasic.detail_actual

    @staticmethod
    def get_detail_ultimate():
        """
        :术语名称:
            最终控制人
        :术语解释:
            最终控制人
        :功能:
            获取最终控制人字段
        """
        return AStockBasic.detail_ultimate

    @staticmethod
    def get_detail_chairman():
        """
        :术语名称:
            董事长
        :术语解释:
            董事长
        :功能:
            获取董事长字段
        """
        return AStockBasic.detail_chairman

    @staticmethod
    def get_detail_secretary():
        """
        :术语名称:
            董秘
        :术语解释:
            董秘
        :功能:
            获取董秘字段
        """
        return AStockBasic.detail_secretary

    @staticmethod
    def get_detail_legal():
        """
        :术语名称:
            法人
        :术语解释:
            法人
        :功能:
            获取法人字段
        """
        return AStockBasic.detail_legal

    @staticmethod
    def get_detail_general_manager():
        """
        :术语名称:
            总经理
        :术语解释:
            总经理
        :功能:
            获取总经理字段
        """
        return AStockBasic.detail_general_manager

    @staticmethod
    def get_detail_regist_capital():
        """
        :术语名称:
            注册资金
        :术语解释:
            注册资金
        :功能:
            获取注册资金字段
        """
        return AStockBasic.detail_regist_capital

    @staticmethod
    def get_detail_staff_num():
        """
        :术语名称:
            员工人数
        :术语解释:
            员工人数
        :功能:
            获取员工人数字段
        """
        return AStockBasic.detail_staff_num

    @staticmethod
    def get_detail_telephone():
        """
        :术语名称:
            电话
        :术语解释:
            电话
        :功能:
            获取电话字段
        """
        return AStockBasic.detail_telephone

    @staticmethod
    def get_detail_fax():
        """
        :术语名称:
            传真
        :术语解释:
            传真
        :功能:
            获取传真字段
        """
        return AStockBasic.detail_fax

    @staticmethod
    def get_detail_zip_code():
        """
        :术语名称:
            邮编
        :术语解释:
            邮编
        :功能:
            获取邮编字段
        """
        return AStockBasic.detail_zip_code

    @staticmethod
    def get_detail_office_address():
        """
        :术语名称:
            办公地址
        :术语解释:
            办公地址
        :功能:
            获取办公地址字段
        """
        return AStockBasic.detail_office_address

    @staticmethod
    def get_detail_company_profile():
        """
        :术语名称:
            公司简介
        :术语解释:
            公司简介
        :功能:
            获取公司简介字段
        """
        return AStockBasic.detail_company_profile

    @staticmethod
    def get_detail_regist_address():
        """
        :术语名称:
            注册地址
        :术语解释:
            注册地址
        :功能:
            获取注册地址字段
        """
        return AStockBasic.detail_regist_address

    @staticmethod
    def get_issue_establish_date():
        """
        :术语名称:
            成立日期
        :术语解释:
            成立日期
        :功能:
            获取成立日期字段
        """
        return AStockBasic.issue_establish_date

    @staticmethod
    def get_issue_price():
        """
        :术语名称:
            发行价格
        :术语解释:
            发行价格
        :功能:
            获取发行价格字段
        """
        return AStockBasic.issue_price

    @staticmethod
    def get_issue_list_date():
        """
        :术语名称:
            上市日期
        :术语解释:
            上市日期
        :功能:
            获取上市日期字段
        """
        return AStockBasic.issue_list_date

    @staticmethod
    def get_issue_pe():
        """
        :术语名称:
            发行市盈率
        :术语解释:
            发行市盈率
        :功能:
            获取发行市盈率字段
        """
        return AStockBasic.issue_pe

    @staticmethod
    def get_issue_estimate():
        """
        :术语名称:
            预计募资
        :术语解释:
            预计募资
        :功能:
            获取预计募资字段
        """
        return AStockBasic.issue_estimate

    @staticmethod
    def get_issue_open_price():
        """
        :术语名称:
            首日开盘价
        :术语解释:
            首日开盘价
        :功能:
            获取首日开盘价字段
        """
        return AStockBasic.issue_open_price

    @staticmethod
    def get_issue_winning_rate():
        """
        :术语名称:
            发行中签率
        :术语解释:
            发行中签率
        :功能:
            获取发行中签率字段
        """
        return AStockBasic.issue_winning_rate

    @staticmethod
    def get_issue_actual():
        """
        :术语名称:
            实际募资
        :术语解释:
            实际募资
        :功能:
            获取实际募资字段
        """
        return AStockBasic.issue_actual

    @staticmethod
    def get_issue_lead_underwriter():
        """
        :术语名称:
            主承销商
        :术语解释:
            主承销商
        :功能:
            获取主承销商字段
        """
        return AStockBasic.issue_lead_underwriter

    @staticmethod
    def get_issue_listing_sponsor():
        """
        :术语名称:
            上市保荐人
        :术语解释:
            上市保荐人
        :功能:
            获取上市保荐人字段
        """
        return AStockBasic.issue_listing_sponsor

    @staticmethod
    def get_issue_nums():
        """
        :术语名称:
            发行数量
        :术语解释:
            发行数量
        :功能:
            获取发行数量字段
        """
        return AStockBasic.issue_nums

    @staticmethod
    def get_roa():
        """
        :术语名称:
            总资产净利率
        :术语解释:
            总资产净利率
        :功能:
            获取总资产净利率字段
        """
        return AStockBasic.roa

    @staticmethod
    def get_roic():
        """
        :术语名称:
            资本回报率
        :术语解释:
            资本回报率
        :功能:
            获取资本回报率字段
        """
        return AStockBasic.roic

    @staticmethod
    def get_parent_net_profit():
        """
        :术语名称:
            归母净利润
        :术语解释:
            归母净利润
        :功能:
            获取归母净利润字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.parent_net_profit

    @staticmethod
    def get_non_net_profit():
        """
        :术语名称:
            扣非净利润
        :术语解释:
            扣非净利润
        :功能:
            获取扣非净利润字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.non_net_profit

    @staticmethod
    def get_income_total():
        """
        :术语名称:
            营业总收入
        :术语解释:
            营业总收入
        :功能:
            获取营业总收入字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.income_total

    @staticmethod
    def get_basic_eps():
        """
        :术语名称:
            基本每股收益
        :术语解释:
            基本每股收益
        :功能:
            获取基本每股收益字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.basic_eps

    @staticmethod
    def get_share_net_asset():
        """
        :术语名称:
            每股净资产
        :术语解释:
            每股净资产
        :功能:
            获取每股净资产字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.share_net_asset

    @staticmethod
    def get_common_fund():
        """
        :术语名称:
            每股资本公积金
        :术语解释:
            每股资本公积金
        :功能:
            获取每股资本公积金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.common_fund

    @staticmethod
    def get_un_profit():
        """
        :术语名称:
            每股未分配利润
        :术语解释:
            每股未分配利润
        :功能:
            获取每股未分配利润字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.un_profit

    @staticmethod
    def get_share_opera_cash():
        """
        :术语名称:
            每股经营现金流
        :术语解释:
            每股经营现金流
        :功能:
            获取每股经营现金流字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.share_opera_cash

    @staticmethod
    def get_gross_margin():
        """
        :术语名称:
            销售毛利率
        :术语解释:
            销售毛利率
        :功能:
            获取销售毛利率字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.gross_margin

    @staticmethod
    def get_sales_margin():
        """
        :术语名称:
            销售净利率
        :术语解释:
            销售净利率
        :功能:
            获取销售净利率字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.sales_margin

    @staticmethod
    def get_roe():
        """
        :术语名称:
            净资产收益率
        :术语解释:
            净资产收益率
        :功能:
            获取净资产收益率字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.roe

    @staticmethod
    def get_roe_diluted():
        """
        :术语名称:
            净资产收益率-摊薄
        :术语解释:
            净资产收益率-摊薄
        :功能:
            获取净资产收益率-摊薄字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.roe_diluted

    @staticmethod
    def get_business_cycle():
        """
        :术语名称:
            营业周期
        :术语解释:
            营业周期
        :功能:
            获取营业周期字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.business_cycle

    @staticmethod
    def get_inventory_turn_rate():
        """
        :术语名称:
            存货周转率
        :术语解释:
            存货周转率
        :功能:
            获取存货周转率字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.inventory_turn_rate

    @staticmethod
    def get_inventory_turn_days():
        """
        :术语名称:
            存货周转天数
        :术语解释:
            存货周转天数
        :功能:
            获取存货周转天数字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.inventory_turn_days

    @staticmethod
    def get_account_turn_days():
        """
        :术语名称:
            应收账款周转天数
        :术语解释:
            应收账款周转天数
        :功能:
            获取应收账款周转天数字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.account_turn_days

    @staticmethod
    def get_current_ratio():
        """
        :术语名称:
            流动比率
        :术语解释:
            流动比率
        :功能:
            获取流动比率字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.current_ratio

    @staticmethod
    def get_quick_ratio():
        """
        :术语名称:
            速动比率
        :术语解释:
            速动比率
        :功能:
            获取速动比率字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.quick_ratio

    @staticmethod
    def get_con_quick_ratio():
        """
        :术语名称:
            保守速动比率
        :术语解释:
            保守速动比率
        :功能:
            获取保守速动比率字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.con_quick_ratio

    @staticmethod
    def get_equity_ratio():
        """
        :术语名称:
            产权比率
        :术语解释:
            产权比率
        :功能:
            获取产权比率字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.equity_ratio

    @staticmethod
    def get_assets_and_liability():
        """
        :术语名称:
            资产负债率
        :术语解释:
            资产负债率
        :功能:
            获取资产负债率字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.assets_and_liability

    @staticmethod
    def get_total_assets_turnover_rate():
        """
        :术语名称:
            总资产周转率
        :术语解释:
            总资产周转率
        :功能:
            获取总资产周转率字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockFinancial.total_assets_turnover_rate

    @staticmethod
    def get_goodssell_labourservice_cash():
        """
        :术语名称:
            销售商品、提供劳务收到的现金
        :术语解释:
            销售商品、提供劳务收到的现金
        :功能:
            获取销售商品、提供劳务收到的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.goodssell_labourservice_cash

    @staticmethod
    def get_taxlevy_refund():
        """
        :术语名称:
            收到的税费返还
        :术语解释:
            收到的税费返还
        :功能:
            获取收到的税费返还字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.taxlevy_refund

    @staticmethod
    def get_operating_cash():
        """
        :术语名称:
            收到其他与经营活动有关的现金
        :术语解释:
            收到其他与经营活动有关的现金
        :功能:
            获取收到其他与经营活动有关的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.operating_cash

    @staticmethod
    def get_operating_inflow_cash():
        """
        :术语名称:
            经营活动现金流入小计
        :术语解释:
            经营活动现金流入小计
        :功能:
            获取经营活动现金流入小计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.operating_inflow_cash

    @staticmethod
    def get_goodsbuy_service_cash():
        """
        :术语名称:
            购买商品、接受劳务支付的现金
        :术语解释:
            购买商品、接受劳务支付的现金
        :功能:
            获取购买商品、接受劳务支付的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.goodsbuy_service_cash

    @staticmethod
    def get_staff_cash():
        """
        :术语名称:
            支付给职工以及为职工支付的现金
        :术语解释:
            支付给职工以及为职工支付的现金
        :功能:
            获取支付给职工以及为职工支付的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.staff_cash

    @staticmethod
    def get_alltaxes_paid():
        """
        :术语名称:
            支付的各项税费
        :术语解释:
            支付的各项税费
        :功能:
            获取支付的各项税费字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.alltaxes_paid

    @staticmethod
    def get_other_operate_cash():
        """
        :术语名称:
            支付其他与经营活动有关的现金
        :术语解释:
            支付其他与经营活动有关的现金
        :功能:
            获取支付其他与经营活动有关的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.other_operate_cash

    @staticmethod
    def get_operating_outflow_cash():
        """
        :术语名称:
            经营活动现金流出小计
        :术语解释:
            经营活动现金流出小计
        :功能:
            获取经营活动现金流出小计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.operating_outflow_cash

    @staticmethod
    def get_net_operate_cash():
        """
        :术语名称:
            经营活动产生的现金流量净额
        :术语解释:
            经营活动产生的现金流量净额
        :功能:
            获取经营活动产生的现金流量净额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.net_operate_cash

    @staticmethod
    def get_invest_return_cash():
        """
        :术语名称:
            收回投资收到的现金
        :术语解释:
            收回投资收到的现金
        :功能:
            获取收回投资收到的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.invest_return_cash

    @staticmethod
    def get_invest_proceeds():
        """
        :术语名称:
            取得投资收益收到的现金
        :术语解释:
            取得投资收益收到的现金
        :功能:
            获取取得投资收益收到的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.invest_proceeds

    @staticmethod
    def get_fixintan_otherassetdispo_cash():
        """
        :术语名称:
            处置固定资产、无形资产和其他长期资产收回的现金净额
        :术语解释:
            处置固定资产、无形资产和其他长期资产收回的现金净额
        :功能:
            获取处置固定资产、无形资产和其他长期资产收回的现金净额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.fixintan_otherassetdispo_cash

    @staticmethod
    def get_subcompany_necash():
        """
        :术语名称:
            处置子公司及其他营业单位收到的现金净额
        :术语解释:
            处置子公司及其他营业单位收到的现金净额
        :功能:
            获取处置子公司及其他营业单位收到的现金净额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.subcompany_necash

    @staticmethod
    def get_invest_other_cash():
        """
        :术语名称:
            收到其他与投资活动有关的现金
        :术语解释:
            收到其他与投资活动有关的现金
        :功能:
            获取收到其他与投资活动有关的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.invest_other_cash

    @staticmethod
    def get_invest_inflow_cash():
        """
        :术语名称:
            投资活动现金流入小计
        :术语解释:
            投资活动现金流入小计
        :功能:
            获取投资活动现金流入小计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.invest_inflow_cash

    @staticmethod
    def get_fixintan_otherasset_acqui_cash():
        """
        :术语名称:
            购建固定资产、无形资产和其他长期资产支付的现金
        :术语解释:
            购建固定资产、无形资产和其他长期资产支付的现金
        :功能:
            获取购建固定资产、无形资产和其他长期资产支付的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.fixintan_otherasset_acqui_cash

    @staticmethod
    def get_invest_paid_cash():
        """
        :术语名称:
            投资支付的现金
        :术语解释:
            投资支付的现金
        :功能:
            获取投资支付的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.invest_paid_cash

    @staticmethod
    def get_suncompany_net_cash():
        """
        :术语名称:
            取得子公司及其他营业单位支付的现金净额
        :术语解释:
            取得子公司及其他营业单位支付的现金净额
        :功能:
            获取取得子公司及其他营业单位支付的现金净额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.suncompany_net_cash

    @staticmethod
    def get_otherpaid_invest_cash():
        """
        :术语名称:
            支付其他与投资活动有关的现金
        :术语解释:
            支付其他与投资活动有关的现金
        :功能:
            获取支付其他与投资活动有关的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.otherpaid_invest_cash

    @staticmethod
    def get_invest_outflow_cash():
        """
        :术语名称:
            投资活动现金流出小计
        :术语解释:
            投资活动现金流出小计
        :功能:
            获取投资活动现金流出小计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.invest_outflow_cash

    @staticmethod
    def get_invest_flow_cash():
        """
        :术语名称:
            投资活动产生的现金流量净额
        :术语解释:
            投资活动产生的现金流量净额
        :功能:
            获取投资活动产生的现金流量净额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.invest_flow_cash

    @staticmethod
    def get_receive_invest_cash():
        """
        :术语名称:
            吸收投资收到的现金
        :术语解释:
            吸收投资收到的现金
        :功能:
            获取吸收投资收到的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.receive_invest_cash

    @staticmethod
    def get_mino_holders_invest_cash():
        """
        :术语名称:
            子公司吸收少数股东投资收到的现金
        :术语解释:
            子公司吸收少数股东投资收到的现金
        :功能:
            获取子公司吸收少数股东投资收到的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.mino_holders_invest_cash

    @staticmethod
    def get_borrow_cash():
        """
        :术语名称:
            取得借款收到的现金
        :术语解释:
            取得借款收到的现金
        :功能:
            获取取得借款收到的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.borrow_cash

    @staticmethod
    def get_bonds_issue_cash():
        """
        :术语名称:
            发行债券收到的现金
        :术语解释:
            发行债券收到的现金
        :功能:
            获取发行债券收到的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.bonds_issue_cash

    @staticmethod
    def get_other_finance_cash():
        """
        :术语名称:
            收到其他与筹资活动有关的现金
        :术语解释:
            收到其他与筹资活动有关的现金
        :功能:
            获取收到其他与筹资活动有关的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.other_finance_cash

    @staticmethod
    def get_finance_inflow_cash():
        """
        :术语名称:
            筹资活动现金流入小计
        :术语解释:
            筹资活动现金流入小计
        :功能:
            获取筹资活动现金流入小计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.finance_inflow_cash

    @staticmethod
    def get_borrowed_repay_cash():
        """
        :术语名称:
            偿还债务支付的现金
        :术语解释:
            偿还债务支付的现金
        :功能:
            获取偿还债务支付的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.borrowed_repay_cash

    @staticmethod
    def get_dividend_porfit_interest_cash():
        """
        :术语名称:
            分配股利、利润或偿付利息支付的现金
        :术语解释:
            分配股利、利润或偿付利息支付的现金
        :功能:
            获取分配股利、利润或偿付利息支付的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.dividend_porfit_interest_cash

    @staticmethod
    def get_sub_pay_minoholders_proceeds():
        """
        :术语名称:
            子公司支付给少数股东的股利、利润或偿付的利息
        :术语解释:
            子公司支付给少数股东的股利、利润或偿付的利息
        :功能:
            获取子公司支付给少数股东的股利、利润或偿付的利息字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.sub_pay_minoholders_proceeds

    @staticmethod
    def get_otherpaid_finance_cash():
        """
        :术语名称:
            支付其他与筹资活动有关的现金
        :术语解释:
            支付其他与筹资活动有关的现金
        :功能:
            获取支付其他与筹资活动有关的现金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.otherpaid_finance_cash

    @staticmethod
    def get_finance_outflow_cash():
        """
        :术语名称:
            筹资活动现金流出小计
        :术语解释:
            筹资活动现金流出小计
        :功能:
            获取筹资活动现金流出小计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.finance_outflow_cash

    @staticmethod
    def get_finance_flow_netcash():
        """
        :术语名称:
            筹资活动产生的现金流量净额
        :术语解释:
            筹资活动产生的现金流量净额
        :功能:
            获取筹资活动产生的现金流量净额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.finance_flow_netcash

    @staticmethod
    def get_ratechange_effect():
        """
        :术语名称:
            汇率变动对现金及现金等价物的影响
        :术语解释:
            汇率变动对现金及现金等价物的影响
        :功能:
            获取汇率变动对现金及现金等价物的影响字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.ratechange_effect

    @staticmethod
    def get_net_increase_cash():
        """
        :术语名称:
            现金及现金等价物净增加额
        :术语解释:
            现金及现金等价物净增加额
        :功能:
            获取现金及现金等价物净增加额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.net_increase_cash

    @staticmethod
    def get_begin_remain_cash():
        """
        :术语名称:
            期初现金及现金等价物余额
        :术语解释:
            期初现金及现金等价物余额
        :功能:
            获取期初现金及现金等价物余额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.begin_remain_cash

    @staticmethod
    def get_end_remain_cash():
        """
        :术语名称:
            期末现金及现金等价物余额
        :术语解释:
            期末现金及现金等价物余额
        :功能:
            获取期末现金及现金等价物余额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.end_remain_cash

    @staticmethod
    def get_assets_decrease_reserve():
        """
        :术语名称:
            资产减值准备
        :术语解释:
            资产减值准备
        :功能:
            获取资产减值准备字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.assets_decrease_reserve

    @staticmethod
    def get_fixedasset_depreciation():
        """
        :术语名称:
            固定资产折旧
        :术语解释:
            固定资产折旧
        :功能:
            获取固定资产折旧字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.fixedasset_depreciation

    @staticmethod
    def get_immaterialasset_amortization():
        """
        :术语名称:
            无形资产摊销
        :术语解释:
            无形资产摊销
        :功能:
            获取无形资产摊销字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.immaterialasset_amortization

    @staticmethod
    def get_deferred_expense():
        """
        :术语名称:
            长期待摊费用摊销
        :术语解释:
            长期待摊费用摊销
        :功能:
            获取长期待摊费用摊销字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.deferred_expense

    @staticmethod
    def get_deal_assets_loss():
        """
        :术语名称:
            处置固定资产、无形资产和其他长期资产的损失
        :术语解释:
            处置固定资产、无形资产和其他长期资产的损失
        :功能:
            获取处置固定资产、无形资产和其他长期资产的损失字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.deal_assets_loss

    @staticmethod
    def get_fixedassets_scrap_loss():
        """
        :术语名称:
            固定资产报废损失
        :术语解释:
            固定资产报废损失
        :功能:
            获取固定资产报废损失字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.fixedassets_scrap_loss

    @staticmethod
    def get_firevalue_change_loss():
        """
        :术语名称:
            公允价值变动损失
        :术语解释:
            公允价值变动损失
        :功能:
            获取公允价值变动损失字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.firevalue_change_loss

    @staticmethod
    def get_finance_expense():
        """
        :术语名称:
            现金流量财务费用
        :术语解释:
            现金流量财务费用
        :功能:
            获取现金流量财务费用字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.finance_expense

    @staticmethod
    def get_invest_loss():
        """
        :术语名称:
            投资损失
        :术语解释:
            投资损失
        :功能:
            获取投资损失字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.invest_loss

    @staticmethod
    def get_decrease_defered_tax_asset():
        """
        :术语名称:
            递延所得税资产减少
        :术语解释:
            递延所得税资产减少
        :功能:
            获取递延所得税资产减少字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.decrease_defered_tax_asset

    @staticmethod
    def get_increase_defered_taxasset_debt():
        """
        :术语名称:
            递延所得税负债增加
        :术语解释:
            递延所得税负债增加
        :功能:
            获取递延所得税负债增加字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.increase_defered_taxasset_debt

    @staticmethod
    def get_decrease_inventory():
        """
        :术语名称:
            存货的减少
        :术语解释:
            存货的减少
        :功能:
            获取存货的减少字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.decrease_inventory

    @staticmethod
    def get_decrease_operate_receivable():
        """
        :术语名称:
            经营性应收项目的减少
        :术语解释:
            经营性应收项目的减少
        :功能:
            获取经营性应收项目的减少字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.decrease_operate_receivable

    @staticmethod
    def get_increase_operate_receivable():
        """
        :术语名称:
            经营性应付项目的增加
        :术语解释:
            经营性应付项目的增加
        :功能:
            获取经营性应付项目的增加字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.increase_operate_receivable

    @staticmethod
    def get_other_cashflow():
        """
        :术语名称:
            其他现金流量
        :术语解释:
            其他现金流量
        :功能:
            获取其他现金流量字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.other_cashflow

    @staticmethod
    def get_net_operate_cashflow_notes():
        """
        :术语名称:
            间接法经营活动产生的现金流量净额
        :术语解释:
            间接法经营活动产生的现金流量净额
        :功能:
            获取间接法经营活动产生的现金流量净额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.net_operate_cashflow_notes

    @staticmethod
    def get_end_cash():
        """
        :术语名称:
            现金的期末余额
        :术语解释:
            现金的期末余额
        :功能:
            获取现金的期末余额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.end_cash

    @staticmethod
    def get_begin_cash():
        """
        :术语名称:
            现金的期初余额
        :术语解释:
            现金的期初余额
        :功能:
            获取现金的期初余额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.begin_cash

    @staticmethod
    def get_begin_cash_equivalents():
        """
        :术语名称:
            现金等价物的期初余额
        :术语解释:
            现金等价物的期初余额
        :功能:
            获取现金等价物的期初余额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.begin_cash_equivalents

    @staticmethod
    def get_netincr_cash_and_equivalents():
        """
        :术语名称:
            间接法现金及现金等价物净增加额
        :术语解释:
            间接法现金及现金等价物净增加额
        :功能:
            获取间接法现金及现金等价物净增加额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockCashFlow.netincr_cash_and_equivalents

    @staticmethod
    def get_total_a_marketvalue():
        """
        :术语名称:
            A股总股本
        :术语解释:
            A股总股本
        :功能:
            获取A股总股本字段
        """
        return AStockStructure.total_a_marketvalue

    @staticmethod
    def get_cir_a_marketvalue():
        """
        :术语名称:
            流通A股
        :术语解释:
            流通A股
        :功能:
            获取流通A股字段
        """
        return AStockStructure.cir_a_marketvalue

    @staticmethod
    def get_limit_a_marketvalue():
        """
        :术语名称:
            限售A股
        :术语解释:
            限售A股
        :功能:
            获取限售A股字段
        """
        return AStockStructure.limit_a_marketvalue

    @staticmethod
    def get_change_reason():
        """
        :术语名称:
            变动原因
        :术语解释:
            变动原因
        :功能:
            获取变动原因字段
        """
        return AStockStructure.change_reason

    @staticmethod
    def get_total_b_marketvalue():
        """
        :术语名称:
            B股总股本
        :术语解释:
            B股总股本
        :功能:
            获取B股总股本字段
        """
        return AStockStructure.total_b_marketvalue

    @staticmethod
    def get_cir_b_marketvalue():
        """
        :术语名称:
            流通B股
        :术语解释:
            流通B股
        :功能:
            获取流通B股字段
        """
        return AStockStructure.cir_b_marketvalue

    @staticmethod
    def get_limit_b_marketvalue():
        """
        :术语名称:
            限售B股
        :术语解释:
            限售B股
        :功能:
            获取限售B股字段
        """
        return AStockStructure.limit_b_marketvalue

    @staticmethod
    def get_total_h_marketvalue():
        """
        :术语名称:
            H股总股本
        :术语解释:
            H股总股本
        :功能:
            获取H股总股本字段
        """
        return AStockStructure.total_h_marketvalue

    @staticmethod
    def get_cir_h_marketvalue():
        """
        :术语名称:
            流通H股
        :术语解释:
            流通H股
        :功能:
            获取流通H股字段
        """
        return AStockStructure.cir_h_marketvalue

    @staticmethod
    def get_limit_h_marketvalue():
        """
        :术语名称:
            限售H股
        :术语解释:
            限售H股
        :功能:
            获取限售H股字段
        """
        return AStockStructure.limit_h_marketvalue

    @staticmethod
    def get_total_cir_marketvalue():
        """
        :术语名称:
            流通总股本
        :术语解释:
            流通总股本
        :功能:
            获取流通总股本字段
        """
        return AStockStructure.total_cir_marketvalue

    @staticmethod
    def get_total_limit_marketvalue():
        """
        :术语名称:
            限售总股本
        :术语解释:
            限售总股本
        :功能:
            获取限售总股本字段
        """
        return AStockStructure.total_limit_marketvalue

    @staticmethod
    def get_holders_a_total():
        """
        :术语名称:
            A股股东总人数
        :术语解释:
            A股股东总人数
        :功能:
            获取A股股东总人数字段
        """
        return AStockShareHolder.holders_a_total

    @staticmethod
    def get_holders_b_total():
        """
        :术语名称:
            B股股东总人数
        :术语解释:
            B股股东总人数
        :功能:
            获取B股股东总人数字段
        """
        return AStockShareHolder.holders_b_total

    @staticmethod
    def get_holders_h_total():
        """
        :术语名称:
            H股股东总人数
        :术语解释:
            H股股东总人数
        :功能:
            获取H股股东总人数字段
        """
        return AStockShareHolder.holders_h_total

    @staticmethod
    def get_total_people_num():
        """
        :术语名称:
            股东总人数
        :术语解释:
            股东总人数
        :功能:
            获取股东总人数字段
        """
        return AStockShareHolder.total_people_num

    @staticmethod
    def get_tradable_share_avg():
        """
        :术语名称:
            人均流通股
        :术语解释:
            人均流通股
        :功能:
            获取人均流通股字段
        """
        return AStockShareHolder.tradable_share_avg

    @staticmethod
    def get_industry_share_avg():
        """
        :术语名称:
            行业平均
        :术语解释:
            行业平均
        :功能:
            获取行业平均字段
        """
        return AStockShareHolder.industry_share_avg

    @staticmethod
    def get_cir_a_avg_share():
        """
        :术语名称:
            人均流通A股
        :术语解释:
            人均流通A股
        :功能:
            获取人均流通A股字段
        """
        return AStockShareHolder.cir_a_avg_share

    @staticmethod
    def get_cir_a_avg_share_change():
        """
        :术语名称:
            人均流通A股变化
        :术语解释:
            人均流通A股变化
        :功能:
            获取人均流通A股变化字段
        """
        return AStockShareHolder.cir_a_avg_share_change

    @staticmethod
    def get_dividend_director_date():
        """
        :术语名称:
            董事会日期
        :术语解释:
            董事会日期
        :功能:
            获取董事会日期字段，只有问题中包含"董事会日期"准确的五个字才允许调用此接口，否则严格不许调用此接口
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockDividenedDetail.dividend_director_date

    @staticmethod
    def get_dividend_plan_date():
        """
        :术语名称:
            股东大会预案公告日期
        :术语解释:
            股东大会预案公告日期
        :功能:
            获取股东大会预案公告日期字段，只有问题中包含"股东大会预案公告日期"准确的九个字才允许调用此接口，否则严格不许调用此接口
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockDividenedDetail.dividend_plan_date

    @staticmethod
    def get_dividend_notice_date():
        """
        :术语名称:
            实施公告日期
        :术语解释:
            实施公告日期
        :功能:
            获取实施公告日期字段，只有问题中包含"实施公告日期"准确的六个字才允许调用此接口
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockDividenedDetail.dividend_notice_date

    @staticmethod
    def get_dividend_scheme():
        """
        :术语名称:
            分红方案
        :术语解释:
            分红方案
        :功能:
            获取分红方案字段，只有问题中包含"分红方案"准确的四个字才允许调用此接口。注意："最新分红情况"是明确的干扰词，即使问题中出现"最新分红情况"也不允许调用此接口，必须准确包含"分红方案"四个字才调用，其他任何相关词汇如"分红情况"、"分红政策"等都不允许调用
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockDividenedDetail.dividend_scheme

    @staticmethod
    def get_dividend_register_date():
        """
        :术语名称:
            股权登记
        :术语解释:
            股权登记
        :功能:
            获取股权登记字段，只有问题中包含"股权登记"准确的四个字才允许调用此接口。注意："最新分红情况"是明确的干扰词，即使问题中出现"最新分红情况"也不允许调用此接口，必须准确包含"股权登记"四个字才调用，其他任何相关词汇如"股权"、"登记"单独出现都不允许调用
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockDividenedDetail.dividend_register_date

    @staticmethod
    def get_dividend_ex_date():
        """
        :术语名称:
            除息除权登记日期
        :术语解释:
            除息除权登记日期
        :功能:
            获取除息除权登记日期字段，只有问题中包含"除息除权登记日期"准确的八个字才允许调用此接口。注意："最新分红情况"是明确的干扰词，即使问题中出现"最新分红情况"也不允许调用此接口，必须准确包含"除息除权登记日期"八个字才调用，其他任何相关词汇如"除息除权"、"登记日期"单独出现都不允许调用
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockDividenedDetail.dividend_ex_date

    @staticmethod
    def get_dividend_amount():
        """
        :术语名称:
            分红总额
        :术语解释:
            分红总额
        :功能:
            获取分红总额字段，只有问题中包含"分红总额"准确的四个字才允许调用此接口。注意："最新分红情况"是明确的干扰词，即使问题中出现"最新分红情况"也不允许调用此接口，必须准确包含"分红总额"四个字才调用，其他任何相关词汇如"分红"、"总额"单独出现都不允许调用
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockDividenedDetail.dividend_amount

    @staticmethod
    def get_dividend_schedule():
        """
        :术语名称:
            分红情况方案进度
        :术语解释:
            分红情况方案进度
        :功能:
            获取分红情况方案进度字段，只有问题中包含"方案进度"准确的四个字才允许调用此接口。注意："最新分红情况"是明确的干扰词，即使问题中出现"最新分红情况"也不允许调用此接口，必须准确包含"方案进度"四个字才调用，其他任何相关词汇如"分红情况"、"分红进度"、"最新分红"等都不允许调用
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockDividenedDetail.dividend_schedule

    @staticmethod
    def get_dividend_pay_ratio():
        """
        :术语名称:
            股息支付率
        :术语解释:
            股息支付率
        :功能:
            获取股息支付率字段，只有问题中包含"股息支付率"准确的五个字才允许调用此接口。注意："最新分红情况"是明确的干扰词，即使问题中出现"最新分红情况"也不允许调用此接口，必须准确包含"股息支付率"五个字才调用，其他任何相关词汇如"股息"、"支付率"单独出现都不允许调用
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockDividenedDetail.dividend_pay_ratio

    @staticmethod
    def get_dividend_ratio():
        """
        :术语名称:
            税前分红率
        :术语解释:
            税前分红率
        :功能:
            获取税前分红率字段，只有问题中包含"税前分红率"准确的五个字才允许调用此接口。注意："最新分红情况"是明确的干扰词，即使问题中出现"最新分红情况"也不允许调用此接口，必须准确包含"税前分红率"五个字才调用，其他任何相关词汇如"税前"、"分红率"单独出现都不允许调用
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockDividenedDetail.dividend_ratio

    @staticmethod
    def get_addition_issue_price():
        """
        :术语名称:
            实际发行价格
        :术语解释:
            实际发行价格
        :功能:
            获取实际发行价格字段
        """
        return AStockDividenedAddition.addition_issue_price

    @staticmethod
    def get_addition_issue_nums():
        """
        :术语名称:
            实际发行数量
        :术语解释:
            实际发行数量
        :功能:
            获取实际发行数量字段
        """
        return AStockDividenedAddition.addition_issue_nums

    @staticmethod
    def get_addition_amount():
        """
        :术语名称:
            实际募资净额
        :术语解释:
            实际募资净额
        :功能:
            获取实际募资净额字段
        """
        return AStockDividenedAddition.addition_amount

    @staticmethod
    def get_addition_price_method():
        """
        :术语名称:
            发行定价方式
        :术语解释:
            发行定价方式
        :功能:
            获取发行定价方式字段
        """
        return AStockDividenedAddition.addition_price_method

    @staticmethod
    def get_addition_date():
        """
        :术语名称:
            增发时间
        :术语解释:
            增发时间
        :功能:
            获取增发时间字段
        """
        return AStockDividenedAddition.addition_date

    @staticmethod
    def get_addition_process():
        """
        :术语名称:
            增发进度
        :术语解释:
            增发进度
        :功能:
            获取增发进度字段
        """
        return AStockDividenedAddition.addition_process

    @staticmethod
    def get_allotment_net_amount():
        """
        :术语名称:
            实际募资净额
        :术语解释:
            实际募资净额
        :功能:
            获取实际募资净额字段
        """
        return AStockDividenedAllotment.allotment_net_amount

    @staticmethod
    def get_allotment_code():
        """
        :术语名称:
            配股代码
        :术语解释:
            配股代码
        :功能:
            获取配股代码字段
        """
        return AStockDividenedAllotment.allotment_code

    @staticmethod
    def get_allotment_brief_name():
        """
        :术语名称:
            配股简称
        :术语解释:
            配股简称
        :功能:
            获取配股简称字段
        """
        return AStockDividenedAllotment.allotment_brief_name

    @staticmethod
    def get_allotment_actual_ratio():
        """
        :术语名称:
            实际配股比例
        :术语解释:
            实际配股比例
        :功能:
            获取实际配股比例字段
        """
        return AStockDividenedAllotment.allotment_actual_ratio

    @staticmethod
    def get_allotment_list_date():
        """
        :术语名称:
            配股上市日
        :术语解释:
            配股上市日
        :功能:
            获取配股上市日字段
        """
        return AStockDividenedAllotment.allotment_list_date

    @staticmethod
    def get_allotment_approval_date():
        """
        :术语名称:
            证监会核准公告日
        :术语解释:
            证监会核准公告日
        :功能:
            获取证监会核准公告日字段
        """
        return AStockDividenedAllotment.allotment_approval_date

    @staticmethod
    def get_allotment_per_price():
        """
        :术语名称:
            每股配股价格
        :术语解释:
            每股配股价格
        :功能:
            获取每股配股价格字段
        """
        return AStockDividenedAllotment.allotment_per_price

    @staticmethod
    def get_allotment_begin_date():
        """
        :术语名称:
            缴款起始日
        :术语解释:
            缴款起始日
        :功能:
            获取缴款起始日字段
        """
        return AStockDividenedAllotment.allotment_begin_date

    @staticmethod
    def get_allotment_end_date():
        """
        :术语名称:
            缴款截止日
        :术语解释:
            缴款截止日
        :功能:
            获取缴款截止日字段
        """
        return AStockDividenedAllotment.allotment_end_date

    @staticmethod
    def get_allotment_regist_date():
        """
        :术语名称:
            股权登记日
        :术语解释:
            股权登记日
        :功能:
            获取股权登记日字段
        """
        return AStockDividenedAllotment.allotment_regist_date

    @staticmethod
    def get_allotment_notice_date():
        """
        :术语名称:
            董事会公告日
        :术语解释:
            董事会公告日
        :功能:
            获取董事会公告日字段，问最新配股情况董事会公告日要调用
        """
        return AStockDividenedAllotment.allotment_notice_date

    @staticmethod
    def get_allotment_process():
        """
        :术语名称:
            配股情况方案进度
        :术语解释:
            配股情况方案进度
        :功能:
            获取配股情况方案进度字段
        """
        return AStockDividenedAllotment.allotment_process

    @staticmethod
    def get_institution_total_nums():
        """
        :术语名称:
            机构数量
        :术语解释:
            机构数量
        :功能:
            获取机构数量字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockPositionSum.institution_total_nums

    @staticmethod
    def get_institution_hold_nums():
        """
        :术语名称:
            累计持有数量
        :术语解释:
            累计持有数量
        :功能:
            获取累计持有数量字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockPositionSum.institution_hold_nums

    @staticmethod
    def get_institution_hold_value():
        """
        :术语名称:
            累计市值
        :术语解释:
            累计市值
        :功能:
            获取累计市值字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockPositionSum.institution_hold_value

    @staticmethod
    def get_institution_hold_ratio():
        """
        :术语名称:
            持仓比例
        :术语解释:
            持仓比例
        :功能:
            获取持仓比例字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockPositionSum.institution_hold_ratio

    @staticmethod
    def get_optype():
        """
        :术语名称:
            出击类型
        :术语解释:
            出击类型
        :功能:
            获取出击类型字段,询问小黄锁时使用
        """
        return DailyThreeMethodLevel0.optype

    @staticmethod
    def get_opprice():
        """
        :术语名称:
            出击价格
        :术语解释:
            出击价格
        :功能:
            获取出击价格字段
        """
        return DailyThreeMethodLevel0.opprice

    @staticmethod
    def get_value_policy():
        """
        :术语名称:
            价值决策
        :术语解释:
            价值决策
        :功能:
            获取价值决策字段
        """
        return DailyCwDataLevel0.value_policy

    @staticmethod
    def get_long_policy():
        """
        :术语名称:
            长线决策
        :术语解释:
            长线决策
        :功能:
            获取长线决策字段
        """
        return DailyCwDataLevel0.long_policy

    @staticmethod
    def get_long_marketopsatus():
        """
        :术语名称:
            六大市场长线决策
        :术语解释:
            六大市场长线决策
        :功能:
            获取六大市场长线决策字段
        """
        return DailyCwDataLevel0.long_marketopsatus

    @staticmethod
    def get_short_policy():
        """
        :术语名称:
            短线决策
        :术语解释:
            短线决策
        :功能:
            获取短线决策字段
        """
        return DailyCwDataLevel0.short_policy

    @staticmethod
    def get_op_policy():
        """
        :术语名称:
            波段决策
        :术语解释:
            波段决策
        :功能:
            获取波段决策字段
        """
        return DailyCwDataLevel0.op_policy

    @staticmethod
    def get_gzhigh():
        """
        :术语名称:
            估值空间风险线
        :术语解释:
            估值空间风险线
        :功能:
            获取估值空间风险线字段
        """
        return DailyGzkjLevel0.gzhigh

    @staticmethod
    def get_gzmid():
        """
        :术语名称:
            估值空间中线
        :术语解释:
            估值空间中线
        :功能:
            获取估值空间中线字段
        """
        return DailyGzkjLevel0.gzmid

    @staticmethod
    def get_gzlow():
        """
        :术语名称:
            估值空间安全线
        :术语解释:
            估值空间安全线
        :功能:
            获取估值空间安全线字段
        """
        return DailyGzkjLevel0.gzlow

    @staticmethod
    def get_cash():
        """
        :术语名称:
            货币资金
        :术语解释:
            货币资金
        :功能:
            获取货币资金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.cash

    @staticmethod
    def get_bill_accounts_receivable():
        """
        :术语名称:
            应收票据及应收账款
        :术语解释:
            应收票据及应收账款
        :功能:
            获取应收票据及应收账款字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.bill_accounts_receivable

    @staticmethod
    def get_trading_assets():
        """
        :术语名称:
            以公允价值计量且其变动计入当期损益的金融资产
        :术语解释:
            以公允价值计量且其变动计入当期损益的金融资产
        :功能:
            获取以公允价值计量且其变动计入当期损益的金融资产字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.trading_assets

    @staticmethod
    def get_bill_receivable():
        """
        :术语名称:
            应收票据
        :术语解释:
            应收票据
        :功能:
            获取应收票据字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.bill_receivable

    @staticmethod
    def get_accounts_receivable():
        """
        :术语名称:
            应收账款
        :术语解释:
            应收账款
        :功能:
            获取应收账款字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.accounts_receivable

    @staticmethod
    def get_advance_payment():
        """
        :术语名称:
            预付款项
        :术语解释:
            预付款项
        :功能:
            获取预付款项字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.advance_payment

    @staticmethod
    def get_other_receivable_ed():
        """
        :术语名称:
            其他应收款(含利息和股利)
        :术语解释:
            其他应收款(含利息和股利)
        :功能:
            获取其他应收款(含利息和股利)字段，问“其他应收款合计”要调用此接口！
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.other_receivable_ed

    @staticmethod
    def get_interest_receive():
        """
        :术语名称:
            应收利息
        :术语解释:
            应收利息
        :功能:
            获取应收利息字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.interest_receive

    @staticmethod
    def get_other_receive():
        """
        :术语名称:
            其他应收款
        :术语解释:
            其他应收款
        :功能:
            获取其他应收款字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.other_receive

    @staticmethod
    def get_inventory():
        """
        :术语名称:
            存货
        :术语解释:
            存货
        :功能:
            获取存货字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.inventory

    @staticmethod
    def get_contrac_assets():
        """
        :术语名称:
            合同资产
        :术语解释:
            合同资产
        :功能:
            获取合同资产字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.contrac_assets

    @staticmethod
    def get_nocurrent_assets_1year():
        """
        :术语名称:
            一年内到期的非流动资产
        :术语解释:
            一年内到期的非流动资产
        :功能:
            获取一年内到期的非流动资产字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.nocurrent_assets_1year

    @staticmethod
    def get_other_current_assets():
        """
        :术语名称:
            其他流动资产
        :术语解释:
            其他流动资产
        :功能:
            获取其他流动资产字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.other_current_assets

    @staticmethod
    def get_total_current_assets():
        """
        :术语名称:
            流动资产合计
        :术语解释:
            流动资产合计
        :功能:
            获取流动资产合计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.total_current_assets

    @staticmethod
    def get_hold_forsale_assets():
        """
        :术语名称:
            可供出售金融资产
        :术语解释:
            可供出售金融资产
        :功能:
            获取可供出售金融资产字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.hold_forsale_assets

    @staticmethod
    def get_hold_maturity_investments():
        """
        :术语名称:
            持有至到期投资
        :术语解释:
            持有至到期投资
        :功能:
            获取持有至到期投资字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.hold_maturity_investments

    @staticmethod
    def get_longterm_equity_invest():
        """
        :术语名称:
            长期股权投资
        :术语解释:
            长期股权投资
        :功能:
            获取长期股权投资字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.longterm_equity_invest

    @staticmethod
    def get_invest_property():
        """
        :术语名称:
            投资性房地产
        :术语解释:
            投资性房地产
        :功能:
            获取投资性房地产字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.invest_property

    @staticmethod
    def get_fixed_assets():
        """
        :术语名称:
            固定资产合计
        :术语解释:
            固定资产合计
        :功能:
            获取固定资产合计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.fixed_assets

    @staticmethod
    def get_fixed_assets_checkup():
        """
        :术语名称:
            固定资产清理
        :术语解释:
            固定资产清理
        :功能:
            获取固定资产清理字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.fixed_assets_checkup

    @staticmethod
    def get_construct_project():
        """
        :术语名称:
            在建工程合计
        :术语解释:
            在建工程合计
        :功能:
            获取在建工程合计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.construct_project

    @staticmethod
    def get_intangible_assets():
        """
        :术语名称:
            无形资产
        :术语解释:
            无形资产
        :功能:
            获取无形资产字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.intangible_assets

    @staticmethod
    def get_good_will():
        """
        :术语名称:
            商誉
        :术语解释:
            商誉
        :功能:
            获取商誉字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.good_will

    @staticmethod
    def get_long_deferred_expense():
        """
        :术语名称:
            长期待摊费用
        :术语解释:
            长期待摊费用
        :功能:
            获取长期待摊费用字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.long_deferred_expense

    @staticmethod
    def get_other_noncurrent_assets():
        """
        :术语名称:
            其他非流动资产
        :术语解释:
            其他非流动资产
        :功能:
            获取其他非流动资产字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.other_noncurrent_assets

    @staticmethod
    def get_total_noncurrent_assets():
        """
        :术语名称:
            非流动资产合计
        :术语解释:
            非流动资产合计
        :功能:
            获取非流动资产合计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.total_noncurrent_assets

    @staticmethod
    def get_total_assets():
        """
        :术语名称:
            资产总计
        :术语解释:
            资产总计
        :功能:
            获取资产总计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.total_assets

    @staticmethod
    def get_shortterm_loan():
        """
        :术语名称:
            短期借款
        :术语解释:
            短期借款
        :功能:
            获取短期借款字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.shortterm_loan

    @staticmethod
    def get_trading_liability():
        """
        :术语名称:
            以公允价值计量且变动计入当期损益的金融负债
        :术语解释:
            以公允价值计量且变动计入当期损益的金融负债
        :功能:
            获取以公允价值计量且变动计入当期损益的金融负债字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.trading_liability

    @staticmethod
    def get_derivative_liability():
        """
        :术语名称:
            衍生金融负债
        :术语解释:
            衍生金融负债
        :功能:
            获取衍生金融负债字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.derivative_liability

    @staticmethod
    def get_notaccounts_payment():
        """
        :术语名称:
            应付票据及应付账款
        :术语解释:
            应付票据及应付账款
        :功能:
            获取应付票据及应付账款字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.notaccounts_payment

    @staticmethod
    def get_notes_payment():
        """
        :术语名称:
            应付票据
        :术语解释:
            应付票据
        :功能:
            获取应付票据字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.notes_payment

    @staticmethod
    def get_accounts_payment():
        """
        :术语名称:
            应付账款
        :术语解释:
            应付账款
        :功能:
            获取应付账款字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.accounts_payment

    @staticmethod
    def get_contract_liability():
        """
        :术语名称:
            合同负债
        :术语解释:
            合同负债
        :功能:
            获取合同负债字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.contract_liability

    @staticmethod
    def get_advance_receipts():
        """
        :术语名称:
            预收款项
        :术语解释:
            预收款项
        :功能:
            获取预收款项字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.advance_receipts

    @staticmethod
    def get_nocurrent_liability_1year():
        """
        :术语名称:
            一年内到期的非流动负债
        :术语解释:
            一年内到期的非流动负债
        :功能:
            获取一年内到期的非流动负债字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.nocurrent_liability_1year

    @staticmethod
    def get_othercurrent_liability():
        """
        :术语名称:
            其他流动负债
        :术语解释:
            其他流动负债
        :功能:
            获取其他流动负债字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.othercurrent_liability

    @staticmethod
    def get_totalcurrent_liability():
        """
        :术语名称:
            流动负债合计
        :术语解释:
            流动负债合计
        :功能:
            获取流动负债合计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.totalcurrent_liability

    @staticmethod
    def get_paid_capital():
        """
        :术语名称:
            实收资本(或股本)
        :术语解释:
            实收资本(或股本)
        :功能:
            获取实收资本(或股本)字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.paid_capital

    @staticmethod
    def get_capital_reserve_fund():
        """
        :术语名称:
            资本公积
        :术语解释:
            资本公积
        :功能:
            获取资本公积字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.capital_reserve_fund

    @staticmethod
    def get_treasury_stock():
        """
        :术语名称:
            库存股
        :术语解释:
            库存股
        :功能:
            获取库存股字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.treasury_stock

    @staticmethod
    def get_other_composite_income():
        """
        :术语名称:
            其他综合收益
        :术语解释:
            其他综合收益
        :功能:
            获取其他综合收益字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.other_composite_income

    @staticmethod
    def get_surplus_reserve_fund():
        """
        :术语名称:
            盈余公积
        :术语解释:
            盈余公积
        :功能:
            获取盈余公积字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.surplus_reserve_fund

    @staticmethod
    def get_retained_profit():
        """
        :术语名称:
            未分配利润
        :术语解释:
            未分配利润
        :功能:
            获取未分配利润字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.retained_profit

    @staticmethod
    def get_parent_shareholder_equity():
        """
        :术语名称:
            归属母公司股东权益合计
        :术语解释:
            归属母公司股东权益合计
        :功能:
            获取归属母公司股东权益合计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.parent_shareholder_equity

    @staticmethod
    def get_minority_interests():
        """
        :术语名称:
            少数股东权益
        :术语解释:
            少数股东权益
        :功能:
            获取少数股东权益字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.minority_interests

    @staticmethod
    def get_total_shareholder_equity():
        """
        :术语名称:
            所有者权益(或股东权益)合计
        :术语解释:
            所有者权益(或股东权益)合计
        :功能:
            获取所有者权益(或股东权益)合计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.total_shareholder_equity

    @staticmethod
    def get_total_liability_equity():
        """
        :术语名称:
            负债和所有者权益(或股东权益)总计
        :术语解释:
            负债和所有者权益(或股东权益)总计
        :功能:
            获取负债和所有者权益(或股东权益)总计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.total_liability_equity

    @staticmethod
    def get_deferred_tax_assets():
        """
        :术语名称:
            递延所得税资产
        :术语解释:
            递延所得税资产
        :功能:
            获取递延所得税资产字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.deferred_tax_assets

    @staticmethod
    def get_total_liability():
        """
        :术语名称:
            总负债合计
        :术语解释:
            总负债合计
        :功能:
            获取总负债合计字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return StockFinancialPropertyBalanceView.total_liability

    @staticmethod
    def get_income():
        """
        :术语名称:
            营业收入
        :术语解释:
            营业收入
        :功能:
            获取营业收入字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.income

    @staticmethod
    def get_cost_total():
        """
        :术语名称:
            营业总成本
        :术语解释:
            营业总成本
        :功能:
            获取营业总成本字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.cost_total

    @staticmethod
    def get_cost():
        """
        :术语名称:
            营业成本
        :术语解释:
            营业成本
        :功能:
            获取营业成本字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.cost

    @staticmethod
    def get_tax_surcharges():
        """
        :术语名称:
            营业税金及附加
        :术语解释:
            营业税金及附加
        :功能:
            获取营业税金及附加字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.tax_surcharges

    @staticmethod
    def get_sell_expense():
        """
        :术语名称:
            销售费用
        :术语解释:
            销售费用
        :功能:
            获取销售费用字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.sell_expense

    @staticmethod
    def get_manage_expense():
        """
        :术语名称:
            管理费用
        :术语解释:
            管理费用
        :功能:
            获取管理费用字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.manage_expense

    @staticmethod
    def get_rd_expense():
        """
        :术语名称:
            研发费用
        :术语解释:
            研发费用
        :功能:
            获取研发费用字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.rd_expense

    @staticmethod
    def get_finance_expense_total():
        """
        :术语名称:
            财务费用
        :术语解释:
            财务费用
        :功能:
            获取财务费用字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.finance_expense_total

    @staticmethod
    def get_interest_finexp():
        """
        :术语名称:
            利息费用(财务费用)
        :术语解释:
            利息费用(财务费用)
        :功能:
            获取利息费用(财务费用)字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.interest_finexp

    @staticmethod
    def get_income_finexp():
        """
        :术语名称:
            利息收入(财务费用)
        :术语解释:
            利息收入(财务费用)
        :功能:
            获取利息收入(财务费用)字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.income_finexp

    @staticmethod
    def get_asset_impairment_loss():
        """
        :术语名称:
            资产减值损失
        :术语解释:
            资产减值损失
        :功能:
            获取资产减值损失字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.asset_impairment_loss

    @staticmethod
    def get_credit_impairment_loss():
        """
        :术语名称:
            信用减值损失
        :术语解释:
            信用减值损失
        :功能:
            获取信用减值损失字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.credit_impairment_loss

    @staticmethod
    def get_income_firevalue_change():
        """
        :术语名称:
            公允价值变动收益
        :术语解释:
            公允价值变动收益
        :功能:
            获取公允价值变动收益字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.income_firevalue_change

    @staticmethod
    def get_income_invest():
        """
        :术语名称:
            投资收益
        :术语解释:
            投资收益
        :功能:
            获取投资收益字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.income_invest

    @staticmethod
    def get_income_invest_associates():
        """
        :术语名称:
            联营和合营企业的投资收益
        :术语解释:
            联营和合营企业的投资收益
        :功能:
            获取联营和合营企业的投资收益字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.income_invest_associates

    @staticmethod
    def get_income_assetdeal():
        """
        :术语名称:
            资产处置收益
        :术语解释:
            资产处置收益
        :功能:
            获取资产处置收益字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.income_assetdeal

    @staticmethod
    def get_operating_profit():
        """
        :术语名称:
            营业利润
        :术语解释:
            营业利润
        :功能:
            获取营业利润字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.operating_profit

    @staticmethod
    def get_income_nonoperating():
        """
        :术语名称:
            营业外收入
        :术语解释:
            营业外收入
        :功能:
            获取营业外收入字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.income_nonoperating

    @staticmethod
    def get_earn_noncurrent_assetss():
        """
        :术语名称:
            非流动资产处置利得
        :术语解释:
            非流动资产处置利得
        :功能:
            获取非流动资产处置利得字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.earn_noncurrent_assetss

    @staticmethod
    def get_expense_nonoperating():
        """
        :术语名称:
            营业外支出
        :术语解释:
            营业外支出
        :功能:
            获取营业外支出字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.expense_nonoperating

    @staticmethod
    def get_loss_nocurrent_assets():
        """
        :术语名称:
            非流动资产处置净损失
        :术语解释:
            非流动资产处置净损失
        :功能:
            获取非流动资产处置净损失字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.loss_nocurrent_assets

    @staticmethod
    def get_profit_total():
        """
        :术语名称:
            利润总额
        :术语解释:
            利润总额
        :功能:
            获取利润总额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.profit_total

    @staticmethod
    def get_cost_incometax():
        """
        :术语名称:
            所得税费用
        :术语解释:
            所得税费用
        :功能:
            获取所得税费用字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.cost_incometax

    @staticmethod
    def get_net_profit():
        """
        :术语名称:
            净利润
        :术语解释:
            净利润
        :功能:
            获取净利润字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.net_profit

    @staticmethod
    def get_operate_profit():
        """
        :术语名称:
            持续经营净利润
        :术语解释:
            持续经营净利润
        :功能:
            获取持续经营净利润字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.operate_profit

    @staticmethod
    def get_minority_profit():
        """
        :术语名称:
            少数股东损益
        :术语解释:
            少数股东损益
        :功能:
            获取少数股东损益字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.minority_profit

    @staticmethod
    def get_comprehensive_total_income():
        """
        :术语名称:
            综合收益总额
        :术语解释:
            综合收益总额
        :功能:
            获取综合收益总额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.comprehensive_total_income

    @staticmethod
    def get_parent_owners_income():
        """
        :术语名称:
            归属于母公司所有者的其他综合收益总额
        :术语解释:
            归属于母公司所有者的其他综合收益总额
        :功能:
            获取归属于母公司所有者的其他综合收益总额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.parent_owners_income

    @staticmethod
    def get_minority_shareholders_income():
        """
        :术语名称:
            归属于少数股东的综合收益总额
        :术语解释:
            归属于少数股东的综合收益总额
        :功能:
            获取归属于少数股东的综合收益总额字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.minority_shareholders_income

    @staticmethod
    def get_diluted_eps():
        """
        :术语名称:
            稀释每股收益
        :术语解释:
            稀释每股收益
        :功能:
            获取稀释每股收益字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return AStockProfitView.diluted_eps

    @staticmethod
    def get_lanchou():
        """
        :术语名称:
            蓝筹股
        :术语解释:
            蓝筹股
        :功能:
            获取是否蓝筹股字段
        """
        return DailyQlstockBasicLevel5.lanchou

    @staticmethod
    def query_lanchou(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            蓝筹股
        :术语解释:
            蓝筹股
        :功能:
            筛选出或剔除指定日期范围内标记为蓝筹股的股票。与get_lanchou()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选蓝筹股，0=剔除蓝筹股
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (DailyQlstockBasicLevel5.lanchou == flag) & DailyQlstockBasicLevel5.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(DailyQlstockBasicLevel5.secucode).where(condition)

    @staticmethod
    def get_ismultild():
        """
        :术语名称:
            为多轮共振
        :术语解释:
            为多轮共振
        :功能:
            获取是否为多轮共振字段
        """
        return DailyQlstockBasicLevel5.ismultild

    @staticmethod
    def query_ismultild(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            为多轮共振
        :术语解释:
            为多轮共振
        :功能:
            筛选出或剔除指定日期范围内标记为为多轮共振的股票。与get_ismultild()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选为多轮共振，0=剔除为多轮共振
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (DailyQlstockBasicLevel5.ismultild == flag) & DailyQlstockBasicLevel5.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(DailyQlstockBasicLevel5.secucode).where(condition)

    @staticmethod
    def get_zldd():
        """
        :术语名称:
            主力大单
        :术语解释:
            主力大单
        :功能:
            获取主力大单字段
        """
        return DailyQlstockBasicLevel5.zldd

    @staticmethod
    def get_warzone():
        """
        :术语名称:
            所属战区
        :术语解释:
            所属战区
        :功能:
            获取所属战区字段
        """
        return DailyQlstockBasicLevel5.warzone

    @staticmethod
    def get_tradecore():
        """
        :术语名称:
            为行业核心
        :术语解释:
            为行业核心
        :功能:
            获取是否为行业核心字段
        """
        return DailyQlstockBasicLevel5.tradecore

    @staticmethod
    def query_tradecore(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            为行业核心
        :术语解释:
            为行业核心
        :功能:
            筛选出或剔除指定日期范围内标记为为行业核心的股票。与get_tradecore()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选为行业核心，0=剔除为行业核心
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (DailyQlstockBasicLevel5.tradecore == flag) & DailyQlstockBasicLevel5.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(DailyQlstockBasicLevel5.secucode).where(condition)

    @staticmethod
    def get_prepare():
        """
        :术语名称:
            为准备股票
        :术语解释:
            为准备股票
        :功能:
            获取是否为准备股票字段
        """
        return DailyQlstockBasicLevel5.prepare

    @staticmethod
    def query_prepare(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            为准备股票
        :术语解释:
            为准备股票
        :功能:
            筛选出或剔除指定日期范围内标记为为准备股票的股票。与get_prepare()搭配使用展示数据。
        :参数:
            flag(int): 1=筛选为准备股票，0=剔除为准备股票
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        condition = (DailyQlstockBasicLevel5.prepare == flag) & DailyQlstockBasicLevel5.date.between(
            get_date(start_date, is_start=True), get_date(end_date)
        )
        return select(DailyQlstockBasicLevel5.secucode).where(condition)

    @staticmethod
    def get_multild():
        """
        :术语名称:
            多轮共振（轮动共振）
        :术语解释:
            多轮共振（轮动共振）
        :功能:
            获取多轮共振（轮动共振）字段
        """
        return DailyQlstockBasicLevel5.multild

    @staticmethod
    def get_bd_ykcsl():
        """
        :术语名称:
            波段策略已开仓数量
        :术语解释:
            波段策略已开仓数量
        :功能:
            获取波段策略已开仓数量字段
        """
        return DailyQlstockBasicLevel5.bd_ykcsl

    @staticmethod
    def get_stock_attribute():
        """
        :术语名称:
            属性掩码
        :术语解释:
            属性掩码
        :功能:
            获取属性掩码字段
        """
        return DailyQlstockBasicLevel5.stock_attribute

    @staticmethod
    def get_stock_attribute_256():
        """
        :术语名称:
            成长白马股
        :术语解释:
            成长白马股
        :功能:
            获取属性掩码是否为成长白马股字段
        """
        return (
            "_calculate_bitmask",
            "stock_attribute_256",
            {"field": DailyQlstockBasicLevel5.stock_attribute, "bit_value": 256, "alias_name": "stock_attribute_256"},
        )

    @staticmethod
    def get_stock_attribute_512():
        """
        :术语名称:
            红利优选股
        :术语解释:
            红利优选股
        :功能:
            获取属性掩码是否为红利优选股字段
        """
        return (
            "_calculate_bitmask",
            "stock_attribute_512",
            {"field": DailyQlstockBasicLevel5.stock_attribute, "bit_value": 512, "alias_name": "stock_attribute_512"},
        )

    @staticmethod
    def query_stock_attribute_256(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            成长白马股
        :术语解释:
            成长白马股
        :功能:
            根据指定日期范围，筛选或剔除属性掩码为成长白马股的股票。
        :参数:
            action(int): 1=筛选成长白马股，0=剔除成长白马股
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选成长白马股
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyQlstockBasicLevel5.stock_attribute,
                start_date=start_date,
                end_date=end_date,
                include_bits=[256],
                exclude_bits=None,
            )
        else:
            # 剔除成长白马股
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyQlstockBasicLevel5.stock_attribute,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[256],
            )

    @staticmethod
    def query_stock_attribute_512(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            红利优选股
        :术语解释:
            红利优选股
        :功能:
            根据指定日期范围，筛选或剔除属性掩码为红利优选股的股票。
        :参数:
            action(int): 1=筛选红利优选股，0=剔除红利优选股
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选红利优选股
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyQlstockBasicLevel5.stock_attribute,
                start_date=start_date,
                end_date=end_date,
                include_bits=[512],
                exclude_bits=None,
            )
        else:
            # 剔除红利优选股
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyQlstockBasicLevel5.stock_attribute,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[512],
            )

    @staticmethod
    def get_ldmarket():
        """
        :术语名称:
            战区轮动
        :术语解释:
            战区轮动
        :功能:
            获取战区轮动字段，仅用于股票指标使用
        """
        return DailyQlldLevel5.ldmarket

    @staticmethod
    def get_ldtrade():
        """
        :术语名称:
            行业轮动
        :术语解释:
            行业轮动
        :功能:
            获取行业轮动字段，仅用于股票指标使用
        """
        return DailyQlldLevel5.ldtrade

    @staticmethod
    def get_ld0z():
        """
        :术语名称:
            0Z轮动
        :术语解释:
            0Z轮动
        :功能:
            获取0Z轮动字段
        """
        return DailyQlldLevel5.ld0z

    @staticmethod
    def get_ldindex():
        """
        :术语名称:
            轮动指数
        :术语解释:
            轮动指数
        :功能:
            获取轮动指数字段
        """
        return DailyQlldLevel5.ldindex

    @staticmethod
    def get_ld_20():
        """
        :术语名称:
            强势轮动_20
        :术语解释:
            强势轮动_20
        :功能:
            获取强势轮动_20字段
        """
        return DailyQlldLevel5.ld_20

    @staticmethod
    def get_ld_60():
        """
        :术语名称:
            强势轮动_60
        :术语解释:
            强势轮动_60
        :功能:
            获取强势轮动_60字段
        """
        return DailyQlldLevel5.ld_60

    @staticmethod
    def get_ld_120():
        """
        :术语名称:
            强势轮动_120
        :术语解释:
            强势轮动_120
        :功能:
            获取强势轮动_120字段
        """
        return DailyQlldLevel5.ld_120

    @staticmethod
    def get_ld_240():
        """
        :术语名称:
            强势轮动_240
        :术语解释:
            强势轮动_240
        :功能:
            获取强势轮动_240字段
        """
        return DailyQlldLevel5.ld_240

    @staticmethod
    def get_hjk_13():
        """
        :术语名称:
            13日黄金坑
        :术语解释:
            13日黄金坑
        :功能:
            获取13日黄金坑字段
        """
        return DailyHjkLevel5.hjk_13

    @staticmethod
    def get_hjk_34():
        """
        :术语名称:
            34日黄金坑
        :术语解释:
            34日黄金坑
        :功能:
            获取34日黄金坑字段
        """
        return DailyHjkLevel5.hjk_34

    @staticmethod
    def get_hjk_60():
        """
        :术语名称:
            60日黄金坑
        :术语解释:
            60日黄金坑
        :功能:
            获取60日黄金坑字段
        """
        return DailyHjkLevel5.hjk_60

    @staticmethod
    def get_pool_type():
        """
        :术语名称:
            入选外资池类型
        :术语解释:
            入选外资池类型
        :功能:
            获取入选外资池类型字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return WzHoldingPoolsLevel15.pool_type

    @staticmethod
    def get_pp_count():
        """
        :术语名称:
            外资持股个数
        :术语解释:
            外资持股个数
        :功能:
            获取外资持股个数字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return WzHoldingPoolsLevel15.pp_count

    @staticmethod
    def get_pp_ratio():
        """
        :术语名称:
            外资持股占比
        :术语解释:
            外资持股占比
        :功能:
            获取外资持股占比字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return WzHoldingPoolsLevel15.pp_ratio

    @staticmethod
    def get_pp_funds():
        """
        :术语名称:
            外资投入总资金
        :术语解释:
            外资投入总资金
        :功能:
            获取外资投入总资金字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return WzHoldingPoolsLevel15.pp_funds

    @staticmethod
    def get_last_quarter_price():
        """
        :术语名称:
            上季平均股价
        :术语解释:
            上季平均股价
        :功能:
            获取上季平均股价字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return WzHoldingPoolsLevel15.last_quarter_price

    @staticmethod
    def get_last_in_date():
        """
        :术语名称:
            最新进场时间
        :术语解释:
            最新进场时间
        :功能:
            获取最新进场时间字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return WzHoldingPoolsLevel15.last_in_date

    @staticmethod
    def get_hold_volume():
        """
        :术语名称:
            QFII持股总量
        :术语解释:
            QFII持股总量
        :功能:
            获取QFII持股总量字段，问QFII持股数量要调用此函数
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return WzHoldingPoolsLevel15.hold_volume

    @staticmethod
    def get_tq():
        """
        :术语名称:
            流通盘
        :术语解释:
            流通盘
        :功能:
            获取流通盘字段
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        """
        return WzHoldingPoolsLevel15.tq

    @staticmethod
    def get_trade_profit_rate():
        """
        :术语名称:
            近一季度大宗收益率
        :术语解释:
            近一季度大宗收益率
        :功能:
            获取近一季度大宗收益率字段
        """
        return DailyDzjmStockStatLevel15.trade_profit_rate

    @staticmethod
    def get_trade_count():
        """
        :术语名称:
            近一季度交易次数
        :术语解释:
            近一季度交易次数
        :功能:
            获取近一季度交易次数字段
        """
        return DailyDzjmStockStatLevel15.trade_count

    @staticmethod
    def get_trade_type():
        """
        :术语名称:
            大宗交易类型
        :术语解释:
            大宗交易类型
        :功能:
            获取大宗交易类型字段
        """
        return DailyDzjmStockDetailLevel15.trade_type

    @staticmethod
    def get_trade_type_0():
        """
        :术语名称:
            其他
        :术语解释:
            其他
        :功能:
            获取大宗交易类型是否为其他字段
        """
        return (
            "_calculate_bitmask",
            "trade_type_0",
            {"field": DailyDzjmStockDetailLevel15.trade_type, "bit_value": 0, "alias_name": "trade_type_0"},
        )

    @staticmethod
    def get_trade_type_1():
        """
        :术语名称:
            机构交易(机构)
        :术语解释:
            机构交易(机构)
        :功能:
            获取大宗交易类型是否为机构交易(机构)字段
        """
        return (
            "_calculate_bitmask",
            "trade_type_1",
            {"field": DailyDzjmStockDetailLevel15.trade_type, "bit_value": 1, "alias_name": "trade_type_1"},
        )

    @staticmethod
    def get_trade_type_2():
        """
        :术语名称:
            焦点交易(焦点)
        :术语解释:
            焦点交易(焦点)
        :功能:
            获取大宗交易类型是否为焦点交易(焦点)字段
        """
        return (
            "_calculate_bitmask",
            "trade_type_2",
            {"field": DailyDzjmStockDetailLevel15.trade_type, "bit_value": 2, "alias_name": "trade_type_2"},
        )

    @staticmethod
    def get_trade_type_4():
        """
        :术语名称:
            对倒交易(对倒)
        :术语解释:
            对倒交易(对倒)
        :功能:
            获取大宗交易类型是否为对倒交易(对倒)字段
        """
        return (
            "_calculate_bitmask",
            "trade_type_4",
            {"field": DailyDzjmStockDetailLevel15.trade_type, "bit_value": 4, "alias_name": "trade_type_4"},
        )

    @staticmethod
    def get_trade_type_8():
        """
        :术语名称:
            股东席位(股东)
        :术语解释:
            股东席位(股东)
        :功能:
            获取大宗交易类型是否为股东席位(股东)字段
        """
        return (
            "_calculate_bitmask",
            "trade_type_8",
            {"field": DailyDzjmStockDetailLevel15.trade_type, "bit_value": 8, "alias_name": "trade_type_8"},
        )

    @staticmethod
    def get_trade_type_16():
        """
        :术语名称:
            溢价涨停(溢停)
        :术语解释:
            溢价涨停(溢停)
        :功能:
            获取大宗交易类型是否为溢价涨停(溢停)字段
        """
        return (
            "_calculate_bitmask",
            "trade_type_16",
            {"field": DailyDzjmStockDetailLevel15.trade_type, "bit_value": 16, "alias_name": "trade_type_16"},
        )

    @staticmethod
    def get_trade_type_32():
        """
        :术语名称:
            折价跌停(折停)
        :术语解释:
            折价跌停(折停)
        :功能:
            获取大宗交易类型是否为折价跌停(折停)字段
        """
        return (
            "_calculate_bitmask",
            "trade_type_32",
            {"field": DailyDzjmStockDetailLevel15.trade_type, "bit_value": 32, "alias_name": "trade_type_32"},
        )

    @staticmethod
    def query_trade_type_0(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            其他
        :术语解释:
            其他
        :功能:
            根据指定日期范围，筛选或剔除大宗交易类型为其他的股票。
        :参数:
            action(int): 1=筛选其他，0=剔除其他
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选其他
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=[0],
                exclude_bits=None,
            )
        else:
            # 剔除其他
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[0],
            )

    @staticmethod
    def query_trade_type_1(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            机构交易(机构)
        :术语解释:
            机构交易(机构)
        :功能:
            根据指定日期范围，筛选或剔除大宗交易类型为机构交易(机构)的股票。
        :参数:
            action(int): 1=筛选机构交易(机构)，0=剔除机构交易(机构)
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选机构交易(机构)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=[1],
                exclude_bits=None,
            )
        else:
            # 剔除机构交易(机构)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[1],
            )

    @staticmethod
    def query_trade_type_2(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            焦点交易(焦点)
        :术语解释:
            焦点交易(焦点)
        :功能:
            根据指定日期范围，筛选或剔除大宗交易类型为焦点交易(焦点)的股票。
        :参数:
            action(int): 1=筛选焦点交易(焦点)，0=剔除焦点交易(焦点)
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选焦点交易(焦点)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=[2],
                exclude_bits=None,
            )
        else:
            # 剔除焦点交易(焦点)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[2],
            )

    @staticmethod
    def query_trade_type_4(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            对倒交易(对倒)
        :术语解释:
            对倒交易(对倒)
        :功能:
            根据指定日期范围，筛选或剔除大宗交易类型为对倒交易(对倒)的股票。
        :参数:
            action(int): 1=筛选对倒交易(对倒)，0=剔除对倒交易(对倒)
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选对倒交易(对倒)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=[4],
                exclude_bits=None,
            )
        else:
            # 剔除对倒交易(对倒)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[4],
            )

    @staticmethod
    def query_trade_type_8(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            股东席位(股东)
        :术语解释:
            股东席位(股东)
        :功能:
            根据指定日期范围，筛选或剔除大宗交易类型为股东席位(股东)的股票。
        :参数:
            action(int): 1=筛选股东席位(股东)，0=剔除股东席位(股东)
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选股东席位(股东)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=[8],
                exclude_bits=None,
            )
        else:
            # 剔除股东席位(股东)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[8],
            )

    @staticmethod
    def query_trade_type_16(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            溢价涨停(溢停)
        :术语解释:
            溢价涨停(溢停)
        :功能:
            根据指定日期范围，筛选或剔除大宗交易类型为溢价涨停(溢停)的股票。
        :参数:
            action(int): 1=筛选溢价涨停(溢停)，0=剔除溢价涨停(溢停)
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选溢价涨停(溢停)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=[16],
                exclude_bits=None,
            )
        else:
            # 剔除溢价涨停(溢停)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[16],
            )

    @staticmethod
    def query_trade_type_32(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            折价跌停(折停)
        :术语解释:
            折价跌停(折停)
        :功能:
            根据指定日期范围，筛选或剔除大宗交易类型为折价跌停(折停)的股票。
        :参数:
            action(int): 1=筛选折价跌停(折停)，0=剔除折价跌停(折停)
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选折价跌停(折停)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=[32],
                exclude_bits=None,
            )
        else:
            # 剔除折价跌停(折停)
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=DailyDzjmStockDetailLevel15.trade_type,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[32],
            )

    @staticmethod
    def get_trade_act():
        """
        :术语名称:
            大宗交易行为
        :术语解释:
            大宗交易行为
        :功能:
            获取大宗交易行为字段
        """
        return DailyDzjmStockDetailLevel15.trade_act

    @staticmethod
    def get_trade_price():
        """
        :术语名称:
            大宗交易成交价
        :术语解释:
            大宗交易成交价
        :功能:
            获取大宗交易成交价字段
        """
        return DailyDzjmStockDetailLevel15.trade_price

    @staticmethod
    def get_trade_vol():
        """
        :术语名称:
            大宗交易成交量
        :术语解释:
            大宗交易成交量
        :功能:
            获取大宗交易成交量字段
        """
        return DailyDzjmStockDetailLevel15.trade_vol

    @staticmethod
    def get_trade_amt():
        """
        :术语名称:
            大宗交易成交额
        :术语解释:
            大宗交易成交额
        :功能:
            获取大宗交易成交额字段
        """
        return DailyDzjmStockDetailLevel15.trade_amt

    @staticmethod
    def get_zhanpan_rate():
        """
        :术语名称:
            大宗交易占盘比
        :术语解释:
            大宗交易占盘比
        :功能:
            获取大宗交易占盘比字段
        """
        return DailyDzjmStockDetailLevel15.zhanpan_rate

    @staticmethod
    def get_trade_rate():
        """
        :术语名称:
            大宗交易成交倍率
        :术语解释:
            大宗交易成交倍率
        :功能:
            获取大宗交易成交倍率字段
        """
        return DailyDzjmStockDetailLevel15.trade_rate

    @staticmethod
    def get_hot_money_name():
        """
        :术语名称:
            席位龙虎榜上榜游资
        :术语解释:
            席位龙虎榜上榜游资
        :功能:
            获取席位龙虎榜上榜游资字段
        """
        return XwlhSeatStatLevel15.hot_money_name

    @staticmethod
    def get_detail_hot_money_name():
        """
        :术语名称:
            游资名称
        :术语解释:
            游资名称
        :功能:
            获取游资名称字段
        """
        return XwlhSeatdetailLevel15.hot_money_name

    @staticmethod
    def get_all_buying():
        """
        :术语名称:
            席位总买
        :术语解释:
            席位总买
        :功能:
            获取席位总买字段
        """
        return XwlhSeatStatLevel15.all_buying

    @staticmethod
    def get_all_selling():
        """
        :术语名称:
            席位总卖
        :术语解释:
            席位总卖
        :功能:
            获取席位总卖字段
        """
        return XwlhSeatStatLevel15.all_selling

    @staticmethod
    def get_continous_days():
        """
        :术语名称:
            席位龙虎榜连续上榜天数
        :术语解释:
            席位龙虎榜连续上榜天数
        :功能:
            获取席位龙虎榜连续上榜天数字段
        """
        return XwlhSeatStatLevel15.continous_days

    @staticmethod
    def get_reason():
        """
        :术语名称:
            异动原因
        :术语解释:
            异动原因
        :功能:
            获取异动原因字段
        """
        return XwlhSeatStatLevel15.reason

    @staticmethod
    def get_seat_id():
        """
        :术语名称:
            席位ID
        :术语解释:
            席位ID
        :功能:
            获取席位ID字段
        """
        return XwlhSeatdetailLevel15.seat_id

    @staticmethod
    def get_seat_name():
        """
        :术语名称:
            席位名称
        :术语解释:
            席位名称
        :功能:
            获取席位名称字段
        """
        return XwlhSeatdetailLevel15.seat_name

    @staticmethod
    def get_buying_amt():
        """
        :术语名称:
            总买
        :术语解释:
            总买
        :功能:
            获取总买字段
        """
        return XwlhSeatdetailLevel15.buying_amt

    @staticmethod
    def get_selling_amt():
        """
        :术语名称:
            总卖
        :术语解释:
            总卖
        :功能:
            获取总卖字段
        """
        return XwlhSeatdetailLevel15.selling_amt

    @staticmethod
    def get_net_amt():
        """
        :术语名称:
            净买额
        :术语解释:
            净买额
        :功能:
            获取净买额字段
        """
        return XwlhSeatdetailLevel15.net_amt

    @staticmethod
    def get_seattype():
        """
        :术语名称:
            席位类型
        :术语解释:
            席位类型
        :功能:
            获取席位类型字段
        """
        return XwlhSeatdetailLevel15.seattype

    @staticmethod
    def get_starseattype():
        """
        :术语名称:
            明星席位类型
        :术语解释:
            明星席位类型
        :功能:
            获取明星席位类型字段
        """
        return XwlhSeatdetailLevel15.starseattype

    @staticmethod
    def get_starseattype_1():
        """
        :术语名称:
            上涨明星席位
        :术语解释:
            上涨明星席位
        :功能:
            获取明星席位类型是否为上涨明星席位字段
        """
        return (
            "_calculate_bitmask",
            "starseattype_1",
            {"field": XwlhSeatdetailLevel15.starseattype, "bit_value": 1, "alias_name": "starseattype_1"},
        )

    @staticmethod
    def get_starseattype_2():
        """
        :术语名称:
            下跌明星席位
        :术语解释:
            下跌明星席位
        :功能:
            获取明星席位类型是否为下跌明星席位字段
        """
        return (
            "_calculate_bitmask",
            "starseattype_2",
            {"field": XwlhSeatdetailLevel15.starseattype, "bit_value": 2, "alias_name": "starseattype_2"},
        )

    @staticmethod
    def get_starseattype_4():
        """
        :术语名称:
            活跃明星席位
        :术语解释:
            活跃明星席位
        :功能:
            获取明星席位类型是否为活跃明星席位字段
        """
        return (
            "_calculate_bitmask",
            "starseattype_4",
            {"field": XwlhSeatdetailLevel15.starseattype, "bit_value": 4, "alias_name": "starseattype_4"},
        )

    @staticmethod
    def query_starseattype_1(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            上涨明星席位
        :术语解释:
            上涨明星席位
        :功能:
            根据指定日期范围，筛选或剔除明星席位类型为上涨明星席位的股票。
        :参数:
            action(int): 1=筛选上涨明星席位，0=剔除上涨明星席位
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选上涨明星席位
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=XwlhSeatdetailLevel15.starseattype,
                start_date=start_date,
                end_date=end_date,
                include_bits=[1],
                exclude_bits=None,
            )
        else:
            # 剔除上涨明星席位
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=XwlhSeatdetailLevel15.starseattype,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[1],
            )

    @staticmethod
    def query_starseattype_2(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            下跌明星席位
        :术语解释:
            下跌明星席位
        :功能:
            根据指定日期范围，筛选或剔除明星席位类型为下跌明星席位的股票。
        :参数:
            action(int): 1=筛选下跌明星席位，0=剔除下跌明星席位
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选下跌明星席位
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=XwlhSeatdetailLevel15.starseattype,
                start_date=start_date,
                end_date=end_date,
                include_bits=[2],
                exclude_bits=None,
            )
        else:
            # 剔除下跌明星席位
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=XwlhSeatdetailLevel15.starseattype,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[2],
            )

    @staticmethod
    def query_starseattype_4(action: int, start_date: str, end_date: str):
        """
        :术语名称:
            活跃明星席位
        :术语解释:
            活跃明星席位
        :功能:
            根据指定日期范围，筛选或剔除明星席位类型为活跃明星席位的股票。
        :参数:
            action(int): 1=筛选活跃明星席位，0=剔除活跃明星席位
            start_date(str): 起始日期（"0d"表示当天）。
            end_date(str): 截止日期（"0d"表示当天）。
        """
        if action == 1:
            # 筛选活跃明星席位
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=XwlhSeatdetailLevel15.starseattype,
                start_date=start_date,
                end_date=end_date,
                include_bits=[4],
                exclude_bits=None,
            )
        else:
            # 剔除活跃明星席位
            return AStockGlobalQuery._query_bitmask_conditions(
                bitmask_field=XwlhSeatdetailLevel15.starseattype,
                start_date=start_date,
                end_date=end_date,
                include_bits=None,
                exclude_bits=[4],
            )

    @staticmethod
    def get_hot_money_id():
        """
        :术语名称:
            游资ID
        :术语解释:
            游资ID
        :功能:
            获取游资ID字段
        """
        return XwlhSeatdetailLevel15.hot_money_id

    @staticmethod
    def get_smyx_num():
        """
        :术语名称:
            私募英雄榜持有本股私募数
        :术语解释:
            私募英雄榜持有本股私募数
        :功能:
            获取私募英雄榜持有本股私募数字段
        """
        return SmxyHoldingStatLevel15.smyx_num

    @staticmethod
    def get_smyx_zc_num():
        """
        :术语名称:
            私募英雄榜增仓私募数
        :术语解释:
            私募英雄榜增仓私募数
        :功能:
            获取私募英雄榜增仓私募数字段
        """
        return SmxyHoldingStatLevel15.smyx_zc_num

    @staticmethod
    def get_buy_amount():
        """
        :术语名称:
            私募英雄榜私募增仓总额
        :术语解释:
            私募英雄榜私募增仓总额
        :功能:
            获取私募英雄榜私募增仓总额字段。当用户明确询问“增仓总额”、“增仓金额”或“增仓多少钱”时，应单独调用此函数。当用户使用模糊词汇如“增仓多少”（未指明股数或金额）时，应同时调用此函数和 get_buy_volume 函数。
        """
        return SmxyHoldingStatLevel15.buy_amount

    @staticmethod
    def get_buy_volume():
        """
        :术语名称:
            私募英雄榜增仓股数
        :术语解释:
            私募英雄榜增仓股数
        :功能:
            获取私募英雄榜增仓股数字段。当用户明确询问“增仓股数”或“增仓多少股”时，应单独调用此函数。当用户使用模糊词汇如“增仓多少”（未指明股数或金额）时，应同时调用此函数和 get_buy_amount 函数。
        """
        return SmxyHoldingStatLevel15.buy_volume

    @staticmethod
    def get_zc_amt():
        """
        :术语名称:
            二级市场180天增仓额
        :术语解释:
            二级市场180天增仓额
        :功能:
            获取二级市场180天高管增仓额字段
        """
        return GgjyHoldStatLevel15.zc_amt

    @staticmethod
    def get_zc_vol():
        """
        :术语名称:
            二级市场180天增仓数量
        :术语解释:
            二级市场180天增仓数量
        :功能:
            获取二级市场180天高管增仓数量字段
        """
        return GgjyHoldStatLevel15.zc_vol

    @staticmethod
    def get_analystcode():
        """
        :术语名称:
            分析师ID
        :术语解释:
            分析师ID
        :功能:
            获取分析师ID字段
        """
        return ReportHotstocksLevel15.analystcode

    @staticmethod
    def get_analystname():
        """
        :术语名称:
            分析师名称
        :术语解释:
            分析师名称
        :功能:
            获取分析师名称字段
        """
        return ReportHotstocksLevel15.analystname

    @staticmethod
    def get_analystnum():
        """
        :术语名称:
            关注分析师数量
        :术语解释:
            关注分析师数量
        :功能:
            获取关注分析师数量字段
        """
        return ReportHotstocksLevel15.analystnum

    @staticmethod
    def get_organs():
        """
        :术语名称:
            接待机构数
        :术语解释:
            接待机构数
        :功能:
            获取接待机构数字段
        """
        return ReportHotstocksLevel15.organs

    @staticmethod
    def get_researchfreq():
        """
        :术语名称:
            调研频率
        :术语解释:
            调研频率
        :功能:
            获取调研频率字段
        """
        return ReportHotstocksLevel15.researchfreq

    @staticmethod
    def get_title():
        """
        :术语名称:
            研报标题
        :术语解释:
            研报标题
        :功能:
            获取研报标题字段
        """
        return ReportHotstocksLevel15.title

    @staticmethod
    def get_synthetic():
        """
        :术语名称:
            综合值
        :术语解释:
            综合值
        :功能:
            获取综合值字段
        """
        return AiStockStrategyLevel20.synthetic

    @staticmethod
    def get_succ_rate():
        """
        :术语名称:
            成功率
        :术语解释:
            成功率
        :功能:
            获取成功率字段
        """
        return AiStockStrategyLevel20.succ_rate

    @staticmethod
    def get_profit_rate():
        """
        :术语名称:
            次均收益
        :术语解释:
            次均收益
        :功能:
            获取次均收益字段
        """
        return AiStockStrategyLevel20.profit_rate

    @staticmethod
    def get_gqzj():
        """
        :术语名称:
            供求资金
        :术语解释:
            供求资金
        :功能:
            获取供求资金字段
        """
        return DailyGqzjLevel20.gqzj

    @staticmethod
    def get_flzj_lsdy():
        """
        :术语名称:
            拉升/打压
        :术语解释:
            拉升/打压
        :功能:
            获取拉升/打压字段
        """
        return DailyGqzjLevel20.flzj_lsdy

    @staticmethod
    def get_flzj_gfsd():
        """
        :术语名称:
            跟风/杀跌
        :术语解释:
            跟风/杀跌
        :功能:
            获取跟风/杀跌字段
        """
        return DailyGqzjLevel20.flzj_gfsd

    @staticmethod
    def get_flzj_cdpy():
        """
        :术语名称:
            抄底/抛压
        :术语解释:
            抄底/抛压
        :功能:
            获取抄底/抛压字段
        """
        return DailyGqzjLevel20.flzj_cdpy

    @staticmethod
    def get_zdt_type():
        """
        :术语名称:
            停板风向标涨跌停类型
        :术语解释:
            停板风向标涨跌停类型
        :功能:
            获取停板风向标涨跌停类型字段
        """
        return AiForecastZdLevel20.zdt_type

    @staticmethod
    def get_zhu_flag():
        """
        :术语名称:
            主力资金异动流入流出状态
        :术语解释:
            主力资金异动流入流出状态
        :功能:
            获取主力资金流入流出状态字段仅当用户问题明确涉及“主力资金流入状态”、“主力资金流出状态”或“主力资金异动状态”时方可调用。若用户仅询问某一具体数据项（如单项资金流入、单项资金流出、增减仓等），禁止调用本函数，应改为使用对应的具体指标函数。
        """
        return FundMoveFlagLevel0.zhu_flag

    @staticmethod
    def get_duo_flag():
        """
        :术语名称:
            多空资金异动流入流出状态
        :术语解释:
            多空资金异动流入流出状态
        :功能:
            获取多空资金流入流出状态字段。仅当用户问题明确涉及“多空资金流入状态”、“多空资金流出状态”或“多空资金异动状态”时方可调用。若用户仅询问某一具体数据项（如单项资金流入、单项资金流出、增减仓等），禁止调用本函数，应改为使用对应的具体指标函数。
        """
        return FundMoveFlagLevel0.duo_flag

    @staticmethod
    def get_gan_flag():
        """
        :术语名称:
            敢死队资金异动流入流出状态
        :术语解释:
            敢死队资金异动流入流出状态
        :功能:
            获取敢死队资金流入流出状态字段。仅当用户问题明确涉及“敢死队资金流入状态”、“敢死队资金流出状态”或“敢死队资金异动状态”时方可调用。若用户仅询问某一具体数据项（如单项资金流入、单项资金流出、增减仓等），禁止调用本函数，应改为使用对应的具体指标函数。
        """
        return FundMoveFlagLevel0.gan_flag

    @staticmethod
    def get_hot_value():
        """
        :术语名称:
            热度值
        :术语解释:
            热度值
        :功能:
            获取热度值字段
        """
        return HwStocksLevel0.hot_value

    @staticmethod
    def get_state():
        """
        :术语名称:
            股票状态
        :术语解释:
            股票状态
        :功能:
            获取股票状态字段
        """
        return HwStocksLevel0.state

    @staticmethod
    def get_category():
        """
        :术语名称:
            类别
        :术语解释:
            类别
        :功能:
            获取类别字段
        """
        return HwStocksLevel0.category

    @staticmethod
    def get_hw_name_to_stock():
        """
        :术语名称:
            股票所属消息榜热词
        :术语解释:
            股票所属消息榜热词
        :功能:
            获取股票所属消息榜热词字段
        """
        return HwStocksLevel0.hw_name_to_stock

    @staticmethod
    def get_dcyf():
        """
        :术语名称:
            量能活跃度
        :术语解释:
            量能活跃度
        :功能:
            获取量能活跃度字段
        """
        return DailyDcyfLevel5.dcyf

    @staticmethod
    def get_forecast_income():
        """
        :术语名称:
            预测营业收入
        :术语解释:
            预测营业收入
        :功能:
            获取预测营业收入字段
        """
        return F10ForecastIndexView.forecast_income

    @staticmethod
    def get_forecast_income_rate():
        return F10ForecastIndexView.forecast_income_rate

    @staticmethod
    def get_forecast_profit():
        """
        :术语名称:
            预测利润总额
        :术语解释:
            预测利润总额
        :功能:
            获取预测利润总额字段
        """
        return F10ForecastIndexView.forecast_profit

    @staticmethod
    def get_forecast_net_profit():
        """
        :术语名称:
            预测净利润
        :术语解释:
            预测净利润
        :功能:
            获取预测净利润字段
        """
        return F10ForecastIndexView.forecast_net_profit

    @staticmethod
    def get_forecast_net_profit_rate():
        return F10ForecastIndexView.forecast_net_profit_rate

    @staticmethod
    def get_forecast_per_cashflow():
        """
        :术语名称:
            预测每股现金流
        :术语解释:
            预测每股现金流
        :功能:
            获取预测每股现金流字段
        """
        return F10ForecastIndexView.forecast_per_cashflow

    @staticmethod
    def get_forecast_per_net_assets():
        """
        :术语名称:
            预测每股净资产
        :术语解释:
            预测每股净资产
        :功能:
            获取预测每股净资产字段
        """
        return F10ForecastIndexView.forecast_per_net_assets

    @staticmethod
    def get_forecast_net_assets_rate():
        """
        :术语名称:
            预测净资产收益率
        :术语解释:
            预测净资产收益率
        :功能:
            获取预测净资产收益率字段
        """
        return F10ForecastIndexView.forecast_net_assets_rate

    @staticmethod
    def get_forecast_dynamic_pe():
        """
        :术语名称:
            预测市盈率（动态）
        :术语解释:
            预测市盈率（动态）
        :功能:
            获取预测市盈率（动态）字段
        """
        return F10ForecastIndexView.forecast_dynamic_pe

    @staticmethod
    def get_shareholder():
        """
        :术语名称:
            高管名字
        :术语解释:
            高管名字
        :功能:
            获取高管名字字段
        """
        return XxbShareholdersLevel0View.shareholder

    @staticmethod
    def get_share_holders_job():
        """
        :术语名称:
            高管职位
        :术语解释:
            高管职位
        :功能:
            获取高管职位字段
        """
        return XxbShareholdersLevel0View.share_holders_job

    @staticmethod
    def get_share_holders_change_volume():
        """
        :术语名称:
            高管增减持数量
        :术语解释:
            高管增减持数量
        :功能:
            获取高管增减持数量字段，用来查询当前高管增减持股票的具体数量，和数量有关时运用，需要搭配范围查询函数查询具体股票数值。
        """
        return XxbShareholdersLevel0View.share_holders_change_volume

    @staticmethod
    def get_share_holders_change_detail():
        """
        :术语名称:
            高管增减持原因
        :术语解释:
            高管增减持原因
        :功能:
            获取高管增减持原因字段，集团增减持原因
        """
        return XxbShareholdersLevel0View.share_holders_change_detail

    @staticmethod
    def get_dhyd13():
        """
        :术语名称:
            股价活跃度13
        :术语解释:
            股价活跃度13
        :功能:
            获取股价活跃度13字段
        """
        return DailyDhydLevel5.dhyd13

    @staticmethod
    def get_dhyd34():
        """
        :术语名称:
            股价活跃度34
        :术语解释:
            股价活跃度34
        :功能:
            获取股价活跃度34字段
        """
        return DailyDhydLevel5.dhyd34

    @staticmethod  # 手写get函数开始
    def get_foreign_institutions_ranking():
        return [
            WzHoldingPoolsLevel15.pool_type,
            WzHoldingPoolsLevel15.pp_count,
            WzHoldingPoolsLevel15.pp_ratio,
            WzHoldingPoolsLevel15.pp_funds,
            WzHoldingPoolsLevel15.last_quarter_price,
            WzHoldingPoolsLevel15.last_in_date,
        ]

    # 高管关注信息
    @staticmethod
    def get_shareholder_follow_info():
        return [
            AStockGlobalQuery.get_shareholder(),
            AStockGlobalQuery.get_share_holders_job(),
            AStockGlobalQuery.get_share_holders_change_volume(),
            AStockGlobalQuery.get_share_holders_change_detail(),
        ]

    @staticmethod
    def get_holding_pool_pp_count():
        """
        :术语名称:
            总外资个数
        :术语解释:
            总外资个数
        :功能:
            获取总外资个数字段，当问题涉及外资持股数量或总外资个数时调用
        """
        return ("_get_wz_pool_data", "pp_count", {"pool_type": 6})

    @staticmethod
    def get_increasing_pool_pp_count():
        """
        :术语名称:
            加仓外资个数
        :术语解释:
            加仓外资个数
        :功能:
            获取加仓外资个数字段，当问题涉及外资加仓数量或加仓外资个数时调用
        """
        return ("_get_wz_pool_data", "pp_count", {"pool_type": 5})

    @staticmethod
    def get_entry_pool_pp_count():
        """
        :术语名称:
            新进外资个数
        :术语解释:
            新进外资个数
        :功能:
            获取新进外资个数字段，当问题涉及外资新进数量或新进外资个数时调用
        """
        return ("_get_wz_pool_data", "pp_count", {"pool_type": 7})

    @staticmethod
    def get_wz_increasing_pool():
        return AStockGlobalQuery._get_wz_pool_columns(pool_type=5)

    @staticmethod
    def get_wz_holding_pool():
        return AStockGlobalQuery._get_wz_pool_columns(pool_type=6)

    @staticmethod
    def get_wz_entry_pool():
        return AStockGlobalQuery._get_wz_pool_columns(pool_type=7)

    @staticmethod
    def _get_wz_pool_columns(pool_type: int):
        """
        为指定的外资池生成一组标准的列请求元组。
        这是遵循 calculate_sum 模式的推荐方式，用于处理一组相关的字段。
        """
        if pool_type not in [5, 6, 7]:
            raise ValueError("无效的 pool_type，应为 5, 6, 或 7。")

        helper_func = "_get_wz_pool_data"
        params = {"pool_type": pool_type}

        # 定义这个池所包含的所有字段
        fields = ["pool_type", "pp_count", "pp_ratio", "pp_funds", "last_quarter_price", "last_in_date"]

        return [(helper_func, field, params) for field in fields]

    @staticmethod
    def _get_wz_pool_data(start_date: str = "0d", end_date: str = "0d", secucode_query=None, pool_type: int = 0):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        if pool_type not in [5, 6, 7]:
            raise ValueError("Invalid pool_type for foreign institutions ranking.")

        unique_id = uuid.uuid4().hex[:6]
        subquery = secucode_query.subquery()
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)

        # 首先根据池类型筛选基础稀疏表
        filtered_wz = (
            select(WzHoldingPoolsLevel15)
            .where(WzHoldingPoolsLevel15.pool_type == pool_type)
            .subquery(f"filtered_wz_{pool_type}_{unique_id}")
        )

        # 生成指定范围内的所有交易日网格
        dates_cte = (
            select(AStockMarket.date.label("date"))
            .where(AStockMarket.date.between(trade_start, trade_end))
            .distinct()
            .cte(f"dates_wz_{pool_type}_{unique_id}")
        )

        # 将交易日与指定的股票代码进行交叉连接，生成完整网格
        trade_secucode_cte = select(dates_cte.c.date, subquery.c.secucode).cte(f"grid_wz_{pool_type}_{unique_id}")

        # 左连接已筛选的稀疏数据，并找到每个网格点对应的最新记录
        joined = (
            select(
                *[
                    trade_secucode_cte.c.date.label("grid_date"),
                    trade_secucode_cte.c.secucode,
                    filtered_wz.c.date,
                    filtered_wz.c.pool_type,
                    filtered_wz.c.pp_count,
                    filtered_wz.c.pp_ratio,
                    filtered_wz.c.pp_funds,
                    filtered_wz.c.last_quarter_price,
                    filtered_wz.c.last_in_date,
                    filtered_wz.c.hold_volume,
                    filtered_wz.c.tq,
                    func.row_number()
                    .over(
                        partition_by=[trade_secucode_cte.c.date, trade_secucode_cte.c.secucode],
                        order_by=filtered_wz.c.date.desc(),
                    )
                    .label("rn"),
                ]
            )
            .select_from(
                trade_secucode_cte.outerjoin(
                    filtered_wz,
                    (filtered_wz.c.secucode == trade_secucode_cte.c.secucode)
                    & (filtered_wz.c.date <= trade_secucode_cte.c.date),
                )
            )
            .cte(f"ranked_wz_{pool_type}_{unique_id}")
        )

        # 最终CTE：只选择每个网格点的最新记录 (rn=1)
        final_cte = (
            select(
                *[
                    joined.c.date,
                    joined.c.secucode,
                    joined.c.pool_type,
                    joined.c.pp_count,
                    joined.c.pp_ratio,
                    joined.c.pp_funds,
                    joined.c.last_quarter_price,
                    joined.c.last_in_date,
                    joined.c.hold_volume,
                    joined.c.tq,
                ]
            )
            .where(joined.c.rn == 1)
            .cte(f"final_wz_pool_{pool_type}_{unique_id}")
        )
        final_cte.class_ = WzHoldingPoolsLevel15
        return final_cte

    @staticmethod
    def get_market_cap(mode: int = 0):
        """
        :术语名称:
            总市值
        :术语解释:
            总市值
        :功能:
            获取总市值字段
        """
        if mode == 1:
            return AStockMarketCur.market_cap
        else:
            return AStockMarket.marketvalue

    @staticmethod
    def get_sell_volume():
        """
        :术语名称:
            私募英雄榜减仓股数
        :术语解释:
            私募英雄榜减仓股数
        :功能:
            获取私募英雄榜减仓股数字段。当用户明确询问“减仓股数”或“减仓多少股”时，应单独调用此函数。当用户使用模糊词汇如“减仓多少”（未指明股数或金额）时，应同时调用此函数和 get_sell_amount 函数。
        """
        return ("_get_negative_field", "sell_volume", {"field": SmxyHoldingStatLevel15.sell_volume})

    @staticmethod
    def get_sell_amount():
        """
        :术语名称:
            私募英雄榜私募减仓总额
        :术语解释:
            私募英雄榜私募减仓总额
        :功能:
            获取私募英雄榜私募减仓总额字段。当用户明确询问“减仓总额”、“减仓金额”或“减仓多少钱”时，应单独调用此函数。当用户使用模糊词汇如“减仓多少”（未指明股数或金额）时，应同时调用此函数和 get_sell_volume 函数。
        """
        return ("_get_negative_field", "sell_amount", {"field": SmxyHoldingStatLevel15.sell_amount})

    @staticmethod
    def get_jc_amt():
        """
        :术语名称:
            二级市场180天减仓额
        :术语解释:
            二级市场180天减仓额
        :功能:
            获取二级市场180天减仓额字段
        """
        return ("_get_negative_field", "jc_amt", {"field": GgjyHoldStatLevel15.jc_amt})

    @staticmethod
    def get_jc_vol():
        """
        :术语名称:
            二级市场180天减仓数量
        :术语解释:
            二级市场180天减仓数量
        :功能:
            获取二级市场180天减仓数量字段
        """
        return ("_get_negative_field", "jc_vol", {"field": GgjyHoldStatLevel15.jc_vol})

    @staticmethod
    def _get_negative_field(field: Field, start_date: str, end_date: str, secucode_query):
        """
        通用取反CTE生成函数。
        对给定字段取反（即乘以 -1），用于反向计算。
        支持稀疏表的forward fill处理逻辑。
        """
        BaseTable = field.class_
        trade_end = get_date(end_date)
        trade_start = get_date(start_date, is_start=True)
        unique_id = uuid.uuid4().hex[:6]
        cte_name = f"cte_negative_{field.key}_{unique_id}"

        # 检查是否为稀疏表
        if BaseTable in sparsedate_views:
            # 稀疏表处理：使用forward fill逻辑
            # 首先构建正常的稀疏表CTE，然后对字段取反

            # 构建股票代码子查询
            stock_subquery = select(BaseTable.secucode).where(BaseTable.secucode.in_(secucode_query)).subquery()

            # 使用标准的稀疏表处理方法获取原始数据（包含原字段）
            original_cte = AStockGlobalQuery._build_sparse_table_cte(
                BaseTable,
                [BaseTable.secucode, BaseTable.date, field],  # 原始字段
                [BaseTable.secucode, BaseTable.date],  # 必需字段
                f"original_{cte_name}",
                unique_id,
                trade_start,
                trade_end,
                stock_subquery,
            )

            # 对原始CTE的结果进行字段取绝对值，并过滤掉空值和0值
            abs_field = func.abs(original_cte.c[field.key]).label(field.key)
            final_cte = (
                select(original_cte.c.secucode, original_cte.c.date, abs_field)
                .where(abs_field.is_not(None), abs_field != 0)
                .cte(cte_name)
            )

            return final_cte
        else:
            # 非稀疏表处理：区分单日期表和多日期表
            if BaseTable in single_date_views:
                # 单日期表处理：只查询最新记录
                if start_date == "0d" and end_date == "0d":
                    # 对于0d0d查询，获取每个股票的最新数据
                    latest_data_subquery = (
                        select(
                            BaseTable.secucode,
                            BaseTable.date,
                            func.abs(field).label(field.key),
                            func.row_number()
                            .over(partition_by=BaseTable.secucode, order_by=desc(BaseTable.date))
                            .label("rn"),
                        )
                        .where(BaseTable.secucode.in_(secucode_query))
                        .subquery()
                    )
                    # 只取每个股票的最新一条记录，并过滤掉空值和0值
                    final_cte = (
                        select(
                            latest_data_subquery.c.secucode,
                            latest_data_subquery.c.date,
                            latest_data_subquery.c[field.key],
                        )
                        .where(latest_data_subquery.c.rn == 1)
                        .where(latest_data_subquery.c[field.key].is_not(None))
                        .where(latest_data_subquery.c[field.key] != 0)
                        .cte(cte_name)
                    )
                else:
                    # 对于指定日期范围的单日期表查询，并过滤掉空值
                    # 注意：field 是 Field 对象，但 func.abs(field) 创建了一个表达式
                    # 需要对表达式添加非空检查
                    field_expr = func.abs(field).label(field.key)
                    final_cte = (
                        select(
                            BaseTable.secucode,
                            BaseTable.date,
                            field_expr,
                        )
                        .where(BaseTable.secucode.in_(secucode_query))
                        .where(BaseTable.date.between(trade_start, trade_end))
                        .where(field_expr.is_not(None))
                        .where(field_expr != 0)
                        .cte(cte_name)
                    )
            else:
                # 多日期表处理：原来的逻辑
                # 对指定字段取绝对值
                abs_subquery = (
                    select(
                        BaseTable.secucode,
                        BaseTable.date,
                        func.abs(field).label(field.key),
                    )
                    .where(BaseTable.secucode.in_(secucode_query))
                    .subquery()
                )

                final_cte = (
                    select(
                        abs_subquery.c.secucode,
                        abs_subquery.c.date,
                        abs_subquery.c[field.key],
                    )
                    .where(abs_subquery.c.date.between(trade_start, trade_end))
                    .where(abs_subquery.c[field.key].is_not(None))
                    .where(abs_subquery.c[field.key] != 0)
                    .cte(cte_name)
                )

            return final_cte

    # @staticmethod
    # def get_in_ranklist_duo_1_times():
    #     """
    #     :术语名称:
    #         上榜一日多空资金榜次数
    #     :术语解释:
    #         上榜一日多空资金榜次数
    #     :功能:
    #         获取上榜一日多空资金榜次数字段
    #     """
    #     return AStockGlobalQuery.calculate_in_ranklist_times(RanklistDuo1Level0)

    # @staticmethod
    # def get_in_ranklist_duo_3_times():
    #     """
    #     :术语名称:
    #         上榜三日多空资金榜次数
    #     :术语解释:
    #         上榜三日多空资金榜次数
    #     :功能:
    #         获取上榜三日多空资金榜次数字段
    #     """
    #     return AStockGlobalQuery.calculate_in_ranklist_times(RanklistDuo3Level0)

    # @staticmethod
    # def get_in_ranklist_gan_13_times():
    #     """
    #     :术语名称:
    #         上榜十三日敢死队资金榜次数
    #     :术语解释:
    #         上榜十三日敢死队资金榜次数
    #     :功能:
    #         获取上榜十三日敢死队资金榜次数字段
    #     """
    #     return AStockGlobalQuery.calculate_in_ranklist_times(RanklistGan13Level0)

    # @staticmethod
    # def get_in_ranklist_fund_times():
    #     """
    #     :术语名称:
    #         上榜资金榜次数
    #     :术语解释:
    #         上榜资金榜次数
    #     :功能:
    #         获取上榜资金榜次数字段
    #     """
    #     return AStockGlobalQuery.calculate_in_ranklist_times(
    #         [RanklistDuo1Level0, RanklistDuo3Level0, RanklistGan13Level0]
    #     )

    @staticmethod
    def get_zhu_days(days: int):
        """
        :术语名称:
            主力资金
        :术语解释:
            主力资金
        :功能:
            获取N日主力资金字段。当问题中仅包含“主力资金”时调用。如果问题中包含“流入”、“流出”等更具体的词汇，则严禁调用此函数，必须使用对应的`_in_`或`_out_`函数。
        :参数:
            days: 天数，用于计算N日主力资金。此函数必须显式传入整数参数。例如days=5表示5日主力资金。
        """
        return AStockGlobalQuery.calculate_diff(DailyZjdxStatLevel5.zhu, days)

    @staticmethod
    def get_zhur_days(days: int):
        """
        :术语名称:
            主力资金增减仓
        :术语解释:
            主力资金增减仓
        :功能:
            获取N日主力资金增减仓字段。当问题中仅包含“主力资金增减仓”时调用。如果问题中包含“增仓”或“减仓”等更具体的词汇，则严禁调用此函数，必须使用对应的`_in_`或`_out_`函数。
        :参数:
            days: 天数，用于计算N日主力资金增减仓。此函数必须显式传入整数参数。例如days=3表示3日主力资金增减仓。
        """
        return AStockGlobalQuery.calculate_diff(DailyZjdxStatLevel5.zhur, days)

    @staticmethod
    def get_duo_days(days: int):
        """
        :术语名称:
            多空资金
        :术语解释:
            多空资金
        :功能:
            获取N日多空资金字段。当问题中仅包含“多空资金”时调用。如果问题中包含“流入”、“流出”等更具体的词汇，则严禁调用此函数，必须使用对应的`_in_`或`_out_`函数。
        :参数:
            days: 天数，用于计算N日多空资金。此函数必须显式传入整数参数。例如days=10表示10日多空资金。
        """
        return AStockGlobalQuery.calculate_diff(DailyZjdxStatLevel5.duo, days)

    @staticmethod
    def get_duor_days(days: int):
        """
        :术语名称:
            多空资金增减仓
        :术语解释:
            多空资金增减仓
        :功能:
            获取N日多空资金增减仓字段。当问题中仅包含“多空资金增减仓”时调用。如果问题中包含“增仓”或“减仓”等更具体的词汇，则严禁调用此函数，必须使用对应的`_in_`或`_out_`函数。
        :参数:
            days: 天数，用于计算N日多空资金增减仓。此函数必须显式传入整数参数。例如days=3表示3日多空资金增减仓。
        """
        return AStockGlobalQuery.calculate_diff(DailyZjdxStatLevel5.duor, days)

    @staticmethod
    def get_gan_days(days: int):
        """
        :术语名称:
            敢死队资金
        :术语解释:
            敢死队资金
        :功能:
            获取N日敢死队资金字段。当问题中仅包含“敢死队资金”时调用。如果问题中仅包含“流入”、“流出”等更具体的词汇，则严禁调用此函数，必须使用对应的`_in_`或`_out_`函数。
        :参数:
            days: 天数，用于计算N日敢死队资金。此函数必须显式传入整数参数。例如days=5表示5日敢死队资金。
        """
        return AStockGlobalQuery.calculate_diff(DailyZjdxStatLevel5.gan, days)

    @staticmethod
    def get_ganr_days(days: int):
        """
        :术语名称:
            敢死队资金增减仓
        :术语解释:
            敢死队资金增减仓
        :功能:
            获取N日敢死队资金增减仓字段。当问题中仅包含“敢死队资金增减仓”时调用。如果问题中包含“增仓”或“减仓”等更具体的词汇，则严禁调用此函数，必须使用对应的`_in_`或`_out_`函数。
        :参数:
            days: 天数，用于计算N日敢死队资金增减仓。此函数必须显式传入整数参数。例如days=3表示3日敢死队资金增减仓。
        """
        return AStockGlobalQuery.calculate_diff(DailyZjdxStatLevel5.ganr, days)

    @staticmethod
    def get_zhu_out_days(days: int):
        """
        :术语名称:
            主力资金流出
        :术语解释:
            主力资金流出
        :功能:
            获取N日主力资金流出字段，问题中明确包含“主力资金流出”时方可调用
        :参数:
            days: 天数，用于计算N日主力资金流出。此函数必须显式传入整数参数。例如days=5表示5日主力资金流出。
        """
        return (
            "_calculate_diff",
            f"zhu_out_{days}d",
            {"field": DailyZjdxStatLevel5.zhu, "days": days, "calculation_mode": "negative", "filter_positive": True},
        )

    @staticmethod
    def get_zhur_out_days(days: int):
        """
        :术语名称:
            主力资金减仓
        :术语解释:
            主力资金减仓
        :功能:
            获取N日主力资金减仓字段，问题中明确包含“主力资金减仓”时方可调用
        :参数:
            days: 天数，用于计算N日主力资金减仓。此函数必须显式传入整数参数。例如days=3表示3日主力资金减仓。
        """
        return (
            "_calculate_diff",
            f"zhur_out_{days}d",
            {"field": DailyZjdxStatLevel5.zhur, "days": days, "calculation_mode": "negative", "filter_positive": True},
        )

    @staticmethod
    def get_duo_out_days(days: int):
        """
        :术语名称:
            多空资金流出
        :术语解释:
            多空资金流出
        :功能:
            获取N日多空资金流出字段，问题中明确包含“多空资金流出”时方可调用
        :参数:
            days: 天数，用于计算N日多空资金流出。此函数必须显式传入整数参数。例如days=10表示10日多空资金流出。
        """
        return (
            "_calculate_diff",
            f"duo_out_{days}d",
            {"field": DailyZjdxStatLevel5.duo, "days": days, "calculation_mode": "negative", "filter_positive": True},
        )

    @staticmethod
    def get_duor_out_days(days: int):
        """
        :术语名称:
            多空资金减仓
        :术语解释:
            多空资金减仓
        :功能:
            获取N日多空资金减仓字段，问题中明确包含“多空资金减仓”时方可调用
        :参数:
            days: 天数，用于计算N日多空资金减仓。此函数必须显式传入整数参数。例如days=3表示3日多空资金减仓。
        """
        return (
            "_calculate_diff",
            f"duor_out_{days}d",
            {"field": DailyZjdxStatLevel5.duor, "days": days, "calculation_mode": "negative", "filter_positive": True},
        )

    @staticmethod
    def get_gan_out_days(days: int):
        """
        :术语名称:
            敢死队资金流出
        :术语解释:
            敢死队资金流出
        :功能:
            获取N日敢死队资金流出字段，问题中明确包含“敢死队资金流出”时方可调用
        :参数:
            days: 天数，用于计算N日敢死队资金流出。此函数必须显式传入整数参数。例如days=5表示5日敢死队资金流出。
        """
        return (
            "_calculate_diff",
            f"gan_out_{days}d",
            {"field": DailyZjdxStatLevel5.gan, "days": days, "calculation_mode": "negative", "filter_positive": True},
        )

    @staticmethod
    def get_ganr_out_days(days: int):
        """
        :术语名称:
            敢死队资金减仓
        :术语解释:
            敢死队资金减仓
        :功能:
            获取N日敢死队资金减仓字段，问题中明确包含“敢死队资金减仓”时方可调用
        :参数:
            days: 天数，用于计算N日敢死队资金减仓。此函数必须显式传入整数参数。例如days=3表示3日敢死队资金减仓。
        """
        return (
            "_calculate_diff",
            f"ganr_out_{days}d",
            {"field": DailyZjdxStatLevel5.ganr, "days": days, "calculation_mode": "negative", "filter_positive": True},
        )

    @staticmethod
    def get_zhu_in_days(days: int):
        """
        :术语名称:
            主力资金流入
        :术语解释:
            主力资金流入
        :功能:
            获取N日主力资金流入字段，问题中明确包含“主力资金流入”时方可调用
        :参数:
            days: 天数，用于计算N日主力资金流入。此函数必须显式传入整数参数。例如days=5表示5日主力资金流入。
        """
        return (
            "_calculate_diff",
            f"zhu_in_{days}d",
            {"field": DailyZjdxStatLevel5.zhu, "days": days, "calculation_mode": "positive", "filter_positive": True},
        )

    @staticmethod
    def get_zhur_in_days(days: int):
        """
        :术语名称:
            主力资金增仓
        :术语解释:
            主力资金增仓
        :功能:
            获取N日主力资金增仓字段，问题中明确包含“主力资金增仓”时方可调用
        :参数:
            days: 天数，用于计算N日主力资金增仓。此函数必须显式传入整数参数。例如days=3表示3日主力资金增仓。
        """
        return (
            "_calculate_diff",
            f"zhur_in_{days}d",
            {"field": DailyZjdxStatLevel5.zhur, "days": days, "calculation_mode": "positive", "filter_positive": True},
        )

    @staticmethod
    def get_duo_in_days(days: int):
        """
        :术语名称:
            多空资金流入
        :术语解释:
            多空资金流入
        :功能:
            获取N日多空资金流入字段，问题中明确包含“多空资金流入”时方可调用
        :参数:
            days: 天数，用于计算N日多空资金流入。此函数必须显式传入整数参数。例如days=10表示10日多空资金流入。
        """
        return (
            "_calculate_diff",
            f"duo_in_{days}d",
            {"field": DailyZjdxStatLevel5.duo, "days": days, "calculation_mode": "positive", "filter_positive": True},
        )

    @staticmethod
    def get_duor_in_days(days: int):
        """
        :术语名称:
            多空资金增仓
        :术语解释:
            多空资金增仓
        :功能:
            获取N日多空资金增仓字段，问题中明确包含“多空资金增仓”时方可调用
        :参数:
            days: 天数，用于计算N日多空资金增仓。此函数必须显式传入整数参数。例如days=3表示3日多空资金增仓。
        """
        return (
            "_calculate_diff",
            f"duor_in_{days}d",
            {"field": DailyZjdxStatLevel5.duor, "days": days, "calculation_mode": "positive", "filter_positive": True},
        )

    @staticmethod
    def get_gan_in_days(days: int):
        """
        :术语名称:
            敢死队资金流入
        :术语解释:
            敢死队资金流入
        :功能:
            获取N日敢死队资金流入字段，问题中明确包含“敢死队资金流入”时方可调用
        :参数:
            days: 天数，用于计算N日敢死队资金流入。此函数必须显式传入整数参数。例如days=5表示5日敢死队资金流入。
        """
        return (
            "_calculate_diff",
            f"gan_in_{days}d",
            {"field": DailyZjdxStatLevel5.gan, "days": days, "calculation_mode": "positive", "filter_positive": True},
        )

    @staticmethod
    def get_ganr_in_days(days: int):
        """
        :术语名称:
            敢死队资金增仓
        :术语解释:
            敢死队资金增仓
        :功能:
            获取N日敢死队资金增仓字段，问题中明确包含“敢死队资金增仓”时方可调用
        :参数:
            days: 天数，用于计算N日敢死队资金增仓。此函数必须显式传入整数参数。例如days=3表示3日敢死队资金增仓。
        """
        return (
            "_calculate_diff",
            f"ganr_in_{days}d",
            {"field": DailyZjdxStatLevel5.ganr, "days": days, "calculation_mode": "positive", "filter_positive": True},
        )

    @staticmethod
    def get_zhu_data():
        return [
            AStockGlobalQuery.get_zhu_days(1),
            AStockGlobalQuery.get_zhu_days(3),
            AStockGlobalQuery.get_zhu_days(13),
        ]

    @staticmethod
    def get_duo_data():
        return [
            AStockGlobalQuery.get_duo_days(1),
            AStockGlobalQuery.get_duo_days(3),
            AStockGlobalQuery.get_duo_days(13),
        ]

    @staticmethod
    def get_gan_data():
        return [
            AStockGlobalQuery.get_gan_days(1),
            AStockGlobalQuery.get_gan_days(3),
            AStockGlobalQuery.get_gan_days(13),
        ]

    @staticmethod
    def get_three_lock():
        """
        :术语名称:
            短线决策(三把锁)
        :术语解释:
            短线决策(三把锁)
        :功能:
            获取短线决策(三把锁)字段，出现小黄锁时不能使用
        """
        return DailyCwDataLevel0.short_policy

    @staticmethod
    def get_capital_flow():
        return [
            AStockGlobalQuery.get_zhu_days(1),
            AStockGlobalQuery.get_zhur_days(1),
            AStockGlobalQuery.get_duo_days(1),
            AStockGlobalQuery.get_duor_days(1),
            AStockGlobalQuery.get_gan_days(1),
            AStockGlobalQuery.get_ganr_days(1),
            AStockGlobalQuery.get_zhu_days(13),
            AStockGlobalQuery.get_zhur_days(13),
            AStockGlobalQuery.get_duo_days(13),
            AStockGlobalQuery.get_duor_days(13),
            AStockGlobalQuery.get_gan_days(13),
            AStockGlobalQuery.get_ganr_days(13),
            AStockGlobalQuery.get_zhu_days(22),
            AStockGlobalQuery.get_zhur_days(22),
            AStockGlobalQuery.get_duo_days(22),
            AStockGlobalQuery.get_duor_days(22),
            AStockGlobalQuery.get_gan_days(22),
            AStockGlobalQuery.get_ganr_days(22),
        ]

    @staticmethod
    def get_adjusted_zdf(adjusted_start_date: str = "0d", adjusted_end_date: str = "0d"):
        # 调整涨跌幅，专门针对类似20日涨跌幅的处理
        return (
            "_get_adjusted_zdf",
            "adjusted_zdf",
            {"adjusted_start_date": adjusted_start_date, "adjusted_end_date": adjusted_end_date},
        )

    @staticmethod
    def _get_adjusted_zdf(
        start_date: str = "0d",
        end_date: str = "0d",
        secucode_query=None,
        adjusted_start_date: str = "0d",
        adjusted_end_date: str = "0d",
    ):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        unique_id = uuid.uuid4().hex[:6]
        adjusted_trade_start = get_date(adjusted_start_date, is_start=True)
        adjusted_trade_end = get_date(adjusted_end_date)
        # 构建日期 × 股票笛卡尔积
        subquery = secucode_query.subquery()

        # 获取 start_date 对应的收盘价
        start_price_subquery = (
            select(AStockMarket.secucode, AStockMarket.close.label("start_close"))
            .where(
                and_(
                    AStockMarket.date <= adjusted_trade_start,  # 改为 <=
                    AStockMarket.secucode.in_(select(subquery.c.secucode)),
                )
            )
            .order_by(AStockMarket.date.desc())  # 按日期倒序
            .limit(1)  # 最近一条
            .subquery()
        )

        # 获取 end_date 对应的收盘价
        end_price_subquery = (
            select(AStockMarket.secucode, AStockMarket.close.label("end_close"))
            .where(
                and_(
                    AStockMarket.date <= adjusted_trade_end,  # 同理
                    AStockMarket.secucode.in_(select(subquery.c.secucode)),
                )
            )
            .order_by(AStockMarket.date.desc())
            .limit(1)
            .subquery()
        )

        # 构建最终 CTE：每只股票的累计涨跌幅
        cte_adjusted_zdf = (
            select(
                subquery.c.secucode,
                (
                    (end_price_subquery.c.end_close - start_price_subquery.c.start_close)
                    / start_price_subquery.c.start_close
                ).label("adjusted_zdf"),
            )
            .join(start_price_subquery, start_price_subquery.c.secucode == subquery.c.secucode)
            .join(end_price_subquery, end_price_subquery.c.secucode == subquery.c.secucode)
            .cte(f"cte_adjusted_zdf_{unique_id}")
        )

        return cte_adjusted_zdf

    @staticmethod
    def get_oversold_pool():
        """
        :术语名称:
            日线入选超跌选股池
        :术语解释:
            日线入选超跌选股池
        :功能:
            获取“是否入选日线超跌选股池”这一字段，用于判断单只股票是否在该超跌选股池中。当用户问题包含“是不是日线超跌选股股票”“是日线超跌选股股票吗”“是否属于日线超跌选股池”“是否为超跌股”“是否在超跌选股池中”“是否入选日线超跌选股池”等判断类语义时，必须调用此字段函数，而不是 query_oversold_pool()。
        """
        return "_get_oversold_pool", "is_in_hjk_oversold", {}

    @staticmethod
    def _get_oversold_pool(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)
        # 构建日期 × 股票笛卡尔积
        subquery = secucode_query.subquery()

        # 新增：获取唯一交易日期子查询，避免逐股票重复
        dates_subquery = (
            select((AStockMarket.date).label("date"))
            .where(AStockMarket.date.between(trade_start, trade_end))
            .distinct()
            .subquery()
        )

        cte_oversold_pool = (
            select(
                subquery.c.secucode,
                dates_subquery.c.date.label("date"),  # 使用唯一日期
                func.has(
                    func.splitByChar(",", func.coalesce(DailyHjkpoolsLevel5.stock_pools, "")), subquery.c.secucode
                ).label("is_in_hjk_oversold"),
            )
            # 通过多个 select_from 实现股票代码与唯一日期的交叉连接（SQLAlchemy 中笛卡尔积的规范写法）
            .select_from(subquery, dates_subquery)
            .outerjoin(
                DailyHjkpoolsLevel5,
                and_(
                    DailyHjkpoolsLevel5.date == dates_subquery.c.date,  # 使用唯一日期连接
                    DailyHjkpoolsLevel5.filter_conditions == "0:1:1:0",
                ),
            )
            .cte(f"cte_oversold_pool_{uuid.uuid4().hex[:6]}")
        )
        return cte_oversold_pool

    @staticmethod
    def get_duo_pool():
        """
        :术语名称:
            长线入选多空资金池
        :术语解释:
            长线入选多空资金池
        :功能:
            获取“是否入选长线入选多空资金池”这一字段，用于判断单只股票是否在该长线多空资金池中。当用户问题包含“是不是多空资金池”“是长线多空资金股票吗”“是否属于多空资金股票”“是否为多空资金股票”“是否在多空资金池中”“是否入选长线多空资金池”等判断类语义时，必须调用此字段函数，而非query_duo_pool()。
        """
        return "_get_duo_pool", "is_duo_pool", {}

    @staticmethod
    def _get_duo_pool(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)
        # 构建日期 × 股票笛卡尔积
        subquery = secucode_query.subquery()

        # 新增：获取唯一交易日期子查询，避免逐股票重复
        dates_subquery = (
            select((AStockMarket.date).label("date"))
            .where(AStockMarket.date.between(trade_start, trade_end))
            .distinct()
            .subquery()
        )

        cte_break_pool = (
            select(
                subquery.c.secucode,
                dates_subquery.c.date.label("date"),  # 使用唯一日期
                func.has(
                    func.splitByChar(",", func.coalesce(DailyLdPoolsLevel5.stock_pools, "")), subquery.c.secucode
                ).label("is_duo_pool"),
            )
            # 通过多个 select_from 实现股票代码与唯一日期的交叉连接（SQLAlchemy 中笛卡尔积的规范写法）
            .select_from(subquery, dates_subquery)
            .outerjoin(
                DailyLdPoolsLevel5,
                and_(
                    DailyLdPoolsLevel5.date == dates_subquery.c.date,  # 使用唯一日期连接
                    DailyLdPoolsLevel5.filter_conditions == "0:7:0:0",
                ),
            )
            .cte(f"cte_break_pool_{uuid.uuid4().hex[:6]}")
        )
        return cte_break_pool

    @staticmethod
    def get_wz_opt_flag_pool():
        """
        :术语名称:
            日线入选战区优化旗池
        :术语解释:
            日线入选战区优化旗池
        :功能:
            获取“是否入选战区优化旗池”这一字段，用于判断单只股票是否在该战区优化旗池中。当用户问题包含“是不是日线战区优化旗股票”“是日线战区优化旗股票吗”“是否属于日线战区优化旗池”“是否为战区优化旗股”“是否在突战区优化旗池中”“是否入选日线战区优化旗池”等判断类语义时，必须调用此字段函数，而非筛选函数 query_wz_opt_flag_pool()。
        """
        return "_get_wz_opt_flag_pool", "is_in_wz_opt_flag", {}

    @staticmethod
    def _get_wz_opt_flag_pool(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)
        # 构建日期 × 股票笛卡尔积
        subquery = secucode_query.subquery()

        # 新增：获取唯一交易日期子查询，避免逐股票重复
        dates_subquery = (
            select((AStockMarket.date).label("date"))
            .where(AStockMarket.date.between(trade_start, trade_end))
            .distinct()
            .subquery()
        )

        cte_break_pool = (
            select(
                subquery.c.secucode,
                dates_subquery.c.date.label("date"),  # 使用唯一日期
                func.has(
                    func.splitByChar(",", func.coalesce(DailyLdPoolsLevel5.stock_pools, "")), subquery.c.secucode
                ).label("is_in_wz_opt_flag"),
            )
            # 通过多个 select_from 实现股票代码与唯一日期的交叉连接（SQLAlchemy 中笛卡尔积的规范写法）
            .select_from(subquery, dates_subquery)
            .outerjoin(
                DailyLdPoolsLevel5,
                and_(
                    DailyLdPoolsLevel5.date == dates_subquery.c.date,  # 使用唯一日期连接
                    DailyLdPoolsLevel5.filter_conditions == "0:6:0:0",
                ),
            )
            .cte(f"cte_break_pool_{uuid.uuid4().hex[:6]}")
        )
        return cte_break_pool

    @staticmethod
    def get_break_pool():
        """
        :术语名称:
            日线入选突破选股池
        :术语解释:
            日线入选突破选股池
        :功能:
            获取“是否入选日线突破选股池”这一字段，用于判断单只股票是否在该突破选股池中。当用户问题包含“是不是日线突破选股股票”“是日线突破选股股票吗”“是否属于日线突破选股池”“是否为突破股”“是否在突破选股池中”“是否入选日线突破选股池”等判断类语义时，必须调用此字段函数，而不是 query_breakthrough_pool()。
        """
        return "_get_break_pool", "is_in_hjk_breakthrough", {}

    @staticmethod
    def _get_break_pool(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)
        # 构建日期 × 股票笛卡尔积
        subquery = secucode_query.subquery()

        # 新增：获取唯一交易日期子查询，避免逐股票重复
        dates_subquery = (
            select((AStockMarket.date).label("date"))
            .where(AStockMarket.date.between(trade_start, trade_end))
            .distinct()
            .subquery()
        )

        cte_break_pool = (
            select(
                subquery.c.secucode,
                dates_subquery.c.date.label("date"),  # 使用唯一日期
                func.has(
                    func.splitByChar(",", func.coalesce(DailyHjkpoolsLevel5.stock_pools, "")), subquery.c.secucode
                ).label("is_in_hjk_breakthrough"),
            )
            # 通过多个 select_from 实现股票代码与唯一日期的交叉连接（SQLAlchemy 中笛卡尔积的规范写法）
            .select_from(subquery, dates_subquery)
            .outerjoin(
                DailyHjkpoolsLevel5,
                and_(
                    DailyHjkpoolsLevel5.date == dates_subquery.c.date,  # 使用唯一日期连接
                    DailyHjkpoolsLevel5.filter_conditions == "0:3:1:0",
                ),
            )
            .cte(f"cte_break_pool_{uuid.uuid4().hex[:6]}")
        )
        return cte_break_pool

    @staticmethod
    def get_volatility_pool():
        """
        :术语名称:
            日线入选震荡选股池
        :术语解释:
            日线入选震荡选股池
        :功能:
            获取“是否入选日线震荡选股池”这一字段，用于判断单只股票是否在该震荡选股池中。当用户问题包含“是不是日线震荡选股股票”“是日线震荡选股股票吗”“是否属于日线震荡选股池”“是否为震荡股”“是否在震荡选股池中”“是否入选日线震荡选股池”等判断类语义时，必须调用此字段函数。
        """
        return "_get_volatility_pool", "is_in_hjk_volatility", {}

    @staticmethod
    def get_xwlhSeat_data():
        return [
            AStockGlobalQuery.get_hot_money_name(),
            AStockGlobalQuery.get_all_buying(),
            AStockGlobalQuery.get_all_selling(),
            AStockGlobalQuery.get_continous_days(),
            AStockGlobalQuery.get_reason(),
        ]

    @staticmethod
    def get_xwlhSeat_detail_data():
        # 前五买榜的席位ID、席位名称、总买、净买额、席位类型、游资名称；
        return [
            AStockGlobalQuery.get_seat_id(),
            AStockGlobalQuery.get_seat_name(),
            AStockGlobalQuery.get_buying_amt(),
            AStockGlobalQuery.get_selling_amt(),
            AStockGlobalQuery.get_seat_type(),
            AStockGlobalQuery.get_detail_hot_money_name(),
            AStockGlobalQuery.get_net_amt(),
        ]

    @staticmethod
    def _get_volatility_pool(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)
        # 构建日期 × 股票笛卡尔积
        subquery = secucode_query.subquery()

        # 新增：获取唯一交易日期子查询，避免逐股票重复
        dates_subquery = (
            select((AStockMarket.date).label("date"))
            .where(AStockMarket.date.between(trade_start, trade_end))
            .distinct()
            .subquery()
        )

        cte_break_pool = (
            select(
                subquery.c.secucode,
                dates_subquery.c.date.label("date"),  # 使用唯一日期
                func.has(
                    func.splitByChar(",", func.coalesce(DailyHjkpoolsLevel5.stock_pools, "")), subquery.c.secucode
                ).label("is_in_hjk_volatility"),
            )
            # 通过多个 select_from 实现股票代码与唯一日期的交叉连接（SQLAlchemy 中笛卡尔积的规范写法）
            .select_from(subquery, dates_subquery)
            .outerjoin(
                DailyHjkpoolsLevel5,
                and_(
                    DailyHjkpoolsLevel5.date == dates_subquery.c.date,  # 使用唯一日期连接
                    DailyHjkpoolsLevel5.filter_conditions == "0:2:1:0",
                ),
            )
            .cte(f"cte_break_pool_{uuid.uuid4().hex[:6]}")
        )
        return cte_break_pool

    @staticmethod
    def get_ldzf():
        return [DailyQlldLevel5.ldtrade, DailyQlldLevel5.ldmarket, DailyQlldLevel5.ld0z]

    @staticmethod
    def get_board_ld():
        """
        :术语名称:
            行业/战区轮动
        :术语解释:
            行业/战区轮动
        :功能:
            获取行业或战区的轮动状态字段。此函数属于查询类指标函数，用于返回行业、板块或战区当前的轮动状态。当用户问题涉及特定行业、板块或战区的“轮动”“强势轮动”“开始轮动”“轮动情况”“对行业轮动状态”等语义时，必须调用本函数。若问题为查询轮动状态且包含以“BR”开头的板块代码（如BR01B00014等），无论是否出现“板块”“行业”或“战区”等字样，均视为行业/战区类查询，必须调用本函数。
        """
        return DailyQlldLevel5.ld0z

    @staticmethod
    def get_private_hero_ranking():
        return [
            SmxyHoldingStatLevel15.smyx_num,
            SmxyHoldingStatLevel15.smyx_zc_num,
            SmxyHoldingStatLevel15.buy_amount,
            SmxyHoldingStatLevel15.sell_amount,
        ]

    @staticmethod
    def get_seat_type():
        """
        :术语名称:
            席位类型
        :术语解释:
            席位类型
        :功能:
            获取席位类型字段
        """
        return XwlhSeatdetailLevel15.seattype

    @staticmethod
    def get_seat_leader_count_last3days():
        """
        :术语名称:
            近三个交易日上席位龙虎榜次数
        :术语解释:
            近三个交易日上席位龙虎榜次数
        :功能:
            获取近三个交易日上席位龙虎榜次数字段
        """
        return "_get_seat_leader_count_last3days", "xwlh_count_3d", {}

    @staticmethod
    def _get_seat_leader_count_last3days(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")

        unique_id = uuid.uuid4().hex[:6]
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)

        # 近几天
        recent_n_days = 3

        subquery = secucode_query.subquery()

        # 日期 × 股票集合
        date_stock = (
            select(AStockMarket.date.distinct().label("date"), subquery.c.secucode.label("secucode"))
            .select_from(AStockMarket)
            .join(subquery, literal(True))  # CROSS JOIN
            .where(AStockMarket.date.between(trade_start, trade_end))
        ).subquery()

        xwlh_query = (
            select(
                XwlhSeatStatLevel15.date.distinct().label("date"),
                XwlhSeatStatLevel15.secucode.label("secucode"),
            )
            .where(
                XwlhSeatStatLevel15.date.between(trade_start, trade_end),
                XwlhSeatStatLevel15.secucode.in_(select(subquery.c.secucode)),
            )
            .subquery()
        )

        xwlh_window = (
            select(
                date_stock.c.date.label("date"),
                date_stock.c.secucode.label("secucode"),
                case((xwlh_query.c.date != datetime.date(1970, 1, 1), 1), else_=0).label("in_rank"),
            )
            .select_from(date_stock)
            .outerjoin(
                xwlh_query, and_(date_stock.c.date == xwlh_query.c.date, date_stock.c.secucode == xwlh_query.c.secucode)
            )
            .subquery()
        )

        # 在一个 CTE 中完成窗口计算 + LEFT JOIN
        xwlh_count_cte = (
            select(
                xwlh_window.c.date,
                xwlh_window.c.secucode,
                func.sum(xwlh_window.c.in_rank)
                .over(partition_by=xwlh_window.c.secucode, order_by=xwlh_window.c.date, rows=(-(recent_n_days - 1), 0))
                .label("xwlh_count_3d"),
            )
            .select_from(xwlh_window)
            .cte(f"xwlh_count_cte_{unique_id}")
        )

        return xwlh_count_cte

    @staticmethod
    def get_seat_leader_inrank_date():
        """
        :术语名称:
            最近上席位龙虎榜日期
        :术语解释:
            最近上席位龙虎榜日期
        :功能:
            获取最近上席位龙虎榜日期字段
        """
        return "_get_seat_leader_inrank_date", "xwlh_date", {}

    @staticmethod
    def _get_seat_leader_inrank_date(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        subquery = secucode_query.subquery()
        unique_id = uuid.uuid4().hex[:6]
        xwlh_inrank_cte = (
            select(XwlhSeatStatLevel15.secucode, func.max(XwlhSeatStatLevel15.date).label("xwlh_date"))
            .where(XwlhSeatStatLevel15.secucode.in_(select(subquery.c.secucode)))
            .group_by(XwlhSeatStatLevel15.secucode)
            .cte(f"xwlh_inrank_cte_{unique_id}")
        )
        return xwlh_inrank_cte

    @staticmethod
    def get_top5_seat_total_net_buy():
        """
        :术语名称:
            前五席排重后总净买
        :术语解释:
            前五席排重后总净买
        :功能:
            获取前五席排重后总净买字段
        """
        return "_get_top5_seat_total_net_buy", "xwlh_net_buy", {}

    @staticmethod
    def _get_top5_seat_total_net_buy(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        subquery = secucode_query.subquery()
        unique_id = uuid.uuid4().hex[:6]

        # 解析日期范围
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)

        if start_date == "0d" and end_date == "0d":
            # 对于0d0d查询，获取指定日期范围内的最新数据
            latest_date_subq = (
                select(XwlhSeatStatLevel15.secucode, func.max(XwlhSeatStatLevel15.date).label("latest_date"))
                .where(
                    and_(
                        XwlhSeatStatLevel15.secucode.in_(select(subquery.c.secucode)),
                        XwlhSeatStatLevel15.date.between(trade_start, trade_end),
                    )
                )
                .group_by(XwlhSeatStatLevel15.secucode)
                .subquery()
            )

            xwlh_top5_seat_total_net_buy_cte = (
                select(
                    XwlhSeatStatLevel15.secucode,
                    XwlhSeatStatLevel15.date.label("date"),
                    (XwlhSeatStatLevel15.all_buying - XwlhSeatStatLevel15.all_selling).label("xwlh_net_buy"),
                )
                .join(
                    latest_date_subq,
                    and_(
                        XwlhSeatStatLevel15.secucode == latest_date_subq.c.secucode,
                        XwlhSeatStatLevel15.date == latest_date_subq.c.latest_date,
                    ),
                )
                .cte(f"xwlh_top5_seat_total_net_buy_cte_{unique_id}")
            )
        else:
            # 对于指定日期查询，直接返回该日期的数据
            xwlh_top5_seat_total_net_buy_cte = (
                select(
                    XwlhSeatStatLevel15.secucode,
                    XwlhSeatStatLevel15.date.label("date"),
                    (XwlhSeatStatLevel15.all_buying - XwlhSeatStatLevel15.all_selling).label("xwlh_net_buy"),
                )
                .where(
                    and_(
                        XwlhSeatStatLevel15.secucode.in_(select(subquery.c.secucode)),
                        XwlhSeatStatLevel15.date.between(trade_start, trade_end),
                    )
                )
                .cte(f"xwlh_top5_seat_total_net_buy_cte_{unique_id}")
            )

        return xwlh_top5_seat_total_net_buy_cte

    @staticmethod
    def get_institutional_amount():
        """
        :术语名称:
            机构专用数量
        :术语解释:
            机构专用数量
        :功能:
            获取机构专用数量字段
        """
        return "_get_institutional_amount", "institutional_seat_count", {}

    @staticmethod
    def _get_institutional_amount(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        subquery = secucode_query.subquery()
        unique_id = uuid.uuid4().hex[:6]

        # 获取每个股票的最新上榜日期
        latest_date_subq = (
            select(XwlhSeatStatLevel15.secucode, func.max(XwlhSeatStatLevel15.date).label("latest_date"))
            .where(XwlhSeatStatLevel15.secucode.in_(select(subquery.c.secucode)))
            .group_by(XwlhSeatStatLevel15.secucode)
            .subquery()
        )

        # 创建基础股票列表（包含所有需要查询的股票）
        base_stocks = select(latest_date_subq.c.secucode, latest_date_subq.c.latest_date).subquery()

        # 使用左连接确保即使没有机构专用记录也能返回0
        institutional_amount_subq = (
            select(
                base_stocks.c.secucode,
                base_stocks.c.latest_date,
                # 使用SUM代替COUNT，对机构专用席位的数量进行求和
                func.sum(case((XwlhSeatdetailLevel15.seat_name == "机构专用", 1), else_=0)).label(
                    "institutional_seat_amount"
                ),
            )
            .select_from(base_stocks)
            .outerjoin(
                XwlhSeatdetailLevel15,
                (XwlhSeatdetailLevel15.secucode == base_stocks.c.secucode)
                & (XwlhSeatdetailLevel15.date == base_stocks.c.latest_date),
            )
            .group_by(base_stocks.c.secucode, base_stocks.c.latest_date)
        ).subquery()

        # 确保返回0值
        institutional_amount_cte = (
            select(
                institutional_amount_subq.c.secucode,
                func.coalesce(institutional_amount_subq.c.institutional_seat_amount, 0).label(
                    "institutional_seat_count"
                ),
            )
        ).cte(f"institutional_amount_cte_{unique_id}")

        return institutional_amount_cte

    @staticmethod
    def get_latest_hot_money_name():
        # 不放到rag里，专门用来对应诊股需求的
        # 最近一次上榜时上榜游资
        return "_get_latest_hot_money_name", "latest_hot_money_name", {}

    @staticmethod
    def _get_latest_hot_money_name(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")

        subquery = secucode_query.subquery()
        unique_id = uuid.uuid4().hex[:6]

        latest_date_subq = (
            select(
                XwlhSeatStatLevel15.secucode.label("secucode"), func.max(XwlhSeatStatLevel15.date).label("latest_date")
            )
            .where(XwlhSeatStatLevel15.secucode.in_(select(subquery.c.secucode)))
            .group_by(XwlhSeatStatLevel15.secucode)
            .subquery()
        )

        hot_money_name_cte = (
            select(
                latest_date_subq.c.secucode.label("secucode"),
                XwlhSeatStatLevel15.hot_money_name.label("latest_hot_money_name"),
            )
            .select_from(
                latest_date_subq.outerjoin(
                    XwlhSeatStatLevel15,
                    (XwlhSeatStatLevel15.secucode == latest_date_subq.c.secucode)
                    & (XwlhSeatStatLevel15.date == latest_date_subq.c.latest_date),
                )
            )
            .cte(f"hot_money_name_cte_{unique_id}")
        )

        return hot_money_name_cte

    @staticmethod
    def get_latest_reason():
        # 最近一次上榜时异动原因
        return "_get_latest_reason", "latest_reason", {}

    @staticmethod
    def _get_latest_reason(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")

        subquery = secucode_query.subquery()
        unique_id = uuid.uuid4().hex[:6]

        latest_date_subq = (
            select(
                XwlhSeatStatLevel15.secucode.label("secucode"), func.max(XwlhSeatStatLevel15.date).label("latest_date")
            )
            .where(XwlhSeatStatLevel15.secucode.in_(select(subquery.c.secucode)))
            .group_by(XwlhSeatStatLevel15.secucode)
            .subquery()
        )

        reason_cte = (
            select(
                latest_date_subq.c.secucode.label("secucode"),
                XwlhSeatStatLevel15.reason.label("latest_reason"),
            )
            .select_from(
                latest_date_subq.outerjoin(
                    XwlhSeatStatLevel15,
                    (XwlhSeatStatLevel15.secucode == latest_date_subq.c.secucode)
                    & (XwlhSeatStatLevel15.date == latest_date_subq.c.latest_date),
                )
            )
            .cte(f"reason_cte_{unique_id}")
        )

        return reason_cte

    @staticmethod
    def _get_conditional_field(condition_field: str, field_obj, condition_params: dict = None):
        """
        :功能:
            根据条件字段控制目标字段的输出，仅当条件成立时输出目标字段，否则为None。
        """
        # 检查是否为有效的 SQL 字段对象
        if not hasattr(field_obj, "class_") or not hasattr(field_obj, "key"):
            raise ValueError("field_obj 必须是 SQL 字段对象（如 RanklistDuo1Level0.duo_1）")

        column_name = field_obj.key

        cte_params = {
            "condition_field": condition_field,
            "field_obj": field_obj,
        }

        if condition_params:
            cte_params.update(condition_params)

        return (
            "_get_conditional_field_cte",
            f"{column_name}",
            cte_params,
        )

    @staticmethod
    def _get_conditional_field_cte(
        start_date: str,
        end_date: str,
        secucode_query,
        condition_field: str,
        field_obj,
        rank_table_name: str,
        rank_column_name: str,
    ):
        """
        实际生成带条件控制的字段 CTE
        """
        # 检查field_obj是否为有效的SQL字段对象
        if not hasattr(field_obj, "class_") or not hasattr(field_obj, "key"):
            raise ValueError("field_obj 必须是SQL字段对象")

        table_class = field_obj.class_
        column_name = field_obj.key

        # 生成条件CTE（检查是否在排行榜中）
        cond_cte = AStockGlobalQuery._get_is_in_ranklist(
            start_date=start_date,
            end_date=end_date,
            secucode_query=secucode_query,
            rank_table_name=rank_table_name,
            rank_column_name=rank_column_name,
            output_column_name=condition_field,
        )

        # 直接查询排行榜表和条件表的联合结果
        joined = (
            select(
                cond_cte.c.secucode.label("secucode"),
                cond_cte.c.date.label("date"),
                case(
                    (cond_cte.c[condition_field] == 1, field_obj),
                    else_=None,
                ).label(column_name),
            )
            .select_from(
                cond_cte.outerjoin(
                    table_class,
                    and_(
                        table_class.secucode == cond_cte.c.secucode,
                        table_class.date == cond_cte.c.date,
                    ),
                )
            )
            .cte(f"cte_cond_{column_name}")
        )
        return joined

    @staticmethod
    def get_duo_1_ranklist_data():
        rank_condition_params = {
            "rank_table_name": "RanklistDuo1Level0",
            "rank_column_name": "duor_1",
        }

        return [
            AStockGlobalQuery._get_is_in_duo_1_rank(),
            AStockGlobalQuery._get_conditional_field(
                condition_field="is_on_duo1",
                field_obj=RanklistDuo1Level0.duo_1,
                condition_params=rank_condition_params,
            ),
            AStockGlobalQuery._get_conditional_field(
                condition_field="is_on_duo1",
                field_obj=RanklistDuo1Level0.duor_1,
                condition_params=rank_condition_params,
            ),
        ]

    @staticmethod
    def _get_is_in_duo_1_rank():
        return (
            "_get_is_in_ranklist",
            "is_on_duo1",
            {
                "rank_table_name": "RanklistDuo1Level0",
                "rank_column_name": "duor_1",
                "output_column_name": "is_on_duo1",
            },
        )

    @staticmethod
    def get_duo_3_ranklist_data():
        rank_condition_params = {
            "rank_table_name": "RanklistDuo3Level0",
            "rank_column_name": "duor_3",
        }

        return [
            AStockGlobalQuery._get_is_in_duo_3_rank(),
            AStockGlobalQuery._get_conditional_field(
                condition_field="is_on_duo3",
                field_obj=RanklistDuo3Level0.duo_3,
                condition_params=rank_condition_params,
            ),
            AStockGlobalQuery._get_conditional_field(
                condition_field="is_on_duo3",
                field_obj=RanklistDuo3Level0.duor_3,
                condition_params=rank_condition_params,
            ),
        ]

    @staticmethod
    def _get_is_in_duo_3_rank():
        return (
            "_get_is_in_ranklist",
            "is_on_duo3",
            {
                "rank_table_name": "RanklistDuo3Level0",
                "rank_column_name": "duor_3",
                "output_column_name": "is_on_duo3",
            },
        )

    @staticmethod
    def get_gan_13_ranklist_data():
        rank_condition_params = {
            "rank_table_name": "RanklistGan13Level0",
            "rank_column_name": "ganr_13",
        }

        return [
            AStockGlobalQuery._get_is_in_gan_13_rank(),
            AStockGlobalQuery._get_conditional_field(
                condition_field="is_on_gan13",
                field_obj=RanklistGan13Level0.gan_13,
                condition_params=rank_condition_params,
            ),
            AStockGlobalQuery._get_conditional_field(
                condition_field="is_on_gan13",
                field_obj=RanklistGan13Level0.ganr_13,
                condition_params=rank_condition_params,
            ),
        ]

    @staticmethod
    def _get_is_in_gan_13_rank():
        return (
            "_get_is_in_ranklist",
            "is_on_gan13",
            {
                "rank_table_name": "RanklistGan13Level0",
                "rank_column_name": "ganr_13",
                "output_column_name": "is_on_gan13",
            },
        )

    @staticmethod
    def _get_is_in_ranklist(
        start_date: str,
        end_date: str,
        secucode_query,
        rank_table_name: str,
        rank_column_name: str,
        output_column_name: str,
    ):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")

        subquery = secucode_query.subquery() if hasattr(secucode_query, "subquery") else secucode_query
        rank_table = globals().get(rank_table_name)
        if rank_table is None:
            raise ValueError(f"找不到榜单表: {rank_table_name}")
        if not hasattr(rank_table, rank_column_name):
            raise ValueError(f"榜单表 {rank_table_name} 不包含列 {rank_column_name}")

        rank_column = getattr(rank_table, rank_column_name)
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)
        unique_id = uuid.uuid4().hex[:6]

        # 计算每日排名
        ranked_subquery = (
            select(
                rank_table.secucode.label("secucode"),
                rank_table.date.label("date"),
                func.row_number().over(partition_by=rank_table.date, order_by=desc(rank_column)).label("rank"),
            )
            .where(
                rank_table.date.between(trade_start, trade_end),
                rank_table.secucode.is_not(None),
                rank_column.is_not(None),
            )
            .subquery(f"ranked_{rank_table_name}_{unique_id}")
        )

        # 前50榜单
        top50_subq = (
            select(ranked_subquery.c.secucode.label("secucode"), ranked_subquery.c.date.label("date"))
            .where(ranked_subquery.c.rank <= 50)
            .subquery(f"top50_{rank_table_name}_{unique_id}")
        )

        #  日期网格（交易日 + 股票）
        dates_cte = (
            select(AStockMarket.date.label("date"))
            .where(AStockMarket.date.between(trade_start, trade_end))
            .distinct()
            .cte(f"dates_grid_{rank_table_name}_{unique_id}")
        )
        uniq_sec_subq = select(subquery.c.secucode).distinct().subquery(f"uniq_sec_{unique_id}")

        secucode_dates_grid_cte = select(
            uniq_sec_subq.c.secucode.label("secucode"), dates_cte.c.date.label("date")
        ).cte(f"secucode_dates_grid_{rank_table_name}_{unique_id}")

        # 必须确保 top50_subq 的 date 在有效范围内才计算 1，否则为 0
        final_cte = (
            select(
                secucode_dates_grid_cte.c.secucode,
                secucode_dates_grid_cte.c.date,
                case(
                    (
                        and_(top50_subq.c.secucode.is_not(None), top50_subq.c.date == secucode_dates_grid_cte.c.date),
                        1,
                    ),
                    else_=0,
                ).label(output_column_name),
            )
            .select_from(
                secucode_dates_grid_cte.outerjoin(
                    top50_subq,
                    and_(
                        secucode_dates_grid_cte.c.secucode == top50_subq.c.secucode,
                        secucode_dates_grid_cte.c.date == top50_subq.c.date,
                    ),
                )
            )
            .cte(f"final_ranklist_{rank_table_name}_{unique_id}")
        )

        return final_cte

    @staticmethod
    def get_price_zone():
        """
        :术语名称:
            股价所处空间
        :术语解释:
            股价所处空间
        :功能:
            获取股价所处空间字段
        """
        return "_get_price_zone", "price_zone", {}

    @staticmethod
    def _get_price_zone(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)
        # 构建日期 × 股票笛卡尔积
        subquery = secucode_query.subquery()
        unique_id = uuid.uuid4().hex[:6]

        # 新增：获取唯一交易日期子查询，避免逐股票重复
        dates_subquery = (
            select((AStockMarket.date).label("date"))
            .where(AStockMarket.date.between(trade_start, trade_end))
            .distinct()
            .subquery()
        )

        trade_secucode_subquery = select(dates_subquery.c.date, subquery.c.secucode).subquery()

        joined_subquery = (
            select(
                *[
                    trade_secucode_subquery.c.date.label("current_date"),
                    trade_secucode_subquery.c.secucode.label("secucode"),
                    DailyGzkjLevel0.gzlow.label("gzlow"),
                    DailyGzkjLevel0.gzhigh.label("gzhigh"),
                    DailyGzkjLevel0.date,
                    func.max(DailyGzkjLevel0.date)
                    .over(partition_by=[trade_secucode_subquery.c.date, trade_secucode_subquery.c.secucode])
                    .label("max_date"),
                ]
            )
            .select_from(
                trade_secucode_subquery.join(
                    DailyGzkjLevel0,
                    (DailyGzkjLevel0.secucode == trade_secucode_subquery.c.secucode)
                    & (DailyGzkjLevel0.date <= trade_secucode_subquery.c.date),
                )
            )
            .subquery()
        )

        # 4. 只保留最近一条记录
        gzkj_filled_subquery = (
            select(
                joined_subquery.c.current_date.label("date"),
                joined_subquery.c.secucode,
                joined_subquery.c.gzlow.label("gzlow"),
                joined_subquery.c.gzhigh.label("gzhigh"),
            )
            .where(joined_subquery.c.date == joined_subquery.c.max_date)
            .subquery()
        )

        cte_price_zone = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                AStockMarket.close.label("close"),
                gzkj_filled_subquery.c.gzlow.label("gzlow"),
                gzkj_filled_subquery.c.gzhigh.label("gzhigh"),
                case(
                    (AStockMarket.close < gzkj_filled_subquery.c.gzlow, 0),
                    (AStockMarket.close <= gzkj_filled_subquery.c.gzhigh, 1),
                    else_=2,
                ).label("price_zone"),
            )
            .select_from(AStockMarket)
            .join(
                gzkj_filled_subquery,
                and_(
                    AStockMarket.secucode == gzkj_filled_subquery.c.secucode,
                    AStockMarket.date == gzkj_filled_subquery.c.date,
                ),
                isouter=True,
            )
            .where(
                AStockMarket.date.between(trade_start, trade_end),
                AStockMarket.secucode.in_(select(subquery.c.secucode)),
            )
            .cte(f"cte_price_zone_{unique_id}")
        )
        return cte_price_zone

    @staticmethod
    def get_seat_leader_ranking():
        return [
            AStockGlobalQuery.get_institutional_amount(),  # AStockGlobalQuery.get_hot_money_name(),
            AStockGlobalQuery.get_seat_leader_inrank_date(),
            AStockGlobalQuery.get_top5_seat_total_net_buy(),
            AStockGlobalQuery.get_latest_hot_money_name(),
            AStockGlobalQuery.get_latest_reason(),
        ]

    @staticmethod
    def get_executive_trading_ranking():
        return [AStockGlobalQuery.get_zc_amt(), AStockGlobalQuery.get_jc_amt()]

    @staticmethod
    def get_rzrq_net_amt_days(days: int):
        """
        :术语名称:
            融资融券净额
        :术语解释:
            融资融券净额
        :功能:
            获取N日融资融券净额字段
        :参数:
            days: 天数，用于计算N日融资融券净额。此函数必须显式传入整数参数。例如days=5表示5日融资融券净额。
        """
        return AStockGlobalQuery.calculate_sum(field=DailyRzrqStatLevel15.rzrq_net_amt, days=days)

    @staticmethod
    def get_rzrq_intensity_days(days: int):
        """
        :术语名称:
            融资融券力度
        :术语解释:
            融资融券力度
        :功能:
            获取N日融资融券力度字段
        :参数:
            days: 天数，用于计算N日融资融券力度。此函数必须显式传入整数参数。例如days=5表示5日融资融券力度。
        """
        return AStockGlobalQuery.calculate_sum(field=DailyRzrqStatLevel15.rzrq_intensity, days=days)

    @staticmethod
    def get_rz_net_amt_days(days: int):
        """
        :术语名称:
            融资净额
        :术语解释:
            融资净额
        :功能:
            获取N日融资净额字段
        :参数:
            days: 天数，用于计算N日融资净额。此函数必须显式传入整数参数。例如days=5表示5日融资净额。
        """
        return AStockGlobalQuery.calculate_sum(field=DailyRzrqStatLevel15.rz_net_amt, days=days)

    @staticmethod
    def get_rq_net_amt_days(days: int):
        """
        :术语名称:
            融券净额
        :术语解释:
            融券净额
        :功能:
            获取N日融券净额字段
        :参数:
            days: 天数，用于计算N日融券净额。此函数必须显式传入整数参数。例如days=5表示5日融券净额。
        """
        return AStockGlobalQuery.calculate_sum(field=DailyRzrqStatLevel15.rq_net_amt, days=days)

    @staticmethod
    def get_rzrq_inflow_days():
        """
        :术语名称:
            融资融券净流入天数
        :术语解释:
            融资融券净流入天数
        :功能:
            获取融资融券净流入天数字段,当问题涉及融资融券净流入天数时使用此字段，如果问题为融资融券数据或杠杆资金相关概括性问题，使用 get_rzrq_data 方法获取更全面的数据字段
        """
        return AStockGlobalQuery.calculate_inflow_days(DailyRzrqStatLevel15.rzrq_net_amt)

    @staticmethod
    def get_rzrq_data():
        return [
            AStockGlobalQuery.calculate_inflow_days(DailyRzrqStatLevel15.rzrq_net_amt),
            DailyRzrqStatLevel15.rzrq_net_amt,
            DailyRzrqStatLevel15.rzrq_intensity,
            AStockGlobalQuery.get_rzrq_net_amt_days(days=5),
            AStockGlobalQuery.get_rzrq_intensity_days(days=5),
        ]

    @staticmethod
    def get_strategies():
        """
        :术语名称:
            智能阿尔法策略评级
        :术语解释:
            策略评级
        :功能:
            获取智能阿尔法策略评级字段，评级值为纯英文字母（如 'A', 'BB', 'AAA'），不包含数字。用户习惯用"数字+字母"（如"3A"）指代多个同样的字母评级。在提取参数时，必须将数字展开为重复的字母。
        """
        return AiStockStrategyLevel20.strategies

    @staticmethod
    def get_alpha():
        return [
            AiStockStrategyLevel20.strategies,
            AiStockStrategyLevel20.synthetic,
            AiStockStrategyLevel20.succ_rate,
            AiStockStrategyLevel20.profit_rate,
        ]

    @staticmethod
    def _get_zdt_common(
        start_date: str = "0d",
        end_date: str = "0d",
        secucode_query=None,
        zdt_type: int = 1,
    ):
        """
        通用实现函数：
        根据 zdt_type（1=涨停，2=跌停）筛选停板风向标相关数据。
        """
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        if zdt_type not in [1, 2]:
            raise ValueError("zdt_type 必须为 1（涨停）或 2（跌停）")

        unique_id = uuid.uuid4().hex[:6]
        subquery = secucode_query.subquery()
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)

        alias_name = "fc_zt_pro" if zdt_type == 1 else "fc_dt_pro"

        filtered = (
            select(
                AiForecastZdLevel20.secucode,
                AiForecastZdLevel20.date,
                AiForecastZdLevel20.zdt_type,
                AiForecastZdLevel20.fc_zdt_pro.label(alias_name),
                AiForecastZdLevel20.zd_in_time,
                AiForecastZdLevel20.zd_out_time,
            )
            .join(AStockBasic, AiForecastZdLevel20.secucode == AStockBasic.secucode)
            .where(
                AiForecastZdLevel20.zdt_type == zdt_type,
                AStockBasic.secuname.notlike("%ST%"),
                AStockBasic.is_tp == 0,
                AStockBasic.is_cwfx == 0,
                AStockBasic.is_qzts == 0,
                AStockBasic.is_tsfx == 0,
                AiForecastZdLevel20.date.between(trade_start, trade_end),
                AiForecastZdLevel20.secucode.in_(select(subquery.c.secucode)),
            )
            .cte(f"{alias_name}_{unique_id}")
        )

        filtered.class_ = AiForecastZdLevel20
        return filtered

    @staticmethod
    def get_fc_zt_pro():
        """
        :术语名称:
            停板风向标15分钟涨停概率
        :术语解释:
            表示个股在未来15分钟内涨停的预测概率。
        :功能:
            获取15分钟涨停概率字段
        """
        return "_get_zdt_common", "fc_zt_pro", {"zdt_type": 1}

    @staticmethod
    def get_fc_dt_pro():
        """
        :术语名称:
            停板风向标15分钟跌停概率
        :术语解释:
            表示个股在未来15分钟内跌停的预测概率。
        :功能:
            获取15分钟跌停概率字段
        """
        return "_get_zdt_common", "fc_dt_pro", {"zdt_type": 2}

    @staticmethod
    def get_zt_forecast():
        return [
            ("_get_zdt_common", "zdt_type", {"zdt_type": 1}),
            ("_get_zdt_common", "fc_zt_pro", {"zdt_type": 1}),
            ("_get_zdt_common", "zd_in_time", {"zdt_type": 1}),
            ("_get_zdt_common", "zd_out_time", {"zdt_type": 1}),
        ]

    @staticmethod
    def get_dt_forecast():
        return [
            ("_get_zdt_common", "zdt_type", {"zdt_type": 2}),
            ("_get_zdt_common", "fc_dt_pro", {"zdt_type": 2}),
            ("_get_zdt_common", "zd_in_time", {"zdt_type": 2}),
            ("_get_zdt_common", "zd_out_time", {"zdt_type": 2}),
        ]

    @staticmethod
    def get_zd_in_time():
        """
        :术语名称:
            停板风向标首次入池时间
        :术语解释:
            停板风向标首次入池时间
        :功能:
            获取停板风向标首次入池时间字段
        """
        return [
            ("_get_zdt_common", "zd_in_time", {"zdt_type": 1}),
            ("_get_zdt_common", "zd_in_time", {"zdt_type": 2}),
        ]

    @staticmethod
    def get_zd_out_time():
        """
        :术语名称:
            停板风向标最后出池时间
        :术语解释:
            停板风向标最后出池时间
        :功能:
            获取停板风向标最后出池时间字段
        """
        return [
            ("_get_zdt_common", "zd_out_time", {"zdt_type": 1}),
            ("_get_zdt_common", "zd_out_time", {"zdt_type": 2}),
        ]

    @staticmethod
    def get_report_publish_time():
        """
        :术语名称:
            研报发布时间
        :术语解释:
            研报发布时间
        :功能:
            获取研报发布时间字段
        """
        report_publish_date = ReportHotstocksLevel15.date.label("report_publish_time")
        report_publish_date.class_ = ReportHotstocksLevel15

        return report_publish_date

    @staticmethod
    def get_dzjm_amount():
        """
        :术语名称:
            近一季度大宗成交额
        :术语解释:
            近一季度大宗成交额
        :功能:
            获取近一季度大宗成交额字段,可以回答大宗交易成交额的问题，但是不能回答最近一次大宗交易成交额的问题
        """
        dzjm_amount = DailyDzjmStockStatLevel15.amount.label("dzjm_amount")
        dzjm_amount.class_ = DailyDzjmStockStatLevel15

        return dzjm_amount

    @staticmethod
    def get_block_trade_disclosure():
        dzjm_amount = DailyDzjmStockStatLevel15.amount.label("dzjm_amount")
        dzjm_amount.class_ = DailyDzjmStockStatLevel15
        latest_trade_date = DailyDzjmStockDetailLevel15.date.label("latest_trade_date")
        latest_trade_date.class_ = DailyDzjmStockDetailLevel15
        dzjm_volume_1d = DailyDzjmStockDetailLevel15.trade_vol.label("dzjm_volume_1d")
        dzjm_volume_1d.class_ = DailyDzjmStockDetailLevel15
        dzjm_amount_1d = DailyDzjmStockDetailLevel15.trade_amt.label("dzjm_amount_1d")
        dzjm_amount_1d.class_ = DailyDzjmStockDetailLevel15
        return [
            DailyDzjmStockStatLevel15.trade_count,
            dzjm_amount,
            DailyDzjmStockStatLevel15.trade_profit_rate,
            latest_trade_date,
            DailyDzjmStockDetailLevel15.trade_type,
            DailyDzjmStockDetailLevel15.trade_act,
            DailyDzjmStockDetailLevel15.trade_price,
            dzjm_volume_1d,
            dzjm_amount_1d,
            DailyDzjmStockDetailLevel15.zhanpan_rate,
            DailyDzjmStockDetailLevel15.trade_rate,
        ]

    @staticmethod
    def get_trade_date():
        """
        :术语名称:
            大宗交易最新日期，也就是最后交易日
        :术语解释:
            大宗交易最新日期，也就是最后交易日。
        :功能:
            获取大宗交易最新日期字段，也就是最后交易日字段，问"最后大宗交易日"必须要查这个函数！
        """
        latest_trade_date = DailyDzjmStockDetailLevel15.date.label("latest_trade_date")
        latest_trade_date.class_ = DailyDzjmStockDetailLevel15

        return latest_trade_date

    @staticmethod
    def get_latest_block_trade():
        latest_trade_date = DailyDzjmStockDetailLevel15.date.label("latest_trade_date")
        latest_trade_date.class_ = DailyDzjmStockDetailLevel15
        dzjm_volume_1d = DailyDzjmStockDetailLevel15.trade_vol.label("dzjm_volume_1d")
        dzjm_volume_1d.class_ = DailyDzjmStockDetailLevel15
        dzjm_amount_1d = DailyDzjmStockDetailLevel15.trade_amt.label("dzjm_amount_1d")
        dzjm_amount_1d.class_ = DailyDzjmStockDetailLevel15
        return [
            latest_trade_date,
            DailyDzjmStockDetailLevel15.trade_type,
            DailyDzjmStockDetailLevel15.trade_act,
            DailyDzjmStockDetailLevel15.trade_price,
            dzjm_volume_1d,
            dzjm_amount_1d,
            DailyDzjmStockDetailLevel15.zhanpan_rate,
            DailyDzjmStockDetailLevel15.trade_rate,
        ]

    @staticmethod
    def get_capital_supply_demand():
        """
        :术语名称:
            供求资金
        :术语解释:
            供求资金
        :功能:
            获取供求资金字段
        """
        return DailyGqzjLevel20.gqzj

    @staticmethod
    def get_tradable_share_avg_growth_rate():
        return "_get_tradable_share_avg_growth_rate", "tradable_share_avg_growth_rate", {}

    @staticmethod
    def _get_tradable_share_avg_growth_rate(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        """
        为人均流通股增长率生成 CTE。
        """
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")

        unique_id = uuid.uuid4().hex[:6]
        subquery = secucode_query.subquery()

        # 使用 row_number() 为每个股票的记录按日期降序排名
        ranked_shareholder_data = (
            select(
                AStockShareHolder.secucode,
                AStockShareHolder.tradable_share_avg,
                func.row_number()
                .over(
                    partition_by=AStockShareHolder.secucode,
                    order_by=AStockShareHolder.date.desc(),
                )
                .label("rn"),
            )
            .where(AStockShareHolder.secucode.in_(select(subquery.c.secucode)))
            .subquery(f"ranked_tradable_share_{unique_id}")
        )

        # 创建两个别名，分别代表最新记录和次新记录
        latest_data = aliased(ranked_shareholder_data, name=f"latest_tradable_{unique_id}")
        previous_data = aliased(ranked_shareholder_data, name=f"previous_tradable_{unique_id}")

        # 连接最新和次新记录，计算增长率并创建最终 CTE
        growth_rate_cte = (
            select(
                latest_data.c.secucode,
                (
                    (latest_data.c.tradable_share_avg - previous_data.c.tradable_share_avg)
                    / func.nullif(previous_data.c.tradable_share_avg, 0)
                ).label("tradable_share_avg_growth_rate"),
            )
            .select_from(
                latest_data.join(
                    previous_data,
                    and_(
                        latest_data.c.secucode == previous_data.c.secucode,
                        latest_data.c.rn == 1,
                        previous_data.c.rn == 2,
                    ),
                )
            )
            .cte(f"tradable_share_avg_growth_rate_cte_{unique_id}")
        )

        return growth_rate_cte

    @staticmethod
    def get_total_people_num_growth_rate():
        return "_get_total_people_num_growth_rate", "total_people_num_growth_rate", {}

    @staticmethod
    def _get_total_people_num_growth_rate(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        """
        为股东总人数增长率生成 CTE。
        """
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")

        unique_id = uuid.uuid4().hex[:6]
        subquery = secucode_query.subquery()

        # 使用 row_number() 为每个股票的记录按日期降序排名
        ranked_shareholder_data = (
            select(
                AStockShareHolder.secucode,
                AStockShareHolder.total_people_num,
                func.row_number()
                .over(
                    partition_by=AStockShareHolder.secucode,
                    order_by=AStockShareHolder.date.desc(),
                )
                .label("rn"),
            )
            .where(AStockShareHolder.secucode.in_(select(subquery.c.secucode)))
            .subquery(f"ranked_total_people_{unique_id}")
        )

        # 创建两个别名，分别代表最新记录和次新记录
        latest_data = aliased(ranked_shareholder_data, name=f"latest_total_people_{unique_id}")
        previous_data = aliased(ranked_shareholder_data, name=f"previous_total_people_{unique_id}")

        # 连接最新和次新记录，计算增长率并创建最终 CTE
        growth_rate_cte = (
            select(
                latest_data.c.secucode,
                (
                    (latest_data.c.total_people_num - previous_data.c.total_people_num)
                    / func.nullif(previous_data.c.total_people_num, 0)
                ).label("total_people_num_growth_rate"),
            )
            .select_from(
                latest_data.join(
                    previous_data,
                    and_(
                        latest_data.c.secucode == previous_data.c.secucode,
                        latest_data.c.rn == 1,
                        previous_data.c.rn == 2,
                    ),
                )
            )
            .cte(f"total_people_num_growth_rate_cte_{unique_id}")
        )

        return growth_rate_cte

    @staticmethod
    def get_ma_days(days: int):
        """
        :术语名称:
            N日MA均线
        :术语解释:
            N日MA均线
        :功能:
            获取N日MA均线字段
        :参数:
            n: 天数，用于计算移动平均。此函数必须显式传入整数参数。例如n=5表示5日均线。
        """
        return "_get_ma_days", f"ma_{days}", {"n": days}

    @staticmethod
    def get_zdf_days(days: int):
        """
        :术语名称:
            涨跌幅
        :术语解释:
            涨跌幅
        :功能:
            获取N日涨跌幅字段
        :参数:
            days: 天数，用于计算涨跌幅区间。此函数必须显式传入整数参数。例如days=5表示5日涨跌幅。
        """
        return "_get_zdf_days", f"zdf_{days}", {"days": days}

    @staticmethod
    def _get_zdf_days(start_date: str = "0d", end_date: str = "0d", secucode_query=None, days=1):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")

        trade_end = get_date(end_date)
        trade_start_original = get_date(start_date, is_start=True)
        key_label = f"zdf_{days}"

        # 性能优化：days=1 时直接使用 AStockMarketCur.zdf 字段（避免窗口函数和CTE）
        if days == 1:
            subquery = secucode_query.subquery()
            return (
                select(AStockMarketCur.secucode, AStockMarketCur.date, AStockMarketCur.zdf.label(key_label))
                .where(
                    AStockMarketCur.secucode.in_(select(subquery.c.secucode)),
                    AStockMarketCur.date.between(trade_start_original, trade_end),
                )
                .subquery()
            )

        # days > 1 时使用窗口函数计算
        subquery = secucode_query.subquery()

        prev_close = func.lagInFrame(AStockMarket.close, days, 0).over(
            partition_by=AStockMarket.secucode, order_by=AStockMarket.date
        )

        zdf_subquery = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                case((prev_close != 0, (AStockMarket.close - prev_close) / prev_close), else_=None).label(key_label),
            )
            .where(AStockMarket.secucode.in_(select(subquery.c.secucode)))
            .subquery()
        )

        zdf_cte = (
            select(zdf_subquery.c.secucode, zdf_subquery.c.date, zdf_subquery.c[key_label])
            .where(zdf_subquery.c.date.between(trade_start_original, trade_end))
            .cte(f"zdf_{days}_cte_{uuid.uuid4().hex[:6]}")
        )

        return zdf_cte

    @staticmethod
    def get_zt_days(days: int):
        """
        :术语名称:
            N日内涨停次数
        :术语解释:
            N日内涨停次数
        :功能:
            获取N日内涨停次数字段
        :参数:
            days: 天数，用于计算N日内的涨停次数。此函数必须显式传入整数参数。例如days=5表示5日内涨停次数。
        """
        return "_get_zt_days", f"zt_{days}d_count", {"days": days}

    @staticmethod
    def _get_zt_days(start_date: str = "0d", end_date: str = "0d", secucode_query=None, days=5):
        """
        计算N日内涨停次数

        :参数:
            start_date: 起始日期
            end_date: 结束日期
            secucode_query: 股票代码查询
            days: 计算涨停次数的天数范围
        """
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")

        if days <= 0:
            raise ValueError(f"days must be greater than 0, current value: {days}")

        trade_end = get_date(end_date)
        # 需要days-1天的历史数据来计算滑动窗口
        trade_start = get_date(start_date, is_start=True, lookback_days=days - 1)

        subquery = secucode_query.subquery()

        key_label = f"zt_{days}d_count"

        # 计算N日内涨停次数：使用窗口函数 sum(zt) OVER (ROWS BETWEEN days-1 PRECEDING AND CURRENT ROW)
        zt_count = func.sum(case((AStockMarket.zt == 1, 1), else_=0)).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-(days - 1), 0),  # ROWS BETWEEN days-1 PRECEDING AND CURRENT ROW
        )

        # 使用窗口函数计算每个股票的涨停次数
        zt_with_rn = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                cast(zt_count, Integer).label(key_label),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.secucode.in_(select(subquery.c.secucode)))
            .where(AStockMarket.date.between(trade_start, trade_end))
            .subquery()
        )

        # 只选择每个股票的最新一天（rn=1）
        zt_cte = (
            select(zt_with_rn.c.secucode, zt_with_rn.c.date, zt_with_rn.c[key_label])
            .where(zt_with_rn.c.rn == 1)
            .cte(f"zt_{days}d_count_cte_{uuid.uuid4().hex[:6]}")
        )

        return zt_cte

    @staticmethod
    def _get_ma_days(start_date: str = "0d", end_date: str = "0d", secucode_query=None, n=5):
        """
        计算N日移动平均线的实际实现方法

        :参数:
            start_date: 起始日期
            end_date: 结束日期
            secucode_query: 股票代码查询
            n: 移动平均的天数
        """
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")

        if n <= 0:
            raise ValueError(f"n must be greater than 0, current value: {n}")

        trade_end = get_date(end_date)
        # 需要n-1天的历史数据来计算移动平均
        trade_start = get_date(start_date, is_start=True, lookback_days=n - 1)

        subquery = secucode_query.subquery()

        key_label = f"ma_{n}"

        # 计算N日移动平均：使用窗口函数
        # ClickHouse中: avg(close) OVER (PARTITION BY secucode ORDER BY date ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)
        # 注意：rows参数使用负数表示PRECEDING，例如 (-(n-1), 0) 表示 ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW
        ma_value = func.avg(AStockMarket.close).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-(n - 1), 0),  # ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW
        )

        # 使用窗口函数计算每个股票的最新记录
        ma_with_rn = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                cast(ma_value, Float).label(key_label),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.secucode.in_(select(subquery.c.secucode)))
            .where(AStockMarket.date.between(trade_start, trade_end))
            .subquery()
        )

        # 只选择每个股票的最新一天（rn=1）
        ma_cte = (
            select(ma_with_rn.c.secucode, ma_with_rn.c.date, ma_with_rn.c[key_label])
            .where(ma_with_rn.c.rn == 1)
            .cte(f"ma_{n}_cte_{uuid.uuid4().hex[:6]}")
        )

        return ma_cte

    @staticmethod
    def get_yoy(field):
        return ("_calculate_growth_metric", f"{field.key}_yoy", {"field": field, "mode": "yoy"})

    @staticmethod
    def get_qoq(field):
        return ("_calculate_growth_metric", f"{field.key}_qoq", {"field": field, "mode": "qoq"})

    @staticmethod
    def get_industry_average(field):
        if isinstance(field, tuple) and len(field) == 3:
            method_name, field_name, params = field
            industry_avg_field_name = f"{field_name}_industry_avg"
            return ("_calculate_industry_average", industry_avg_field_name, {"field": field})

        return ("_calculate_industry_average", f"{field.key}_industry_avg", {"field": field})

    @staticmethod
    def _get_filter_condition(filter_type: str):
        """
        统一的筛选条件方法，返回不同filter_type对应的筛选条件

        :参数:
            filter_type: 筛选类型，支持 'stock', 'board', 'warzone'
        :返回:
            筛选条件或None
        """
        if filter_type == "stock":
            return (AStockBasic.type == "A股") & (AStockBasic.is_ts == 0)
        elif filter_type == "board":
            return (AStockBasic.type == "板块") & (AStockBasic.secucode.like("BR01B01%"))
        elif filter_type == "warzone":
            return (AStockBasic.type == "板块") & (
                AStockBasic.secucode.in_(
                    [
                        "BR01B00014",
                        "BR01B00036",
                        "BR01B00037",
                        "BR01B00035",
                        "BR01B00007",
                        "BR01B00045",
                        "BR01B00034",
                    ]
                )
            )
        return None

    @staticmethod
    def _resolve_column_from_cte(col, cte_tables):
        """
        从CTE字典中解析列引用
        :param col: 列对象（可以是字段、tuple或其他类型）
        :param cte_tables: CTE字典
        :return: 解析后的列引用
        """
        if isinstance(col, tuple):
            # tuple类型：(func_name, field_name, params)
            func_name, field_name, _ = col
            return cte_tables[f"{func_name}_{field_name}"].c[field_name]
        elif hasattr(col, "class_"):
            # 表字段类型
            return cte_tables[col.class_].c[col.key]
        else:
            # 标量值，直接返回
            return col

    @staticmethod
    def _handle_multi_primary_key_columns(columns: list, start_date: str, end_date: str):
        multi_primary_key_tables = []

        # 添加额外主键
        for item in columns:
            if hasattr(item, "class_") and item.class_ in extra_primary_key_views.keys():
                if item.class_ not in multi_primary_key_tables:
                    multi_primary_key_tables.append(item.class_)
                for col in extra_primary_key_views.get(item.class_):
                    if col not in columns:
                        columns.append(col)

        # 添加日期
        if start_date == "0d" and end_date == "0d":
            columns.insert(0, AStockMarketCur.secucode)
            if multi_primary_key_tables:
                columns.insert(0, AStockMarketCur.date)
        elif "q" in start_date and "q" in end_date:
            columns.insert(0, AStockFinancial.secucode)
            if multi_primary_key_tables:
                columns.insert(0, AStockFinancial.date)
        else:
            columns.insert(0, AStockMarket.secucode)
            if multi_primary_key_tables:
                columns.insert(0, AStockMarket.date)

        return columns

    @staticmethod
    def _build_query_base(
        columns: list, start_date: str, end_date: str, secucode_query=None, filter_type: str = "stock"
    ):
        """
        私有辅助方法，用于构建查询的基础部分（CTE和JOINs）。
        :return: 一个元组，包含 (基础查询对象, CTEs字典)。
        """
        unique_id = uuid.uuid4().hex[:6]

        all_columns = AStockGlobalQuery._flatten_columns(columns)

        if AStockBasic.type not in columns:
            all_columns.append(AStockBasic.type)

        ordered_columns, involved_tables, tuple_columns = AStockGlobalQuery._categorize_columns(all_columns)

        # 预处理AStockMarketCur相关逻辑
        required_market_cur_fields = {
            item.key
            for item in ordered_columns
            if hasattr(item, "class_") and item.class_ == AStockMarketCur and item.key not in ["secucode", "date"]
        }

        # 智能添加必要的表
        if AStockMarketCur in involved_tables and not required_market_cur_fields:
            involved_tables.remove(AStockMarketCur)

        # 确保始终包含AStockBasic
        if AStockBasic not in involved_tables:
            involved_tables.append(AStockBasic)

        # 预处理AStockMarketCur相关逻辑
        required_market_cur_fields = {
            item.key
            for item in ordered_columns
            if hasattr(item, "class_") and item.class_ == AStockMarketCur and item.key not in ["secucode", "date"]
        }

        # 智能添加必要的表
        if AStockMarketCur in involved_tables and not required_market_cur_fields:
            involved_tables.remove(AStockMarketCur)

        # 初始化查询条件列表
        conditions = []

        if secucode_query is None:
            subquery = select(AStockBasic.secucode).subquery()
        else:
            # 检查是否为tuple格式的方法调用
            if isinstance(secucode_query, tuple):
                if len(secucode_query) == 3:
                    # tuple格式: (function_name, filter_name, params)
                    function_name, filter_name, params = secucode_query
                    if hasattr(AStockGlobalQuery, function_name):
                        # 调用对应的方法
                        function = getattr(AStockGlobalQuery, function_name)
                        result = function(**params)
                        # 检查方法返回的是否是(query, conditions)元组
                        if isinstance(result, tuple) and len(result) == 2:
                            query_obj, additional_conditions = result
                            subquery = query_obj.subquery()
                            # 将额外的条件添加到查询条件中
                            if additional_conditions:
                                conditions.extend(additional_conditions)
                        else:
                            subquery = result.subquery()
                    else:
                        raise ValueError(f"Unknown function: {function_name}")
                elif len(secucode_query) == 2:
                    # 直接的(query, conditions)元组格式
                    query_obj, additional_conditions = secucode_query
                    subquery = query_obj.subquery()
                    # 将额外的条件添加到查询条件中
                    if additional_conditions:
                        conditions.extend(additional_conditions)
                else:
                    raise ValueError(
                        f"Invalid secucode_query tuple format: expected length 2 or 3, got {len(secucode_query)}"
                    )
            else:
                subquery = secucode_query.subquery()

        # 动态确定 base_table
        base_table = None
        for table in involved_tables:
            if table in multidate_views:
                if base_table is None:
                    base_table = table
                    break

        if base_table is None:
            for table in involved_tables:
                if table in sparsedate_views:
                    if base_table is None:
                        base_table = table
                        break

        if base_table is None:
            for table in involved_tables:
                if table in single_date_views:
                    if base_table is None:
                        base_table = table
                        break

        if base_table is None and tuple_columns:
            base_table = AStockMarket
            if base_table not in involved_tables:
                involved_tables.append(base_table)

        if base_table is None:
            raise ValueError("No valid base table found from columns")

        # 如果主表是稀疏表，则日期来源为该主表
        is_special_views = base_table in special_views
        if is_special_views:
            modified_ordered_columns = []
            date_column_replaced = False
            for item in ordered_columns:
                # 检查当前列是否是来自 single_date_views 的 'date' 列
                if hasattr(item, "class_") and item.class_ in single_date_views and item.key == "date":
                    if not date_column_replaced:
                        # 替换为 base_table 的 date 列
                        base_date_col = getattr(base_table, "date")
                        modified_ordered_columns.append(base_date_col)
                        date_column_replaced = True
                    # 如果已经替换过一次，则忽略后续来自 single_date_views 的 date 列，避免重复
                else:
                    modified_ordered_columns.append(item)
            # 使用修改后的列列表进行后续操作
            ordered_columns = modified_ordered_columns

        # 显示实际报告日期
        if tuple_columns:
            date_col_index = -1
            for i, col in enumerate(ordered_columns):
                if hasattr(col, "key") and col.key == "date":
                    date_col_index = i
                    break

            if date_col_index != -1:
                first_tuple_func, first_tuple_field, _ = tuple_columns[0]
                ordered_columns[date_col_index] = ("_use_cte_date_", first_tuple_func, first_tuple_field)

        # 构建 CTEs
        # 默认日期范围（非盘后表使用）
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)

        cte_tables = {}
        for table in involved_tables:
            table_columns = [col for col in ordered_columns if hasattr(col, "class_") and col.class_ == table]
            if table == base_table and hasattr(table, "date"):
                required_cols = [table.secucode, table.date]
            elif table in multidate_views or table in sparsedate_views:
                required_cols = [table.secucode, table.date]
            else:
                # 有问题，所有的表都会有date，但是之前设计的比如hq_current_view和hq_basic_view不该有date
                required_cols = [table.secucode, table.date] if hasattr(table, "date") else [table.secucode]

            final_cte_cols = []
            seen_col_keys_list = []

            # 1. 首先添加必须的列 (secucode, date)，并记录它们的 key
            for col in required_cols:
                if col.key not in seen_col_keys_list:
                    final_cte_cols.append(col)
                    seen_col_keys_list.append(col.key)

            # 2. 接着按原始顺序添加其他需要的列
            for col in table_columns:
                if col.key not in seen_col_keys_list:
                    final_cte_cols.append(col)
                    seen_col_keys_list.append(col.key)

            labeled_cols = [col.label(col.key) for col in final_cte_cols]

            if table in sparsedate_views:
                # --- 对稀疏表做 forward fill ---
                # 1. 生成交易日 CTE
                if start_date == "0d" and end_date == "0d":
                    # 对于0d0d的情况，直接获取每个股票的最新数据，不需要生成日期CTE
                    # 使用窗口函数获取每个股票的最新记录
                    latest_data_cte = (
                        select(
                            table.secucode,
                            table.date,
                            *[col.label(col.key) for col in table_columns if col.key not in ["secucode", "date"]],
                            func.row_number().over(partition_by=table.secucode, order_by=desc(table.date)).label("rn"),
                        )
                        .where(table.secucode.in_(select(subquery.c.secucode)))
                        .cte(f"latest_data_{table.__name__}_{unique_id}")
                    )

                    # 只取每个股票的最新一条记录
                    cte = (
                        select(
                            table.secucode,
                            table.date.label("date"),
                            *[
                                latest_data_cte.c[col.key]
                                for col in table_columns
                                if col.key not in ["secucode", "date"]
                            ],
                        )
                        .select_from(
                            latest_data_cte.join(
                                table,
                                and_(
                                    table.secucode == latest_data_cte.c.secucode, table.date == latest_data_cte.c.date
                                ),
                            )
                        )
                        .where(latest_data_cte.c.rn == 1)
                        .cte(f"cte_{table.__name__}_{unique_id}")
                    )
                elif "q" in start_date and "q" in end_date:
                    dates_cte = (
                        select(table.date.label("date"))
                        .where(
                            table.secucode.in_(select(subquery.c.secucode)), table.date.between(trade_start, trade_end)
                        )
                        .distinct()
                        .cte(f"dates_{table.__name__}_{unique_id}")
                    )
                    # 2. CROSS JOIN secucode 子查询，得到所有 date × secucode
                    trade_secucode_cte = select(dates_cte.c.date, subquery.c.secucode).cte(
                        f"dates_{table.__name__}_secucode_{unique_id}"
                    )

                    # 3. LEFT JOIN 原表 + max 保留最近一条记录
                    joined = (
                        select(
                            *[
                                trade_secucode_cte.c.date.label("current_date"),
                                trade_secucode_cte.c.secucode.label("secucode"),
                                *[col.label(col.key) for col in table_columns if col.key not in ["secucode", "date"]],
                                table.date,  # 直接选择 table.date 用于比较
                                func.max(table.date)
                                .over(partition_by=[trade_secucode_cte.c.date, trade_secucode_cte.c.secucode])
                                .label("max_date"),
                            ]
                        )
                        .select_from(
                            trade_secucode_cte.join(
                                table,
                                (table.secucode == trade_secucode_cte.c.secucode)
                                & (table.date <= trade_secucode_cte.c.date),
                            )
                        )
                        .cte(f"cte_{table.__name__}_ranked_{unique_id}")
                    )

                    # 4. 只保留最近一条记录
                    cte = (
                        select(
                            joined.c.current_date.label("date"),
                            joined.c.secucode,
                            *[joined.c[col.key] for col in table_columns if col.key not in ["secucode", "date"]],
                        )
                        .where(joined.c.date == joined.c.max_date)
                        .cte(f"cte_{table.__name__}_{unique_id}")
                    )
                else:
                    dates_cte = (
                        select(AStockMarket.date.label("date"))
                        .where(AStockMarket.date.between(trade_start, trade_end))
                        .distinct()
                        .cte(f"dates_{table.__name__}_{unique_id}")
                    )
                    # 2. CROSS JOIN secucode 子查询，得到所有 date × secucode
                    trade_secucode_cte = select(dates_cte.c.date, subquery.c.secucode).cte(
                        f"dates_{table.__name__}_secucode_{unique_id}"
                    )

                    # 3. LEFT JOIN 原表 + max 保留最近一条记录
                    joined = (
                        select(
                            *[
                                trade_secucode_cte.c.date.label("current_date"),
                                trade_secucode_cte.c.secucode.label("secucode"),
                                *[col.label(col.key) for col in table_columns if col.key not in ["secucode", "date"]],
                                table.date,  # 直接选择 table.date 用于比较
                                func.max(table.date)
                                .over(partition_by=[trade_secucode_cte.c.date, trade_secucode_cte.c.secucode])
                                .label("max_date"),
                            ]
                        )
                        .select_from(
                            trade_secucode_cte.join(
                                table,
                                (table.secucode == trade_secucode_cte.c.secucode)
                                & (table.date <= trade_secucode_cte.c.date),
                            )
                        )
                        .cte(f"cte_{table.__name__}_ranked_{unique_id}")
                    )

                    # 4. 只保留最近一条记录
                    cte = (
                        select(
                            joined.c.current_date.label("date"),
                            joined.c.secucode,
                            *[joined.c[col.key] for col in table_columns if col.key not in ["secucode", "date"]],
                        )
                        .where(joined.c.date == joined.c.max_date)
                        .cte(f"cte_{table.__name__}_{unique_id}")
                    )
            elif table in multidate_views:
                # 盘后表特殊处理：当查询0d0d时，获取最新一条数据且日期为盘后表中的日期
                if table in AFTER_MARKET_TABLES and start_date == "0d" and end_date == "0d":
                    # 对于盘后表的0d0d查询，获取每个股票的最新数据
                    # 获取该表的最新数据日期，对于盘后表应该查询表中的实际最新日期
                    trade_date = (
                        select(table.date)
                        .where(table.secucode.is_not(None))
                        .order_by(table.date.desc())
                        .limit(1)
                        .scalar_subquery()
                    )

                    latest_data_cte = (
                        select(
                            table.secucode,
                            table.date,
                            *[col.label(col.key) for col in table_columns if col.key not in ["secucode", "date"]],
                            func.row_number().over(partition_by=table.secucode, order_by=desc(table.date)).label("rn"),
                        )
                        .where(table.date == trade_date, table.secucode.in_(select(subquery.c.secucode)))
                        .cte(f"latest_data_{table.__name__}_{unique_id}")
                    )
                    # 只取每个股票的最新一条记录
                    cte = select(
                        latest_data_cte.c.secucode,
                        latest_data_cte.c.date,
                        *[latest_data_cte.c[col.key] for col in table_columns if col.key not in ["secucode", "date"]],
                    ).cte(f"cte_{table.__name__}_{unique_id}")
                else:
                    cte = (
                        select(*labeled_cols)
                        .where(
                            table.date.between(trade_start, trade_end), table.secucode.in_(select(subquery.c.secucode))
                        )
                        .cte(f"cte_{table.__name__}_{unique_id}")
                    )
            else:
                cte = (
                    select(*labeled_cols)
                    .where(table.secucode.in_(select(subquery.c.secucode)))
                    .cte(f"cte_{table.__name__}_{unique_id}")
                )
            cte_tables[table] = cte

        tuple_cte_generators = {}
        for func_name, field_name, extra_params in tuple_columns:
            params_key = (func_name, json.dumps(extra_params, sort_keys=True, default=str))
            if params_key not in tuple_cte_generators:
                func_obj = getattr(AStockGlobalQuery, func_name)
                # 调用函数并传递参数
                secucode_query_with_fallback = (
                    secucode_query if secucode_query is not None else select(AStockBasic.secucode)
                )
                cte = func_obj(
                    start_date=start_date,
                    end_date=end_date,
                    secucode_query=secucode_query_with_fallback,
                    **extra_params,
                )
                tuple_cte_generators[params_key] = cte

        # 2. 填充 cte_tables，将 (func, field) 映射到共享的 CTE
        #    这是为了让后续的 SELECT 和 ORDER BY 逻辑保持不变
        for func_name, field_name, extra_params in tuple_columns:
            params_key = (func_name, json.dumps(extra_params, sort_keys=True, default=str))
            # cte_tables 的键 保持 f"{func_name}_{field_name}"
            # 但它指向的是 tuple_cte_generators 中共享的 CTE 对象
            cte_tables[f"{func_name}_{field_name}"] = tuple_cte_generators[params_key]

        # --- 特殊处理：如果查询请求了 date 字段但没有日期相关的表 ---
        has_date_request = any(
            (hasattr(item, "key") and item.key == "date") or (isinstance(item, tuple) and item[1] == "date")
            for item in ordered_columns
        )

        if has_date_request and start_date == "0d" and end_date == "0d":
            # 检查是否有日期相关的表
            has_date_table = any(
                table in multidate_views or table in sparsedate_views or table == AStockMarketCur
                for table in involved_tables
            )

            if not has_date_table:
                # 标记需要特殊处理 AStockMarketCur（使用 LEFT JOIN）
                if not hasattr(AStockMarketCur, "_use_left_join"):
                    AStockMarketCur._use_left_join = True

                # 添加 AStockMarketCur 表以获取日期
                if AStockMarketCur not in involved_tables:
                    involved_tables.append(AStockMarketCur)

                # 构建 AStockMarketCur 的 CTE
                market_cur_cte = (
                    select(
                        AStockMarketCur.secucode,
                        AStockMarketCur.date,
                    )
                    .where(AStockMarketCur.secucode.in_(select(subquery.c.secucode)))
                    .cte(f"cte_AStockMarketCur_{unique_id}")
                )
                cte_tables[AStockMarketCur] = market_cur_cte

        # 从 base_table 的 CTE 构建查询
        base_cte = cte_tables[base_table]

        # 使用工具类进行列映射，减少重复代码
        class TraditionalColumnMapper:
            """传统查询的列映射器"""

            def __init__(self, ordered_columns, cte_tables, base_cte, involved_tables):
                self.ordered_columns = ordered_columns
                self.cte_tables = cte_tables
                self.base_cte = base_cte
                self.involved_tables = involved_tables
                self.mapped_columns = []

            def map_columns(self):
                """映射所有列"""
                for item in self.ordered_columns:
                    if isinstance(item, tuple):
                        self._map_tuple_column(item)
                    elif hasattr(item, "class_"):
                        self._map_table_column(item)
                    else:
                        self._map_generic_column(item)
                return self.mapped_columns

            def _map_tuple_column(self, item):
                """映射元组列"""
                if item[0] == "_use_cte_date_":
                    _, func_name, field_name = item
                    cte = self.cte_tables[f"{func_name}_{field_name}"]
                    # 优先使用CTE的date，回退到base_cte
                    if "date" in cte.c:
                        self.mapped_columns.append(cte.c.date.label("date"))
                    elif hasattr(self.base_cte.c, "date"):
                        self.mapped_columns.append(self.base_cte.c.date.label("date"))
                else:
                    func_name, field_name, _ = item
                    cte = self.cte_tables[f"{func_name}_{field_name}"]
                    if field_name:
                        self.mapped_columns.append(cte.c[field_name].label(field_name))

            def _map_table_column(self, item):
                """映射表列"""
                if item.class_ not in self.involved_tables:
                    # 处理不在involved_tables中的字段
                    if item.class_ == AStockMarketCur:
                        if item.key == "secucode" and AStockBasic in self.involved_tables:
                            self.mapped_columns.append(self.cte_tables[AStockBasic].c.secucode.label(item.key))
                        elif item.key == "date" and hasattr(self.base_cte.c, "date"):
                            self.mapped_columns.append(self.base_cte.c.date.label(item.key))
                    # 其他情况跳过
                else:
                    # 标准映射
                    if (
                        item.class_ == AStockMarketCur
                        and item.key == "secucode"
                        and AStockBasic in self.involved_tables
                    ):
                        self.mapped_columns.append(self.cte_tables[AStockBasic].c.secucode.label(item.key))
                    else:
                        self.mapped_columns.append(self.cte_tables[item.class_].c[item.key].label(item.key))

            def _map_generic_column(self, item):
                """映射通用列"""
                self.mapped_columns.append(item.label(item.key))

        # 使用TraditionalColumnMapper进行列映射
        mapper = TraditionalColumnMapper(ordered_columns, cte_tables, base_cte, involved_tables)
        mapped_columns = mapper.map_columns()

        # 添加 distinct() 确保 secucode 去重
        if secucode_query is not None:
            query = select(*mapped_columns).select_from(base_cte)
        else:
            has_multi_primary_key = any(table in extra_primary_key_views for table in involved_tables)
            if has_multi_primary_key:
                query = select(*mapped_columns).select_from(base_cte)
            else:
                query = select(base_cte.c.secucode).distinct().select_from(base_cte)

        filter_condition = AStockGlobalQuery._get_filter_condition(filter_type)
        if filter_condition is not None:
            query = query.join(AStockBasic, base_cte.c.secucode == AStockBasic.secucode).where(filter_condition)

        # 3. 动态 join 其他表
        for table in involved_tables:
            if table == base_table:
                continue
            join_cte = cte_tables[table]
            join_condition = base_cte.c.secucode == join_cte.c.secucode
            # 只有当两个表都是同类型的多日期表时才进行日期JOIN
            # 避免对不同含义的日期字段进行错误JOIN（如交易日期vs发布日期）
            if base_table in multidate_views and table in multidate_views:
                join_condition = and_(join_condition, base_cte.c.date == join_cte.c.date)
            # 检查是否需要使用 LEFT JOIN
            if hasattr(table, "_use_left_join") and table._use_left_join:
                query = query.outerjoin(join_cte, join_condition)
                # 清理标记
                delattr(table, "_use_left_join")
            else:
                if secucode_query is not None:
                    query = query.outerjoin(join_cte, join_condition)
                else:
                    query = query.join(join_cte, join_condition)

        # 3.5 动态join tuple cte
        joined_tuple_ctes = set()

        for func_name, field_name, extra_params in tuple_columns:
            cte = cte_tables[f"{func_name}_{field_name}"]
            # 检查我们是否已经 JOIN 过这个 *对象*
            if cte in joined_tuple_ctes:
                continue  # 如果 join 过了，就跳过
            join_condition = base_cte.c.secucode == cte.c.secucode
            # 只有当base_table和tuple CTE的表都是同类型的多日期表时，才进行日期JOIN
            # 避免对不同语义的日期字段进行错误JOIN（如交易日期vs发布日期）
            tuple_table = getattr(cte, "class_", None)
            if "date" in cte.c and (base_table in multidate_views) and tuple_table and (tuple_table in multidate_views):
                join_condition = and_(join_condition, base_cte.c.date == cte.c.date)
            query = query.join(cte, join_condition)
            # 将这个 CTE 对象标记为已 JOIN
            joined_tuple_ctes.add(cte)

        if secucode_query is not None:
            # 多主键联表逻辑
            mapped_dict = {col.key: col for col in mapped_columns}
            common_keys = set(subquery.c.keys()) & {col.key for col in mapped_columns}

            on_clause = None
            if common_keys:
                on_clause = and_(
                    *[or_(subquery.c[key].is_(None), mapped_dict[key] == subquery.c[key]) for key in common_keys]
                )

            query = query.join(subquery, on_clause)

            # 传统排序逻辑
            date_cols = [
                col
                for col in ordered_columns
                if (hasattr(col, "key") and col.key == "date")
                or (isinstance(col, tuple) and col[0] == "_use_cte_date_")
            ]
            if date_cols:
                # 如果日期列已经被重定向到某个CTE，则按那个CTE的日期排序
                if isinstance(date_cols[0], tuple) and date_cols[0][0] == "_use_cte_date_":
                    _, func_name, field_name = date_cols[0]
                    cte_for_date = cte_tables[f"{func_name}_{field_name}"]
                    # 检查用于排序的CTE是否有date列
                    if "date" in cte_for_date.c:
                        query = query.order_by(desc(cte_for_date.c.date))
                    # 如果没有，则回退到按基准表排序
                    elif base_table in multidate_views or base_table in sparsedate_views:
                        query = query.order_by(desc(base_cte.c.date))

                # 否则，按基准表的日期排序（如果它是日期相关的表）
                elif base_table in multidate_views or base_table in sparsedate_views:
                    query = query.order_by(desc(base_cte.c.date))

            # ✅ 自动扩展排序逻辑（对第4个及之后字段自动ORDER BY DESC）
            if len(ordered_columns) > 3:
                for item in ordered_columns[3:]:
                    if isinstance(item, tuple):
                        func_name, field_name, _ = item
                        cte_key = f"{func_name}_{field_name}"
                        cte = cte_tables.get(cte_key)
                        if cte is not None and field_name in cte.c:
                            query = query.order_by(cte.c[field_name].desc())
                    elif hasattr(item, "class_"):
                        cte = cte_tables.get(item.class_)
                        if cte is not None and item.key in cte.c:
                            query = query.order_by(cte.c[item.key].desc())
                    else:
                        # 处理表达式排序
                        query = query.order_by(item.desc())

            # 对于盘后表的0d0d查询，确保最终结果只包含最新日期的数据
            if start_date == "0d" and end_date == "0d" and involved_tables:
                # 检查是否涉及盘后表
                has_aftermarket_table = any(table in AFTER_MARKET_TABLES for table in involved_tables)
                if has_aftermarket_table:
                    # 找到第一个盘后表作为基准
                    for table in involved_tables:
                        if table in AFTER_MARKET_TABLES and table in cte_tables:
                            # 获取该表的最新数据日期
                            latest_date_subquery = (
                                select(table.date).order_by(table.date.desc()).limit(1).scalar_subquery()
                            )
                            # 限制查询结果只包含最新日期的数据
                            query = query.where(cte_tables[table].c.date == latest_date_subquery)
                            break

            query = query.order_by(base_cte.c.secucode.desc())

        query = query.distinct()

        return query, cte_tables

    @staticmethod
    def query_columns(
        columns,
        start_date: str = "0d",
        end_date: str = "0d",
        secucode_query=None,
        filter_type: str = "stock",
    ):
        if secucode_query is None:
            raise ValueError("secucode_query must be provided")
        # 设计说明：
        # 可返回list，tuple，对象
        # list可套娃，成员也可以是list，tuple，对象
        # 使用自定义字段一定要label否则会报错没有key
        # tuple存在的意义是为了解决计算指标需要用到日期和股票代码的情况，如近三个交易日上榜次数
        # tuple的返回值规定为三部分，
        # 第一部分是字符串，代表可执行函数名，可执行函数的返回值是一个cte,
        # 第二部分给出要筛选的字段名，用于在最终筛选阶段呈现出来，
        # 第三部分是需要透传的参数，没有也要填{}
        # 在tuple的get函数cte实现时，如果筛选了日期记得一定.label("date"),防止联表判断条件出错

        query, _ = AStockGlobalQuery._build_query_base(columns, start_date, end_date, secucode_query, filter_type)

        return query

    # 通过股票代码筛选的函数，固定放入prompt
    @staticmethod
    def query_stock_by_secucodes(secucodes: list[str], start_date: str = "0d", end_date: str = "0d"):
        """
        :术语名称:
            指定代码查询
        :术语解释:
            根据用户提供的股票代码或板块代码进行精确查询
        :功能:
            在指定的股票或板块集合上进行查询
            当用户问题中出现以SZHQ、SHHQ等类似开头的股票/板块代码时调用
            BR开头的为板块代码
        :参数:
            secucodes: 股票代码列表
        """
        return select(AStockMarketCur.secucode).where(AStockMarketCur.secucode.in_(secucodes))

    @staticmethod
    def query_field_in_range(field, interval: str = "(-inf,inf)", start_date: str = "0d", end_date: str = "0d"):
        """
        :术语名称:
            字段区间筛选
        :术语解释:
            筛选出指定数值字段在某个区间内的股票
        :功能:
            筛选出指定字段在数值区间内的股票，本函数核心在于精确解析区间字符串的数学边界，确保开区间、闭区间的语义被无歧义地处理。
            区间格式可灵活处理边界（如严格大于/小于，或包含等于）。 支持开区间、闭区间或半开区间。
            支持开区间、闭区间或半开区间格式如"[low,high]","(low,high)"等
            low/high可为数值、查询指标函数或"inf"/"-inf"
        :参数:
            field: 查询字段，只可传入查询指标函数，不要遗漏括号。查询指标函数间**不可进行计算**，查询指标间比较应该通过interval传入而非计算
            interval (str): 数值区间的字符串表示，支持以下几种格式：
                - 闭区间 "[low, high]"，包含边界值。
                - 左开右闭区间 "(low, high]"，不包含左边界值，包含右边界值。
                - 左闭右开区间 "[low, high)"，包含左边界值，不包含右边界值。
                - 开区间 "(low, high)"，不包含任何边界值。
                其中，low 和 high 可以是具体的数值、查询指标函数或 "inf"/"-inf" 表示无穷大或无穷小。当输入具体数值时，应采用科学计数法表示，例如5亿应写作5e8。
                默认值为 "(-inf, inf)"，即无限制。
            start_date (str): 开始日期。
            end_date (str): 结束日期
        """

        def parse_interval(interval_str):
            interval_str = interval_str.strip()
            if interval_str == "(-inf,inf)":
                return float("-inf"), float("inf"), False, False, None, None  # Boundaries irrelevant for inf

            if not (interval_str.startswith(("(", "[")) and interval_str.endswith((")", "]"))):
                raise ValueError(
                    f"Invalid interval format: {interval_str}. Must start with '(' or '[' and end with ')' or ']'."
                )

            open_br = interval_str[0]
            close_br = interval_str[-1]
            low_inclusive = open_br == "["
            high_inclusive = close_br == "]"

            inside = interval_str[1:-1].strip()
            if "," not in inside:
                raise ValueError(f"Invalid interval format: {interval_str}. Missing comma separator.")

            low_str, high_str = [part.strip() for part in inside.split(",", 1)]

            def get_precision(s: str):
                """
                返回数字字符串的小数精度：
                - '22.40' -> 2
                - '3' -> 0
                - 非数字（如函数表达式、inf/-inf）返回 None
                """
                s = s.strip()

                # 函数表达式不计算精度
                if ".get_" in s:
                    return None

                # 无穷不计算精度
                if s in ("inf", "-inf"):
                    return None

                # 匹配标准数字 + 科学计数法（不用 Decimal）
                # 支持示例"1.23e-3""1e-3""1.234e+2""22.40""3""100e+3""2.50e2""3.1400e0"
                number_pattern = r"^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$"
                if not re.match(number_pattern, s):
                    return None

                # 分离 base 和 exponent
                if "e" in s.lower():
                    base, exp_part = re.split(r"[eE]", s)
                    exponent = int(exp_part)  # 安全，因为正则保证合法
                else:
                    base = s
                    exponent = None

                # 计算 base 的小数位
                if "." in base:
                    fraction_len = len(base.split(".", 1)[1])
                else:
                    fraction_len = 0

                # 无指数：直接返回 fraction_len
                if exponent is None:
                    return fraction_len

                # 有指数：按科学计数法规则调整小数位
                if exponent < 0:
                    # 1.23e-3 → 小数位 = 2 + 3 = 5
                    return fraction_len + (-exponent)
                else:
                    # exponent > 0
                    # 1.234e+2 → 123.4 → 小数位 = max(3 - 2, 0) = 1
                    return max(fraction_len - exponent, 0)

            low_accuracy = get_precision(low_str)
            high_accuracy = get_precision(high_str)

            # Parse low (assume numeric/inf for this version; functions not supported in string format)
            if low_str == "-inf" or low_str == "inf":
                low = float("-inf")
            else:
                try:
                    low = float(low_str)
                except ValueError:
                    if ".get_" in low_str:
                        low = eval(low_str, {}, {"AStockGlobalQuery": AStockGlobalQuery})
                    else:
                        low = float("-inf")

            # Parse high
            if high_str == "inf" or high_str == "-inf":
                high = float("inf")
            else:
                try:
                    high = float(high_str)
                except ValueError:
                    if ".get_" in high_str:
                        high = eval(high_str, {}, {"AStockGlobalQuery": AStockGlobalQuery})
                    else:
                        high = float("inf")

            if isinstance(low, (int, float)) and low > high and low != float("-inf") and high != float("inf"):
                raise ValueError(f"Invalid interval: low ({low}) > high ({high}).")

            return low, high, low_inclusive, high_inclusive, low_accuracy, high_accuracy

        low, high, low_inclusive, high_inclusive, low_accuracy, high_accuracy = parse_interval(interval)

        # 90%和70%筹码集中度有下限0，应该写在new_stock_selector_rag.py里，但是匹配逻辑太麻烦，所以在这里塞了一下
        if (
            (str(field) in ("AStockMarketCur.cd_70", "AStockMarketCur.cd_90", "WzHoldingPoolsLevel15.pp_funds"))
            and isinstance(low, (int, float))
            and low < 0
        ):
            low = 0

        # 收集所有相关列（新版本interval为数值/Inf，不添加函数列；若需函数比较，使用旧版函数）
        columns = [field]
        if not isinstance(low, (int, float)):
            columns.append(low)

        if not isinstance(high, (int, float)):
            columns.append(high)

        # 调用基础构建函数
        columns = AStockGlobalQuery._handle_multi_primary_key_columns(columns, start_date, end_date)
        query, cte_tables = AStockGlobalQuery._build_query_base(columns, start_date, end_date)

        # 使用统一的列解析方法
        field_col = AStockGlobalQuery._resolve_column_from_cte(field, cte_tables)

        # 添加特定的大小比较条件
        conditions = []

        if low != float("-inf"):
            low_val = (
                AStockGlobalQuery._resolve_column_from_cte(low, cte_tables)
                if not isinstance(low, (int, float))
                else low
            )
            if low_inclusive:
                epsilon = 0
                if low_accuracy is not None:
                    low_accuracy = max(low_accuracy, 4)
                    epsilon = 5 * 10 ** (-(low_accuracy + 1))
                conditions.append(field_col >= (low_val - epsilon))
            else:
                conditions.append(field_col > low_val)

        if high != float("inf"):
            high_val = (
                AStockGlobalQuery._resolve_column_from_cte(high, cte_tables)
                if not isinstance(high, (int, float))
                else high
            )
            if high_inclusive:
                epsilon = 0
                if high_accuracy is not None:
                    high_accuracy = max(high_accuracy, 4)
                    epsilon = 5 * 10 ** (-(high_accuracy + 1))
                conditions.append(field_col < high_val + epsilon)
            else:
                conditions.append(field_col < high_val)

        if conditions:
            query = query.where(and_(*conditions))

        return query

    @staticmethod
    def query_field_like(field, pattern: str, start_date: str = "0d", end_date: str = "0d"):
        """
        :术语名称:
            字段模糊匹配
        :术语解释:
            筛选出指定文本字段中包含特定关键词的股票
        :功能:
            筛选出指定纯文本字段中包含特定关键词的股票
            专用于文本内容匹配（如主营业务、策略评级、异动原因等）
        :参数:
            field: 查询字段，只可传入查询指标函数，不要遗漏括号。
            pattern: 用于匹配的关键词或文本片段。
            start_date (str): 开始日期。
            end_date (str): 结束日期。
        """

        # 使用 _build_query_base 统一处理（包括盘后表日期处理）
        columns = [field]
        columns = AStockGlobalQuery._handle_multi_primary_key_columns(columns, start_date, end_date)
        _, cte_tables = AStockGlobalQuery._build_query_base(columns, start_date, end_date)

        # 使用统一的列解析方法
        field_col = AStockGlobalQuery._resolve_column_from_cte(field, cte_tables)

        # 获取base_table用于构建返回字段
        if hasattr(field, "class_"):
            base_table = field.class_
            base_cte = cte_tables[base_table]
        else:
            # 对于tuple类型，从cte_tables推断
            base_table = None
            for table, cte in cte_tables.items():
                if not isinstance(table, str):
                    base_table = table
                    base_cte = cte
                    break

        # 判断是否在extra_primary_key_views中
        is_extra_primary_key = base_table and base_table in extra_primary_key_views.keys()

        # 构建返回查询（返回secucode, date, field_value, similarity）
        field_value_alias = f"{field.key if hasattr(field, 'key') else field[1]}_value"

        pattern_length = func.length(pattern)
        max_length = func.greatest(func.length(field_col), pattern_length)
        edit_distance = func.levenshteinDistance(func.upper(field_col), func.upper(pattern))

        # 基础相似度：1 - (编辑距离 / 较长字符串长度)
        base_similarity = cast(text("1.0"), Numeric(10, 6)) - (edit_distance / cast(max_length, Numeric(10, 6)))

        # 简化的相似度算法：
        # 对于短模式（长度 <= 8），如果包含核心关键词，则大幅降低阈值要求
        similarity_expr = base_similarity

        # 动态阈值：
        # - 短模式且包含关键词：使用0.3阈值
        # - 长模式：使用0.8阈值
        similarity_threshold = case(
            (
                pattern_length <= 8,
                case(
                    (field_col.like("%换手率%"), cast(text("0.7"), Numeric(10, 6))),
                    (field_col.like("%涨幅%"), cast(text("0.7"), Numeric(10, 6))),
                    (field_col.like("%跌幅%"), cast(text("0.7"), Numeric(10, 6))),
                    else_=cast(text("0.8"), Numeric(10, 6)),
                ),
            ),
            else_=cast(text("0.8"), Numeric(10, 6)),
        )

        condition = or_(similarity_expr >= similarity_threshold, func.upper(field_col).like(f"%{pattern.upper()}%"))

        if is_extra_primary_key:
            if base_table and base_table in multidate_views:
                query = select(
                    base_cte.c.secucode,
                    base_cte.c.date,
                    field_col.label(field_value_alias),
                ).where(condition)
            elif base_table and hasattr(base_table, "date"):
                query = select(
                    base_cte.c.secucode,
                    base_cte.c.date,
                    field_col.label(field_value_alias),
                ).where(condition)
            else:
                query = select(base_cte.c.secucode, field_col.label(field_value_alias)).where(condition)

        else:
            query = select(base_cte.c.secucode).where(condition)

        return query

    @staticmethod
    def query_date_field_in_range(
        field, low_date_str: str = None, high_date_str: str = None, start_date: str = "0d", end_date: str = "0d"
    ):
        """
        :术语名称:
            日期字段范围筛选
        :术语解释:
            筛选出日期字段在指定时间范围内的股票
        :功能:
            专门用于筛选今天的日期字段在指定时间范围或等于特定日期的股票。**当用户问题中出现"最后交易日为"、"交易日期为"、"日期是"、"等于某天"等关键词时，必须调用此函数！** 适用场景包括但不限于：上市日期、限售解禁日期、定向增发日期、资产重组日期、大宗交易最后交易日、股权登记日、除权除息日等日期相关查询。
        :参数:
            field: 要查询的日期字段
            low_date_str (str | None): 范围的开始日期。对于负向日期（如-5y），表示"在这个时间以内"的数据。如果为 None，则没有下限。
            high_date_str (str | None): 范围的结束日期 。对于负向日期（如-5y），表示"超过这个时间以外"的数据。如果为 None，则没有下限。如果为 None，则没有上限。
        """
        if low_date_str is None and high_date_str is None:
            raise ValueError("必须为日期范围查询提供 low_date_str 或 high_date_str 中的至少一个。")

        # 获取字段所属的表信息
        field_table = None
        if hasattr(field, "class_"):
            field_table = field.class_

        # 检查字段是否为Date类型
        is_date_field = False
        try:
            # 方式1：直接从字段获取类型（对于Label字段）
            if hasattr(field, "type"):
                is_date_field = str(field.type).upper() in ("DATE", "DATETIME", "TIMESTAMP")
            # 方式2：通过表结构检查（对于普通字段）
            elif field_table and hasattr(field_table, "__table__"):
                from sqlalchemy import inspect

                mapper = inspect(field_table)
                for column in mapper.columns:
                    if column.key == field.key:
                        is_date_field = str(column.type).upper() in ("DATE", "DATETIME", "TIMESTAMP")
                        break
        except Exception:
            # 如果检测失败，默认为非Date类型
            is_date_field = False

        # 构建基础查询：从字段所在的表直接查询，避免复杂的JOIN
        if field_table:
            # 直接查询字段所在的表，返回secucode
            table_query = select(field_table.secucode)

            # 构建日期过滤条件
            conditions = []

            if is_date_field:
                # Date类型字段处理
                field_for_comparison = field

                # 基础有效性条件
                conditions.extend(
                    [
                        field_for_comparison.is_not(None),
                    ]
                )

                # 日期范围条件
                if low_date_str is not None:
                    low_date = get_date(low_date_str, is_start=True)
                    conditions.append(field_for_comparison >= low_date)

                if high_date_str is not None:
                    high_date = get_date(high_date_str, is_start=True)
                    conditions.append(field_for_comparison <= high_date)

            else:
                # String或Int类型字段处理
                field_as_string = func.toString(field)
                cleaned_field_col = func.replaceAll(field_as_string, "'", "")

                # 基础有效性与格式验证条件
                string_format_valid = func.match(cleaned_field_col, r"^\d{4}-\d{2}-\d{2}$")
                int_format_valid = func.match(cleaned_field_col, r"^\d{8}$")

                conditions.extend(
                    [
                        cleaned_field_col != "0",
                        cleaned_field_col.is_not(None),
                        cleaned_field_col != "",
                        or_(string_format_valid, int_format_valid),
                    ]
                )

                # 日期范围条件
                if low_date_str is not None:
                    low_date = get_date(low_date_str, is_start=True)
                    conditions.append(func.toDate(cleaned_field_col) >= low_date)

                if high_date_str is not None:
                    high_date = get_date(high_date_str, is_start=True)
                    conditions.append(func.toDate(cleaned_field_col) <= high_date)

            # 应用条件
            if conditions:
                table_query = table_query.where(and_(*conditions))

            # 确保只返回有效的secucode，并且去重
            table_query = table_query.where(field_table.secucode.is_not(None)).distinct()

            return table_query

        else:
            # 如果无法确定字段所属表，使用原有逻辑作为fallback
            # 收集所有相关列
            columns = [field]

            # 调用基础构建函数
            columns = AStockGlobalQuery._handle_multi_primary_key_columns(columns, start_date, end_date)
            query, cte_tables = AStockGlobalQuery._build_query_base(columns, start_date, end_date)

            # 使用统一的列解析方法
            field_col = AStockGlobalQuery._resolve_column_from_cte(field, cte_tables)

            # 初始化条件列表
            conditions = []

            # 根据字段类型选择最佳处理方式
            if is_date_field:
                field_for_comparison = field_col

                # 更智能的无效日期检测：排除1970年的日期（Unix纪元默认值）和明显过早的日期
                # 使用合理的业务规则：只接受1990年以后的日期（中国股市建立时间）
                # 使用ClickHouse兼容的函数
                from sqlalchemy import literal, text

                conditions.extend(
                    [
                        field_for_comparison.is_not(None),
                        field_for_comparison > literal("1990-01-01"),  # 使用literal确保字面量比较
                    ]
                )
            else:
                # String或Int类型字段：需要统一处理
                field_as_string = func.toString(field_col)
                cleaned_field_col = func.replaceAll(field_as_string, "'", "")

                # 基础有效性与格式验证条件
                # 支持两种格式：YYYY-MM-DD 字符串格式 或 YYYYMMDD 数字格式
                string_format_valid = func.match(cleaned_field_col, r"^\d{4}-\d{2}-\d{2}$")
                int_format_valid = func.match(cleaned_field_col, r"^\d{8}$")

                conditions.extend(
                    [
                        cleaned_field_col != "0",
                        cleaned_field_col.is_not(None),
                        cleaned_field_col != "",
                        or_(string_format_valid, int_format_valid),  # 支持两种格式
                    ]
                )
                field_for_comparison = cleaned_field_col

            if low_date_str is not None:
                low_date = get_date(low_date_str, is_start=True)

                # 对于日期范围查询，low_date_str 总是表示起始日期（>= 条件）
                # 即使是 "-3d" 这样的相对日期，也表示"从3天前开始"
                if is_date_field:
                    # Date类型字段：直接比较
                    conditions.append(field_for_comparison >= low_date)
                else:
                    # String/Int类型字段：需要转换为Date再比较
                    conditions.append(func.toDate(field_for_comparison) >= low_date)

            if high_date_str is not None:
                high_date = get_date(high_date_str, is_start=True)

                # 对于日期范围查询，high_date_str 总是表示结束日期（<= 条件）
                # 即使是相对日期，也表示"到指定日期为止"
                if is_date_field:
                    # Date类型字段：直接比较
                    conditions.append(field_for_comparison <= high_date)
                else:
                    # String/Int类型字段：需要转换为Date再比较
                    conditions.append(func.toDate(field_for_comparison) <= high_date)

            query = query.where(and_(*conditions))

            return query

    @staticmethod
    def query_top_n_by_field(
        n: int, filter_type: str, start_date: str = "0d", end_date: str = "0d", subquery=None, field=None
    ):
        """
        :术语名称:
            指标排名筛选
        :术语解释:
            根据指定指标对股票进行排序并截取前N名或后N名
        :功能:
            根据指定指标对股票进行排序，并截取前 N 名或后 N 名。

            当用户问题涉及**最值**或**排名**的问题时调用此函数。
        :参数:
            n (int): 截取数量。
                1. **最值模式 (n=1 / n=-1)**:
                   - 当问题包含"最"字（如"最高"、"最大"、"最多"、"最强"等），且**未明确指定具体数量**（即没说"前5"、"前10"）时，**必须强制设为 1**（或 -1）。
                   - 即使问题中的名词是复数（如"哪些股票...最多"），只要强调了"最"，依然取 n=1。

                **【常规逻辑】**
                2. **指定数量模式**: 如"前5名"-> n=5，"倒数10名"-> n=-10。
                3. **默认兜底模式 (n=10)**: 仅当问题是模糊的"排名"、"排行"、"有哪些"，且**既无"最"字也无具体数字**时，才默认 n=10。
            filter_type (str): 筛选范围类型。可选: 'stock' (股票), 'board' (板块/行业), 'warzone' (特定战区)。
            start_date (str): 统计周期的开始日期。
            end_date (str): 统计周期的结束日期。
            subquery (Select | None): 前置筛选条件（嵌套查询）。当用户限定了范围（例如："在**ChatGPT概念股**中找出涨幅最高的..."），需先调用范围查询函数（如 query_concept），将其结果作为 subquery 传入。若无限定范围，则传 None。
            field (Column | func | None): 排序指标（如 `get_close`, `get_zdf` 等）。若用户未指定具体指标，传 None (此时默认按**涨跌幅**排序)。
        """
        # 自动映射资金榜 subquery 到对应的排序字段
        if subquery is not None and field is not None:
            subquery_str = str(subquery).lower()

            # 检查是否为资金榜相关的 subquery 并自动调整 field
            if "ranklist_duo1_level0_view" in subquery_str:
                field = RanklistDuo1Level0.duor_1
            elif "ranklist_duo3_level0_view" in subquery_str:
                field = RanklistDuo3Level0.duor_3
            elif "ranklist_gan13_level0_view" in subquery_str:
                field = RanklistGan13Level0.ganr_13

        if field is None:
            field = AStockGlobalQuery.get_zdf_days(1)

        # 使用 _build_query_base 统一处理（包括盘后表日期处理）
        columns = [field]
        columns = AStockGlobalQuery._handle_multi_primary_key_columns(columns, start_date, end_date)
        _, cte_tables = AStockGlobalQuery._build_query_base(
            columns, start_date, end_date, secucode_query=subquery, filter_type=filter_type
        )

        # 使用统一的列解析方法
        field_col = AStockGlobalQuery._resolve_column_from_cte(field, cte_tables)

        limit_num = abs(n)
        is_descending = n > 0

        # 获取字段标识符用于命名
        if isinstance(field, tuple):
            field_identifier = field[1]
        else:
            field_identifier = getattr(field, "key", str(field))

        base_cte = None
        if hasattr(field, "class_"):
            base_cte = cte_tables.get(field.class_)
        elif isinstance(field, tuple):
            func_name, field_name, _ = field
            base_cte = cte_tables.get(f"{func_name}_{field_name}")
        else:
            base_cte = next(
                (cte for table, cte in cte_tables.items() if not isinstance(table, str) and hasattr(cte.c, "date")),
                None,
            )

        # 获取筛选条件
        filter_condition = AStockGlobalQuery._get_filter_condition(filter_type) if filter_type else None

        has_date_field = base_cte is not None and hasattr(base_cte.c, "date")

        unique_id = None

        # 如果CTE有date字段，使用row_number获取每只股票的最新数据
        if has_date_field:
            unique_id = str(uuid.uuid4()).replace("-", "")[:8]

        if has_date_field:
            cte_select = select(
                base_cte.c.secucode,
                field_col.label("sort_value"),
                func.row_number()
                .over(partition_by=base_cte.c.secucode, order_by=desc(base_cte.c.date))
                .label("row_num"),
            ).where(field_col.is_not(None))

            if filter_condition is not None:
                cte_select = cte_select.join(AStockBasic, base_cte.c.secucode == AStockBasic.secucode).where(
                    filter_condition
                )

            # 生成 CTE
            latest_data_cte = cte_select.cte(f"cte_latest_data_{field_identifier}_{unique_id}")

            sort_order = latest_data_cte.c.sort_value.desc() if is_descending else latest_data_cte.c.sort_value.asc()

            query = (
                select(latest_data_cte.c.secucode)
                .where(latest_data_cte.c.row_num == 1)
                .order_by(sort_order)
                .limit(limit_num)
            )

            return query

        else:
            secucode_col = base_cte.c.secucode if base_cte else text("secucode")
            query = select(secucode_col)

            if filter_condition is not None:
                if base_cte:
                    query = query.join(AStockBasic, secucode_col == AStockBasic.secucode).where(filter_condition)
                else:
                    query = query.where(secucode_col.in_(select(AStockBasic.secucode).where(filter_condition)))

            order_clause = field_col.desc() if is_descending else field_col.asc()

            query = query.where(field_col.is_not(None)).order_by(order_clause).limit(limit_num)

            return query

    # @staticmethod
    # def query_price_in_range(
    #     low: Union[Field,float] = float('-inf'), high: Union[Field,float] = float('inf'), start_date: str , end_date: str
    # ):
    #     """
    #     :术语名称:
    #         收盘价(股价)在指定范围内
    #     :术语解释:
    #         回答收盘价(股价)大于、等于、小于某指标有哪些或某指标大于、等于、小于收盘价(股价)的股票有哪些
    #     :功能:
    #         筛选出收盘价(股价)在指定范围内的股票,查询出收盘价>=low,收盘价=<high的股票
    #     :参数:
    #         low: 最小价格,可传具体数值或查询指标函数
    #         high: 最大价格,:可传具体数值或查询指标函数
    #         start_date (str): 开始日期
    #         end_date (str): 结束日期
    #     :量化解释:
    #         收盘价(股价)在指定范围内的股票
    #     """
    #     query = (
    #         select(AStockMarket.secucode)
    #         .distinct()
    #         .join(AStockBasic, AStockMarket.secucode == AStockBasic.secucode)
    #     )
    #     conditions = []

    #     conditions.append(AStockBasic.type == 'A股')

    #     # === low 处理 ===
    #     if isinstance(low, (int, float)):
    #         conditions.append(AStockMarket.close >= low)
    #     else:
    #         other_table = low.table
    #         col_names = [c.name for c in other_table.c]
    #         if "date" in col_names:
    #             query = query.join(
    #                 other_table,
    #                 (AStockMarket.secucode == other_table.c.secucode)
    #                 & (AStockMarket.date == other_table.c.date),
    #             )
    #         else:
    #             query = query.join(
    #                 other_table,
    #                 AStockMarket.secucode == other_table.c.secucode,
    #             )
    #         conditions.append(AStockMarket.close >= low)

    #     # === high 处理 ===
    #     if isinstance(high, (int, float)):
    #         conditions.append(AStockMarket.close <= high)
    #     else:
    #         other_table = high.table
    #         col_names = [c.name for c in other_table.c]
    #         if "date" in col_names:
    #             query = query.join(
    #                 other_table,
    #                 (AStockMarket.secucode == other_table.c.secucode)
    #                 & (AStockMarket.date == other_table.c.date),
    #             )
    #         else:
    #             query = query.join(
    #                 other_table,
    #                 AStockMarket.secucode == other_table.c.secucode,
    #             )
    #         conditions.append(AStockMarket.close <= high)

    #     # === 日期条件 ===
    #     conditions.append(
    #         AStockMarket.date.between(
    #             get_date(start_date, is_start=True),
    #             get_date(end_date),
    #         )
    #     )

    #     return query.where(and_(*conditions))
    # AStockBasic
    @staticmethod
    def query_market(value: str, start_date: str, end_date: str):
        """
        :术语名称:
            市场
        :术语解释:
            市场指股票所属的交易所或指数体系,代表股票的流通平台和选股范围，影响流动性、估值水平及制度规则。
        :功能:
            筛选出指定市场的股票
        :参数:
            value(str): 名称，如'沪深300'
        :量化解释:
            对市场字段进行精确匹配
        :术语位置:
            提示词
        """
        # 特殊处理：A股市场，返回股票全集
        if value == "A股":
            query = select(AStockBasic.secucode).select_from(AStockBasic).where(AStockBasic.type == "A股")
            return query

        subquery = (  # 查询行业市场概念等的通用查询
            select(func.groupArray(AStockBasic.secucode).label("secuname"))
            .where((AStockBasic.type == "板块") & (AStockBasic.secuname.like(f"%{value}%")))
            .scalar_subquery()
        )

        # 主查询 - 使用假设已定义的数据库函数
        query = (
            select(AStockBasic.secucode)
            .select_from(AStockBasic)
            .where((AStockBasic.type == "A股") & func.hasAny(func.splitByString(",", AStockBasic.market), subquery))
        )
        return query

    @staticmethod
    def query_not_market(value: str, start_date: str, end_date: str):
        """
        :术语名称:
            非指定市场
        :术语解释:
            非指定市场指不属于目标交易所范围的股票，常用于排除不相关市场个股进行定向筛选分析。
        :功能:
            筛选出指定市场外的股票
        :参数:
            value(str): 市场名称，如'上证50'
        :量化解释:
            对市场字段进行模糊匹配
        """

        return select(AStockBasic.secucode).where(AStockBasic.market != value)

    @staticmethod
    def query_detail_main_business(concept: str, start_date: str, end_date: str):
        """
        :术语名称:
            主营业务
        :术语解释:
            主营业务指企业核心经营活动中持续产生主要收入与利润的业务板块，涵盖公司核心产品、服务类型及市场定位，当问题包含‘主要做什么’‘靠什么盈利’‘核心业务’‘产品构成’等语义时，可使用该功能。
        :功能:
            筛选出主营业务包含指定关键词的个股
        :参数:
            concept(str): 主营业务名称
        :量化解释:
            对主营业务字段进行模糊匹配
        :术语位置:
            提示词
        """
        return select(AStockBasic.secucode).where(AStockBasic.detail_main_business.like(f"%{concept}%"))

    @staticmethod
    def query_concept(concept_code: str, start_date: str, end_date: str):
        """
        :术语名称:
            概念
        :术语解释:
            概念是按题材或行业逻辑划分的板块分类，反映市场对某类股票的共识预期，如国产替代、新能源等，常用于主题投资、轮动博弈及情绪驱动交易。
        :功能:
            筛选包含指定概念的股票。问某某(概念代码)的股票时，必须调用此函数而非query_stock_by_secucodes
        :参数:
            concept_code(str): 概念代码
        :量化解释:
            对概念字段进行模糊匹配
        """

        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%{concept_code}%"))

    @staticmethod
    def query_trade(trade_code: str, start_date: str, end_date: str):
        """
        :术语名称:
            行业
        :术语解释:
            行业指按相似主营业务或产业链归类的一组股票，体现市场对某一经济领域的关注，常用于板块轮动、行业比较与主题投资分析。
        :功能:
            筛选出属于指定行业/板块的股票。问某某(行业代码)的股票时，必须调用此函数而非query_stock_by_secucodes
        :参数:
            trade_code(str): 行业代码
        :量化解释:
            对所属行业字段进行模糊匹配
        """

        return select(AStockBasic.secucode).where(AStockBasic.trade == trade_code)

    @staticmethod
    def query_area(area_code: str, start_date: str, end_date: str):
        """
        :术语名称:
            地域
        :术语解释:
            筛选属于指定地域的股票，可选地域包括黑龙江, 内蒙古, 安徽, 重庆, 广西, 北京, 河北, 上海, 广东, 宁夏, 青海, 吉林, 四川, 云南, 河南, 陕西, 江苏, 湖北, 新疆, 江西, 山西, 山东, 辽宁, 西藏, 湖南, 海南, 甘肃, 天津, 深圳, 福建, 贵州, 浙江。
        :功能:
            筛选出属于指定地域的股票，问某某(地域代码)的股票时，必须调用此函数而非query_stock_by_secucodes
        :参数:
            area_code(str): 地域代码
        :量化解释:
            根据地域板块代码筛选area字段等于对应地域板块代码的股票
        """

        return select(AStockBasic.secucode).where(AStockBasic.area == area_code)

    @staticmethod
    def query_controlling_share_holder(name: str, start_date: str, end_date: str):
        """
        :术语名称:
            控制股东
        :术语解释:
            控制股东指通过多数股权或实际控制权主导公司战略决策的核心股东，常见于国资委或大型集团控股企业，影响治理结构与经营方向。
        :功能:
            筛选出控制股东为指定名称的股票
        :参数:
            name(str): 股东名称
        :量化解释:
            判断股东名称是否包含指定名称
        """
        if AStockBasic.detail_ctrl_shareholder is None:
            return select(AStockMarket.secucode).where(False)

        else:
            return select(AStockMarket.secucode).where(AStockBasic.detail_ctrl_shareholder == name)

    @staticmethod
    def query_shhq(start_date: str, end_date: str):
        """
        :术语名称:
            沪A/沪市
        :术语解释:
            指在上海证券交易所上市的A股股票。
        :功能:
            筛选出属于上海证券交易所的A股股票，如果问题中仅筛选沪市（沪A）股票，请使用此函数进行筛选。
        :量化解释:
            无
        """
        return select(AStockBasic.secucode).where(AStockBasic.market == "SHHQ")

    @staticmethod
    def query_szhq(start_date: str, end_date: str):
        """
        :术语名称:
            深A/深市
        :术语解释:
            指在深圳证券交易所上市的A股股票。
        :功能:
            筛选出属于深圳证券交易所的A股股票，如果问题中仅筛选深市（深A）股票，请使用此函数进行筛选。
        :量化解释:
            无
        """
        return select(AStockBasic.secucode).where(AStockBasic.market == "SZHQ")

    @staticmethod
    def query_bjhq(start_date: str, end_date: str):
        """
        :术语名称:
            北A/北交所
        :术语解释:
            指在北京证券交易所上市的A股股票。
        :功能:
            筛选出属于北京证券交易所的A股股票，如果问题中仅筛选北交所（北A）股票，请使用此函数进行筛选。
        :量化解释:
            无
        """
        return select(AStockBasic.secucode).where(AStockBasic.market == "BJHQ")

    # ──────────── 老版组合条件START ────────────
    # #AStockMarket
    # @staticmethod
    # def query_volume_gt_avg(
    #     days: int = 5, factor: float = 1.0, value_lookback_days: int = 0, start_date: str , end_date: str
    # ):
    #     """
    #     :术语名称:
    #         m天前成交量大于n日平均成交量的若干倍
    #     :术语解释:
    #         成交量超过平均量若干倍描述股票市场中特定时间窗口（m天前）的成交量与n日平均成交量的倍数比较关系，用于识别异常交易量信号。涵盖用户对历史成交量相对均值的偏离分析需求，包括但不限于以下语义场景：'m天前成交量超过n日均量线的X倍'、'过去m日成交量相比n日平均的放大倍数'、'某日成交量显著高于近期n日均值的阈值条件'。支持动态时间参数（如3天前、5日均线）和任意倍数（如1.5倍、2倍）的组合查询，可扩展至与价格波动、技术指标联动的多维筛选条件。
    #     :功能:
    #         筛选m天前成交量大于n日平均成交量的若干倍
    #     :参数:
    #         days (int): 平均成交量回溯天数。
    #         factor (float): 倍数阈值（如 2.0 表示大于平均的2倍）。
    #         value_lookback_days(int): 成交量回溯天数。
    #         start_date (str): 开始日期。
    #         end_date (str): 结束日期。
    #     :量化解释:
    #         成交量 > n日平均成交量 * 阈值
    #     """

    #     if value_lookback_days <= 0:
    #         # 对每个交易日计算符合条件的个股
    #         daily_avg_volume = select(
    #             AStockMarket.secucode,
    #             AStockMarket.date,  # type: ignore
    #             AStockMarket.volume,  # type: ignore
    #             func.avg(AStockMarket.volume)
    #             .over(
    #                 partition_by=AStockMarket.secucode,
    #                 order_by=AStockMarket.date,  # type: ignore
    #                 rows=(-(days - 1), 0),
    #             )
    #             .label("avg_volume"),
    #         ).subquery()

    #         # 筛选当日成交量 > 平均成交量 * factor 的个股
    #         result = (
    #             select(daily_avg_volume.c.secucode)
    #             .select_from(daily_avg_volume)
    #             .where(
    #                 (daily_avg_volume.c.volume > daily_avg_volume.c.avg_volume * factor)
    #                 & (
    #                     daily_avg_volume.c.date.between(
    #                         get_date(start_date, is_start=True),
    #                         get_date(end_date),
    #                     )
    #                 )
    #             )
    #             .order_by(daily_avg_volume.c.date)
    #         ).distinct()
    #     else :
    #         daily_avg_volume = select(
    #             AStockMarket.secucode,
    #             AStockMarket.date,  # type: ignore
    #             AStockMarket.volume,  # type: ignore
    #             func.avg(AStockMarket.volume)
    #             .over(
    #                 partition_by=AStockMarket.secucode,
    #                 order_by=AStockMarket.date,  # type: ignore
    #                 rows=(-(days - 1), 0),
    #             )
    #             .label("avg_volume"),
    #             func.any(AStockMarket.volume).over(
    #                 partition_by=AStockMarket.secucode,  # 按股票代码分组
    #                 order_by=AStockMarket.date,  # 按交易日期排序
    #                 rows=(-value_lookback_days, -value_lookback_days),  # 窗口范围：当前行往前(lookback_days-1)行到当前行（共lookback_days天）
    #             ).label("prev_avg_volume"),
    #         ).subquery()

    #         # 筛选value_lookback_days天前成交量 > 平均成交量 * factor 的个股
    #         result = (
    #             select(daily_avg_volume.c.secucode)
    #             .select_from(daily_avg_volume)
    #             .where(
    #                 (daily_avg_volume.c.prev_avg_volume > daily_avg_volume.c.avg_volume * factor)
    #                 & (
    #                     daily_avg_volume.c.date.between(
    #                         get_date(start_date, is_start=True),
    #                         get_date(end_date),
    #                     )
    #                 )
    #             )
    #             .order_by(daily_avg_volume.c.date)
    #         ).distinct()

    #     return result

    # @staticmethod
    # def query_close_gt_open_recent(
    #     days: int, min_count: int, start_date: str , end_date: str
    # ):
    #     """
    #     :术语名称:
    #         近N日内有M天收盘价高于开盘价
    #     :术语解释:
    #         过去N日内有M天收阳线反映多头主导格局，是典型的短线强势信号，用于筛选强势股、判断波段行情起点等场景，与红三兵、多头排列等K线形态概念直接关联。
    #     :功能:
    #         筛选近N日内有M天收阳线的个股。
    #     :参数:
    #         days (int): 回溯天数范围。
    #         min_count (int): 至少满足条件的天数。
    #     :量化解释:
    #         统计n日内收阳线的天数 >= m
    #     """
    #     # 计算近N日内收盘价大于开盘价的天数
    #     close_gt_open_days = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         func.count(case((AStockMarket.close > AStockMarket.open, 1)))
    #         .over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(1 - days, 0),  # 包含当前日在内共days天
    #         )
    #         .label(f"cnt_close_gt_open_{days}"),
    #     ).subquery()

    #     # 筛选满足条件的天数
    #     result = (
    #         select(
    #             close_gt_open_days.c.secucode,
    #         )
    #         .select_from(close_gt_open_days)
    #         .where(
    #             (close_gt_open_days.c[f"cnt_close_gt_open_{days}"] >= min_count)
    #             & (
    #                 close_gt_open_days.c.date.between(
    #                     get_date(start_date, is_start=True),
    #                     get_date(end_date),
    #                 )
    #             )
    #         )
    #     ).distinct()

    #     return result

    # @staticmethod
    # def query_active_stock(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         股性活跃
    #     :术语解释:
    #         股性活跃指股价波动频繁、成交量大的特性，常见于短线交易及市场热点阶段，与技术指标敏感联动且伴随低位整理形态
    #     :功能:
    #         筛选股性活跃的个股。
    #     :参数:
    #         start_date(str): 起始日期
    #         end_date(str): 结束日期
    #     :量化解释:
    #         当日成交量>最近20日均量的2倍以上,涨跌幅 > 0.05,振幅>0.08,最近5个交易日至少有3天收盘价大于开盘价
    #     """
    #     return intersect(
    #         AStockGlobalQuery.query_volume_gt_avg(20, 2.0, 0,start_date, end_date),
    #         AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_close(),0.05, 200, start_date, end_date),
    #         AStockGlobalQuery.query_field_in_range(
    #             AStockGlobalQuery.get_amplitude(),0.08, 200, start_date, end_date
    #         ),  # 振幅在hq_current_view表中
    #         AStockGlobalQuery.query_close_gt_open_recent(5, 3, start_date, end_date),
    #     )

    # @staticmethod
    # def query_long_term_low(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         长期低位
    #     :术语解释:
    #         长期低位指资产价格持续低于历史均值且市场关注度低迷，反映估值洼地特征，常用于逆向投资策略并与超卖信号、支撑位分析形成关联场景。
    #     :功能:
    #         筛选长期低位的个股。
    #     :量化解释:
    #         股价创250日阶段新低，成交量<=250日的最低成交量
    #     """
    #     cond1 = AStockGlobalQuery.query_breakout_low_days_gt(250)

    #     min_volume_250 = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         AStockMarket.volume,
    #         func.min(AStockMarket.volume)
    #         .over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(
    #                 -249,
    #                 0,
    #             ),  # SQLAlchemy 里的 ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
    #         )
    #         .label("min_volume_250"),
    #     ).subquery()

    #     # 查询特定日期（比如今天），volume < 250日最小成交量
    #     cond2 = select(min_volume_250.c.secucode).where(
    #         min_volume_250.c.date.between(
    #             get_date(start_date, is_start=True), get_date(end_date)
    #         ),  # 今天
    #         min_volume_250.c.volume <= min_volume_250.c.min_volume_250,
    #     ).distinct()

    #     return intersect(cond1, cond2)

    # @staticmethod
    # def query_low_position_arrange(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         低位整理
    #     :术语解释:
    #         低位整理指资产价格低于历史均值且市场关注度较高，反映估值低迷特征，常用于逆向投资策略并与超卖信号、支撑位分析形成关联场景。
    #     :功能:
    #         筛选低位整理的个股。
    #     :量化解释:
    #         收盘价高于过去20个交易日的最低价
    #     """
    #     min_close_price_20 = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         AStockMarket.close,
    #         func.min(AStockMarket.close)
    #         .over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(-19, 0),
    #         )
    #         .label("min_close_price_20"),
    #     ).subquery()

    #     close_lt_20_min = select(min_close_price_20.c.secucode).where(
    #         min_close_price_20.c.date.between(
    #             get_date(start_date, is_start=True), get_date(end_date)
    #         ),
    #         min_close_price_20.c.close > min_close_price_20.c.min_close_price_20,
    #     ).distinct()
    #     return close_lt_20_min

    # @staticmethod
    # def query_stock_limit_up(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         日期范围内涨停
    #     :术语解释:
    #         涨停指股票价格在单日交易中达到交易所规定的涨幅上限，触发交易限制的强势市场行为，核心特征包含价格波动触及阈值、买方资金集中涌入及流动性阶段性收缩。该术语既涵盖单日涨停形态（如昨日涨停），也包含连续多日涨停的极端行情，需结合交易量异动、主力资金封板强度及市场情绪持续性进行多维分析。特别需兼容时间范围函数特征，支持对历史涨停天数、价格区间、排除风险警示股等复合条件的精准映射。
    #     :功能:
    #         筛选出日期范围内涨停的股票
    #     :参数:
    #         start_date (str): 开始日期。
    #         end_date (str): 结束日期。
    #     :量化解释:
    #         查询涨停标志来判断是否涨停
    #     """

    #     return select(AStockMarket.secucode).where(AStockMarket.zt == 1 , AStockMarket.date.between(get_date(start_date, is_start=True), get_date(end_date))).distinct()

    # @staticmethod
    # def query_open_low_walk_low():
    #     """
    #     :术语名称:
    #         低开低走
    #     :术语解释:
    #         低开低走指开盘低于前日收盘且持续下跌，反映弱势信号及市场情绪低迷，常见于涨停后次日抛压明显的情境，关联均线支撑、主力动向等分析维度。
    #     :功能:
    #         筛选低开低走的股票
    #     :量化解释:
    #         根据低开低走字段判断是否低开低走
    #     """
    #     return NotImplementedError
    #     if AStockMarket.low_open_down is None:
    #         return select(AStockMarket.secucode).where(False)

    #     return select(AStockMarket.secucode).where(AStockMarket.low_open_down == True)

    # @staticmethod
    # def query_breaking_the_prior_high(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         突破前期高点
    #     :术语解释:
    #         突破前期高点指技术分析中价格突破历史阻力位，预示趋势延续信号，常用于科技股等强势板块选股，关联上升通道及成交量配合。
    #     :功能:
    #         筛选出突破前期高点的股票
    #     :量化解释:
    #         当日收盘价 > 过去30天（不含当日）的最高收盘价
    #     """
    #     subquery = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         AStockMarket.close,
    #         func.max(AStockMarket.close)
    #         .over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(-(30), -1),  # 前30天不含当前行
    #         )
    #         .label("max_close_30d"),
    #     ).subquery()

    #     result = (
    #         select(subquery.c.secucode)
    #         .select_from(subquery)
    #         .where(
    #             (
    #                 subquery.c.date.between(
    #                     get_date(start_date, is_start=True),
    #                     get_date(end_date),
    #                 )
    #             )
    #             & (subquery.c.close > subquery.c.max_close_30d)
    #         )
    #         .distinct()
    #         .order_by(subquery.c.date)
    #     )
    #     return result

    # @staticmethod
    # def query_consecutive_up_day(count_low = None, count_high = None, start_date: str = '0d', end_date: str = '0d'):
    #     """
    #     :术语名称:
    #         连涨天数/连续涨停
    #     :术语解释:
    #         连涨天数衡量股票连续交易日收盘价上涨的持续性，结合涨停次数可识别短期强势股，常用于技术分析中的动量策略与市场情绪研判，关联波动率、量价配合等指标。
    #     :功能:
    #         遍历指定时间范围内的每一天，判断该天是否是某只股票的连续上涨区间的最后一天。若满足连涨天数要求（向前回溯统计），则选中该股票
    #         (连涨2天:count_low=2,count_high=2
    #         连涨2-3天:count_low=2,count_high=3
    #         连涨3天以上:count_low=3,count_high=None)
    #     :参数:
    #         count_low(int): 最低连涨天数
    #         count_high(int): 最高连涨天数
    #         start_date (str): 开始日期
    #         end_date (str): 结束日期
    #     :量化解释:
    #         时间范围内连涨天数大于count_low小于count_high（如有）
    #     """
    #     # Step0: 获取时间边界（拉宽窗口）
    #     buffer = count_high if count_high is not None else count_low
    #     buffer_start = get_date(start_date, is_start=True, lookback_days=buffer)
    #     real_start = get_date(start_date, is_start=True)
    #     real_end = get_date(end_date)

    #     # Step1：拉宽时间窗口数据子查询（不加任何筛选）
    #     subquery = (
    #         select(
    #             AStockMarket.secucode,
    #             AStockMarket.date,
    #             AStockMarket.zdf,
    #             case((AStockMarket.zdf > 0, 1), else_=0).label("is_up"),
    #         )
    #         .where(AStockMarket.date.between(buffer_start, real_end))
    #         .subquery("price_flagged")
    #     )

    #     # Step2：分组 group_id —— 连续上涨段落
    #     grouped = (
    #         select(
    #             subquery.c.secucode,
    #             subquery.c.date,
    #             subquery.c.zdf,
    #             subquery.c.is_up,
    #             func.sum(case((subquery.c.is_up == 0, 1), else_=0))
    #             .over(partition_by=subquery.c.secucode, order_by=subquery.c.date)
    #             .label("group_id")
    #         )
    #         .subquery("price_grouped")
    #     )

    #     # Step3：rank 每段连续上涨的天数，只保留 is_up == 1 的
    #     ranked = (
    #         select(
    #             grouped.c.secucode,
    #             grouped.c.date,
    #             grouped.c.zdf,
    #             grouped.c.is_up,
    #             func.row_number()
    #             .over(partition_by=(grouped.c.secucode, grouped.c.group_id), order_by=grouped.c.date)
    #             .label("consecutive_up_days")
    #         )
    #         .where(grouped.c.is_up == 1)
    #         .subquery("price_with_rank")
    #     )

    #     # Step4：筛选符合真实时间区间 + 连涨天数区间
    #     condition = [
    #         ranked.c.date.between(real_start, real_end),
    #         ranked.c.consecutive_up_days >= count_low
    #     ]
    #     if count_high is not None:
    #         condition.append(ranked.c.consecutive_up_days <= count_high)

    #     query = (
    #         select(ranked.c.secucode)
    #         .where(*condition)
    #         .distinct()
    #         .order_by(ranked.c.date)
    #     )

    #     return query

    # @staticmethod
    # def query_consecutive_volume_up_day(count_low, count_high,start_date: str = '0d', end_date: str = '0d'):
    #     """
    #     :术语名称:
    #         成交量连增天数
    #     :术语解释:
    #         成交量连增天数衡量一只股票在连续交易日中成交量持续上升的强度，是市场活跃度与资金关注度的重要信号之一。
    #         可用于判断资金是否持续流入，结合价格走势判断是否形成量价配合，常见于趋势跟踪和主力吸筹分析中。
    #     :功能:
    #         筛选出在指定时间范围内，具有一定连续成交量递增天数的股票
    #         (例如: 连续5天成交量递增: count_low=5, count_high=5
    #         连续3-5天递增: count_low=3, count_high=5
    #         连续5天以上: count_low=5, count_high=None)
    #     :参数:
    #         count_low(int): 最低连续成交量递增天数
    #         count_high(int): 最高连续成交量递增天数（可选，None 表示无上限）
    #         start_date (str): 开始日期
    #         end_date (str): 结束日期
    #     :量化解释:
    #         在指定时间范围内，股票每天成交量比前一交易日更大，连续上涨的天数在 count_low 至 count_high（如有）之间。
    #     """
    #     # 计算buffer长度
    #     buffer = count_high if count_high is not None else count_low
    #     buffer_start = get_date(start_date, is_start=True,lookback_days=buffer)
    #     real_start = get_date(start_date, is_start=True)
    #     real_end = get_date(end_date)

    #     # Step1：拉宽时间窗口数据子查询（不加任何筛选）
    #     subquery = (
    #         select(
    #             AStockMarket.secucode,
    #             AStockMarket.date,
    #             AStockMarket.volume,
    #             func.lagInFrame(AStockMarket.volume)
    #                 .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date)
    #                 .label("prev_volume")
    #         )
    #         .where(AStockMarket.date.between(buffer_start, real_end))
    #         .subquery("volume_flagged")
    #     )

    #     # Step2：构造完整中间表，加上标记
    #     volume_flagged = (
    #         select(
    #             subquery.c.secucode,
    #             subquery.c.date,
    #             subquery.c.volume,
    #             case((subquery.c.volume > subquery.c.prev_volume, 1), else_=0).label("is_up"),
    #         )
    #         .subquery("volume_with_flag")
    #     )

    #     # Step3：分组 group_id —— 断点在 is_up == 0 的地方
    #     grouped = (
    #         select(
    #             volume_flagged.c.secucode,
    #             volume_flagged.c.date,
    #             volume_flagged.c.volume,
    #             volume_flagged.c.is_up,
    #             func.sum(
    #                 case((volume_flagged.c.is_up == 0, 1), else_=0)
    #             )
    #             .over(partition_by=volume_flagged.c.secucode, order_by=volume_flagged.c.date)
    #             .label("group_id")
    #         )
    #         .subquery("volume_grouped")
    #     )

    #     # Step4：每个连续段内打上连续编号（只给 is_up == 1 的行打）
    #     ranked = (
    #         select(
    #             grouped.c.secucode,
    #             grouped.c.date,
    #             grouped.c.volume,
    #             grouped.c.is_up,
    #             func.row_number()
    #             .over(partition_by=(grouped.c.secucode, grouped.c.group_id), order_by=grouped.c.date)
    #             .label("consecutive_volume_up_days")
    #         )
    #         .where(grouped.c.is_up == 1)
    #         .subquery("volume_with_rank")
    #     )

    #     # Step5：最终筛选符合时间范围 + 连续条件
    #     condition = [
    #         ranked.c.date.between(real_start, real_end),
    #         ranked.c.consecutive_volume_up_days >= count_low
    #     ]
    #     if count_high is not None:
    #         condition.append(ranked.c.consecutive_volume_up_days <= count_high)

    #     query = (
    #         select(ranked.c.secucode)
    #         .where(*condition)
    #         .distinct()
    #         .order_by(ranked.c.date)
    #     ).distinct()

    #     return query

    # @staticmethod
    # def query_oversold_stock():
    #     """
    #     :术语名称:
    #         超跌股
    #     :术语解释:
    #         超跌股指股价短期大幅偏离内在价值，具有技术超卖特征的金融标的，常见于行业调整后的价值修复机会，关联抄底策略与市场情绪指标。
    #     :功能:
    #         筛选出超跌股票
    #     :量化解释:
    #         股价低于过去60个交易日最低收盘价的70%
    #     """
    #     # 过去60个交易日最低收盘价子查询
    #     min_price_60_subquery = AStockGlobalQuery.calculate_min_close_price(60)

    #     # 今日收盘价
    #     today_close = AStockGlobalQuery.calculate_field_days_ago(AStockMarket.close, 0)

    #     return select(AStockMarket.secucode).where(
    #         today_close < min_price_60_subquery * 0.7
    #     )

    # @staticmethod
    # def calculate_min_close_price(days: int):
    #     """
    #     :功能:
    #         计算过去指定天数内的最低收盘价的子查询。
    #     :参数:
    #         days (int): 回溯天数。
    #     :量化解释:
    #         通过时间范围计算过去指定天数内的最低收盘价
    #     """
    #     return (
    #         select(
    #             func.min(AStockMarket.close),
    #         )
    #         .where(
    #             AStockMarket.date.between(
    #                 func.date("now", f"-{days} days"), func.date("now")  # 过去指定天数
    #             ),
    #             AStockMarket.secucode == col(AStockMarket.secucode),
    #         )
    #         .scalar_subquery()
    #     )

    # @staticmethod
    # def calculate_field_days_ago(field: Field, days: int):
    #     """
    #     :功能:
    #         筛选出AStockMarketg表中,多个交易日期前的field1_today值
    #     :参数:
    #         field(Field): 对应的数据表中需要比较的字段 如AStockMarket.close_price
    #         days(int): 回溯天数
    #     :量化解释:
    #         通过 回朔天数days 计算 AStockMarket表 中的 字段field 回朔天数个交易日期前的字段值
    #     """
    #     # 为内层查询创建表别名
    #     inner = aliased(AStockMarket)

    #     # 获取多个交易日期前数据（子查询）
    #     if days == 0:
    #         days_ago = func.date(AStockMarket.date, f"0d")
    #     elif days > 0:
    #         days_ago = func.date(AStockMarket.date, f"-{days}d")
    #     else:
    #         raise ValueError("days参数必须大于等于0")

    #     # 获取昨日的 value1（子查询）
    #     days_ago_value1 = (
    #         select(field)
    #         .where(
    #             inner.secucode == col(AStockMarket.secucode),
    #             inner.date == days_ago,
    #         )
    #         .scalar_subquery()
    #     )

    #     return days_ago_value1

    # @staticmethod
    # def query_cross_above(
    #     value1: Union[Field, float, int],
    #     value2: Union[Field, float, int],
    #     start_date: str = "0d",
    #     end_date: str = "0d",
    # ):
    #     """
    #     :术语名称:
    #         上穿
    #     :术语解释:
    #         上穿是股票技术分析术语，指的是一根K线从下向上穿越另一根K线的过程,用于识别趋势反转和买入机会，常见于均线系统应用，关联移动平均线和市场动态。
    #     :功能:
    #         通用交叉上穿筛选方法，支持多种字段类型
    #     :参数:
    #         value1(Union[Field, float]): 第一条线(字段或数值)
    #         value2(Union[Field, float]): 第二条线(字段或数值)
    #         start_date(str): 开始日期
    #         end_date(str): 结束日期
    #     :量化解释:
    #         今日上穿并且昨日未上穿
    #         今日上穿条件：今日第一条线 > 第二条线
    #         昨日未上穿条件：昨日第一条线 <= 第二条线
    #     """
    #     return NotImplementedError

    #     def value_expr(value):
    #         if isinstance(value, (float, int)):
    #             return literal(value)

    #         value_field_name = getattr(value, "key", str(value))
    #         if value_field_name.startswith("cyc"):
    #             days = int(value_field_name[3:])
    #             return func.sum(AStockMarket.amount).over(
    #                 partition_by=AStockMarket.secucode,
    #                 order_by=AStockMarket.date,
    #                 rows=(1 - days, 0),
    #             ) / func.sum(AStockMarket.volume).over(
    #                 partition_by=AStockMarket.secucode,
    #                 order_by=AStockMarket.date,
    #                 rows=(1 - days, 0),
    #             )
    #         elif value_field_name.startswith("ma"):
    #             days = int(value_field_name[2:])
    #             return func.avg(AStockMarket.close).over(
    #                 partition_by=AStockMarket.secucode,
    #                 order_by=AStockMarket.date,
    #                 rows=(1 - days, 0),
    #             )
    #         elif value_field_name.startswith("vol"):
    #             days = int(value_field_name[3:])
    #             return func.avg(AStockMarket.volume).over(
    #                 partition_by=AStockMarket.secucode,
    #                 order_by=AStockMarket.date,
    #                 rows=(1 - days, 0),
    #             )
    #         else:
    #             return value

    #     # 第一个CTE: 计算每日的指标值
    #     daily_values = select(
    #         AStockMarket.secucode.label("secucode"),
    #         AStockMarket.date.label("date"),
    #         value_expr(value1).label("value1"),
    #         value_expr(value2).label("value2"),
    #     ).subquery()

    #     # 第二个CTE: 获取前一日的数据
    #     cross_points = select(
    #         daily_values.c.secucode,
    #         daily_values.c.date,
    #         daily_values.c.value1,
    #         daily_values.c.value2,
    #         func.any(daily_values.c.value1)
    #         .over(
    #             partition_by=daily_values.c.secucode,
    #             order_by=daily_values.c.date,
    #             rows=(-1, -1),
    #         )
    #         .label("prev_value1"),
    #         func.any(daily_values.c.value2)
    #         .over(
    #             partition_by=daily_values.c.secucode,
    #             order_by=daily_values.c.date,
    #             rows=(-1, -1),
    #         )
    #         .label("prev_value2"),
    #     ).select_from(daily_values).subquery()

    #     # 最终查询: 筛选上穿条件
    #     result = (
    #         select(
    #             cross_points.c.secucode,
    #         )
    #         .select_from(cross_points)
    #         .where(
    #             (cross_points.c.value1 > cross_points.c.value2)  # 今日上穿
    #             & (
    #                 cross_points.c.prev_value1 < cross_points.c.prev_value2
    #             )  # 昨日未上穿
    #             & (
    #                 cross_points.c.date.between(
    #                     get_date(start_date, is_start=True),
    #                     get_date(end_date),
    #                 )
    #             )
    #         )
    #         .order_by(cross_points.c.date, cross_points.c.secucode)
    #     ).distinct()

    #     return result

    # @staticmethod
    # def query_opportunity_stock():
    #     """
    #     :术语名称:
    #         机会股
    #     :术语解释:
    #         机会指特定行业在政策、供需及市场周期下的投资潜力，如军工受国防预算、地缘局势及技术升级驱动，涉及装备制造和军民融合。
    #     :功能:
    #         筛选出机会股
    #     :量化解释:
    #         收盘价大于20日均线，20日均线大于60日均线，涨幅大于0.2
    #     """
    #     queryition = (
    #         AStockMarket.close
    #         > AStockGlobalQuery.calculate_average_line(AStockMarket.close, 20)
    #     ) & (
    #         AStockGlobalQuery.calculate_average_line(AStockMarket.close, 20)
    #         > AStockGlobalQuery.calculate_average_line(AStockMarket.close, 60)
    #     ) & AStockMarket.zdf > 0.2
    #     return select(AStockMarket.secucode).where(queryition)

    # @staticmethod
    # def query_abnormal_stock(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         异动股
    #     :术语解释:
    #         有异动的股票指价格或成交量突现异常波动的证券，常伴随重大事件披露、主力资金异动或市场情绪突变，需结合量价背离、板块联动等特征研判短期市场动向
    #     :功能:
    #         筛选出异动股票
    #     :量化解释:
    #         1.当日成交量 > 5日成交量平均线的2倍
    #         2.当日振幅 > 8%
    #         3.股价突破20日内最高点
    #         4.收盘涨幅 > 6%
    #     """
    #     # 1. 当日成交量 > 5日成交量平均线的2倍
    #     volume_gt_volume5 =  AStockGlobalQuery.query_volume_gt_avg(5, 2, start_date, end_date)

    #     # 2. 当日振幅 > 8%
    #     amplitude_gt_8 = AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_amplitude(),0.08, float("inf"), start_date, end_date)

    #     # 3. 股价突破20日内最高点
    #     break_high = AStockGlobalQuery.query_breakout_new_high_days(20)

    #     # 4. 收盘涨幅 > 6%
    #     pct_gt_6 = AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_zdf(),0.06, float("inf"), start_date, end_date)

    #     return intersect(volume_gt_volume5, amplitude_gt_8, break_high, pct_gt_6)

    # @staticmethod
    # def calculate_industry_average(field):
    #     """
    #     :功能:
    #         计算指定字段的行业平均值（子查询）
    #     :参数:
    #         field (Union[Field, str]): 需要计算行业平均值的字段对象或字段名称
    #     :量化解释:
    #         根据股票代码所属行业,计算该 字段field 的行业平均值
    #     """
    #     # 自动解析字段所属表
    #     if hasattr(field, "class_"):
    #         data_table = field.class_
    #         field_name = field.key
    #     else:
    #         raise ValueError(f"无效的字段: {field}")

    #     # 创建别名
    #     AliasData = aliased(data_table)
    #     AliasBasic = aliased(AStockBasic)

    #     # 构建行业匹配条件
    #     industry_match = AliasBasic.trade == (
    #         select(AStockBasic.trade)
    #         .where(AStockBasic.secucode == AliasData.secucode)
    #         .scalar_subquery()
    #     )

    #     # 计算行业平均值的子查询
    #     industry_avg = (
    #         select(func.avg(getattr(AliasData, field_name)).label("industry_avg"))
    #         .join(AliasBasic, AliasBasic.secucode == AliasData.secucode)
    #         .where(industry_match)
    #         .scalar_subquery()
    #     )

    #     return industry_avg

    # @staticmethod
    # def query_low_priced_stocks():
    #     """
    #     :术语名称:
    #         低价股
    #     :术语解释:
    #         低价股指交易价格显著低于市场平均水平的股票，具有低单价、小市值、高波动性特征，适用于小额资金分散投资或短期波段策略。其语义覆盖模糊价格区间（如‘低价’‘便宜股’）、相对估值（‘低于行业均值’）等场景，兼容用户未明确数值的区间表达（如‘价格很低’‘底部区域’），同时关联破净股、低市盈率等价值投资维度，捕捉价格洼地与潜在风险的双重属性。
    #     :功能:
    #         筛选出低价股票
    #     :量化解释:
    #         股价小于过去250个交易日的最低收盘价的1.2倍
    #     """
    #     min_close_price_250 = AStockGlobalQuery.calculate_min_close_price(250)

    #     return select(AStockMarket.secucode).where(
    #         AStockMarket.close < min_close_price_250 * 1.2
    #     )

    # @staticmethod
    # def query_not(field: Field):
    #     """
    #     :术语名称:
    #         非,剔除
    #     :术语解释
    #         非是股票筛选的核心逻辑术语，用于排除特定板块如科创板，常见于风险管理和投资组合优化，关联逻辑操作与市场分类规则。
    #     :功能:
    #         筛选出指定字段值为非的股票，如'非科创板'
    #     :参数:
    #         field(str): 字段名
    #     :量化解释:
    #         Boolean类型字段的非运算
    #     """
    #     # 获取字段类型
    #     try:
    #         col_type = field.property.columns[0].type
    #     except AttributeError:
    #         raise ValueError("输入必须是SQLAlchemy字段对象")

    #     # 验证是否为布尔类型
    #     if not isinstance(col_type, Boolean):
    #         raise ValueError(
    #             f"字段类型必须为Boolean，实际为{col_type.__class__.__name__}"
    #         )

    #     return select(AStockMarket.secucode).where(~field)

    # @staticmethod
    # def calculate_sealed_to_volume_ratio():
    #     """
    #     :功能:
    #         计算封单成交比
    #     :量化解释:
    #         封单量与成交量的比值
    #     """
    #     return NotImplementedError
    #     if AStockMarket.volume is None or AStockMarket.seal_order_volume is None:
    #         return False

    #     if AStockMarket.volume == 0:
    #         return False  # 避免除以零

    #     return AStockMarket.seal_order_volume / AStockMarket.volume

    # @staticmethod
    # def query_strongest_stocks():
    #     """
    #     :术语名称:
    #         最强股票
    #     :术语解释:
    #         最强股票是阶段内涨幅领先、成交活跃、资金集中流入的强势个股，常用于短线追涨与强势股轮动策略中。
    #     :功能:
    #         筛选出最强股票
    #     :量化解释:
    #         5日均线上穿10日均线，10日均线上穿20日均线，成交量大于20日平均成交量的2倍,macd金叉
    #     """
    #     volume_20 = AStockGlobalQuery.calculate_average_line(AStockMarket.volume, 20)
    #     return NotImplementedError
    #     # return intersect(
    #     #     AStockGlobalQuery.query_cross_above(AStockMarket.ma5, AStockMarket.ma10),
    #     #     AStockGlobalQuery.query_cross_above(AStockMarket.ma10, AStockMarket.ma20),
    #     #     select(AStockMarket.secucode).where(AStockMarket.volume > volume_20),
    #     #     select(AStockMarket.secucode).where(AStockMarket.macd_golden_cross),
    #     # )

    # @staticmethod
    # def query_short_term_potential_stocks(start_date: str = '0d', end_date: str = '0d'):
    #     """
    #     :术语名称:
    #         短线潜力股
    #     :术语解释:
    #         短线潜力股是具备短期上涨动能、技术形态良好、资金异动频繁的热门个股，常用于捕捉快速收益机会。
    #     :功能:
    #         筛选出短线潜力股
    #     :量化解释:
    #         1.成交量＞5日平均成交量
    #         2.股价创5日新高；
    #         3.股价＞MA20日均线
    #         4.MACD金叉
    #         5.上市时间＞100天
    #     """
    #     return NotImplementedError
    #     # 成交量大于5日平均成交量
    #     volume_5 = AStockGlobalQuery.query_volume_gt_avg(5, 1, start_date=start_date, end_date=end_date)
    #     # 股价创5日新高
    #     high_5 = AStockGlobalQuery.query_breakout_new_high_days(5, start_date, end_date)
    #     # 股价大于MA20日均线
    #     price_gt_ma20 = AStockGlobalQuery.query_price_in_range(AStockMarket.ma20, float('inf'), start_date, end_date)
    #     # MACD金叉
    #     macd_golden_cross = AStockGlobalQuery.query_macd_golden_cross(start_date, end_date)
    #     # 上市时间大于100天
    #     listed_time = (
    #         select(AStockBasic.secucode)
    #         .where(
    #             func.match(AStockBasic.issue_list_date, r'^\d{4}-\d{2}-\d{2}$'),
    #             func.toDate(AStockBasic.issue_list_date) <= func.today() - text("100")
    #         )
    #     )
    #     return intersect(volume_5, high_5, price_gt_ma20, macd_golden_cross, listed_time)

    # @staticmethod
    # def query_turning_upward(days: int, start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         n日均线拐头向上
    #     :术语解释:
    #         拐头向上指股价趋势由平缓转为上升，适用于趋势交易与技术分析。
    #     :功能:
    #         筛选出拐头向上趋势的股票
    #     :参数:
    #         days (int): n
    #         start_date (str): 开始日期
    #         end_date (str): 结束日期
    #     :量化解释:
    #         当日n日均线大于前一天的n日均线，并且前一日n日均线小于前前日的n日均线
    #     """
    #     ma_daily_values = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         AStockMarket.close,
    #         func.avg(AStockMarket.close)
    #         .over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(1 - days, 0),
    #         )
    #         .label(f"ma"),
    #     )

    #     ma_points = select(
    #         ma_daily_values.c.secucode,
    #         ma_daily_values.c.date,
    #         ma_daily_values.c.ma.label("today_ma"),
    #         func.any(ma_daily_values.c.ma)
    #         .over(
    #             partition_by=ma_daily_values.c.secucode,
    #             order_by=ma_daily_values.c.date,
    #             rows=(-1, -1),
    #         )
    #         .label("yes_ma"),
    #         func.any(ma_daily_values.c.ma)
    #         .over(
    #             partition_by=ma_daily_values.c.secucode,
    #             order_by=ma_daily_values.c.date,
    #             rows=(-2, -2),
    #         )
    #         .label("before_yes_ma"),
    #     ).select_from(ma_daily_values)

    #     return select(ma_points.c.secucode).where(
    #         (ma_points.c.yes_ma < ma_points.c.before_yes_ma)
    #         & (ma_points.c.today_ma > ma_points.c.yes_ma)
    #         & (
    #             ma_points.c.date.between(
    #                 get_date(start_date, is_start=True), get_date(end_date)
    #             )
    #         )
    #     )

    # @staticmethod
    # def query_recent_main_fund_ratio(days: int, min_ratio: float):
    #     """
    #     :术语名称:
    #         主力净流入
    #     :术语解释:
    #         拐头向上指股价趋势由平缓转为上升，适用于趋势交易与技术分析。
    #     :功能:
    #         近 N 日主力净流入比例平均值大于某个阈值
    #     :参数:
    #         days (int): 统计的交易日数量，例如传入 5 表示最近 5 个交易日（不含当天）。
    #         min_ratio (float): 最低主力净流入比例阈值，例如 0.02 表示近 N 日主力资金日均净流入比例需大于 2%。
    #     :量化解释:
    #         子查询统计近 N 日主力净流入比例平均值,  判断平均值是否大于阈值
    #     """
    #     recent_avg = (
    #         select(func.avg(AStockMarket.zhu))
    #         .where(
    #             AStockMarket.secucode == col(AStockMarket.secucode),
    #             AStockMarket.date.between(
    #                 func.date("now", f"-{days}d"), func.date("now", "-1d")
    #             ),
    #         )
    #         .scalar_subquery()
    #     )
    #     return select(AStockMarket.secucode).where(recent_avg > min_ratio)

    # @staticmethod
    # def query_main_force_accumulation(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         主力建仓
    #     :术语解释:
    #         主力建仓指机构资金在低位分批吸筹，常出现在半导体等板块启动前，预示后续上涨潜力。
    #     :功能:
    #         筛选出主力建仓的股票
    #     :参数:
    #         start_date (str): 开始日期
    #         end_date (str): 结束日期
    #     :量化解释:
    #         当日成交量＞20日平均成交量 且 单日涨幅>5% 且 MA5日线上穿MA30日线 且 连续5日涨幅＞0
    #     """
    #     return NotImplementedError
    #     cond1 = AStockGlobalQuery.query_volume_gt_avg(20, 1, start_date, end_date)

    #     cond2 = AStockGlobalQuery.query_price_change_in_range(
    #         0.05, 200, start_date, end_date
    #     )

    #     cond3 = AStockGlobalQuery.query_cross_above(
    #         AStockMarket.ma5, AStockMarket.ma30, start_date, end_date
    #     )

    #     consecutive_zdf = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         func.count(case((AStockMarket.zdf > 0, 1)))
    #         .over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(-4, 0),
    #         )
    #         .label("cnt_positive_zdf_5"),
    #     ).subquery()

    #     cond4 = (
    #         select(consecutive_zdf.c.secucode)
    #         .select_from(consecutive_zdf)
    #         .where(
    #             (consecutive_zdf.c.cnt_positive_zdf_5 == 5)
    #             & (
    #                 consecutive_zdf.c.date.between(
    #                     get_date(start_date, is_start=True),
    #                     get_date(end_date),
    #                 )
    #             )
    #         )
    #     ).distinct()
    #     return intersect(cond1, cond2, cond3, cond4)

    # @staticmethod
    # def query_first_daily_limit_up(start_date: str = '0d', end_date: str = '0d'):
    #     """
    #     :术语名称:
    #         首板涨停
    #     :术语解释:
    #         首板涨停指某股票在特定交易日内首次达到涨幅上限（如10%/20%），其特征包含首次触发涨停机制、伴随显著放量及市场关注度跃升。该术语常用于捕捉个股启动行情，覆盖场景包括一段时间内首次涨停识别、技术突破信号判断、主力资金介入迹象分析，以及与连板走势的差异性对比。语义强化了时间范围限定（如'近一个月首次'）、价格波动阈值、量价关系突变等核心要素，可匹配'第一次涨停何时出现''首板后如何操作'等泛化提问。
    #     :功能:
    #         筛选出首板涨停股票
    #     :量化解释:
    #         如果问时间点，则返回20天内，只有该时间点涨停一次的股票；如果是时间段，则返回该时间段内最后一天涨停的股票
    #         比如：近20天首板涨停的股票：20天内只有今天涨停
    #         比如：5.21首板涨停的股票：5.21向前推20天，只有5.21涨停一次的股票
    #     """
    #     # 默认算近20天首板涨停
    #     days = 20

    #     # 如果用户问近n天、近n月，则算n天首板涨停
    #     if start_date != end_date:
    #         try:
    #             value = int(start_date[:-1])
    #             unit = start_date[-1].lower()
    #         except (ValueError, IndexError):
    #             raise ValueError("日期参数格式错误，应为nX格式（如1d、-2m等）")
    #         if unit == 'd':
    #             days = abs(value)
    #         elif unit == 'm':
    #             days = abs(value) * 30

    #     no_zt = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         AStockMarket.zt,
    #         func.sum(AStockMarket.zt)
    #         .over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(1-days, -1),
    #         )
    #         .label(f"no_zt"),
    #     )
    #     return select(
    #         no_zt.c.secucode
    #     ).where(no_zt.c.date == get_date(end_date), no_zt.c.zt == 1, no_zt.c.no_zt == 0).distinct()

    # @staticmethod
    # def query_short_term_opening_report(start_date: str = '0d', end_date: str = '0d'):
    #     """
    #     :术语名称:
    #         短线开报告
    #     :术语解释:
    #         短线开报告指基于短期市场动态或突发行业事件发布的交易指引，聚焦时效性价格波动，常用于航运等事件驱动型板块，关联技术信号与资金流向监测。
    #     :功能:
    #         筛选出短线开报告的股票
    #     :量化解释:
    #         1.当日成交量＞20日平均成交量
    #         2.收盘价上穿MA20日均线
    #         3.5日涨幅＞15%
    #         4.主力资金净流入连续3日＞0
    #     """
    #     return NotImplementedError
    #     # 当日成交量＞20日平均成交量
    #     volume_gt_20_avg = AStockGlobalQuery.query_volume_gt_avg(20, 1, start_date, end_date)

    #     # 收盘价上穿MA20日均线
    #     close_price_cross_ma20 = AStockGlobalQuery.query_cross_above(AStockMarket.close, AStockMarket.ma20, start_date, end_date)

    #     # 5日涨幅＞15%
    #     zdf_daily_query = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         func.sum(AStockMarket.zdf)
    #         .over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(-4, 0),
    #         )
    #         .label("sum_zdf_5")
    #     ).subquery()
    #     zdf_gt_15_percent = select(zdf_daily_query.c.secucode).where(zdf_daily_query.c.sum_zdf_5 > 0.15, zdf_daily_query.c.date.between(get_date(start_date, is_start=True), get_date(end_date))).distinct()

    #     # 主力资金净流入连续3日＞0
    #     consecutive_main_fund = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         func.count(case((AStockMarket.zhu > 0, 1)))
    #         .over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(-2, 0),
    #         )
    #         .label("cnt_positive_main_fund_3"),
    #     ).subquery()

    #     main_fund_consecutive = (
    #         select(consecutive_main_fund.c.secucode)
    #         .select_from(consecutive_main_fund)
    #         .where(
    #             (consecutive_main_fund.c.cnt_positive_main_fund_3 >= 3)
    #             & (
    #                 consecutive_main_fund.c.date.between(
    #                     get_date(start_date, is_start=True),
    #                     get_date(end_date),
    #                 )
    #             )
    #         )
    #     ).distinct()
    #     return intersect(volume_gt_20_avg, close_price_cross_ma20, zdf_gt_15_percent, main_fund_consecutive)

    # @staticmethod
    # def query_capital_infow_and_increasing_position(start_date: str = '0d', end_date: str = '0d'):
    #     """
    #     :术语名称:
    #         主力资金、资金流入增仓
    #     :术语解释:
    #         主力资金指对股价有重大影响的大额机构资金，其净流入情况是识别热门个股的关键指标，常用于追踪资金流入前十名个股、捕捉短线爆发机会等场景，与资金流向、机构动向等概念直接关联。
    #     :功能:
    #         筛选出主力资金、资金流入增仓的股票
    #     :量化解释:
    #         主力资金净流入率>0
    #     """
    #     return select(AStockMarket.secucode).where(
    #         AStockMarket.zhu > 0,
    #         AStockMarket.date.between(
    #                 get_date(start_date, is_start=True), get_date(end_date)
    #             ))

    # @staticmethod
    # def query_limit_up_count(
    #     count_low: Union[int, float] = float("-inf"),
    #     count_high: Union[int, float] = float("inf"),
    #     start_date: str = '0d',
    #     end_date: str = '0d'
    # ):
    #     """
    #     :术语名称:
    #         涨停次数在范围内
    #     :术语解释:
    #         涨停次数/个数在范围内是量化选股的关键筛选条件，其专业定义为股票在指定时间周期内触及涨停板的价格波动次数是否符合预设阈值。该术语通过等于（如『涨停次数恰好3次』）、涨停次数大于（＞5次）、涨停次数小于（＜2次）、涨停次数区间包含（介于2-4次）及临界值匹配（涨停次数至少/不超过X次）等逻辑运算符构建动态过滤规则，核心特征涵盖多时间维度统计（日/周/月）、动态窗口期适配（如『过去10个交易日』）及异常频次预警机制。在应用场景中支撑高频策略回测（如『连续3日涨停』识别）、主力资金动向分析（『月内涨停超8次个股』）以及波动率衍生指标计算，常与连板天数、封单金额、换手率等指标形成组合策略。其语义覆盖精确数值匹配、开放式区间（涨停次数＞=X）、封闭式区间（X≤涨停次数≤Y）三类场景，并能解析『以上/以下』（如『涨停次数5次以上』）、『以内/之外』（如『涨停次数3次以内』）等自然语言表达，确保对『最近一周涨停超过3次』『本月涨停次数在2到5次之间』等复杂查询意图的精准响应。
    #     :功能:
    #         筛选指定日期范围内累计涨停次数符合要求的股票
    #     :参数:
    #         count_low(int): 涨停次数最低阈值
    #         count_high(int): 涨停次数最高阈值
    #         start_date(str): 开始日期
    #         end_date(str): 结束日期
    #     :量化解释:
    #         涨停次数>=count_low, 涨停次数<=count_high
    #     """
    #     main_query = (
    #         select(AStockMarket.secucode)
    #         .where(
    #             AStockMarket.date.between(
    #                 get_date(start_date, is_start=True), get_date(end_date)
    #             ),
    #             AStockMarket.zt == 1,
    #         )
    #         .group_by(AStockMarket.secucode)
    #         .having((func.count() >= count_low) & (func.count() <= count_high))
    #     ).distinct()
    #     return main_query

    # @staticmethod
    # def query_multi_ma_breakout(value: int, start_date: str = '-5d', end_date: str = '0d'):
    #     """
    #     :术语名称:
    #         一阳穿n线
    #     :术语解释:
    #         一阳穿n线是股票技术形态，由单根阳线突破多条均线，伴随成交量放大，反映多方强势，用于短期趋势判断与买入时机参考，关联均线系统及支撑压力位。
    #     :功能:
    #         筛选出一阳穿n线的股票
    #     :参数:
    #         value(int): 上穿数量（常见数量为2、3、4、5、6）
    #     :量化解释:
    #         穿3线: 收盘价>MA5, 收盘价>MA10, 收盘价>MA20
    #         穿4线: 收盘价>MA5, 收盘价>MA10, 收盘价>MA20, 收盘价>MA60
    #         穿5线: 收盘价>MA5, 收盘价>MA10, 收盘价>MA20, 收盘价>MA60, 收盘价>MA120
    #         穿6线: 收盘价>MA5, 收盘价>MA10, 收盘价>MA20, 收盘价>MA60, 收盘价>MA120, 收盘价>MA250
    #     """
    #     if not 2 <= value <= 6:
    #         return select(AStockMarket.secucode).where(False)

    #     # 根据value值确定需要的均线周期
    #     required_periods = {
    #         2: [5, 10],
    #         3: [5, 10, 20],
    #         4: [5, 10, 20, 60],
    #         5: [5, 10, 20, 60, 120],
    #         6: [5, 10, 20, 60, 120, 250]
    #     }[value]

    #     # 构建包含所有必要均线的子查询
    #     ma_subquery = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         AStockMarket.close,
    #         AStockMarket.open,
    #         AStockMarket.low,
    #         *[
    #             func.avg(AStockMarket.close).over(
    #                 partition_by=AStockMarket.secucode,
    #                 order_by=AStockMarket.date,
    #                 rows=(1-period, 0)  # 当前日及前period-一日
    #             ).label(f'ma{period}')
    #             for period in required_periods
    #         ]
    #     ).where(
    #         AStockMarket.date.between(
    #             get_date(start_date, is_start=True, lookback_days = required_periods[-1]),
    #             get_date(end_date)
    #             )
    #     ).subquery()

    #     # 构建突破条件
    #     conditions = [
    #         ma_subquery.c.close > ma_subquery.c[f'ma{period}']
    #         for period in required_periods
    #     ]

    #     # 构建最低价小于
    #     conditions_low = [
    #         ma_subquery.c.low < ma_subquery.c[f'ma{period}']
    #         for period in required_periods
    #     ]

    #     result = select(
    #         ma_subquery.c.secucode,
    #     ).select_from(ma_subquery).where(
    #         ma_subquery.c.close > ma_subquery.c.open,
    #         and_(*conditions),
    #         and_(*conditions_low),
    #         (ma_subquery.c.date.between(
    #             get_date(start_date, is_start=True),
    #             get_date(end_date)
    #         ))
    #     )

    #     return result

    # @staticmethod
    # def query_strong_performance_in_sector_rotation():
    #     """
    #     :术语名称:
    #         轮动强势
    #     :术语解释:
    #         轮动强势股指行业轮动中资金集中涌入、短期领涨且技术面强势的个股，反映市场热点迁移规律，常用于捕捉轮动行情中的超额收益波段，关联资金流向与板块轮动节奏。
    #     :功能:
    #         筛选出轮动强势股票
    #     :量化解释:
    #         收盘价大于20日均线，收盘价大于前一天的收盘价，成交量大于5日平均成交量
    #     """
    #     ma_20 = AStockGlobalQuery.calculate_average_line(AStockMarket.close, 20)
    #     volume_5 = AStockGlobalQuery.calculate_average_line(AStockMarket.volume, 5)
    #     return select(AStockMarket.secucode).where(
    #         (AStockMarket.close > ma_20)
    #         & (
    #             AStockMarket.close
    #             > AStockGlobalQuery.calculate_field_days_ago(AStockMarket.close, 1)
    #         )
    #         & (AStockMarket.volume > volume_5)
    #     )

    # @staticmethod
    # def query_call_auction():
    #     """
    #     :术语名称:
    #         集合竞价
    #     :术语解释:
    #         集合竞价是股市开盘前的订单撮合机制，投资者通过它抢筹沪深主板股票以形成开盘价，用于价格发现和流动性管理，关联成交量波动和交易策略。
    #     :功能:
    #         筛选出集合竞价股票
    #     :量化解释:
    #         今天的开盘价大于前一天的收盘价
    #     """
    #     prev_close_price = AStockGlobalQuery.calculate_field_days_ago(
    #         AStockMarket.close, 1
    #     )
    #     return select(AStockMarket.secucode).where(
    #         AStockMarket.open > prev_close_price
    #     )

    # @staticmethod
    # def query_aggressive_accumulation():
    #     """
    #     :术语名称:
    #         抢筹
    #     :术语解释:
    #         抢筹指市场资金受利好驱动集中买入，常见于集合竞价阶段，伴随高成交与股价异动，关联主力动向及沪深主板交易热度。
    #     :功能:
    #         筛选出抢筹的股票
    #     :量化解释:
    #         今天的成交量比过去五天的平均成交量高出50%以上，且今天的收盘价比开盘价高
    #     """
    #     volume_5 = AStockGlobalQuery.calculate_average_line(AStockMarket.volume, 5)
    #     volume_queryition = AStockMarket.volume > volume_5 * 1.5

    #     price_queryition = (
    #         AStockMarket.close
    #         > AStockGlobalQuery.calculate_field_days_ago(AStockMarket.close, 1)
    #     )

    #     return select(AStockMarket.secucode).where(volume_queryition & price_queryition)

    # @staticmethod
    # def query_main_fund_accumulation():
    #     """
    #     :术语名称:
    #         主力连续增仓
    #     :术语解释:
    #         主力连续增仓指主力资金持续流入特定股票，表现为机构持仓递增与筹码集中度变化，常用于监测资金异动及预判市场趋势的信号指标。
    #     :功能:
    #         筛选出主力连续增仓的股票
    #     :量化解释:
    #         连续3天主力资金大于0
    #     """
    #     return NotImplementedError
    #     three_days_ago = (datetime.now() - datetime.timedelta(days=3)).strftime("%Y-%m-%d")

    #     # 子查询：统计每只股票在最近3天内主力资金>0的天数
    #     query = (
    #         select(AStockMarket.secucode)
    #         .where(
    #             AStockMarket.zhu > 0, AStockMarket.date >= three_days_ago
    #         )
    #         .group_by(AStockMarket.secucode)
    #         .having(func.count() >= 3)
    #     )

    #     return query

    # @staticmethod
    # def calculate_volume_ratio():
    #     """
    #     :功能:
    #         计算量比

    #     :量化解释:
    #         量比 = 当前成交量 / 过去5天平均成交量

    #     """
    #     # 为内层查询的表定义别名
    #     sub_astockmarket = aliased(AStockMarket, name="sub_astockmarket")

    #     # 计算过去5天的平均成交量
    #     avg_volume_subquery = (
    #         select(func.avg(sub_astockmarket.volume))
    #         .where(
    #             sub_astockmarket.secucode == AStockMarket.secucode,  # 关联外层查询的股票代码
    #             sub_astockmarket.date.between(
    #                 func.date("now", "-5d"), func.date("now", "-1d")  # 5天前  # 昨天
    #             ),
    #         )
    #         .correlate(AStockMarket)  # 明确关联外层查询
    #         .scalar_subquery()
    #     )

    #     # 当前成交量与过去5天平均成交量的比值
    #     volume_ratio = AStockMarket.volume / avg_volume_subquery

    #     return volume_ratio

    # @staticmethod
    # def query_upward_trend():
    #     """
    #     :术语名称:
    #         上行
    #     :术语解释:
    #         上行指价格走势持续上涨、表现强劲的个股，常受资金青睐，适合趋势跟踪与短中线操作。
    #     :功能:
    #         筛选出上行的股票
    #     :量化解释:
    #         当天收盘价大于前一天收盘价
    #     """
    #     return select(AStockMarket.secucode).where(
    #         AStockGlobalQuery.calculate_field_days_ago(AStockMarket.close, 0)
    #         > AStockGlobalQuery.calculate_field_days_ago(AStockMarket.close, 1)
    #     )

    # @staticmethod
    # def query_large_amplitude():
    #     """
    #     :术语名称:
    #         振幅大
    #     :术语解释:
    #         振幅大指价格波动剧烈，具备高风险高收益特征，适合短线交易和投机操作。
    #     :功能:
    #         筛选出振幅大的股票
    #     :量化解释:
    #         振幅大于0.05
    #     """
    #     return select(AStockMarket.secucode).where(AStockMarket.amplitude > 0.05)

    # @staticmethod
    # def query_undervalued():
    #     """
    #     :术语名称:
    #         低估值
    #     :术语解释:
    #         低估值指市盈率、市净率等指标处于较低水平，具备价值投资吸引力，常见于业绩稳定但被市场忽视的板块。
    #     :功能:
    #         筛选出低估值的股票
    #     :量化解释:
    #         市盈率小于10,市净率小于1.5
    #     """
    #     return NotImplementedError

    #     if AStockMarket.pe_ratio is None or AStockMarket.pb_ratio is None:
    #         return False

    #     return select(AStockMarket.secucode).where(
    #         (AStockMarket.pe_ratio < 10) & (AStockMarket.pb_ratio < 1.5)
    #     )

    # @staticmethod
    # def query_breakout(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         突破平台
    #     :术语解释:
    #         突破平台指股价成功突破长期横盘整理区域，伴随成交量放大，预示趋势启动与后市上涨机会。
    #     :功能:
    #         筛选出突破平台的股票
    #     :量化解释:
    #         1.当日收盘价＞20日最高价
    #         2.前一交易日收盘价≤20日最高价
    #         3.成交量≥5日成交量平均值1.2倍
    #     """
    #     # 第一个CTE: 计算每日的指标值
    #     daily_values = select(
    #         AStockMarket.secucode.label("secucode"),
    #         AStockMarket.date.label("date"),
    #         AStockMarket.close,
    #         func.max(AStockMarket.close).over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(-19, 0),
    #         ).label("max_20_price"),
    #     ).subquery()

    #     # 第二个CTE: 获取前一日的数据
    #     cross_points = select(
    #         daily_values.c.secucode,
    #         daily_values.c.date,
    #         daily_values.c.close,
    #         daily_values.c.max_20_price,
    #         func.any(daily_values.c.close)
    #         .over(
    #             partition_by=daily_values.c.secucode,
    #             order_by=daily_values.c.date,
    #             rows=(-1, -1),
    #         )
    #         .label("prev_close_price"),
    #         func.any(daily_values.c.max_20_price)
    #         .over(
    #             partition_by=daily_values.c.secucode,
    #             order_by=daily_values.c.date,
    #             rows=(-1, -1),
    #         )
    #         .label("prev_max_20_price"),
    #     ).select_from(daily_values).subquery()

    #     cross_above = (
    #         select(
    #             cross_points.c.secucode,
    #         )
    #         .select_from(cross_points)
    #         .where(
    #             (cross_points.c.close > cross_points.c.max_20_price)
    #             & (
    #                 cross_points.c.prev_close_price < cross_points.c.prev_max_20_price
    #             )
    #             & (
    #                 cross_points.c.date.between(
    #                     get_date(start_date, is_start=True),
    #                     get_date(end_date),
    #                 )
    #             )
    #         )
    #     ).distinct()

    #     # 成交量≥5日成交量平均值1.2倍
    #     volume_gt_5 = AStockGlobalQuery.query_volume_gt_avg(5, 1.2, start_date, end_date)

    #     return intersect(cross_above, volume_gt_5)

    # @staticmethod
    # def get_date_offset(offset: int):
    #     """
    #     获取相对于当前日期的第N个交易日
    #     :param offset: 偏移量 (0=当天, 1=前1个交易日, 2=前2个交易日...)
    #     :return: 返回对应交易日的子查询
    #     """
    #     return (
    #         select(AStockMarket.date)
    #         .where(AStockMarket.secucode == "SHHQ000001")  # 使用上证指数作为基准
    #         .order_by(AStockMarket.date.desc())
    #         .limit(1)
    #         .offset(offset)
    #         .scalar_subquery()
    #     )

    # @staticmethod
    # def calculate_average_line(field: str, days: int, days_ago: int = 0):
    #     """
    #     :功能:
    #         计算指定天数的均线的股票(均线、成本均线、均量线)
    #     :参数:
    #         field(Field): 均线的字段名,如AStockMarket.ma60
    #         days(int): 均线天数
    #         days_ago(int): 历史均线参数，默认0，即当日均线，如为1,则为昨日均线
    #     :量化解释:
    #         过去days天的field的均值
    #     """
    #     return NotImplementedError
    #     # 获取字段名（用于识别计算类型）
    #     field_name = getattr(field, "key", str(field))

    #     calculation = None
    #     # 查找匹配的计算规则
    #     if days == 0:
    #         calculation = None
    #         for prefix, func_builder in CALCULATION_MAP.items():
    #             if field_name.startswith(prefix):
    #                 # 提取天数（如cyc5 -> 5）
    #                 try:
    #                     calc_days = int(field_name[len(prefix) :])
    #                     days = calc_days
    #                     calculation = func_builder(days)
    #                     break
    #                 except (ValueError, IndexError):
    #                     continue

    #     # 如果没有找到特定规则，使用默认的简单平均
    #     if calculation is None:
    #         calculation = func.avg(field)

    #     # 获取交易日期范围
    #     end_date = AStockGlobalQuery.get_date_offset(days_ago)
    #     start_date = AStockGlobalQuery.get_date_offset(days_ago + days - 1)

    #     inner = aliased(AStockMarket)
    #     return (
    #         select(calculation)
    #         .where(
    #             inner.secucode == col(AStockMarket.secucode),
    #             inner.date.between(start_date, end_date),
    #         )
    #         .scalar_subquery()
    #     )

    # @staticmethod
    # def query_heavy_loser(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         大面股
    #     :术语解释:
    #         大面股指筹码集中度高且股价波动剧烈的个股，易受主力操控，适合短线资金博弈和高风险操作。
    #     :功能:
    #         筛选出大面股,即大幅下跌的股票
    #     :量化解释:
    #         跌幅超过10%的股票列表
    #     """
    #     return select(AStockMarket.secucode).where(AStockMarket.zdf < -0.1, AStockMarket.date.between(get_date(start_date, is_start=True), get_date(end_date)))

    # @staticmethod
    # def query_strong_stocks():
    #     """
    #     :术语名称:
    #         强票股
    #     :术语解释:
    #         强票股指市场表现强劲、成交活跃且具较高上涨潜力的个股，常为资金重点关注和追捧对象。
    #     :功能:
    #         筛选出强票股
    #     :量化解释:
    #         涨幅大于5%且成交量大于5日平均成交量的1.5倍
    #     """
    #     volume_5 = AStockGlobalQuery.calculate_average_line(AStockMarket.volume, 5)
    #     return select(AStockMarket.secucode).where(
    #         (AStockMarket.zdf > 0.05) & (AStockMarket.volume > volume_5 * 1.5)
    #     )

    # @staticmethod
    # def query_heavy_volume(start_date: str = '0d', end_date: str = '0d'):
    #     """
    #     :术语名称:
    #         放量股票
    #     :术语解释:
    #         放量股票指成交量显著高于历史均值的证券品种，核心特征包含成交额激增伴随价格异动、技术形态突破（如平台/底部/低位突破）、时间窗口限定（如近5日/15日周期）及事件驱动要素（如连续涨停）。该术语涵盖量价共振现象，既包含底部反转阶段的低位放量，也涉及上升通道中的突破性增量，通过多维参数识别成交量异常波动与价格趋势的耦合关系，适配不同市场位置（底部/高位）与时间维度的量能分析需求。
    #     :功能:
    #         筛选放量股票,即大量成交的股票
    #     :量化解释:
    #         当日成交量 > 5日平均成交量的1.8倍
    #     """
    #     return AStockGlobalQuery.query_volume_gt_avg(5, 1.8, start_date, end_date)

    # @staticmethod
    # def query_low_level(start_date: str = '0d', end_date: str = '0d'):
    #     """
    #     :术语名称:
    #         低位
    #     :术语解释:
    #         低位股票指价格处于历史波动区间相对底部区域、估值修复空间较大的标的，其核心特征包含成交量异动（如低位放量）、技术形态突破（如平台整理后的量价齐升）以及资金面信号（如主力资金持续流入）。该术语关联底部反转、超跌反弹等场景，常与‘放量突破’‘底部堆量’‘价格平台’等形态组合出现，并可通过时间窗口（如近5日、15日内）叠加涨停次数、资金流入强度等动态指标进行筛选。语义覆盖‘地位’‘底部’等近义表达，同时兼容量价共振、资金埋伏、估值洼地等关联维度，确保对用户查询中涉及价格位置、技术信号、时间周期及潜力特征的多元需求实现精准映射。
    #     :功能:
    #         筛选出低位/底部/股票
    #     :量化解释:
    #         股价≤近30日最低价的1.2倍
    #     """
    #     min_close_price_30 = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         AStockMarket.close,
    #         func.min(AStockMarket.close)
    #         .over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(-29, 0),
    #         )
    #         .label("min_close_price_30"),
    #     ).subquery()

    #     close_lt_30_min = select(min_close_price_30.c.secucode).where(
    #         min_close_price_30.c.date.between(
    #             get_date(start_date, is_start=True), get_date(end_date)
    #         ),
    #         min_close_price_30.c.close <= min_close_price_30.c.min_close_price_30 * 1.2,
    #     ).distinct()
    #     return close_lt_30_min

    # @staticmethod
    # def query_sustained_increase(start_date: str = '0d', end_date: str = '0d'):
    #     """
    #     :术语名称:
    #         持续上涨
    #     :术语解释:
    #         持续上涨指股票价格受积极因素推动连续攀升，具备趋势稳定、交易活跃等特征，常用于趋势投资及市场情绪分析。
    #     :功能:
    #         筛选出持续上涨的股票
    #     :参数:
    #         start_date (str): 开始日期
    #         end_date (str): 结束日期
    #     :量化解释:
    #         连涨天数大于等于五天且成交量连增天数大于等于五天
    #     """
    #     return intersect(
    #         AStockGlobalQuery.query_consecutive_up_day(5,None,start_date,end_date),
    #         AStockGlobalQuery.query_consecutive_volume_up_day(5,None,start_date,end_date),
    #     )

    # @staticmethod
    # def calculate_continuous_increase(
    #     field: Field, days: int, include_today: bool = True
    # ) -> BinaryExpression:
    #     """
    #     :功能:
    #         计算指定字段是否连续N天增加
    #     :参数:
    #         field (Field): 要检查的字段
    #         days (int): 连续增加的天数
    #         include_today (bool): 是否包含今天的数据（默认为True）
    #     :量化解释:
    #         检查指定字段是否连续N天单调递增
    #     :示例:
    #         # zdf连续5天增加
    #         queryition = AStockGlobalQuery.calculate_continuous_increase(
    #             AStockMarket.zdf,
    #             5
    #         )
    #     """
    #     # 构建连续增长条件
    #     queryitions = []
    #     end = days if include_today else days - 1

    #     for i in range(end):
    #         # 获取i天前的字段值
    #         current_value = AStockGlobalQuery.calculate_field_days_ago(field, i)
    #         # 获取i+1天前的字段值
    #         prev_value = AStockGlobalQuery.calculate_field_days_ago(field, i + 1)
    #         # 添加增长条件
    #         queryitions.append(current_value > prev_value)

    #     return and_(*queryitions)

    # @staticmethod
    # def query_continuous_institutional_accumulation(days: int):
    #     """
    #     :术语名称:
    #         机构连续n天增仓
    #     :术语解释:
    #         机构连续n天增仓指机构投资者持续增加持仓的主动行为，反映资金动向与市场信心，常用于筛选主力看好标的，关联持仓周期、融资变化与中长期趋势研判
    #     :功能:
    #         筛选出机构连续n天增仓的股票(一个月最多算20个交易日)
    #     :参数:
    #         days (int): 连续增仓天数。
    #     :量化解释:
    #         机构持仓比例连续n期增长
    #     """
    #     return NotImplementedError
    #     increase_queryition = AStockGlobalQuery.calculate_continuous_increase(
    #         AStockMarket.institution_holding_ratio, days
    #     )

    #     return (
    #         select(AStockMarket.secucode)
    #         .where(
    #             increase_queryition,
    #             AStockMarket.institution_holding_ratio > 0,  # 确保当前持仓为正
    #         )
    #         .group_by(AStockMarket.secucode)
    #     )

    # @staticmethod
    # def query_major_uptrend():
    #     """
    #     :术语名称:
    #         主升浪
    #     :术语解释:
    #         主升浪指股价上升趋势中量价配合的主要上涨阶段，用于技术分析捕捉强势股持续趋势及判断市场强度与入场时机。
    #     :功能:
    #         筛选出主升浪的股票
    #     :量化解释:
    #         收盘价大于20日均线，20日均线大于60日均线，涨幅大于0.3
    #     """
    #     ma_20 = AStockGlobalQuery.calculate_average_line(AStockMarket.close, 20)
    #     ma_60 = AStockGlobalQuery.calculate_average_line(AStockMarket.close, 60)
    #     return select(AStockMarket.secucode).where(
    #         (AStockMarket.close > ma_20)
    #         & (AStockMarket.close > ma_60)
    #         & (AStockMarket.zdf > 0.3)
    #     )

    # @staticmethod
    # def query_capital_entry(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         资金入场
    #     :术语解释:
    #         资金入场指大额资金流入推动交易活跃的市场行为，特征为成交量放大与股价低位蓄势，常用于挖掘潜力股投资机会，关联市场情绪、主力动向及价值洼地等核心维度。
    #     :功能:
    #         筛选出资金入场的股票
    #     :参数:
    #         start_date (str): 开始日期。
    #         end_date (str): 结束日期。
    #     :量化解释:
    #         成交量≥近20日平均成交量的5倍
    #     """
    #     return AStockGlobalQuery.query_volume_gt_avg(20, 5, 0, start_date, end_date)

    # @staticmethod
    # def query_potential_stock(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         潜力股
    #     :术语解释:
    #         潜力股指当前估值较低但具备高成长性与增值空间的股票，核心特征包括行业前景向好、财务指标稳健、市场关注度提升及被低估的价值潜力。适用于长期投资策略，需结合基本面分析、行业周期评估及市场情绪判断，识别具备持续盈利改善、技术突破或政策利好的标的，覆盖用户对‘潜力’相关形容词的搜索场景，如寻找被低估资产、评估增长动能及预测中长期回报，同时关联价值洼地、黑马标的、成长性筛选等语义维度。
    #     :功能:
    #         筛选出潜力股
    #     :量化解释:
    #         MA均线多头排列
    #     """
    #     return select(AStockMarket.secucode).where(AStockMarket.ma_dtpl == True,
    #         AStockMarket.date.between(get_date(start_date, is_start=True), get_date(end_date))
    #     ).distinct()

    # @staticmethod
    # def query_long_up_shadow_line(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         潜力股
    #     :术语解释:
    #         长上影线是股票K线形态中实体上方延伸出显著细长影线的技术指标，反映价格冲高后遭遇强烈抛压回落，常出现在阶段性顶部或阻力位。其核心特征表现为多空力量博弈中卖方占据优势，实体较短而影线超实体2倍以上，与射击之星、吊颈线等形态形成技术共振。该形态可作为短期反转预警信号，应用于趋势转折点判断、止盈止损位设定，需结合成交量与均线系统验证信号强度，关联压力位突破、筹码分布及动量衰竭等分析维度。
    #     :量化解释:
    #         当日最高价减去收盘价和开盘价中的较大值/当日最高价和最低价的差值 * 100 大于0.7
    #     """
    #     subquery = select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         AStockMarket.high,
    #         AStockMarket.low,
    #         # 使用case when来确定max(收盘价,开盘价)
    #         case(
    #             (AStockMarket.close >= AStockMarket.open, AStockMarket.close),
    #             else_=AStockMarket.open
    #         ).label('max_price')
    #     ).where(
    #         AStockMarket.date.between(
    #             get_date(start_date, is_start=True),
    #             get_date(end_date)
    #         ),
    #         AStockMarket.high != AStockMarket.low
    #     ).subquery()

    #     return select(subquery.c.secucode).where(
    #         # 计算长上影线条件：(最高价 - max(收盘价,开盘价)) / (最高价 - 最低价) * 100 > 0.7
    #         (subquery.c.high - subquery.c.max_price) /
    #         (subquery.c.high - subquery.c.low) * 100 > 0.7
    #     ).distinct()

    # @staticmethod
    # def query_just_started(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         刚启动
    #     :术语解释:
    #         刚启动指股票价格结束低位盘整后首次出现显著上涨趋势的技术形态，核心特征包括成交量明显放大、突破关键均线压力位及MACD等技术指标转强，常用于捕捉趋势反转初期的短线机会。该形态反映主力资金介入与市场情绪转暖，常与量价配合、筹码集中度、板块轮动等概念联动，投资者通过识别刚启动信号制定右侧交易策略，需结合基本面支撑与市场环境综合判断。
    #     :功能:
    #         查询刚启动的股票
    #     :量化解释:
    #         连续两日下跌，第三日（近今日）止跌回升
    #     """
    #     daily_values = select(
    #         AStockMarket.secucode.label("secucode"),
    #         AStockMarket.date.label("date"),
    #         AStockMarket.zdf.label("pct_change"),
    #         func.any(AStockMarket.zdf)
    #             .over(
    #                 partition_by=AStockMarket.secucode,
    #                 order_by=AStockMarket.date,
    #                 rows=(-1, -1)  # 尝试获取前一行
    #             )
    #             .label("prev1_pct_change"),
    #         func.any(AStockMarket.zdf)
    #             .over(
    #                 partition_by=AStockMarket.secucode,
    #                 order_by=AStockMarket.date,
    #                 rows=(-2, -2)  # 尝试获取前两行
    #             )
    #             .label("prev2_pct_change"),
    #     ).subquery()

    #     result = (
    #         select(
    #             daily_values.c.secucode,
    #         )
    #         .select_from(daily_values)
    #         .where(
    #             (daily_values.c.prev2_pct_change < 0) &
    #             (daily_values.c.prev1_pct_change < 0) &
    #             (daily_values.c.zdf > 0) &
    #             (daily_values.c.date.between(
    #                 get_date(start_date, is_start=True),
    #                 get_date(end_date),
    #             ))
    #         )
    #         .order_by(daily_values.c.date, daily_values.c.secucode)
    #     ).distinct()

    #     return result

    # @staticmethod
    # def query_popular(start_date: str , end_date: str ):
    #     """
    #     :术语名称:
    #         人气股票
    #     :术语解释:
    #         人气股票指市场关注度与交易活跃度显著高于平均水平的个股，通常反映散户及机构投资者的短期情绪聚焦，涵盖搜索指数、社交媒体讨论量、资金流向异动等综合指标，适用于筛选热门题材、趋势跟踪或波动策略中的高流动性标的。
    #     :功能:
    #         查询人气股票
    #     :量化解释:
    #         收盘价高于5日均线，收盘价高于10日均线，成交量是5日均线的1.5倍
    #     """
    #     # 1 收盘价高于5日均线，收盘价高于10日均线
    #     subquery1 = AStockGlobalQuery.query_multi_ma_breakout(2,start_date, end_date)

    #     # 2 成交量是5日均量的1.5倍
    #     subquery2 = AStockGlobalQuery.query_volume_gt_avg(5, 1.5, 0, start_date, end_date)

    #     return intersect(subquery1, subquery2)

    # @staticmethod
    # def query_short_line_strong_stock(start_date:str='0d',end_date:str='0d'):
    #     """
    #     :术语名称:
    #         短线强势个股
    #     :术语解释:
    #         短线强势个股指短期内价格动能强劲、成交量活跃且受市场热点或资金推动的股票，适用于波段交易与趋势跟踪策略。
    #     :功能:
    #         筛选出短线强势股票
    #     :量化解释:
    #         近5日涨停次数＞0，MA均线多头排列，成交量＞5日成交量平均值1.8倍，收盘价创20日新高
    #     """
    #     buffer_days = 20  # 20日窗口，保证所有计算有数据
    #     buffer_start = get_date(start_date, is_start=True, lookback_days=buffer_days)
    #     real_start = get_date(start_date, is_start=True)
    #     real_end = get_date(end_date)

    #     # Step 1: 拉宽时间窗口，计算近5日平均成交量、近5日涨停次数、过去20日最高收盘价(不含当天)
    #     base = (
    #         select(
    #             AStockMarket.secucode,
    #             AStockMarket.date,
    #             AStockMarket.zt,
    #             AStockMarket.volume,
    #             AStockMarket.close,
    #             func.avg(AStockMarket.volume)
    #                 .over(
    #                     partition_by=AStockMarket.secucode,
    #                     order_by=AStockMarket.date,
    #                     rows=(-4, 0)  # 近5日含当天
    #                 )
    #                 .label("avg_5d_volume"),
    #             func.sum(
    #                 case((AStockMarket.zt == True, 1), else_=0)
    #             )
    #             .over(
    #                 partition_by=AStockMarket.secucode,
    #                 order_by=AStockMarket.date,
    #                 rows=(-4, 0)  # 近5日涨停次数
    #             )
    #             .label("limit_up_5d_count"),
    #             func.max(AStockMarket.close)
    #                 .over(
    #                     partition_by=AStockMarket.secucode,
    #                     order_by=AStockMarket.date,
    #                     rows=(-20, -1)  # 过去20日（不含当天）最高收盘价
    #                 )
    #                 .label("max_close_20d_prior"),
    #         )
    #         .where(AStockMarket.date.between(buffer_start, real_end))
    #         .subquery("base")
    #     )

    #     # Step 2: 标记成交量放量和收盘价是否创20日新高
    #     flagged = (
    #         select(
    #             base.c.secucode,
    #             base.c.date,
    #             base.c.volume,
    #             base.c.close,
    #             base.c.avg_5d_volume,
    #             base.c.zt_5d_count,
    #             base.c.max_close_20d_prior,
    #             case(
    #                 (base.c.volume > base.c.avg_5d_volume * 1.8, 1),
    #                 else_=0
    #             ).label("volume_spike"),
    #             case(
    #                 (base.c.close > base.c.max_close_20d_prior, 1),
    #                 else_=0
    #             ).label("close_20d_high")
    #         )
    #         .where(base.c.date.between(real_start, real_end))
    #         .subquery("flagged")
    #     )

    #     # Step 3: 筛选符合条件的记录，注意遍历每个交易日
    #     filtered = (
    #         select(flagged.c.secucode)
    #         .where(
    #             flagged.c.zt_5d_count > 0,
    #             flagged.c.volume_spike == 1,
    #             flagged.c.close_20d_high == 1,
    #         )
    #         .distinct()
    #     )

    #     # **最后返回你原来的交集语句，保持不变**
    #     return intersect(filtered, AStockGlobalQuery.query_ma_condition("多头排列", start_date, end_date))

    # @staticmethod
    # def query_hgh_dividend():
    #     """
    #     :术语名称:
    #         高股息
    #     :术语解释:
    #         高股息条件指股票具备持续稳定分红能力及较高股息收益率指标，满足稳健收益需求，关联股息率筛选、盈利稳定性评估及分红政策分析。
    #     :功能:
    #         筛选出高股息的股票
    #     :量化解释:
    #         最新股息率>5%,每股未分配利润>0.1，基本每股收益>0.2,流通股本<1000000
    #     """
    #     return NotImplementedError
    #     query = (
    #         select(AStockMarket.secucode)
    #         .join(AStockFinancial, AStockMarket.secucode == AStockFinancial.secucode)
    #         .where(
    #             AStockMarket.dividend_yield > 0.05,
    #             AStockFinancial.un_profit > 0.1,
    #             AStockFinancial.basic_eps > 0.2,
    #             AStockMarket.circulating_shares < 1000000,
    #         )
    #     )
    #     return query
    # #AStockMarketCur
    # @staticmethod
    # def query_ma_condition(ma_condition:str):
    #     """
    #     :术语名称:
    #         ma条件
    #     :术语解释:
    #         ma是自定义字段，用来标识当前股票满足的均线相关条件
    #     :功能:
    #         筛选出上升通道股票
    #     :参数:
    #         ma_condition(str):满足的ma条件，可选字段包括多头排列
    #     :量化解释:
    #         对ma字段进行模糊匹配
    #     """
    #     return select(AStockMarketCur.secucode).where(AStockMarketCur.ma == ma_condition)

    # @staticmethod
    # def query_ascending_channel():
    #     """
    #     :术语名称:
    #         上升通道
    #     :术语解释:
    #         上升通道指技术分析中股价沿趋势线上行，呈现高点低点逐步抬升形态，常见于多头行情，用于识别支撑阻力及科技股等板块趋势延续信号。
    #     :功能:
    #         筛选出上升通道股票
    #     :量化解释:
    #         MA均线多头排列
    #     """
    #     return AStockGlobalQuery.query_ma_condition("多头排列")

    # @staticmethod
    # def query_breakout_low_days_gt(days: int):
    #     """
    #     :术语名称:
    #         创阶段新低的天数大于指定阈值
    #     :术语解释:
    #         今日创阶段新低天数大于阈值是重要的超跌信号，表明股价持续弱势探底，常用于识别超卖反弹机会，与支撑位突破、市场情绪冰点等概念高度关联。
    #     :功能:
    #         筛选创阶段新低的天数大于指定阈值的个股。
    #     :参数:
    #         days (int): 新低天数要求. 近 250 天最低。
    #     :量化解释:
    #         创阶段新低天数 >= 天数
    #     """
    #     if AStockMarketCur.period_low is None:
    #         return select(AStockMarketCur.secucode).where(False)

    #     return select(AStockMarketCur.secucode).where(AStockMarketCur.period_low >= days)

    # @staticmethod
    # def query_breakout_new_high_days(days: int):
    #     """
    #     :术语名称:
    #         创阶段新高
    #     :术语解释:
    #         创阶段新高指股价突破特定周期（如20日、月线）历史峰值，反映趋势延续与市场情绪升温，常用于技术分析及突破策略制定，关联均线系统与动量指标。
    #     :功能:
    #         筛选创n日新高股票
    #     :参数:
    #         days (int): 新高天数
    #     :量化解释:
    #         创阶段新高天数 >= 天数
    #     """
    #     if AStockMarketCur.period_high is None:
    #         return select(AStockMarketCur.secucode).where(False)

    #     return select(AStockMarketCur.secucode).where(AStockMarketCur.period_high >= days)

    # @staticmethod
    # def query_macd_golden_cross():
    #     """
    #     :术语名称:
    #         MACD金叉股票
    #     :术语解释:
    #         MACD金叉股票指快线上穿慢线的技术形态，反映短期动能增强与趋势反转信号，用于识别买入时机及构建交易策略，关联均线系统与动量指标分析。
    #     :功能:
    #         筛选出MACD金叉的股票
    #     :量化解释:
    #         MACD金叉的股票
    #     """
    #     return NotImplementedError
    #     return select(AStockMarketCur.secucode).where(
    #         AStockMarketCur.macd_golden_cross == 1
    #     )

    # @staticmethod
    # def query_cci_low_than_ne_100():
    #     """
    #     :术语名称:
    #         CCI低于-100
    #     :术语解释:
    #         CCI是一种衡量价格与平均价格偏离程度的技术指标，常用于识别超买或超卖状态，辅助交易决策，涉及金融市场中的趋势分析和交易策略。
    #     :功能:
    #         筛选出CCI低于-100的股票
    #     :量化解释:
    #         CCI值为-100以下的股票
    #     """
    #     return select(AStockMarketCur.secucode).where(
    #         (AStockMarketCur.cci_ne100 == 1)
    #     ).distinct()

    # @staticmethod
    # def query_low_cci_level():
    #     """
    #     :术语名称:
    #         cci处于低位
    #     :术语解释:
    #         CCI指标处于低位是重要的技术超卖信号，反映股价短期偏离过大，常用于捕捉底部反弹机会，与超跌反弹、底部背离等反转形态直接关联。
    #     :功能:
    #         筛选出cci处于低位的股票
    #     :量化解释:
    #         cci < -100
    #     """

    #     return AStockGlobalQuery.query_cci_low_than_ne_100()

    # @staticmethod
    # def query_hotspot():
    #     """
    #     :术语名称:
    #         热点股票
    #     :术语解释:
    #         热点股票是指当前市场关注度较高、交易活跃且受短期事件驱动的股票，其特征包括异常放大的成交量、价格波动显著、频繁出现在财经媒体报道及投资者讨论中。典型场景涵盖行业政策利好、突发财报超预期、概念题材炒作或产业链变革等情境，常伴随资金集中流入与板块轮动效应，需结合市场情绪指标、机构调研动态及舆情热度进行识别，与市场主线行情、龙虎榜数据、投资者行为模式存在强关联。
    #     :功能:
    #         筛选出热点股票
    #     :量化解释:
    #         热点行业或者热点概念的股票
    #     """
    #     result = (
    #         select(AStockMarketCur.secucode)
    #         .where(
    #             # 对于ClickHouse，查询字符串字段不为空的正确方式是：
    #             # 字段不是NULL且不是空字符串''
    #             AStockMarketCur.hot_value > 5
    #         )
    #     )

    #     return result

    # @staticmethod
    # def query_hot_industry():
    #     """
    #     :术语名称:
    #         热门行业板块
    #     :术语解释:
    #         热门行业板块是指在特定时期内因政策扶持、产业发展趋势、宏观经济环境或市场资金关注而表现突出的行业类别。其特征通常包括行业整体成交量放大、板块指数显著上涨、龙头个股带动效应明显，且在资本市场和财经媒体中频繁被提及。热门行业板块往往与市场主线行情高度相关，能够吸引资金集中流入，形成短期或中期的投资热点，对个股价格走势和资金博弈具有较强的驱动作用。
    #     :功能:
    #         筛选出热门行业板块
    #     :量化解释:
    #         热点行业字段为1
    #     """
    #     result = (
    #         select(AStockMarketCur.secucode)
    #         .where(
    #             # 对于ClickHouse，查询字符串字段不为空的正确方式是：
    #             # 字段不是NULL且不是空字符串''
    #             ((AStockMarketCur.hot_industry_trade != None) & (AStockMarketCur.hot_industry_trade != ''))
    #         )
    #     )

    #     return result
    # #AStockFinancial
    # @staticmethod
    # def query_performance_growth_ratio(
    #     percent: float = 0.3, start_date: str = "0q", end_date: str = "0q"
    # ):
    #     """
    #     :术语名称:
    #         业绩增长比例
    #     :术语解释:
    #         业绩增长指企业营收利润持续提升的财务指标，核心特征为同比增幅与行业对比，应用于财报分析、投资选股场景，关联营收增长率、净利润及行业景气周期
    #     :功能:
    #         筛选出业绩增长的股票
    #     :参数:
    #         percent(float): 业绩增长比例, 百分比要转换为小数点传参, 默认为30%要传0.3
    #         start_date(str): 起始日期
    #         end_date(str): 结束日期
    #     :量化解释:
    #         归母净利润同比增长率> percent
    #     """
    #     return select(AStockFinancial.secucode).where(
    #         AStockFinancial.parent_net_profit_yoy > percent,
    #         AStockFinancial.date.between(
    #             get_report_date(start_date, is_start=True), get_report_date(end_date)
    #         ),
    #     ).distinct()

    # @staticmethod
    # def query_high_growth(start_date: str = '0d', end_date: str = '0d'):
    #     """
    #     :术语名称:
    #         高成长股票
    #     :术语解释:
    #         高成长股票指营收增速快、盈利潜力突出的企业证券，常用于长期增长策略，受行业趋势与创新驱动，伴随较高风险波动和估值溢价特征。
    #     :功能:
    #         筛选出高成长的股票
    #     :量化解释:
    #         1.营收同比增长率>=20%
    #         2.净利润同比增长率>=20%
    #         3.净资产收益率>=15%
    #         4.毛利率 >= 20%
    #     """
    #     # 1. 营业总收入同比增长率
    #     revenue_yoy = AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_income_total(),0.2, float("inf"), start_date, end_date)
    #     # 2. 净利润同比增长率
    #     net_profit_yoy = AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_parent_net_profit_yoy(),0.2, float("inf"), start_date, end_date)
    #     # 3. 净资产收益率
    #     roe = AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_roe(),0.15, float("inf"), start_date, end_date)
    #     # 4. 毛利率
    #     gross_margin = AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_gross_margin(),0.2, float("inf"), start_date, end_date)

    #     return intersect(revenue_yoy, net_profit_yoy, roe, gross_margin)

    # @staticmethod
    # def query_high_quality_stocks(start_date: str = "0q", end_date: str = "0q"):
    #     """
    #     :术语名称:
    #         绩优股
    #     :术语解释:
    #         绩优股指盈利稳定、财务状况良好的上市公司，兼具低估值与龙头特征，是价值投资的核心标的，可结合新兴消费场景筛选低位潜力股，关联蓝筹、成长性等概念。
    #     :功能:
    #         筛选出绩优股票
    #     :量化解释:
    #         归母净利润同比增长率 > 30% 且 销售毛利率>40% 且 净资产收益率＞15%
    #     """
    #     # 归母净利润同比增长率 > 30%
    #     net_profit_yoy = AStockGlobalQuery.query_field_in_range(
    #         AStockGlobalQuery.get_parent_net_profit_yoy(),0.3, float("inf"), start_date, end_date
    #     )
    #     # 销售毛利率>40%
    #     gross_margin = AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_gross_margin(),0.4, float("inf"), start_date, end_date)

    #     # 净资产收益率＞15%
    #     roe = AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_roe(),0.15, float("inf"), start_date, end_date)

    #     return intersect(net_profit_yoy, gross_margin, roe)

    # @staticmethod
    # def calculate_growth_rate(
    #     field: Field,
    #     current_date: str,
    #     previous_date: str,
    #     growth_type: str = "yoy",
    #     period: Optional[int] = None,
    # ) -> BinaryExpression:
    #     """
    #     :功能:
    #         计算财务指标的增长率（返回SQL表达式），自动关联当前股票代码
    #     :参数:
    #         field: 要计算的字段对象（如AStockFinancial.net_profit）
    #         current_date: 当前报告期（支持'最新'/'上期'/季度缩写/具体日期）
    #         previous_date: 基期报告期
    #         growth_type: 增长率类型(yoy/qoq/cagr)
    #         period: CAGR计算需要的期数
    #     :量化解释:
    #         通过公式指定字段的增长率，支持'yoy'/'qoq'/'cagr'三种类型
    #         公式: 'yoy'/'qoq'类型: (当前值 - 基值) / 基值 ; 'cagr'类型: (当前值 / 基值) ^ (1/期数) - 1
    #     """
    #     return NotImplementedError
    #     from sqlalchemy.sql import func, select

    #     # 创建别名以便在同一查询中多次引用同一表
    #     curr = aliased(AStockFinancial, name="current")
    #     prev = aliased(AStockFinancial, name="previous")

    #     # 获取当前期数值（子查询）
    #     current_value = (
    #         select(field)
    #         .where(
    #             curr.secucode == col(AStockFinancial.secucode),  # 关联外部查询的股票代码
    #             curr.reportdate == AStockGlobalQuery.query_report_date(current_date),
    #         )
    #         .scalar_subquery()
    #     )

    #     # 获取基期数值（子查询）
    #     base_value = (
    #         select(field)
    #         .where(
    #             prev.secucode == col(AStockFinancial.secucode),  # 关联外部查询的股票代码
    #             prev.reportdate == AStockGlobalQuery.query_report_date(previous_date),
    #         )
    #         .scalar_subquery()
    #     )

    #     # 计算增长率表达式
    #     if growth_type in ("yoy", "qoq"):
    #         growth_expr = (current_value - base_value) / func.nullif(base_value, 0)
    #     elif growth_type == "cagr":
    #         if period is None:
    #             raise ValueError("CAGR计算需要period参数")
    #         growth_expr = (
    #             func.power(
    #                 func.nullif(current_value, 0) / func.nullif(base_value, 0),
    #                 1.0 / period,
    #             )
    #             - 1
    #         )
    #     else:
    #         raise ValueError(f"不支持的增长率类型: {growth_type}")

    #     return growth_expr

    # @staticmethod
    # def query_performance_doubled(start_date: str = "0q", end_date: str = "0q"):
    #     """
    #     :术语名称:
    #         业绩翻倍
    #     :术语解释:
    #         业绩翻倍指企业季度净利润激增超100%，体现高增长性，常关联财报期与新零售、机器人等热点板块，成为市场关注焦点
    #     :功能:
    #         筛选出业绩翻倍的股票
    #     :参数:
    #         start_date: str
    #         end_date: str
    #     :量化解释:
    #         归母净利润同比增长率>100%
    #     """
    #     return select(AStockFinancial.secucode).where(
    #         AStockFinancial.parent_net_profit_yoy > 1,
    #         AStockFinancial.date.between(
    #             get_report_date(start_date, is_start=True), get_report_date(end_date)
    #         ),
    #     ).distinct()

    # @staticmethod
    # def query_cash_dividend_of_10_CNY_per_10_shares():
    #     """
    #     :术语名称:
    #         分红10派10
    #     :术语解释:
    #         分红10派10指上市公司每10股派发10元现金红利，涉及股权登记和除权处理，用于股东回报及股息率计算，常见于年报披露和投资决策分析。
    #     :功能:
    #         筛选出分红10派10的股票
    #     :量化解释:
    #         每股未分配利润>=10
    #     """
    #     if AStockFinancial.un_profit is None:
    #         return False
    #     return select(AStockFinancial.secucode).where(AStockFinancial.un_profit > 10)

    # @staticmethod
    # def query_white_horse_stock(start_date: str = "0q", end_date: str = "0q"):
    #     """
    #     :术语名称:
    #         白马股
    #     :术语解释:
    #         白马股指业绩稳定、品牌知名度高且具持续增长能力的优质蓝筹股，常受机构投资者青睐。
    #     :功能:
    #         筛选出白马股
    #     :量化解释:
    #         净资产收益率>20%
    #     :参数:
    #         start_date(str): 起始日期
    #         end_date(str): 结束日期
    #     """
    #     return AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_roe(),0.2,float('inf'),start_date,end_date)

    # @staticmethod
    # def query_board_leader_stock(start_date: str = "0q", end_date: str = "0q"):
    #     """
    #     :术语名称:
    #         龙头,龙头股
    #     :术语解释:
    #         龙头指行业领先企业，具备高市占率、技术壁垒及持续业绩增长特征，应用于股票筛选与板块分析，关联成长股、绩优股等投资概念,总市值排名前五的公司
    #     :功能:
    #         筛选出龙头股,筛选出某概念行业板块的龙头,筛选某概念行业板块龙头股.
    #     :量化解释:
    #         1.净资产收益率≥15%；
    #         2.归母净利润连续3年增长≥20%；
    #         3.主力净流入占比连续5日>0.5%
    #     :参数:
    #         start_date(str): 起始日期
    #         end_date(str): 结束日期
    #     """
    #     return NotImplementedError
    #     # 1.净资产收益率≥15%
    #     query1 = AStockGlobalQuery.query_roe_in_range(0.15, start_date=start_date, end_date=end_date)

    #     # 2.归母净利润连续3年增长≥20%
    #     three_year_report_date = select(AStockFinancial.date).where(
    #         AStockFinancial.date % 10000 == 1231,
    #         AStockFinancial.date <= get_report_date(start_date, is_start=True),
    #     ).group_by(AStockFinancial.date).order_by(AStockFinancial.date.desc()).limit(1).offset(2)

    #     query2 = select(AStockFinancial.secucode).where(
    #         AStockFinancial.date >= three_year_report_date,
    #     ).group_by(AStockFinancial.secucode).having(
    #         func.min(AStockFinancial.parent_net_profit_yoy) > 0.2,
    #         func.count(AStockFinancial.parent_net_profit_yoy) > 2
    #     ).distinct()

    #     # 3.主力净流入占比连续5日>0.5%
    #     consecutive_main_fund= select(
    #         AStockMarket.secucode,
    #         AStockMarket.date,
    #         func.count(case((AStockMarket.zhu > 0.005, 1)))
    #         .over(
    #             partition_by=AStockMarket.secucode,
    #             order_by=AStockMarket.date,
    #             rows=(-4, 0),
    #         )
    #         .label("cnt_consecutive_main_fund_5"),
    #     ).subquery()

    #     query3 = (
    #         select(consecutive_main_fund.c.secucode)
    #         .select_from(consecutive_main_fund)
    #         .where(
    #             (consecutive_main_fund.c.cnt_consecutive_main_fund_5 == 5)
    #             & (
    #                 consecutive_main_fund.c.date.between(
    #                     get_date("0d", is_start=True),
    #                     get_date("0d"),
    #                 )
    #             )
    #         )
    #     ).distinct()

    #     return intersect(query1, query2, query3)
    # ──────────── 老版组合条件END
    @staticmethod
    def query_all_time_high_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            今日创历史新高的股票
        :术语解释:
            筛选今日创历史新高的股票
        :功能:
            筛选出今日创历史新高的股票
        """
        query = (
            select(
                AStockMarketCur.secucode,
                AStockMarketCur.all_time_high.label("历史新高"),
            )
            .where(AStockMarketCur.all_time_high == 1)
            .order_by(AStockMarketCur.secucode)
        )
        return query

    @staticmethod
    def query_all_time_low_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            今日创历史新低的股票
        :术语解释:
            筛选今日创历史新低的股票
        :功能:
            筛选出今日创历史新低的股票
        """
        query = (
            select(
                AStockMarketCur.secucode,
                AStockMarketCur.all_time_low.label("历史新低"),
            )
            .where(AStockMarketCur.all_time_low == 1)
            .order_by(AStockMarketCur.secucode)
        )
        return query

    @staticmethod
    def query_period_high_stocks(days: int, start_date: str, end_date: str):
        """
        :术语名称:
            今日创指定天数及以上新高的股票
        :术语解释:
            筛选今日创指定天数及以上新高的股票
        :功能:
            筛选今日创指定天数及以上新高的股票，例如30天新高包含30天及以上新高
        :参数:
            days (int): 天数
        """
        query = (
            select(
                AStockMarketCur.secucode,
                AStockMarketCur.period_high.label("阶段新高天数"),
            )
            .where(AStockMarketCur.period_high >= days)
            .order_by(AStockMarketCur.period_high.desc())
        )
        return query

    @staticmethod
    def query_period_low_stocks(days: int, start_date: str, end_date: str):
        """
        :术语名称:
            今日创指定天数及以上新低的股票
        :术语解释:
            筛选今日创指定天数及以上新低的股票
        :功能:
            筛选今日创指定天数及以上新低的股票，例如30天新低包含30天及以上新低
        :参数:
            days (int): 天数
        """
        query = (
            select(
                AStockMarketCur.secucode,
                AStockMarketCur.period_low.label("阶段新低天数"),
            )
            .where(AStockMarketCur.period_low >= days)
            .order_by(AStockMarketCur.period_low.desc())
        )
        return query

    @staticmethod
    def _query_bitmask_conditions(
        bitmask_field,
        start_date: str,
        end_date: str,
        include_bits: list[int] = None,
        exclude_bits: list[int] = None,
    ):
        """
        :功能:
            构建位掩码条件的通用函数，支持包含和排除位运算过滤，同时支持日期范围过滤
        :参数:
            bitmask_field: SQL字段对象，用于位运算的字段
            start_date: 起始日期
            end_date: 结束日期
            include_bits: 必须包含的位列表，如[1, 32]表示必须同时包含位1和位32
            exclude_bits: 必须排除的位列表，如[16]表示不能包含位16
        :返回:
            包含 secucode、date 和原始字段的查询对象
        """
        conditions = []
        # 从 bit mask_field 所在表获取表对象
        table_class = bitmask_field.class_

        # 使用统一的日期范围处理函数
        start, _, end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            table_class, start_date, end_date, lookback_days=0
        )

        # 构建包含条件：所有指定位都必须存在
        if include_bits:
            include_conditions = []
            for bit in include_bits:
                # 位运算：检查指定位是否为1 (trade_type & bit == bit)
                include_condition = func.bitAnd(bitmask_field, bit) == bit
                include_conditions.append(include_condition)
            # 所有包含条件必须同时满足
            conditions.append(and_(*include_conditions))

        # 构建排除条件：所有指定位都不能存在
        if exclude_bits:
            exclude_conditions = []
            for bit in exclude_bits:
                # 位运算：检查指定位是否为0 (trade_type & bit != bit)
                exclude_condition = func.bitAnd(bitmask_field, bit) != bit
                exclude_conditions.append(exclude_condition)
            conditions.append(and_(*exclude_conditions))

        # 添加日期范围条件
        date_condition = table_class.date.between(start, end)
        conditions.append(date_condition)

        combined_condition = and_(*conditions) if conditions else true()

        # 构建并返回查询对象
        query = (
            select(
                table_class.secucode,
                table_class.date,
                bitmask_field,
            )
            .where(combined_condition)
            .order_by(table_class.date.desc())
        )

        return query

    @staticmethod
    def query_stocks_in_daily_long_short_flow_ranking(start_date: str, end_date: str):
        """
        :术语名称:
            上榜一日多空资金榜
        :术语解释:
            一日多空资金榜
        :功能:
            筛选出指定日期范围内上过一日多空榜的股票（例如"筛选上过一日榜的股票"、"找出上榜一日多空榜的股票"、"一日多空榜有哪些股票"等筛选类问题）。这是一个筛选函数，当用户需要“上榜股票列表”时调用。调用此筛选函数时，必须同时搭配 get_duo_1_ranklist_data() 函数来一同展示相关的榜单数据。严禁当用户是询问某只特定股票是否上榜时使用此函数，应使用 get_duo_1_ranklist_data()。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        ranked_subquery = (
            select(
                RanklistDuo1Level0.secucode,
                RanklistDuo1Level0.date,
                RanklistDuo1Level0.duo_1,
                RanklistDuo1Level0.duor_1,
                func.row_number()
                .over(
                    partition_by=(RanklistDuo1Level0.date, RanklistDuo1Level0.secucode),
                    order_by=desc(RanklistDuo1Level0.duor_1),
                )
                .label("rownum"),
                func.row_number()
                .over(partition_by=RanklistDuo1Level0.date, order_by=desc(RanklistDuo1Level0.duor_1))
                .label("rank"),
            )
            .where(
                RanklistDuo1Level0.date.between(
                    get_date(start_date, is_start=True),
                    get_date(end_date),
                )
            )
            .subquery()
        )

        return (
            select(ranked_subquery.c.secucode)
            .distinct()
            .where(and_(ranked_subquery.c.rownum == 1, ranked_subquery.c.rank <= 50))
            .order_by(ranked_subquery.c.duor_1.desc())
        )

    # ThreeDayLongShortFlowRanking
    @staticmethod
    def query_stocks_in_three_day_long_short_flow_ranking(start_date: str, end_date: str):
        """
        :术语名称:
            上榜三日多空资金榜
        :术语解释:
            三日多空资金榜
        :功能:
            筛选出指定日期范围内上过三日多空榜的股票（例如“筛选上过三日榜的股票”）。这是一个筛选函数，当用户需要“上榜股票列表”时调用。调用此筛选函数时，必须同时搭配 get_duo_3_ranklist_data() 函数来一同展示相关的榜单数据。严禁当用户是询问某只特定股票是否上榜时使用此函数，此时使用 get_duo_3_ranklist_data()。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        ranked_subquery = (
            select(
                RanklistDuo3Level0.secucode,
                RanklistDuo3Level0.date,
                RanklistDuo3Level0.duo_3,
                RanklistDuo3Level0.duor_3,
                func.row_number()
                .over(
                    partition_by=(RanklistDuo3Level0.date, RanklistDuo3Level0.secucode),
                    order_by=desc(RanklistDuo3Level0.duor_3),
                )
                .label("rownum"),
                func.row_number()
                .over(partition_by=RanklistDuo3Level0.date, order_by=desc(RanklistDuo3Level0.duor_3))
                .label("rank"),
            )
            .where(
                RanklistDuo3Level0.date.between(
                    get_date(start_date, is_start=True),
                    get_date(end_date),
                )
            )
            .subquery()
        )

        return (
            select(ranked_subquery.c.secucode)
            .distinct()
            .where(and_(ranked_subquery.c.rownum == 1, ranked_subquery.c.rank <= 50))
            .order_by(ranked_subquery.c.duor_3.desc())
        )

    # ThirteenDayDaredevilFundsRanking
    @staticmethod
    def query_stocks_in_thirteen_day_dare_devil_funds_ranking(start_date: str, end_date: str):
        """
        :术语名称:
            上榜十三日敢死队资金榜
        :术语解释:
            十三日敢死队资金榜
        :功能:
            筛选出指定日期范围内上过十三日敢死队榜的股票（例如“筛选上过十三日敢死队榜的股票”）。这是一个筛选函数，当用户需要“上榜股票列表”时调用。调用此筛选函数时，必须同时搭配 get_gan_13_ranklist_data() 函数来一同展示相关的榜单数据。严禁当用户是询问某只特定股票是否上榜时使用此函数，应使用 get_gan_13_ranklist_data()。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        ranked_subquery = (
            select(
                RanklistGan13Level0.secucode,
                RanklistGan13Level0.date,
                RanklistGan13Level0.ganr_13,
                RanklistGan13Level0.gan_13,
                func.row_number()
                .over(
                    partition_by=(RanklistGan13Level0.date, RanklistGan13Level0.secucode),
                    order_by=desc(RanklistGan13Level0.ganr_13),
                )
                .label("rownum"),
                func.row_number()
                .over(partition_by=RanklistGan13Level0.date, order_by=desc(RanklistGan13Level0.ganr_13))
                .label("rank"),
            )
            .where(
                RanklistGan13Level0.date.between(
                    get_date(start_date, is_start=True),
                    get_date(end_date),
                )
            )
            .subquery()
        )

        return (
            select(ranked_subquery.c.secucode)
            .distinct()
            .where(and_(ranked_subquery.c.rownum == 1, ranked_subquery.c.rank <= 50))
            .order_by(ranked_subquery.c.ganr_13.desc())
        )

    # ThreeLocks
    @staticmethod
    def query_three_lock_open_position(start_date: str, end_date: str):
        """
        :术语名称:
            三把锁开仓
        :术语解释:
            三把锁开仓(买入)信号是指南针公司总结的重要指示指标,指示潜力股票
        :功能:
            筛选出指定日期范围出现过三把锁开仓信号的股票，搭配get_optype使用
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            三把锁开仓信号
        """
        return (
            select(DailyThreeMethodLevel0.secucode)
            .distinct()
            .where(
                (DailyThreeMethodLevel0.optype == 1)
                & (
                    DailyThreeMethodLevel0.date.between(
                        get_date(start_date, is_start=True),
                        get_date(end_date),
                    )
                )
            )
        )

    @staticmethod
    def query_green_lock(start_date: str, end_date: str):
        """
        :术语名称:
            小绿锁
        :术语解释:
            小绿锁信号是指南针公司总结的指示股票处于卖出时机的指标
        :功能:
            筛选出指定日期范围出现过小绿锁信号的股票，搭配get_optype使用
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            小绿锁信号
        """
        return (
            select(DailyThreeMethodLevel0.secucode)
            .distinct()
            .where(
                (DailyThreeMethodLevel0.optype == 2)
                & (
                    DailyThreeMethodLevel0.date.between(
                        get_date(start_date, is_start=True),
                        get_date(end_date),
                    )
                )
            )
        )

    @staticmethod
    def query_yellow_lock(start_date: str, end_date: str):
        """
        :术语名称:
            小黄锁
        :术语解释:
            小黄锁信号是指南针公司总结的指示股票应进一步观望的指标
        :功能:
            筛选出指定日期范围出现过小黄锁信号的股票，和get_optype搭配使用
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            小黄锁信号
        """
        return (
            select(DailyThreeMethodLevel0.secucode)
            .distinct()
            .where(
                (DailyThreeMethodLevel0.optype == 3)
                & (
                    DailyThreeMethodLevel0.date.between(
                        get_date(start_date, is_start=True),
                        get_date(end_date),
                    )
                )
            )
        )

    @staticmethod
    def _query_ld_pools(field, values, start_date: str = "0d", end_date: str = "0d", is_stock: bool = True):
        """
        :参数:
            field: DailyQlldLevel5 中的字段，如 DailyQlldLevel5.ldmarket / DailyQlldLevel5.ldtrade / DailyQlldLevel5.ld0z
            values: 匹配条件，如 [2, 3]、1、0、5 等
            start_date(str): 起始日期
            end_date(str): 结束日期
            is_stock(bool): 是否只查询股票，True时只返回股票代码，False时返回板块
        """
        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)
        unique_id = uuid.uuid4().hex[:6]

        # 擒龙平台默认过滤条件
        ql_default_filter_cte = (
            select(AStockBasic.secucode)
            .join(DailyQlstockBasicLevel5, AStockBasic.secucode == DailyQlstockBasicLevel5.secucode)
            .where(
                DailyQlstockBasicLevel5.is_new == 0,
                AStockBasic.is_tp == 0,
                AStockBasic.is_cwfx == 0,
                AStockBasic.is_qzts == 0,
                AStockBasic.is_tsfx == 0,
                DailyQlstockBasicLevel5.date == select(func.max(DailyQlstockBasicLevel5.date)).scalar_subquery(),
            )
            .cte(f"ql_default_filter_cte_{unique_id}")
        )

        # 股票池过滤（来自 daily_ldpools_level5_view）
        ld_conditions = ["0:1:1:0", "0:2:1:0", "0:3:1:0", "0:4:1:0", "0:5:1:0"]
        ld_pool_codes_cte = (
            select(
                func.trim(func.arrayJoin(func.splitByChar(literal(","), DailyLdPoolsLevel5.stock_pools))).label(
                    "secucode"
                )
            )
            .where(DailyLdPoolsLevel5.filter_conditions.in_(ld_conditions))
            .cte(f"ld_pool_codes_{unique_id}")
        )

        # 构建基础查询条件
        base_conditions = [
            (field.in_(values) if isinstance(values, (list, tuple, set)) else field == values),
            DailyQlldLevel5.date.between(trade_start, trade_end),
        ]

        if is_stock:
            # 股票查询：使用原有股票池筛选逻辑
            base_conditions.append(DailyQlldLevel5.secucode.in_(select(ld_pool_codes_cte.c.secucode)))
            base_conditions.append(DailyQlldLevel5.secucode.in_(select(ql_default_filter_cte.c.secucode)))

        # 主查询
        q = (
            select(DailyQlldLevel5.secucode)
            .distinct()
            .join(AStockBasic, AStockBasic.secucode == DailyQlldLevel5.secucode)
            .where(*base_conditions)
        )

        return q

    @staticmethod
    def query_ldmarket_blank(start_date: str, end_date: str):
        """
        :术语名称:
            对战区轮空
        :术语解释:
            对战区轮空，表示市场暂未进入轮动阶段。
        :功能:
            筛选出指定日期范围内，对战区轮动状态为轮空的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ldmarket, 0, start_date, end_date)

    @staticmethod
    def query_ldmarket_start_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对战区开始轮动
        :术语解释:
            对战区开始轮动，表示市场进入初步轮动阶段。
        :功能:
            筛选出指定日期范围内，对战区轮动状态为开始轮动的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ldmarket, 1, start_date, end_date)

    @staticmethod
    def query_ldmarket_start_strong_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对战区开始强势轮动
        :术语解释:
            对战区开始强势轮动，表示市场进入强势轮动阶段的启动期。
        :功能:
            筛选出指定日期范围内，对战区轮动状态为开始强势轮动的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ldmarket, [2, 3], start_date, end_date)

    @staticmethod
    def query_ldmarket_in_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对战区轮动中
        :术语解释:
            对战区轮动中，表示市场处于持续轮动阶段。
        :功能:
            筛选出指定日期范围内，对战区轮动状态为轮动中的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ldmarket, 4, start_date, end_date)

    @staticmethod
    def query_ldmarket_in_strong_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对战区强势轮动中
        :术语解释:
            对战区强势轮动中，表示市场处于高强度轮动阶段。
        :功能:
            筛选出指定日期范围内，对战区轮动状态为强势轮动中的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ldmarket, 5, start_date, end_date)

    @staticmethod
    def query_ldtrade_blank(start_date: str, end_date: str):
        """
        :术语名称:
            对行业轮空
        :术语解释:
            对行业轮空，表示市场暂未进入轮动阶段。
        :功能:
            筛选出指定日期范围内，对行业轮动状态为轮空的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ldtrade, 0, start_date, end_date)

    @staticmethod
    def query_ldtrade_start_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对行业开始轮动
        :术语解释:
            对行业开始轮动，表示市场进入初步轮动阶段。
        :功能:
            筛选出指定日期范围内，对行业轮动状态为开始轮动的股票和行业
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ldtrade, 1, start_date, end_date)

    @staticmethod
    def query_ldtrade_start_strong_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对行业开始强势轮动
        :术语解释:
            对行业开始强势轮动，表示市场进入强势轮动阶段的启动期。
        :功能:
            筛选出指定日期范围内，对行业轮动状态为开始强势轮动的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ldtrade, [2, 3], start_date, end_date)

    @staticmethod
    def query_ldtrade_in_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对行业轮动中
        :术语解释:
            对行业轮动中，表示市场处于持续轮动阶段。
        :功能:
            筛选出指定日期范围内，对行业轮动状态为轮动中的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ldtrade, 4, start_date, end_date)

    @staticmethod
    def query_ldtrade_in_strong_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对行业强势轮动中
        :术语解释:
            对行业强势轮动中，表示市场处于高强度轮动阶段。
        :功能:
            筛选出指定日期范围内，对行业轮动状态为强势轮动中的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ldtrade, 5, start_date, end_date)

    @staticmethod
    def query_ld0z_blank(start_date: str, end_date: str):
        """
        :术语名称:
            对0Z轮空
        :术语解释:
            对0Z轮空，表示市场暂未进入轮动阶段。
        :功能:
            筛选出指定日期范围内，对0Z轮动状态为轮空的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ld0z, 0, start_date, end_date)

    @staticmethod
    def query_ld0z_start_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对0Z开始轮动
        :术语解释:
            对0Z开始轮动，表示市场进入初步轮动阶段。
        :功能:
            筛选出指定日期范围内，对0Z轮动状态为开始轮动的股票和行业
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ld0z, 1, start_date, end_date)

    @staticmethod
    def query_ld0z_start_strong_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对0Z开始强势轮动
        :术语解释:
            对0Z开始强势轮动，表示市场进入强势轮动阶段的启动期。
        :功能:
            筛选出指定日期范围内，对0Z轮动状态为开始强势轮动的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ld0z, [2, 3], start_date, end_date)

    @staticmethod
    def query_ld0z_in_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对0Z轮动中
        :术语解释:
            对0Z轮动中，表示市场处于持续轮动阶段。
        :功能:
            筛选出指定日期范围内，对0Z轮动状态为轮动中的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ld0z, 4, start_date, end_date)

    @staticmethod
    def query_ld0z_in_strong_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            对0Z强势轮动中
        :术语解释:
            对0Z强势轮动中，表示市场处于高强度轮动阶段。
        :功能:
            筛选出指定日期范围内，对0Z轮动状态为强势轮动中的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ld0z, 5, start_date, end_date)

    @staticmethod
    def query_ld_blank(start_date: str, end_date: str):
        """
        :术语名称:
            轮空的行业/战区
        :术语解释:
            轮空的行业/战区，表示轮动状态为轮空的板块，市场暂未进入轮动阶段。
        :功能:
            筛选出指定日期范围内，轮动状态为轮空的行业/战区。当问题是“轮空的行业”或“轮空的战区”时，使用该方法。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ld0z, 0, start_date, end_date, is_stock=False)

    @staticmethod
    def query_ld_start_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            开始轮动的行业/战区
        :术语解释:
            开始轮动的行业/战区，表示轮动状态为“开始轮动”的板块，市场进入初步轮动阶段。
        :功能:
            筛选出指定日期范围内，轮动状态为开始轮动的行业/战区。当问题是“开始轮动的行业”或“开始轮动的战区”时，使用该方法。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ld0z, 1, start_date, end_date, is_stock=False)

    @staticmethod
    def query_ld_start_strong_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            开始强势轮动的行业/战区
        :术语解释:
            开始强势轮动的行业/战区，表示轮动状态为“开始强势轮动”的板块，市场进入强势轮动阶段的启动期。
        :功能:
            筛选出指定日期范围内，轮动状态为开始强势轮动的行业/战区。当问题是“开始强势轮动的行业”或“开始强势轮动的战区”时，使用该方法。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ld0z, [2, 3], start_date, end_date, is_stock=False)

    @staticmethod
    def query_ld_in_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            轮动中的行业/战区
        :术语解释:
            轮动中的行业/战区，表示轮动状态为“轮动中”的板块，市场处于持续轮动阶段。
        :功能:
            筛选出指定日期范围内，轮动状态为轮动中的行业/战区。当问题是“轮动中的行业”或“轮动中的战区”时，使用该方法。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ld0z, 4, start_date, end_date, is_stock=False)

    @staticmethod
    def query_ld_in_strong_rotation(start_date: str, end_date: str):
        """
        :术语名称:
            强势轮动中的行业/战区
        :术语解释:
            强势轮动中的行业/战区，表示轮动状态为“强势轮动中”的板块，市场处于高强度轮动阶段。
        :功能:
            筛选出指定日期范围内，轮动状态为强势轮动中的行业/战区。当问题是“强势轮动中的行业”或“强势轮动中的战区”时，使用该方法。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return AStockGlobalQuery._query_ld_pools(DailyQlldLevel5.ld0z, 5, start_date, end_date, is_stock=False)

    # InvestmentDecision
    @staticmethod
    def query_value_policy_status(value_policy: int, start_date: str, end_date: str):
        """
        :术语名称:
            价值决策开减仓状态
        :术语解释:
            价值决策开减仓状态是指根据价值投资策略对股票仓位的操作状态，包括建仓、持仓或减仓等决策动作。
        :功能:
            筛选出指定日期范围价值决策开减仓状态为指定状态的股票
        :参数:
            value_policy(int):0-空仓，1-开仓，2-持仓，3-清仓
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            价值决策字段满足指定要求
        """
        if value_policy != 0:
            return (
                select(DailyCwDataLevel0.secucode)
                .distinct()
                .where(
                    (DailyCwDataLevel0.value_policy == value_policy)
                    & (
                        DailyCwDataLevel0.date.between(
                            get_date(start_date, is_start=True),
                            get_date(end_date),
                        )
                    )
                )
            )
        else:
            return (
                select(DailyCwDataLevel0.secucode)
                .distinct()
                .where(
                    (DailyCwDataLevel0.value_policy.in_([0, 4]))
                    & (
                        DailyCwDataLevel0.date.between(
                            get_date(start_date, is_start=True),
                            get_date(end_date),
                        )
                    )
                )
            )

    @staticmethod
    def query_long_policy_status(long_policy: int, start_date: str, end_date: str):
        """
        :术语名称:
            长线决策开减仓状态
        :术语解释:
            长线决策开减仓状态是指根据长线投资策略对股票仓位的操作状态，包括建仓、持仓或减仓等决策动作。
        :功能:
            筛选出指定日期范围长线决策开减仓状态为指定状态的股票
        :参数:
            long_policy(int):0-空仓，1-开仓，2-持仓，3-清仓
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            长线决策字段满足指定要求
        """
        if long_policy != 0:
            return (
                select(DailyCwDataLevel0.secucode)
                .distinct()
                .where(
                    (DailyCwDataLevel0.long_policy == long_policy)
                    & (
                        DailyCwDataLevel0.date.between(
                            get_date(start_date, is_start=True),
                            get_date(end_date),
                        )
                    )
                )
            )
        else:
            return (
                select(DailyCwDataLevel0.secucode)
                .distinct()
                .where(
                    (DailyCwDataLevel0.long_policy.in_([0, 4]))
                    & (
                        DailyCwDataLevel0.date.between(
                            get_date(start_date, is_start=True),
                            get_date(end_date),
                        )
                    )
                )
            )

    @staticmethod
    def query_long_marketopsatus_status(long_marketopsatus: int, start_date: str, end_date: str):
        """
        :术语名称:
            六大市场长线决策开减仓状态
        :术语解释:
            六大市场长线决策开减仓状态是指根据六大市场趋势分析对股票仓位的操作状态，包括建仓、持仓或减仓等决策动作。
        :功能:
            筛选出指定日期范围六大市场长线决策开减仓状态为指定状态的股票
        :参数:
            long_marketopsatus(int):0-空仓，1-开仓，2-持仓，3-清仓
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            六大市场长线决策字段满足指定要求
        """
        if long_marketopsatus != 0:
            return (
                select(DailyCwDataLevel0.secucode)
                .distinct()
                .where(
                    (DailyCwDataLevel0.long_marketopsatus == long_marketopsatus)
                    & (
                        DailyCwDataLevel0.date.between(
                            get_date(start_date, is_start=True),
                            get_date(end_date),
                        )
                    )
                )
            )
        else:
            return (
                select(DailyCwDataLevel0.secucode)
                .distinct()
                .where(
                    (DailyCwDataLevel0.long_marketopsatus.in_([0, 4]))
                    & (
                        DailyCwDataLevel0.date.between(
                            get_date(start_date, is_start=True),
                            get_date(end_date),
                        )
                    )
                )
            )

    @staticmethod
    def query_short_policy_status(short_policy: int, start_date: str, end_date: str):
        """
        :术语名称:
            短线决策开减仓状态
        :术语解释:
            短线决策开减仓状态是指根据短线操作策略对股票仓位的操作状态，包括建仓、持仓或减仓等决策动作。
        :功能:
            筛选出指定日期范围短线决策开减仓状态为指定状态的股票
        :参数:
            short_policy(int):0-空仓，1-开仓，2-持仓，3-清仓
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            短线决策字段满足指定要求
        """
        if short_policy != 0:
            return (
                select(DailyCwDataLevel0.secucode)
                .distinct()
                .where(
                    (DailyCwDataLevel0.short_policy == short_policy)
                    & (
                        DailyCwDataLevel0.date.between(
                            get_date(start_date, is_start=True),
                            get_date(end_date),
                        )
                    )
                )
            )
        else:
            return (
                select(DailyCwDataLevel0.secucode)
                .distinct()
                .where(
                    (DailyCwDataLevel0.short_policy.in_([0, 4]))
                    & (
                        DailyCwDataLevel0.date.between(
                            get_date(start_date, is_start=True),
                            get_date(end_date),
                        )
                    )
                )
            )

    @staticmethod
    def query_op_policy_status(op_policy: int, start_date: str, end_date: str):
        """
        :术语名称:
            波段决策开减仓状态
        :术语解释:
            波段决策开减仓状态是指根据波段操作策略对股票仓位的操作状态，包括建仓、持仓或减仓等决策动作。
        :功能:
            筛选出指定日期范围波段决策开减仓状态为指定状态的股票
        :参数:
            op_policy(int):0-空仓，1-开仓，2-持仓，3-清仓，参数仅限整数(0-3)，严禁传中文或函数等其他形式参数。
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            波段决策字段满足指定要求
        """
        if op_policy != 0:
            return (
                select(DailyCwDataLevel0.secucode)
                .distinct()
                .where(
                    (DailyCwDataLevel0.op_policy == op_policy)
                    & (
                        DailyCwDataLevel0.date.between(
                            get_date(start_date, is_start=True),
                            get_date(end_date),
                        )
                    )
                )
            )
        else:
            return (
                select(DailyCwDataLevel0.secucode)
                .distinct()
                .where(
                    (DailyCwDataLevel0.op_policy.in_([0, 4]))
                    & (
                        DailyCwDataLevel0.date.between(
                            get_date(start_date, is_start=True),
                            get_date(end_date),
                        )
                    )
                )
            )

    @staticmethod
    def query_foreign_institutions_ranking_pool(pool_type: int, start_date: str, end_date: str):
        """
        :术语名称:
            入选外资机构榜池
        :术语解释:
            外资机构榜池指根据一定标准筛选出的外资重点操作股票集合，包括持股池、加仓池和入场池，用于跟踪外资投资动向。
        :功能:
            查询结果函数（**必须作为第一部分调用**），根据池类型与日期范围筛选外资机构榜池股票。仅当问题涉及“外资加仓池”“外资持股池”“外资入场池”时调用。
            pool_type=5 → 外资加仓池，默认指标：get_wz_increasing_pool()；pool_type=6 → 外资持股池，默认指标：get_wz_holding_pool()；pool_type=7 → 外资入场池，默认指标：get_wz_entry_pool()；
            若用户只说“外资XXX池的股票”，**第一部分必须调用 query_foreign_institutions_ranking_pool(...)**，第二部分仅返回对应的**一个**核心指标（见上表）
            若用户明确提到具体指标词（如“资金”、“占比”、“外资个数”），则第二部分改为对应指标函数（如 get_pp_funds(), get_pp_ratio(), get_entry_pool_pp_count()）。
            严禁将指标函数作为查询函数调用（例如不要生成 query_wz_entry_pool() 之类不存在的查询）；禁止同时输出多个无关类别指标。
        :参数:
            pool_type(int): 池类型，5-外资加仓池，6-外资持股池，7-外资入场池
            start_date(str): 起始日期
            end_date(str): 结束日期
        :时间参数约束(至关重要):
            1. **粒度强制**：该数据为低频财报数据，仅接受 [年份+季度] 或 [年份+年报]。**严禁**使用"今天"、"近3日"等天数时间。
            2. **年份推断**：若用户仅提供年份（如"2024年"），必须将其锁定为 **"2024年年报"**。
            3. **默认行为**：若未提供时间，默认为 **"最新一期季报"**。
        :量化解释:
            无
        """
        # 特殊处理当日查询：如果传入"0d"，查询最新可用日期的数据
        if start_date == "0d" and end_date == "0d":
            # 获取指定pool_type的最新日期
            latest_date_subq = (
                select(WzHoldingPoolsLevel15.date)
                .where(WzHoldingPoolsLevel15.pool_type == pool_type)
                .order_by(desc(WzHoldingPoolsLevel15.date))
                .limit(1)
                .scalar_subquery()
            )

            q = (
                select(
                    WzHoldingPoolsLevel15.secucode.label("secucode"),
                    WzHoldingPoolsLevel15.date.label("date"),
                    WzHoldingPoolsLevel15.pool_type.label("pool_type"),
                    WzHoldingPoolsLevel15.pp_count.label("pp_count"),
                    WzHoldingPoolsLevel15.pp_ratio.label("pp_ratio"),
                    WzHoldingPoolsLevel15.pp_funds.label("pp_funds"),
                    WzHoldingPoolsLevel15.last_quarter_price.label("last_quarter_price"),
                    WzHoldingPoolsLevel15.last_in_date.label("last_in_date"),
                )
                .join(AStockBasic, AStockBasic.secucode == WzHoldingPoolsLevel15.secucode)
                .where(
                    and_(
                        WzHoldingPoolsLevel15.date == latest_date_subq,
                        WzHoldingPoolsLevel15.pool_type == pool_type,
                        AStockBasic.type == "A股",
                    )
                )
                .distinct()
            )
        else:
            # 解析日期参数（处理日期范围查询）
            start_date_obj = get_date(start_date)
            end_date_obj = get_date(end_date)

            # 根据日期范围筛选指定 pool_type 的记录
            q = (
                select(
                    WzHoldingPoolsLevel15.secucode.label("secucode"),
                    WzHoldingPoolsLevel15.date.label("date"),
                    WzHoldingPoolsLevel15.pool_type.label("pool_type"),
                    WzHoldingPoolsLevel15.pp_count.label("pp_count"),
                    WzHoldingPoolsLevel15.pp_ratio.label("pp_ratio"),
                    WzHoldingPoolsLevel15.pp_funds.label("pp_funds"),
                    WzHoldingPoolsLevel15.last_quarter_price.label("last_quarter_price"),
                    WzHoldingPoolsLevel15.last_in_date.label("last_in_date"),
                )
                .join(AStockBasic, AStockBasic.secucode == WzHoldingPoolsLevel15.secucode)
                .where(
                    and_(
                        WzHoldingPoolsLevel15.date >= start_date_obj,
                        WzHoldingPoolsLevel15.date <= end_date_obj,
                        WzHoldingPoolsLevel15.pool_type == pool_type,
                        AStockBasic.type == "A股",
                    )
                )
                .distinct()
            )

        return q

    @staticmethod
    def query_zhu_flow(flow_type: int, start_date: str, end_date: str):
        """
        :术语名称:
            主力资金异动流入流出状态
        :术语解释:
            主力资金异动流入流出状态
        :功能:
            筛选指定日期范围内主力资金出现流入或流出异动标识的股票。仅当用户问题明确涉及“主力资金流入状态”、“主力资金流出状态”或“主力资金异动状态”等整体状态时方可调用。本函数仅用于筛选整体状态类问题（如“主力资金流出的股票”），禁止与任何具体指标函数（如 get_zhu_out_days()、get_zhu_in_days() 、get_zhu_data() 、get_zhu_days()等）。若用户仅询问具体数值或增减变化内容，禁止调用本函数，应改为使用对应的具体指标函数。
        :参数:
            flow_type(int): 1-流入，2-流出
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        return (
            select(FundMoveFlagLevel0.secucode)
            .distinct()
            .where(
                (FundMoveFlagLevel0.zhu_flag == flow_type)
                & (
                    FundMoveFlagLevel0.date.between(
                        get_date(start_date, is_start=True),
                        get_date(end_date),
                    )
                )
            )
        )

    @staticmethod
    def query_duo_flow(flow_type: int, start_date: str, end_date: str):
        """
        :术语名称:
            多空资金异动流入流出状态
        :术语解释:
            多空资金异动流入流出状态
        :功能:
            筛选指定日期范围内多空资金出现流入或流出异动标识的股票。仅当用户问题明确涉及“多空资金流入状态”、“多空资金流出状态”或“多空资金异动状态”等整体状态时方可调用。本函数仅用于筛选整体状态类问题（如“多空资金流出的股票”、“多空资金流入状态的股票”），禁止与任何具体指标函数（如 get_duo_out_days()、get_duo_in_days() 、get_duo_data() 、get_duo_days()等）混用。若用户仅询问具体数值、增减变化或资金规模（如“流入多少”、“增减仓比例”等），禁止调用本函数，应改为使用对应的具体指标函数。
        :参数:
            flow_type(int): 1-流入，2-流出
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        return (
            select(FundMoveFlagLevel0.secucode)
            .distinct()
            .where(
                (FundMoveFlagLevel0.duo_flag == flow_type)
                & (
                    FundMoveFlagLevel0.date.between(
                        get_date(start_date, is_start=True),
                        get_date(end_date),
                    )
                )
            )
        )

    @staticmethod
    def query_gan_flow(flow_type: int, start_date: str, end_date: str):
        """
        :术语名称:
            敢死队资金异动流入流出状态
        :术语解释:
            敢死队资金异动流入流出状态
        :功能:
            筛选指定日期范围内敢死队资金出现流入或流出异动标识的股票。仅当用户问题明确涉及“敢死队资金流入状态”、“敢死队资金流出状态”或“敢死队资金异动状态”等整体状态时方可调用。本函数仅用于筛选整体状态类问题（如“敢死队资金流出的股票”），禁止与任何具体指标函数（如 get_gan_out_days()、get_gan_in_days() 、get_gan_data() 、get_gan_days()等）。若用户仅询问具体数值或增减变化内容，禁止调用本函数，应改为使用对应的具体指标函数。
        :参数:
            flow_type(int): 1-流入，2-流出
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        return (
            select(FundMoveFlagLevel0.secucode)
            .distinct()
            .where(
                (FundMoveFlagLevel0.gan_flag == flow_type)
                & (
                    FundMoveFlagLevel0.date.between(
                        get_date(start_date, is_start=True),
                        get_date(end_date),
                    )
                )
            )
        )

    @staticmethod
    def query_oversold_pool(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            日线超跌选股股票
        :术语解释:
            日线超跌选股股票
        :功能:
            用于筛选出在指定日期范围内入选“日线超跌选股池”的所有股票集合。仅当用户问题包含“有哪些股票”“筛选出”“选出”“属于日线超跌选股池的股票有哪些”等语义时调用此函数。严禁用于回答“是否属于”“是不是”“是否为超跌股”“是否在超跌选股池中”“是否为日线超跌选股股票”等判断类问题。调用时需要get_oversold_pool()进行数据展示。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        # 从股票池中拆分出每日的股票代码列表
        pool_stocks_subquery = (
            select(
                DailyHjkpoolsLevel5.date,
                func.arrayJoin(func.splitByChar(",", func.coalesce(DailyHjkpoolsLevel5.stock_pools, ""))).label(
                    "secucode"
                ),
            )
            .where(
                and_(
                    DailyHjkpoolsLevel5.filter_conditions == "0:1:1:0",
                    DailyHjkpoolsLevel5.date.between(get_date(start_date, is_start=True), get_date(end_date)),
                )
            )
            .subquery()
        )

        # 将股票池与基础信息表和状态表连接，进行最终筛选
        return (
            select(pool_stocks_subquery.c.secucode)
            .distinct()
            .select_from(pool_stocks_subquery)
            .join(AStockBasic, pool_stocks_subquery.c.secucode == AStockBasic.secucode)
            .join(
                DailyQlstockBasicLevel5,
                and_(
                    pool_stocks_subquery.c.secucode == DailyQlstockBasicLevel5.secucode,
                    pool_stocks_subquery.c.date == DailyQlstockBasicLevel5.date,
                ),
            )
            .where(
                and_(
                    AStockBasic.is_cwfx == 0,
                    AStockBasic.is_qzts == 0,
                    AStockBasic.is_tsfx == 0,
                    DailyQlstockBasicLevel5.is_new == 0,
                )
            )
        )

    @staticmethod
    def query_breakthrough_pool(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            日线突破选股股票
        :术语解释:
            日线突破选股股票
        :功能:
            用于筛选出在指定日期范围内入选“日线突破选股池”的所有股票集合。仅当用户问题包含“有哪些股票”“筛选出”“选出”“属于日线突破选股池的股票有哪些”“日线突破选股池包含哪些股票”等筛选类语义时调用此函数。严禁用于回答“是否属于”“是不是”“是否为突破股”“是否在突破选股池中”“是否为日线突破选股股票”“是日线突破选股股票吗”等判断类问题。调用时需要get_breakthrough_pool()进行数据展示。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        # 从股票池中拆分出每日的股票代码列表
        pool_stocks_subquery = (
            select(
                DailyHjkpoolsLevel5.date,
                func.arrayJoin(func.splitByChar(",", func.coalesce(DailyHjkpoolsLevel5.stock_pools, ""))).label(
                    "secucode"
                ),
            )
            .where(
                and_(
                    DailyHjkpoolsLevel5.filter_conditions == "0:3:1:0",
                    DailyHjkpoolsLevel5.date.between(get_date(start_date, is_start=True), get_date(end_date)),
                )
            )
            .subquery()
        )

        # 将股票池与基础信息表和状态表连接，进行最终筛选
        return (
            select(pool_stocks_subquery.c.secucode)
            .distinct()
            .select_from(pool_stocks_subquery)
            .join(AStockBasic, pool_stocks_subquery.c.secucode == AStockBasic.secucode)
            .join(
                DailyQlstockBasicLevel5,
                and_(
                    pool_stocks_subquery.c.secucode == DailyQlstockBasicLevel5.secucode,
                    pool_stocks_subquery.c.date == DailyQlstockBasicLevel5.date,
                ),
            )
            .where(
                and_(
                    AStockBasic.is_cwfx == 0,
                    AStockBasic.is_qzts == 0,
                    AStockBasic.is_tsfx == 0,
                    DailyQlstockBasicLevel5.is_new == 0,
                )
            )
        )

    @staticmethod
    def query_low_price_pool(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            股价低位(低价股)
        :术语解释:
            股价低位(低价股)
        :功能:
            用于筛选出在指定日期范围内股价处于低位的所有股票集合。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            period_low >= 60，表示今日创60日及以上新低
        """
        query = (
            select(
                AStockMarketCur.secucode,
                AStockMarketCur.period_low.label("period_low"),
            )
            .where(AStockMarketCur.period_low >= 60)
            .order_by(AStockMarketCur.period_low.desc())
        )
        return query

    @staticmethod
    def query_good_performance(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            业绩好
        :术语解释:
            业绩好
        :功能:
            用于筛选出在指定日期范围内业绩良好的所有股票集合。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            营业总收入同比增长率 >= 20% AND 归母净利润同比增长率 >= 5%
            使用最新一期财务数据进行对比
        """
        # 获取最新财务数据
        latest_financial = select(
            AStockFinancial.secucode,
            AStockFinancial.date,
            AStockFinancial.income_total_yoy,
            AStockFinancial.parent_net_profit_yoy,
            func.row_number()
            .over(
                partition_by=AStockFinancial.secucode,
                order_by=AStockFinancial.date.desc(),
            )
            .label("rn"),
        ).subquery("latest_financial")

        # 筛选最新一期且符合业绩条件的股票
        query = (
            select(latest_financial.c.secucode)
            .where(
                and_(
                    latest_financial.c.rn == 1,  # 最新一期
                    latest_financial.c.income_total_yoy >= 0.20,  # 营业总收入同比增长率 >= 20%
                    latest_financial.c.parent_net_profit_yoy >= 0.05,  # 归母净利润同比增长率 >= 5%
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_high_quality_stock(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            优质股
        :术语解释:
            优质股
        :功能:
            用于筛选出在指定日期范围内的优质股票集合。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            1. 净资产收益率 >= 20%
            2. 归母净利润同比增长率 >= 5%
            3. 营业总收入同比增长率 >= 20%
            使用最新一期财务数据进行对比
        """
        # 获取最新财务数据
        latest_financial = select(
            AStockFinancial.secucode,
            AStockFinancial.date,
            AStockFinancial.roe,  # 净资产收益率
            AStockFinancial.parent_net_profit_yoy,  # 归母净利润同比增长率
            AStockFinancial.income_total_yoy,  # 营业总收入同比增长率
            func.row_number()
            .over(
                partition_by=AStockFinancial.secucode,
                order_by=AStockFinancial.date.desc(),
            )
            .label("rn"),
        ).subquery("latest_financial")

        # 筛选最新一期且符合优质股条件的股票
        query = (
            select(latest_financial.c.secucode)
            .where(
                and_(
                    latest_financial.c.rn == 1,  # 最新一期
                    latest_financial.c.roe >= 0.20,  # 净资产收益率 >= 20%
                    latest_financial.c.parent_net_profit_yoy >= 0.05,  # 归母净利润同比增长率 >= 5%
                    latest_financial.c.income_total_yoy >= 0.20,  # 营业总收入同比增长率 >= 20%
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_small_increase(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            涨幅不大（小）
        :术语解释:
            涨幅不大（小）
        :功能:
            用于筛选出在指定日期范围内涨幅不大或涨幅小的所有股票集合。当且仅当用户问题模糊，语义为涨幅不大、涨幅较小等表达涨幅且涨幅小时选取。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            20日涨跌幅 > 0 AND 20日涨跌幅 < 0.15
            AND 60日涨跌幅 > 0 AND 60日涨跌幅 < 0.3
            AND 120日涨跌幅 > 0 AND 120日涨跌幅 < 0.5
        """
        # 固定参数
        n1, n2, n3 = 20, 60, 120  # 短期、中期、长期周期
        threshold1, threshold2, threshold3 = 0.15, 0.3, 0.5  # 各周期阈值

        # 获取日期范围
        _, start, end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            AStockMarket, start_date, end_date, lookback_days=n3
        )

        close_float = cast(AStockMarket.close, Float)

        # 计算N1日涨跌幅
        prev_close_n1 = func.lagInFrame(AStockMarket.close, n1, 0).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
        )
        prev_close_n1_float = cast(prev_close_n1, Float)
        zdf_n1 = case(
            (prev_close_n1_float != 0, (close_float - prev_close_n1_float) / prev_close_n1_float),
            else_=None,
        )

        # 计算N2日涨跌幅
        prev_close_n2 = func.lagInFrame(AStockMarket.close, n2, 0).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
        )
        prev_close_n2_float = cast(prev_close_n2, Float)
        zdf_n2 = case(
            (prev_close_n2_float != 0, (close_float - prev_close_n2_float) / prev_close_n2_float),
            else_=None,
        )

        # 计算N3日涨跌幅
        prev_close_n3 = func.lagInFrame(AStockMarket.close, n3, 0).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
        )
        prev_close_n3_float = cast(prev_close_n3, Float)
        zdf_n3 = case(
            (prev_close_n3_float != 0, (close_float - prev_close_n3_float) / prev_close_n3_float),
            else_=None,
        )

        # 使用 CTE 计算涨跌幅
        zdf_cte = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                zdf_n1.label("zdf_n1"),
                zdf_n2.label("zdf_n2"),
                zdf_n3.label("zdf_n3"),
            )
            .where(AStockMarket.date.between(start, end))
            .subquery("zdf_cte")
        )

        # 筛选涨幅不大的股票（涨幅必须大于0）
        query = (
            select(zdf_cte.c.secucode)
            .where(
                and_(
                    zdf_cte.c.zdf_n1 > 0,  # 20日涨跌幅 > 0
                    zdf_cte.c.zdf_n1 < threshold1,  # 20日涨跌幅 < 0.15
                    zdf_cte.c.zdf_n2 > 0,  # 60日涨跌幅 > 0
                    zdf_cte.c.zdf_n2 < threshold2,  # 60日涨跌幅 < 0.3
                    zdf_cte.c.zdf_n3 > 0,  # 120日涨跌幅 > 0
                    zdf_cte.c.zdf_n3 < threshold3,  # 120日涨跌幅 < 0.5
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_fund_large_inflow(
        fund_type: int,
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            资金大幅流入(多)
        :术语解释:
            资金大幅流入(多)
        :功能:
            用于筛选出指定日期范围内资金大幅流入的所有股票集合，当且仅当用户问题模糊，语义为资金大幅流入、资金多、资金流入多等表达资金多时选取。
        :参数:
            fund_type(int): 资金类型，1=主力资金，2=多空资金，3=敢死队资金，默认为1
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            T 日资金/T-1 日资金 > 1.5，且 T 日资金 > 0
        """
        # 验证 fund_type 参数
        valid_fund_types = {1, 2, 3}
        if fund_type not in valid_fund_types:
            raise ValueError(f"fund_type 必须是 {valid_fund_types} 中的一个，当前值为: {fund_type}")

        # 获取资金字段
        fund_field_map = {
            1: DailyZjdxStatLevel5.zhu,  # 主力资金
            2: DailyZjdxStatLevel5.duo,  # 多空资金
            3: DailyZjdxStatLevel5.gan,  # 敢死队资金
        }
        fund_field = fund_field_map[fund_type]

        # 获取日期范围（需要3天数据来计算T和T-1的增量）
        _, start, end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            DailyZjdxStatLevel5, start_date, end_date, lookback_days=2
        )

        # 计算T日资金增量 = 累计(T) - 累计(T-1)
        fund_t = fund_field - func.lagInFrame(fund_field, 1, 0).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        )

        # 计算T-1日资金增量 = 累计(T-1) - 累计(T-2)
        fund_t_1 = func.lagInFrame(fund_field, 1, 0).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        ) - func.lagInFrame(fund_field, 2, 0).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        )

        fund_delta = (
            select(
                DailyZjdxStatLevel5.secucode,
                DailyZjdxStatLevel5.date,
                cast(fund_t, Float).label("fund_t"),
                cast(fund_t_1, Float).label("fund_t_1"),
            )
            .where(DailyZjdxStatLevel5.date.between(start, end))
            .subquery("fund_delta")
        )

        # 构建 T 日资金/T-1 日资金 > 1.5，且 T 日资金 > 0 的条件
        query = (
            select(fund_delta.c.secucode)
            .where(
                and_(
                    fund_delta.c.fund_t > 0,  # T 日资金 > 0
                    fund_delta.c.fund_t_1 != 0,  # T-1 日资金不为0（避免除零错误）
                    (fund_delta.c.fund_t / fund_delta.c.fund_t_1) > 1.5,  # T 日资金/T-1 日资金 > 1.5
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_ma5_cross_ma20(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            5日均线上穿20日均线
        :术语解释:
            5日均线上穿20日均线
        :功能:
            用于筛选出指定日期5日均线大于20日均线的所有股票集合。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            5日MA > 20日MA
        """
        # 获取日期范围（需要20天历史数据来计算20日均线）
        _, start, end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            AStockMarket, start_date, end_date, lookback_days=20
        )

        # 计算5日均线
        ma_5 = func.avg(AStockMarket.close).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-4, 0),  # ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )

        # 计算20日均线
        ma_20 = func.avg(AStockMarket.close).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-19, 0),  # ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )

        # 使用窗口函数计算每个股票的最新记录
        ma_data = (
            select(
                AStockMarket.secucode,
                cast(ma_5, Float).label("ma_5"),
                cast(ma_20, Float).label("ma_20"),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.date.between(start, end))
            .subquery("ma_data")
        )

        # 筛选5日MA > 20日MA的股票
        query = (
            select(ma_data.c.secucode)
            .where(
                and_(
                    ma_data.c.rn == 1,  # 只选择每个股票的最新一天
                    ma_data.c.ma_5 > ma_data.c.ma_20,  # 5日MA > 20日MA
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_strong_stock(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            强势股
        :术语解释:
            强势股
        :功能:
            用于筛选出指定日期范围内处于强势状态的所有股票集合。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            1. MA均线多头排列
            2. 量比 > 2
            3. T日主力资金/T-1日主力资金 > 2，且T日主力资金 > 0
        """
        # 获取日期范围
        start, end = get_date(start_date, is_start=True), get_date(end_date)

        # 条件1: MA均线多头排列
        ma_aligned = (
            select(AStockMarket.secucode, AStockMarket.date)
            .where(and_(AStockMarket.ma_dtpl == 1, AStockMarket.date.between(start, end)))
            .subquery("ma_aligned")
        )

        # 条件2: 量比 > 2
        high_volume = (
            select(AStockMarketCur.secucode, AStockMarketCur.date)
            .where(AStockMarketCur.qrr > 2)
            .subquery("high_volume")
        )

        # 条件3: T日主力资金/T-1日主力资金 > 2，且T日主力资金 > 0
        # 获取日期范围（需要3天数据来计算T和T-1的增量）
        _, fund_start, fund_end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            DailyZjdxStatLevel5, start_date, end_date, lookback_days=2
        )

        # 使用主力资金（fund_type=1）
        fund_field = DailyZjdxStatLevel5.zhu

        # 计算T日资金增量 = 累计(T) - 累计(T-1)
        fund_t = fund_field - func.lagInFrame(fund_field, 1, 0).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        )

        # 计算T-1日资金增量 = 累计(T-1) - 累计(T-2)
        fund_t_1 = func.lagInFrame(fund_field, 1, 0).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        ) - func.lagInFrame(fund_field, 2, 0).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        )

        fund_delta = (
            select(
                DailyZjdxStatLevel5.secucode,
                DailyZjdxStatLevel5.date,
                cast(fund_t, Float).label("fund_t"),
                cast(fund_t_1, Float).label("fund_t_1"),
            )
            .where(DailyZjdxStatLevel5.date.between(fund_start, fund_end))
            .subquery("fund_delta")
        )

        strong_fund = (
            select(fund_delta.c.secucode, fund_delta.c.date)
            .where(
                and_(
                    fund_delta.c.fund_t > 0,  # T 日资金 > 0
                    fund_delta.c.fund_t_1 != 0,  # T-1 日资金不为0（避免除零错误）
                    (fund_delta.c.fund_t / fund_delta.c.fund_t_1) > 2.0,  # T 日资金/T-1 日资金 > 2
                )
            )
            .distinct()
            .subquery("strong_fund")
        )

        # 三个条件取交集：同时满足均线多头排列、量比>2、主力资金强势
        query = (
            select(ma_aligned.c.secucode)
            .join(
                high_volume,
                and_(ma_aligned.c.secucode == high_volume.c.secucode, ma_aligned.c.date == high_volume.c.date),
            )
            .join(
                strong_fund,
                and_(ma_aligned.c.secucode == strong_fund.c.secucode, ma_aligned.c.date == strong_fund.c.date),
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_profitable(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            财报盈利
        :术语解释:
            财报盈利
        :功能:
            用于筛选出最新一期净利润大于0的所有股票集合。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            最新一期净利润 > 0
        """
        # 使用row_number()获取每个股票的最新一期财报数据
        latest_profit = select(
            AStockProfitView.secucode,
            AStockProfitView.date,
            AStockProfitView.net_profit,
            func.row_number()
            .over(partition_by=AStockProfitView.secucode, order_by=AStockProfitView.date.desc())
            .label("rn"),
        ).subquery("latest_profit")

        # 筛选最新一期且净利润 > 0 的股票
        query = (
            select(latest_profit.c.secucode)
            .where(
                and_(
                    latest_profit.c.rn == 1,  # 只选择最新一期
                    latest_profit.c.net_profit > 0,  # 净利润 > 0
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_low_start_pool(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            低位启动
        :术语解释:
            低位启动
        :功能:
            用于筛选出在指定日期范围内处于低位启动状态的所有股票集合。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            1. period_low >= 60（近60个交易日创历史新低）
            2. qrr > 2（量比>2）
            3. 连续5个交易日主力资金>0（从最新日期往前连续5天）
        """
        # 条件1: 从当前数据筛选 period_low >= 60 AND qrr > 2
        low_price_condition = select(AStockMarketCur.secucode).where(
            and_(
                AStockMarketCur.period_low >= 60,
                AStockMarketCur.qrr > 2,
            )
        )

        # 条件2: 从最新日期往前连续5天主力资金增量>0
        # 查6天数据，计算每天增量，然后从最新日期往前检查连续5天增量>0
        _, start, end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            DailyZjdxStatLevel5, start_date, end_date, lookback_days=5
        )

        # Step 1: 计算每天的增量
        delta_calc = (
            select(
                DailyZjdxStatLevel5.secucode,
                DailyZjdxStatLevel5.date,
                (
                    DailyZjdxStatLevel5.zhu
                    - func.lagInFrame(DailyZjdxStatLevel5.zhu, 1, 0).over(
                        partition_by=DailyZjdxStatLevel5.secucode,
                        order_by=DailyZjdxStatLevel5.date,
                    )
                ).label("delta"),
            )
            .where(DailyZjdxStatLevel5.date.between(start, end))
            .subquery("delta_calc")
        )

        # 从最新日期往前计算连续正增量天数
        consecutive_count = (
            select(
                delta_calc.c.secucode,
                func.sum(case((delta_calc.c.delta > 0, 1), else_=0))
                .over(
                    partition_by=delta_calc.c.secucode,
                    order_by=delta_calc.c.date.desc(),
                )
                .label("consecutive_positive_days"),
            )
            .where(delta_calc.c.date == end)
            .subquery("consecutive_count")
        )

        main_fund_consecutive = (
            select(consecutive_count.c.secucode).where(consecutive_count.c.consecutive_positive_days >= 5).distinct()
        )

        # 联合两个条件：取交集
        return intersect(low_price_condition, main_fund_consecutive)

    @staticmethod
    def query_low_valuation_pe(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            低估值
        :术语解释:
            低估值
        :功能:
            用于筛选出在指定日期范围内处于低估值状态的所有股票集合。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            PE 百分位 = 历史中低于当前 PE 的天数 / 总历史天数 × 100%＜30%
        """
        # CTE1: 获取当前PE（从AStockMarketCur.pe_ratio_static）
        current_pe = (
            select(
                AStockMarketCur.secucode,
                AStockMarketCur.pe_ratio_static.label("current_pe"),
            )
            .join(AStockBasic, AStockMarketCur.secucode == AStockBasic.secucode)
            .where(
                AStockBasic.type == "A股",
                AStockBasic.is_ts == 0,
                AStockMarketCur.pe_ratio_static.isnot(None),
                AStockMarketCur.pe_ratio_static != 0,
            )
            .subquery("current_pe")
        )

        # CTE2: 先获取每个股票的251条历史数据（使用子查询预先限制）
        # 这种方式可以避免处理全部历史数据，大幅提升性能
        history_with_rn = (
            select(
                AStockMarket.secucode,
                AStockMarket.pe_static,
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .join(AStockBasic, AStockMarket.secucode == AStockBasic.secucode)
            .where(
                AStockBasic.type == "A股",
                AStockBasic.is_ts == 0,
                AStockMarket.pe_static.isnot(None),
                AStockMarket.pe_static != 0,
            )
            .subquery("history_with_rn")
        )

        history_pe_limited = (
            select(history_with_rn.c.secucode, history_with_rn.c.pe_static, history_with_rn.c.rn)
            .select_from(history_with_rn)
            .where(history_with_rn.c.rn <= 251)
            .subquery("history_pe_limited")
        )

        # CTE3: 排除最新一天（rn=1），使用剩余的250条历史数据计算百分位
        pe_percentile_calc = (
            select(
                history_pe_limited.c.secucode,
                # 计算历史数据中低于当前PE的天数
                func.count().filter(history_pe_limited.c.pe_static < current_pe.c.current_pe).label("days_below_pe"),
                # 计算总天数
                func.count().label("total_days"),
            )
            .join(current_pe, history_pe_limited.c.secucode == current_pe.c.secucode)
            .where(history_pe_limited.c.rn > 1)  # 排除最新一天
            .group_by(history_pe_limited.c.secucode, current_pe.c.current_pe)
            .subquery("pe_percentile_calc")
        )

        # CTE4: 筛选百分位 < 30% 的股票
        result = (
            select(pe_percentile_calc.c.secucode)
            .where(
                and_(
                    pe_percentile_calc.c.total_days > 0,
                    (
                        cast(pe_percentile_calc.c.days_below_pe, Float)
                        / cast(pe_percentile_calc.c.total_days, Float)
                        * 100
                    )
                    < 30,
                )
            )
            .distinct()
        )

        return result

    @staticmethod
    def query_duo_pool(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            多空资金池股票
        :术语解释:
            多空资金池股票
        :功能:
            用于筛选出在指定日期范围内入选“多空资金股票池”的所有股票集合。仅当用户问题包含“有哪些股票”“筛选出”“选出”“属于多空资金选股池的股票有哪些”“多空资金选股池包含哪些股票”等筛选类语义时调用此函数。严禁用于回答“是否属于”“是不是”“是否为多空资金池”“是否在多空资金选股池中”“是否为多空资金股票”“是多空资金股票吗”等判断类问题。调用时需要 get_duo_pool()进行数据展示。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        # 从股票池中拆分出每日的股票代码列表
        pool_stocks_subquery = (
            select(
                DailyLdPoolsLevel5.date,
                func.arrayJoin(func.splitByChar(",", func.coalesce(DailyLdPoolsLevel5.stock_pools, ""))).label(
                    "secucode"
                ),
            )
            .where(
                and_(
                    DailyLdPoolsLevel5.filter_conditions == "0:7:0:0",
                    DailyLdPoolsLevel5.date.between(get_date(start_date, is_start=True), get_date(end_date)),
                )
            )
            .subquery()
        )

        # 将股票池与基础信息表和状态表连接，进行最终筛选
        return (
            select(pool_stocks_subquery.c.secucode)
            .distinct()
            .select_from(pool_stocks_subquery)
            .join(AStockBasic, pool_stocks_subquery.c.secucode == AStockBasic.secucode)
            .join(
                DailyQlstockBasicLevel5,
                and_(
                    pool_stocks_subquery.c.secucode == DailyQlstockBasicLevel5.secucode,
                    pool_stocks_subquery.c.date == DailyQlstockBasicLevel5.date,
                ),
            )
            .where(
                and_(
                    AStockBasic.is_cwfx == 0,
                    AStockBasic.is_qzts == 0,
                    AStockBasic.is_tsfx == 0,
                    DailyQlstockBasicLevel5.is_new == 0,
                )
            )
        )

    @staticmethod
    def query_wz_opt_flag_pool(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            日线战区优化旗股票
        :术语解释:
            日线战区优化旗股票
        :功能:
            用于筛选出在指定日期范围内入选“日线战区优化旗股票池”的所有股票集合。仅当用户问题包含“有哪些股票”“筛选出”“选出”“属于战区优化旗选股池的股票有哪些”“日线战区优化旗选股池包含哪些股票”等筛选类语义时调用此函数。严禁用于回答“是否属于”“是不是”“是否为战区优化旗”“是否在战区优化旗选股池中”“是否为战区优化旗股票”“是日线战区优化旗股票吗”等判断类问题。调用时需要 get_wz_opt_flag_pool()进行数据展示。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        # 从股票池中拆分出每日的股票代码列表
        pool_stocks_subquery = (
            select(
                DailyLdPoolsLevel5.date,
                func.arrayJoin(func.splitByChar(",", func.coalesce(DailyLdPoolsLevel5.stock_pools, ""))).label(
                    "secucode"
                ),
            )
            .where(
                and_(
                    DailyLdPoolsLevel5.filter_conditions == "0:6:0:0",
                    DailyLdPoolsLevel5.date.between(get_date(start_date, is_start=True), get_date(end_date)),
                )
            )
            .subquery()
        )

        # 将股票池与基础信息表和状态表连接，进行最终筛选
        return (
            select(pool_stocks_subquery.c.secucode)
            .distinct()
            .select_from(pool_stocks_subquery)
            .join(AStockBasic, pool_stocks_subquery.c.secucode == AStockBasic.secucode)
            .join(
                DailyQlstockBasicLevel5,
                and_(
                    pool_stocks_subquery.c.secucode == DailyQlstockBasicLevel5.secucode,
                    pool_stocks_subquery.c.date == DailyQlstockBasicLevel5.date,
                ),
            )
            .where(
                and_(
                    AStockBasic.is_cwfx == 0,
                    AStockBasic.is_qzts == 0,
                    AStockBasic.is_tsfx == 0,
                    DailyQlstockBasicLevel5.is_new == 0,
                )
            )
        )

    @staticmethod
    def query_volatility_pool(
        start_date: str,
        end_date: str,
    ):
        """
        :术语名称:
            日线震荡选股股票
        :术语解释:
            日线震荡选股股票
        :功能:
            用于筛选出在指定日期范围内入选“日线震荡选股池”的所有股票集合。仅当用户问题包含“有哪些股票”“筛选出”“选出”“属于日线震荡选股池的股票有哪些”“日线震荡选股池包含哪些股票”等筛选类语义时调用此函数。严禁用于回答“是否属于”“是不是”“是否为突破股”“是否在震荡选股池中”“是否为日线震荡选股股票”“是日线震荡选股股票吗”等判断类问题，判断类问题应该使用get_volatility_pool。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        # 从股票池中拆分出每日的股票代码列表
        pool_stocks_subquery = (
            select(
                DailyHjkpoolsLevel5.date,
                func.arrayJoin(func.splitByChar(",", func.coalesce(DailyHjkpoolsLevel5.stock_pools, ""))).label(
                    "secucode"
                ),
            )
            .where(
                and_(
                    DailyHjkpoolsLevel5.filter_conditions == "0:2:1:0",
                    DailyHjkpoolsLevel5.date.between(get_date(start_date, is_start=True), get_date(end_date)),
                )
            )
            .subquery()
        )

        # 将股票池与基础信息表和状态表连接，进行最终筛选
        return (
            select(pool_stocks_subquery.c.secucode)
            .distinct()
            .select_from(pool_stocks_subquery)
            .join(AStockBasic, pool_stocks_subquery.c.secucode == AStockBasic.secucode)
            .join(
                DailyQlstockBasicLevel5,
                and_(
                    pool_stocks_subquery.c.secucode == DailyQlstockBasicLevel5.secucode,
                    pool_stocks_subquery.c.date == DailyQlstockBasicLevel5.date,
                ),
            )
            .where(
                and_(
                    AStockBasic.is_cwfx == 0,
                    AStockBasic.is_qzts == 0,
                    AStockBasic.is_tsfx == 0,
                    DailyQlstockBasicLevel5.is_new == 0,
                )
            )
        )

    @staticmethod
    def query_performance_forecast_big_increase(start_date: str, end_date: str):
        """
        :术语名称:
            业绩预告盈利大增
        :术语解释:
            指公司在业绩预告中披露本期利润较上期大幅增长的情况。
        :功能:
            筛选出在指定日期范围内，业绩预告类型为“盈利大增”的股票。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        return (
            select(AStockMarketCur.secucode)
            .distinct()
            .where(
                (AStockMarketCur.performance_forecast == 1),
                (AStockMarketCur.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
        )

    @staticmethod
    def query_performance_forecast_small_increase(start_date: str, end_date: str):
        """
        :术语名称:
            业绩预告盈利略增
        :术语解释:
            指公司在业绩预告中披露利润较上期略有增长的情况。
        :功能:
            筛选出在指定日期范围内，业绩预告类型为“盈利略增”的股票。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        return (
            select(AStockMarketCur.secucode)
            .distinct()
            .where(
                (AStockMarketCur.performance_forecast == 2),
                (AStockMarketCur.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
        )

    @staticmethod
    def query_performance_forecast_big_decrease(start_date: str, end_date: str):
        """
        :术语名称:
            业绩预告盈利大减
        :术语解释:
            指公司在业绩预告中披露本期利润较上期大幅减少的情况。
        :功能:
            筛选出在指定日期范围内，业绩预告类型为“盈利大减”的股票。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        return (
            select(AStockMarketCur.secucode)
            .distinct()
            .where(
                (AStockMarketCur.performance_forecast == 3),
                (AStockMarketCur.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
        )

    @staticmethod
    def query_performance_forecast_small_decrease(start_date: str, end_date: str):
        """
        :术语名称:
            业绩预告盈利略降
        :术语解释:
            指公司在业绩预告中披露利润较上期略有下降的情况。
        :功能:
            筛选出在指定日期范围内，业绩预告类型为“盈利略降”的股票。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        return (
            select(AStockMarketCur.secucode)
            .distinct()
            .where(
                (AStockMarketCur.performance_forecast == 4),
                (AStockMarketCur.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
        )

    @staticmethod
    def query_performance_forecast_turn_profit(start_date: str, end_date: str):
        """
        :术语名称:
            业绩预告扭亏为盈
        :术语解释:
            指公司在业绩预告中披露由亏损转为盈利的情况。
        :功能:
            筛选出在指定日期范围内，业绩预告类型为“扭亏为盈”的股票。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        return (
            select(AStockMarketCur.secucode)
            .distinct()
            .where(
                (AStockMarketCur.performance_forecast == 5),
                (AStockMarketCur.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
        )

    @staticmethod
    def query_performance_forecast_turn_loss(start_date: str, end_date: str):
        """
        :术语名称:
            业绩预告由盈转亏
        :术语解释:
            指公司在业绩预告中披露由盈利转为亏损的情况。
        :功能:
            筛选出在指定日期范围内，业绩预告类型为“由盈转亏”的股票。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        return (
            select(AStockMarketCur.secucode)
            .distinct()
            .where(
                (AStockMarketCur.performance_forecast == 6),
                (AStockMarketCur.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
        )

    @staticmethod
    def query_performance_forecast_loss_increase(start_date: str, end_date: str):
        """
        :术语名称:
            业绩预告亏损增加
        :术语解释:
            指公司在业绩预告中披露亏损幅度进一步扩大的情况。
        :功能:
            筛选出在指定日期范围内，业绩预告类型为“亏损增加”的股票。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        return (
            select(AStockMarketCur.secucode)
            .distinct()
            .where(
                (AStockMarketCur.performance_forecast == 7),
                (AStockMarketCur.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
        )

    @staticmethod
    def query_performance_forecast_loss_decrease(start_date: str, end_date: str):
        """
        :术语名称:
            业绩预告亏损减少
        :术语解释:
            指公司在业绩预告中披露亏损幅度较上期减少的情况。
        :功能:
            筛选出在指定日期范围内，业绩预告类型为“亏损减少”的股票。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        return (
            select(AStockMarketCur.secucode)
            .distinct()
            .where(
                (AStockMarketCur.performance_forecast == 8),
                (AStockMarketCur.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
        )

    @staticmethod
    def query_recently_listed_stocks(days: int, start_date: str, end_date: str):
        """
        :术语名称:
            发布时间为指定天数之内的股票
        :术语解释:
            查询上市日期在指定天数之内的股票，即最近新发行的股票
        :功能:
            查询上市日期在指定天数之内的股票，发布时间为指定天数之内的股票
        :参数:
            days (int): 天数
        """
        import datetime

        # 计算日期范围
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days)

        # 格式化为字符串
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        query = (
            select(
                AStockBasic.secucode,
            )
            .where(
                AStockBasic.issue_list_date.isnot(None),
                # 使用字符串比较筛选日期范围
                AStockBasic.issue_list_date.between(start_date_str, end_date_str),
            )
            .order_by(AStockBasic.issue_list_date.desc())
        )
        return query

    @staticmethod
    def query_top_five_sell_list(start_date: str, end_date: str):
        """
        :术语名称:
            前五卖榜
        :术语解释:
            前五卖榜
        :功能:
            查询前五卖榜的股票，仅用于"前五卖榜"筛选。当且仅当用户问题明确出现"前五"/"前五席"且语义指向"前五卖榜"时才可调用。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return (
            select(
                XwlhSeatdetailLevel15.secucode,
                XwlhSeatdetailLevel15.date,
                XwlhSeatdetailLevel15.seattype.label("seattype"),
                XwlhSeatdetailLevel15.reasonid,
            )
            .distinct()
            .where(
                (XwlhSeatdetailLevel15.seattype == 2),
                (XwlhSeatdetailLevel15.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
        )

    @staticmethod
    def query_top_five_buy_list(start_date: str, end_date: str):
        """
        :术语名称:
            前五买榜
        :术语解释:
            前五买榜
        :功能:
            查询前五买榜的股票，仅用于"前五买榜"筛选。当且仅当用户问题明确出现"前五"/"前五席"且语义指向"前五买榜"时才可调用。
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return (
            select(
                XwlhSeatdetailLevel15.secucode,
                XwlhSeatdetailLevel15.date,
                XwlhSeatdetailLevel15.seattype.label("seattype"),
                XwlhSeatdetailLevel15.reasonid,
            )
            .distinct()
            .where(
                (XwlhSeatdetailLevel15.seattype == 1),
                (XwlhSeatdetailLevel15.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
        )

    @staticmethod
    def query_hot_stock_info(start_date: str, end_date: str):
        """
        :术语名称:
            某个交易日热门股票
        :术语解释:
            某个交易日热门股票指在特定交易日内受到市场广泛关注、交易热度较高的股票。
        :功能:
            查询某个交易日热门股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """

        return (
            select(HotInfoView.secucode)
            .distinct()
            .where(
                (HotInfoView.type == 1),
                (HotInfoView.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
            .order_by(HotInfoView.sortid.desc())
        )

    @staticmethod
    def query_hot_board_info(start_date: str, end_date: str):
        """
        :术语名称:
            某个交易日热门板块
        :术语解释:
            某个交易日热门板块指在特定交易日内受到市场广泛关注、交易热度较高的板块。
        :功能:
            查询某个交易日热门板块
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """

        return (
            select(HotInfoView.secucode)
            .distinct()
            .where(
                (HotInfoView.type == 2),
                (HotInfoView.secucode.like(f"%BR01%")),
                (HotInfoView.date.between(get_date(start_date, is_start=True), get_date(end_date))),
            )
            .order_by(HotInfoView.sortid.desc())
        )

    @staticmethod
    def query_st(flag: int, start_date: str, end_date: str):
        """
        :术语名称:
            ST股
        :术语解释:
            ST股
        :功能:
            筛选出或剔除指定日期范围内标记为ST股的股票。
        :参数:
            flag(int): 1=筛选ST股，0=剔除ST股
            start_date(str): 起始日期
            end_date(str): 结束日期
        :量化解释:
            无
        """
        pattern = "%ST%"
        name_cond = AStockBasic.secuname.like(pattern) if flag == 1 else AStockBasic.secuname.not_like(pattern)

        condition = (name_cond) & AStockBasic.date.between(get_date(start_date, is_start=True), get_date(end_date))

        return select(AStockBasic.secucode).where(condition)

    @staticmethod
    def query_commercial_space_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            商业航天
        :术语解释:
            商业航天
        :功能:
            查询商业航天概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02579%"))

    @staticmethod
    def query_computer_application_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            计算机应用
        :术语解释:
            计算机应用
        :功能:
            查询计算机应用行业板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.trade.like(f"%BR01B01252%"))

    @staticmethod
    def query_artificial_intelligence_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            人工智能(AI)概念板块
        :术语解释:
            人工智能(AI)概念板块
        :功能:
            查询人工智能(AI)概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02191%"))

    @staticmethod
    def query_innovative_medicine_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            创新药
        :术语解释:
            创新药
        :功能:
            查询创新药概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02330%"))

    @staticmethod
    def query_is_not_bank_trade_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            不属于银行的股票
        :术语解释:
            不属于银行的股票
        :功能:
            查询不属于银行行业板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.trade.notlike(f"%BR01B01162%"))

    @staticmethod
    def query_ninghugaosu_stock(start_date: str, end_date: str):
        """
        :术语名称:
            宁沪高速
        :术语解释:
            宁沪高速
        :功能:
            查询宁沪高速股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600377")

    @staticmethod
    def query_ai_application_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            人工智能应用(AI应用)
        :术语解释:
            人工智能应用(AI应用)
        :功能:
            查询人工智能应用(AI应用)概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ688365",
                    "SZHQ301171",
                    "SZHQ002400",
                    "SHHQ600986",
                    "SZHQ002131",
                    "SZHQ300063",
                    "SZHQ300766",
                    "SZHQ300058",
                    "SZHQ300987",
                    "SHHQ603598",
                    "SZHQ002217",
                    "SZHQ000681",
                    "SHHQ600880",
                    "SZHQ300418",
                ]
            )
        )

    @staticmethod
    def query_space_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            航空
        :术语解释:
            航空
        :功能:
            查询航空概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            or_(AStockBasic.concept.like(f"%BR01B02579%"), AStockBasic.concept.like(f"%BR01B02024%"))
        )

    @staticmethod
    def query_tech_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            科技股
        :术语解释:
            科技股
        :功能:
            查询科技概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            or_(
                AStockBasic.concept.like(f"%BR01B02191%"),
                AStockBasic.concept.like(f"%BR01B01031%"),
                AStockBasic.concept.like(f"%BR01B02512%"),
                AStockBasic.concept.like(f"%BR01B02567%"),
                AStockBasic.concept.like(f"%BR01B02579%"),
                AStockBasic.concept.like(f"%BR01B02024%"),
                AStockBasic.concept.like(f"%BR01B02551%"),
                AStockBasic.concept.like(f"%BR01B02182%"),
            )
        )

    @staticmethod
    def query_doubao_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            豆包概念板块的股票
        :术语解释:
            豆包概念板块的股票
        :功能:
            查询豆包概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ300496",
                    "SHHQ688332",
                    "SZHQ688018",
                    "SHHQ000063",
                    "SZHQ300442",
                    "SHHQ688787",
                    "SZHQ301085",
                    "SZHQ300232",
                    "SZHQ002400",
                    "SHHQ603598",
                    "SZHQ603533",
                    "SZHQ300494",
                    "SHHQ301171",
                    "SZHQ300063",
                ]
            )
        )

    @staticmethod
    def query_3d_printing_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            3D打印概念板块的股票
        :术语解释:
            3D打印概念板块的股票
        :功能:
            查询3D打印概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02105%"))

    @staticmethod
    def query_ai_chip_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            人工智能芯片概念板块的股票
        :术语解释:
            人工智能芯片概念板块的股票
        :功能:
            查询人工智能芯片概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02511%"))

    @staticmethod
    def query_ai_medical_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            人工智能医疗概念板块的股票
        :术语解释:
            人工智能医疗概念板块的股票
        :功能:
            查询人工智能医疗概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02665%"))

    @staticmethod
    def query_ai_agent_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            人工智能智能体概念板块的股票
        :术语解释:
            人工智能智能体概念板块的股票
        :功能:
            查询人工智能智能体概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02826%"))

    @staticmethod
    def query_A_market_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            A股市场股票
        :术语解释:
            A股市场股票
        :功能:
            查询A股市场股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.type == "A股")

    @staticmethod
    def query_humanoid_robot_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            人形机器人概念板块的股票
        :术语解释:
            人形机器人概念板块的股票
        :功能:
            查询人形机器人概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02528%"))

    @staticmethod
    def query_energy_storage_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            储能概念板块的股票
        :术语解释:
            储能概念板块的股票
        :功能:
            查询储能概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02279%"))

    @staticmethod
    def query_photoresist_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            光刻胶概念板块的股票
        :术语解释:
            光刻胶概念板块的股票
        :功能:
            查询光刻胶概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02252%"))

    @staticmethod
    def query_transformer_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            变压器概念板块的股票
        :术语解释:
            变压器概念板块的股票
        :功能:
            查询变压器概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02649%"))

    @staticmethod
    def query_memory_chip_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            存储芯片概念板块的股票
        :术语解释:
            存储芯片概念板块的股票
        :功能:
            查询存储芯片概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02523%"))

    @staticmethod
    def query_hainan_free_trade_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            海南自贸概念板块的股票
        :术语解释:
            海南自贸概念板块的股票
        :功能:
            查询海南自贸概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02836%"))

    @staticmethod
    def query_wet_electronic_shemicals_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            湿电子化学品概念板块的股票
        :术语解释:
            湿电子化学品概念板块的股票
        :功能:
            查询湿电子化学品概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02684%"))

    @staticmethod
    def query_titanium_powder_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            钛白粉概念板块的股票
        :术语解释:
            钛白粉概念板块的股票
        :功能:
            查询钛白粉概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02462%"))

    @staticmethod
    def query_quantum_technology_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            量子计算概念板块的股票
        :术语解释:
            量子计算概念板块的股票
        :功能:
            查询量子计算概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02156%"))

    @staticmethod
    def query_controlled_nuclear_fusion_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            可控核聚变概念板块的股票
        :术语解释:
            可控核聚变概念板块的股票
        :功能:
            查询可控核聚变概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02564%"))

    @staticmethod
    def query_smart_wearable_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            智能穿戴概念板块的股票
        :术语解释:
            智能穿戴概念板块的股票
        :功能:
            查询智能穿戴概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02127%"))

    @staticmethod
    def query_aerospace_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            航天信息股票
        :术语解释:
            航天信息股票
        :功能:
            查询航天信息的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600271")

    @staticmethod
    def query_advanced_manufacturing_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            先进制造概念板块的股票
        :术语解释:
            先进制造概念板块的股票
        :功能:
            查询先进制造概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02656%"))

    @staticmethod
    def query_game_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            游戏概念板块的股票
        :术语解释:
            游戏概念板块的股票
        :功能:
            查询游戏概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02257%"))

    @staticmethod
    def query_gpu_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            图形处理器(GPU)概念板块的股票
        :术语解释:
            图形处理器(GPU)概念板块的股票
        :功能:
            查询图形处理器(GPU)概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02689%"))

    @staticmethod
    def query_securities_insurance_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            证券保险行业板块的股票
        :术语解释:
            证券保险行业板块的股票
        :功能:
            查询证券保险行业板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.trade.like(f"%BR01B01161%"))

    @staticmethod
    def query_st_baoying_stock(start_date: str, end_date: str):
        """
        :术语名称:
            *ST宝鹰股票
        :术语解释:
            *ST宝鹰股票
        :功能:
            查询*ST宝鹰的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002047")

    @staticmethod
    def query_autonomous_driving_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            无人驾驶概念板块的股票
        :术语解释:
            无人驾驶概念板块的股票
        :功能:
            查询无人驾驶概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02163%"))

    @staticmethod
    def query_drone_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            无人机概念板块的股票
        :术语解释:
            无人机概念板块的股票
        :功能:
            查询无人机概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02160%"))

    @staticmethod
    def query_brain_engineering_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            人脑工程概念板块的股票
        :术语解释:
            人脑工程概念板块的股票
        :功能:
            查询人脑工程概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02453%"))

    @staticmethod
    def query_tmt_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            TMT概念板块的股票
        :术语解释:
            TMT概念板块的股票
        :功能:
            查询TMT概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02178%"))

    @staticmethod
    def query_kangqiangdianzi_stock(start_date: str, end_date: str):
        """
        :术语名称:
            康强电子股票
        :术语解释:
            康强电子股票
        :功能:
            查询康强电子的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002119")

    @staticmethod
    def query_innovative_medicine_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            创新药概念板块的股票
        :术语解释:
            创新药概念板块的股票
        :功能:
            查询创新药概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02330%"))

    @staticmethod
    def query_kuajingtong_stock(start_date: str, end_date: str):
        """
        :术语名称:
            跨境通股票
        :术语解释:
            跨境通股票
        :功能:
            查询跨境通的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002640")

    @staticmethod
    def query_tiancicailiao_stock(start_date: str, end_date: str):
        """
        :术语名称:
            天赐材料股票
        :术语解释:
            天赐材料股票
        :功能:
            查询天赐材料的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002709")

    @staticmethod
    def query_military_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            军工概念板块的股票
        :术语解释:
            军工概念板块的股票
        :功能:
            查询军工概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02164%"))

    @staticmethod
    def query_media_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            传媒服务行业板块的股票
        :术语解释:
            传媒服务行业板块的股票
        :功能:
            查询传媒行业板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.trade.like(f"%BR01B01251%"))

    @staticmethod
    def query_low_altitude_economy_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            低空经济概念板块的股票
        :术语解释:
            低空经济概念板块的股票
        :功能:
            查询低空经济概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02567%"))

    @staticmethod
    def query_liquid_cooling_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            液冷概念板块的股票
        :术语解释:
            液冷概念板块的股票
        :功能:
            查询液冷概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02524%"))

    @staticmethod
    def query_perovskite_battery_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            钙钛矿电池概念板块的股票
        :术语解释:
            钙钛矿电池概念板块的股票
        :功能:
            查询钙钛矿电池概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02321%"))

    @staticmethod
    def query_analog_chip_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            模拟芯片概念板块的股票
        :术语解释:
            模拟芯片概念板块的股票
        :功能:
            查询模拟芯片概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02687%"))

    @staticmethod
    def query_cpo_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            CPO概念板块的股票
        :术语解释:
            CPO概念板块的股票
        :功能:
            查询CPO概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02498%"))

    @staticmethod
    def query_space_military_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            航天军工概念板块的股票
        :术语解释:
            航天军工概念板块的股票
        :功能:
            查询航天军工概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02024%"))

    @staticmethod
    def query_component_trade_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            元件行业板块的股票
        :术语解释:
            元件行业板块的股票
        :功能:
            查询元件行业板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.trade.like(f"%BR01B01033%"))

    @staticmethod
    def query_jing_jin_ji_integration_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            京津冀自贸概念板块的股票
        :术语解释:
            京津冀自贸概念板块的股票
        :功能:
            查询京津冀一体化概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02139%"))

    @staticmethod
    def query_multimodal_large_model_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            多模态AI概念板块的股票
        :术语解释:
            多模态AI概念板块的股票
        :功能:
            查询多模态大模型概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02440%"))

    @staticmethod
    def query_aigc_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            AIGC概念板块的股票
        :术语解释:
            AIGC概念板块的股票
        :功能:
            查询AIGC概念概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02496%"))

    @staticmethod
    def query_shennongzhongye_stock(start_date: str, end_date: str):
        """
        :术语名称:
            神农种业股票
        :术语解释:
            神农种业股票
        :功能:
            查询神农种业股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ300189")

    @staticmethod
    def query_jiufengnengyuan_stock(start_date: str, end_date: str):
        """
        :术语名称:
            九丰能源股票
        :术语解释:
            九丰能源股票
        :功能:
            查询九丰能源股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ605090")

    @staticmethod
    def query_semiconductor_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            半导体材料概念板块的股票
        :术语解释:
            半导体材料概念板块的股票
        :功能:
            查询半导体材料概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02660%"))

    @staticmethod
    def query_brain_computer_interface_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            脑机接口概念板块的股票
        :术语解释:
            脑机接口概念板块的股票
        :功能:
            查询脑机接口概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02530%"))

    @staticmethod
    def query_nuclear_power_concept_stocks(start_date: str, end_date: str):
        """
        :术语名称:
            核电概念板块的股票
        :术语解释:
            核电概念板块的股票
        :功能:
            查询核电概念板块的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.concept.like(f"%BR01B02147%"))

    @staticmethod
    def query_zhongguohejian_stock(start_date: str, end_date: str):
        """
        :术语名称:
            中国核建股票
        :术语解释:
            中国核建股票
        :功能:
            查询中国核建的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ601611")

    @staticmethod
    def query_huihuang_keji_stock(start_date: str, end_date: str):
        """
        :术语名称:
            辉煌科技股票
        :术语解释:
            辉煌科技股票
        :功能:
            查询辉煌科技的股票
        :参数:
            start_date(str): 起始日期
            end_date(str): 结束日期
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002296")

    @staticmethod
    def query_longtou(start_date: str, end_date: str, trade_or_concept: int, subquery=None):
        """
        :术语名称:
            龙头股
        :术语解释:
            龙头股
        :功能:
            查询指定日期范围内的龙头股，支持两种模式：1. 全市场龙头 (trade_or_concept=0): 用户问题中**完全没有提及**任何具体的行业、概念、地域或板块名称。示例：“筛选出龙头股”、“今天的龙头”。2. 板块/行业/概念内龙头 (trade_or_concept=1): 用户问题中**明确提及**了具体的行业、概念或地域名称和代码。示例：“电子半导体(行业代码:BR01B01031)行业的龙头”。
        :参数:
            start_date(str): 起始日期，格式为相对日期或具体日期，如'0d'表示当日
            end_date(str): 结束日期，格式为相对日期或具体日期，如'0d'表示当日
            trade_or_concept(int): 龙头类型，0=全市场龙头，1=板块/行业/概念内龙头
            subquery: 子查询对象，用于基本面龙头模式中筛选特定行业/地域/概念的股票范围
        """
        if trade_or_concept == 0:
            # 不和行业概念相关的龙头筛选
            return AStockGlobalQuery._query_longtou_normal(start_date, end_date)
        else:
            # 和行业概念相关的龙头筛选
            return AStockGlobalQuery._query_longtou_board_concept(start_date, end_date, subquery)

    @staticmethod
    def _query_longtou_normal(start_date: str, end_date: str):
        """
        不和行业概念相关的龙头：
            1.当日涨幅超5%
            2.当日成交量较前一日增长 ≥ 50%
            3.出现MA均线多头排列；
            4.连续3天主力净流入> 0
        """
        start = get_date(start_date, is_start=True)
        end = get_date(end_date)

        # 当日涨幅超5%
        zdf_condition = (AStockMarket.zdf > 0.05) & AStockMarket.date.between(start, end)
        query1 = select(AStockMarket.secucode).where(zdf_condition)

        # 当日成交量较前一日增长 ≥ 50%
        unique_id = uuid.uuid4().hex[:6]

        volume_with_lag = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                AStockMarket.volume,
                func.lagInFrame(AStockMarket.volume, 1, 0)
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date)
                .label("prev_volume"),
            )
            .where(AStockMarket.date.between(get_date(start_date, is_start=True, lookback_days=1), get_date(end_date)))
            .subquery(f"volume_with_lag_{unique_id}")
        )

        query2 = select(volume_with_lag.c.secucode).where(
            volume_with_lag.c.volume >= (volume_with_lag.c.prev_volume * 1.5),
            volume_with_lag.c.date.between(start, end),
        )

        # MA均线多头排列
        ma_dtpl_condition = AStockMarket.ma_dtpl & AStockMarket.date.between(start, end)
        query3 = select(AStockMarket.secucode).where(ma_dtpl_condition)

        # 连续3天主力净流入> 0
        consecutive_main_fund = (
            select(
                DailyZjdxStatLevel5.secucode,
                func.count(case((DailyZjdxStatLevel5.zhu > 0, 1)))
                .over(
                    partition_by=DailyZjdxStatLevel5.secucode,
                    order_by=DailyZjdxStatLevel5.date,
                    rows=(-2, 0),  # 最近3天（包括今天）
                )
                .label("consecutive_positive_days"),
            )
            .where(
                DailyZjdxStatLevel5.date.between(
                    get_date(start_date, is_start=True, lookback_days=2), get_date(end_date)
                )
            )
            .subquery(f"consecutive_main_fund_{unique_id}")
        )

        query4 = select(consecutive_main_fund.c.secucode).where(consecutive_main_fund.c.consecutive_positive_days >= 3)

        # 使用intersect组合所有条件
        result = intersect(query1, query2, query3, query4)
        return result

    @staticmethod
    def _query_longtou_board_concept(start_date: str, end_date: str, subquery=None):
        """
        和行业概念相关的龙头：
            1.净资产收益率≥15%；
            2.归母净利润连续3年增长≥10%；
            3.流通市值前10
        """
        if subquery is None:
            raise ValueError("subquery must be provided for concept/trade related longtou query")

        # 获取财务报告日期范围
        end_report = get_date(end_date)
        unique_id = uuid.uuid4().hex[:6]

        # 获取子查询中的股票代码
        if hasattr(subquery, "subquery"):
            secucode_subquery = subquery.subquery()
            secucode_column = secucode_subquery.c.secucode
        else:
            secucode_column = subquery

        # 1. 净资产收益率≥15% - 获取每个股票的最新一期财报
        roe_ranked = (
            select(
                AStockFinancial.secucode,
                AStockFinancial.roe,
                func.row_number()
                .over(partition_by=AStockFinancial.secucode, order_by=AStockFinancial.date.desc())
                .label("rn"),
            )
            .where(AStockFinancial.secucode.in_(select(secucode_column)))
            .subquery(f"roe_ranked_{unique_id}")
        )

        roe_subquery = select(roe_ranked.c.secucode).where(roe_ranked.c.rn == 1, roe_ranked.c.roe >= 0.15)

        # 归母净利润连续3年增长≥10%
        three_year_dates = (
            select(AStockFinancial.date)
            .where(
                func.toMonth(AStockFinancial.date) == 12,
                func.toDayOfMonth(AStockFinancial.date) == 31,
                AStockFinancial.date <= end_report,
            )
            .group_by(AStockFinancial.date)
            .order_by(AStockFinancial.date.desc())
            .limit(3)
            .subquery(f"three_year_dates_{unique_id}")
        )

        # 检查连续3年归母净利润增长率≥20%
        profit_growth_check = (
            select(AStockFinancial.secucode)
            .where(
                AStockFinancial.date.in_(select(three_year_dates.c.date)),
                AStockFinancial.parent_net_profit_yoy >= 0.10,
                AStockFinancial.secucode.in_(select(secucode_column)),
            )
            .group_by(AStockFinancial.secucode)
            .having(func.count(AStockFinancial.secucode) >= 3)
            .subquery(f"profit_growth_{unique_id}")
        )

        # 流通市值前10（在指定概念/行业内）
        market_cap_top10 = (
            select(AStockMarketCur.secucode)
            .where(AStockMarketCur.market_value.isnot(None), AStockMarketCur.secucode.in_(select(secucode_column)))
            .order_by(AStockMarketCur.market_value.desc())
            .limit(10)
            .subquery(f"market_cap_top10_{unique_id}")
        )

        roe_query = select(roe_subquery.c.secucode)
        profit_growth_query = select(profit_growth_check.c.secucode)
        market_cap_query = select(market_cap_top10.c.secucode)

        final_query = intersect(roe_query, profit_growth_query, market_cap_query)

        return final_query

    @staticmethod
    def _calculate_bitmask(
        field: Field, bit_value: int, start_date: str, end_date: str, secucode_query, alias_name: str = None
    ):
        """
        为位运算字段生成独立的 CTE。
        """
        table = field.class_
        start, _, end, _ = AStockGlobalQuery._get_aftermarket_date_range(table, start_date, end_date, lookback_days=0)
        unique_id = uuid.uuid4().hex[:6]

        if hasattr(secucode_query, "subquery"):
            secucode_subquery = secucode_query.subquery()
            secucode_column = secucode_subquery.c.secucode
        else:
            secucode_column = secucode_query

        if not alias_name:
            alias_name = f"{field.key}_{bit_value}"

        if bit_value == 0:
            condition = field == 0
        else:
            condition = func.bitAnd(field, bit_value) != 0

        # 生成 CTE
        cte = (
            select(table.secucode, table.date, condition.cast(Integer).label(alias_name))
            .where(table.date.between(start, end), table.secucode.in_(select(secucode_column)))
            .distinct()
            .cte(f"cte_bitmask_{alias_name}_{unique_id}")
        )

        return cte

    @staticmethod
    def _calculate_diff(
        field: Field,
        days: int,
        start_date: str,
        end_date: str,
        secucode_query,
        calculation_mode: str = "default",
        filter_positive: bool = None,
    ):
        """
        统一的差值计算函数，支持不同的计算模式和筛选条件。

        参数:
            field: 要计算的字段对象
            days: 天数
            start_date: 开始日期
            end_date: 结束日期
            secucode_query: 股票代码查询
            calculation_mode: 计算模式
                - "default": 当前值 - N天前值 (原始calculate_diff逻辑)
                - "positive": 当前值 - N天前值 (流入)
                - "negative": N天前值 - 当前值 (流出)
            filter_positive: 是否筛选正值
                - None: 不筛选 (原始calculate_diff逻辑)
                - True: 只保留正值 (流入天数/流出天数)
                - False: 不筛选 (保留所有值，但使用特定的计算模式)
        """
        BaseTable = field.class_
        trade_end = get_date(end_date)
        trade_start_original = get_date(start_date, is_start=True)

        # 根据计算模式确定字段标签和计算公式
        if calculation_mode == "positive":
            key_label = f"{field.key.split('_')[0]}_in_{days}d"
            # 流入计算：当前值 - N天前值
            calculation_expr = field - func.lagInFrame(field, days).over(
                partition_by=BaseTable.secucode, order_by=BaseTable.date
            )
            cte_prefix = "cte_pos_diff"
        elif calculation_mode == "negative":
            key_label = f"{field.key.split('_')[0]}_out_{days}d"
            # 流出计算：N天前值 - 当前值
            calculation_expr = (
                func.lagInFrame(field, days).over(partition_by=BaseTable.secucode, order_by=BaseTable.date) - field
            )
            cte_prefix = "cte_neg_diff"
        else:  # default mode
            key_label = f"{field.key}_{days}d"
            # 默认计算：当前值 - N天前值
            calculation_expr = field - func.lagInFrame(field, days).over(
                partition_by=BaseTable.secucode, order_by=BaseTable.date
            )
            cte_prefix = "cte_diff"

        # 确保secucode_query是子查询格式
        if hasattr(secucode_query, "subquery"):
            secucode_subquery = secucode_query.subquery()
            secucode_column = secucode_subquery.c.secucode
        else:
            secucode_subquery = secucode_query
            secucode_column = secucode_query

        diff_subquery = (
            select(
                BaseTable.secucode,
                BaseTable.date,
                calculation_expr.label(key_label),
            )
            .where(BaseTable.secucode.in_(select(secucode_column)))
            .subquery()
        )

        # 构建筛选条件
        conditions = [diff_subquery.c.date.between(trade_start_original, trade_end)]

        # 如果需要筛选正值
        if filter_positive is True:
            conditions.append(diff_subquery.c[key_label] > 0)

        final_cte = (
            select(diff_subquery.c.secucode, diff_subquery.c.date, diff_subquery.c[key_label])
            .where(and_(*conditions))
            .cte(f"{cte_prefix}_{field.key}_{days}d_{uuid.uuid4().hex[:6]}")
        )

        return final_cte

    @staticmethod
    def calculate_diff(
        field: Field,
        days: int,
    ):
        """
        :功能:
            查询动态计算单个数据 N 日指标(数据库中对应字段数据为总和数据，需要减法计算)。
            此方法返回一个元组，由 query_columns 方法解析以生成并加入一个计算差值的 CTE。
        :参数:
            field: 要计算的字段对象 (例如 DailyZjdxStatLevel5.zhu).
            days: 计算多少个交易日前的差值 (例如 1, 13, 22).
        :量化解释:
            通过公式计算指定字段的资金/增减仓。
            支持'zhu'/'zhur'/'duo'/'duor'/'gan'/'ganr'六种类型。
            公式: 最新值 - N天前值
        """
        return ("_calculate_diff", f"{field.key}_{days}d", {"field": field, "days": days})

    @staticmethod
    def _calculate_sum(field: Field, days: int, start_date: str, end_date: str, secucode_query):
        """
        为 calculate_sum 生成 CTE。
        在指定时间窗口内使用窗口函数计算移动总和。
        """
        BaseTable = field.class_

        # 使用统一的日期范围处理函数
        trade_start_original, trade_start_extended, trade_end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            BaseTable, start_date, end_date, lookback_days=days
        )

        key_label = f"{field.key}_{days}d"

        sum_subquery = (
            select(
                BaseTable.secucode,
                BaseTable.date,
                func.sum(field)
                .over(partition_by=BaseTable.secucode, order_by=BaseTable.date, rows=((days - 1) * -1, 0))
                .label(key_label),
            )
            .where(BaseTable.date.between(trade_start_extended, trade_end), BaseTable.secucode.in_(secucode_query))
            .subquery()
        )

        final_cte = (
            select(sum_subquery.c.secucode, sum_subquery.c.date, sum_subquery.c[key_label])
            .where(sum_subquery.c.date.between(trade_start_original, trade_end))
            .cte(f"cte_sum_{field.key}_{days}d_{uuid.uuid4().hex[:6]}")
        )

        return final_cte

    @staticmethod
    def _calculate_industry_average(field, start_date: str, end_date: str, secucode_query):
        """
        计算指定字段在每个行业内的平均值（基于hq_basic_view.trade字段），并返回包含行业平均值的CTE。
        行业均值 = 同一行业内所有股票某个字段的平均值
        """
        # 生成唯一的标识符，用于CTE命名
        unique_id = uuid.uuid4().hex[:6]

        if isinstance(field, tuple):
            # 处理元组格式
            func_name, field_name = field[0], field[1]
            params = field[2] if len(field) > 2 else {}

            base_stock_query = select(AStockBasic.secucode).where(
                AStockBasic.is_ts == 0, AStockBasic.trade.isnot(None), AStockBasic.type == "A股"
            )

            internal_func = getattr(AStockGlobalQuery, func_name)
            source = internal_func(start_date=start_date, end_date=end_date, secucode_query=base_stock_query, **params)

            # 定义操作对象
            target_col = source.c[field_name]  # 要计算均值的列
            date_col = source.c.date  # 日期列
            secucode_col = source.c.secucode  # 股票代码列
            key_label_base = field_name

            extra_filters = []

        else:
            #  处理字段对象
            actual_field = field
            BaseTable = actual_field.class_
            key_label_base = field.key

            # 获取日期范围
            trade_start, _, trade_end, _ = AStockGlobalQuery._get_aftermarket_date_range(
                BaseTable, start_date, end_date, lookback_days=0
            )

            # 定义操作对象
            source = BaseTable
            target_col = actual_field
            date_col = BaseTable.date
            secucode_col = BaseTable.secucode

            # 对象分支需要显式过滤日期
            extra_filters = [BaseTable.date.between(trade_start, trade_end)]

        key_label = f"{key_label_base}_industry_avg"

        stock_data_cte = (
            select(
                AStockBasic.trade,
                secucode_col.label("secucode"),
                date_col.label("date"),
                target_col.label("target_value"),
                func.avg(target_col).over(partition_by=AStockBasic.trade).label(key_label),
            )
            .select_from(source)
            .join(AStockBasic, secucode_col == AStockBasic.secucode)
            .where(
                target_col.isnot(None),
                AStockBasic.is_ts == 0,
                AStockBasic.trade.isnot(None),
                *extra_filters,
            )
        ).cte(f"stock_data_{unique_id}")

        # 返回包含行业平均值的查询
        final_query = select(
            stock_data_cte.c.secucode,
            stock_data_cte.c.date,
            func.round(stock_data_cte.c[key_label], 2).label(key_label),
        )

        return final_query.cte(f"final_cte_industry_avg_{key_label_base}_{unique_id}")

    @staticmethod
    def calculate_sum(
        field: Field,
        days: int,
    ):
        """
        :功能:
            查询动态计算单个数据 N 日指标(数据库中对应字段数据为单日数据，需要加法计算)。
            此方法返回一个元组，由 query_columns 方法解析以生成并加入一个计算移动总和的 CTE。
        :参数:
            field: 要计算的字段对象 (例如 DailyRzrqStatLevel15.rzrq_net_amt).
            days: 计算多少个交易日前的和值 (例如 3, 5).
        :量化解释:
            通过公式计算指定字段的多日数据总和。
            支持'rzrq_net_amt'/'rz_net_amt'/'rq_net_amt'/'rzrq_intensity'类型。
            公式: 最新值 + 前1天值 + ... + 前N天值
        """
        return ("_calculate_sum", f"{field.key}_{days}d", {"field": field, "days": days})

    @staticmethod
    def _calculate_inflow_days(field: Field, start_date: str, end_date: str, secucode_query):
        """
        为 calculate_inflow_days 生成 CTE。
        计算连续净流入天数。
        """
        BaseTable = field.class_
        key_label = f"{field.key}_inflow_days"

        lookback_days = 30

        # 使用统一的日期范围处理函数
        trade_start_original, trade_start_extended, trade_end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            BaseTable, start_date, end_date, lookback_days=lookback_days
        )
        unique_id = uuid.uuid4().hex[:6]

        # 标记正/非正
        flagged_cte = (
            select(
                BaseTable.secucode,
                BaseTable.date,
                case((field > 0, 1), else_=0).label("is_positive"),
            )
            .where(
                BaseTable.date.between(trade_start_extended, trade_end),
                BaseTable.secucode.in_(secucode_query),
            )
            .cte(f"inflow_flagged_{unique_id}")
        )

        # 用非正作为断点，累计得到 group_id
        grouped_cte = select(
            flagged_cte.c.secucode,
            flagged_cte.c.date,
            flagged_cte.c.is_positive,
            func.sum(case((flagged_cte.c.is_positive == 0, 1), else_=0))
            .over(partition_by=flagged_cte.c.secucode, order_by=flagged_cte.c.date)
            .label("group_id"),
        ).cte(f"inflow_grouped_{unique_id}")

        # 在同一 group_id 内对 is_positive 做累计求和，得到连续净流入天数
        inflow_days_expr = func.sum(case((grouped_cte.c.is_positive == 1, 1), else_=0)).over(
            partition_by=[grouped_cte.c.secucode, grouped_cte.c.group_id], order_by=grouped_cte.c.date
        )

        ranked_cte = select(
            grouped_cte.c.secucode,
            grouped_cte.c.date,
            case((grouped_cte.c.is_positive == 1, inflow_days_expr), else_=0).label(key_label),
        ).cte(f"inflow_ranked_{unique_id}")

        # 裁剪回用户请求的时间区间并返回 CTE
        final_cte = (
            select(
                ranked_cte.c.secucode,
                ranked_cte.c.date,
                ranked_cte.c[key_label],
            )
            .where(ranked_cte.c.date.between(trade_start_original, trade_end))
            .cte(f"cte_inflow_{field.key}_{uuid.uuid4().hex[:6]}")
        )

        return final_cte

    @staticmethod
    def calculate_inflow_days(
        field: Field,
    ):
        """
        :功能:
            查询动态计算单个数据净流入天数(单个指标连续>0的天数)。
            此方法返回一个元组，由 query_columns 方法解析以生成并加入一个计算连续天数的 CTE。
        :参数:
            field: 要计算的字段对象 (例如 DailyRzrqStatLevel15.rzrq_net_amt).
        :量化解释:
            通过公式计算指定字段的净流入天数。
            支持'rzrq_net_amt'类型。
            公式: 连续多日净流入（数据>0）天数
        """
        return ("_calculate_inflow_days", f"{field.key}_inflow_days", {"field": field})

    RANKLIST_KEY_LABEL_MAP = {
        RanklistDuo1Level0: "ranklist_duo_1_times",
        RanklistDuo3Level0: "ranklist_duo_3_times",
        RanklistGan13Level0: "ranklist_gan_13_times",
    }

    RANKLIST_MAP = {
        RanklistDuo1Level0: RanklistDuo1Level0.duor_1,
        RanklistDuo3Level0: RanklistDuo3Level0.duor_3,
        RanklistGan13Level0: RanklistGan13Level0.ganr_13,
    }

    @staticmethod
    def _calculate_in_ranklist_times(fields: List[SQLModel], start_date: str, end_date: str, secucode_query):
        """
        为 calculate_in_ranklist_times 生成 CTE。
        根据传入的一个或多个榜单模型，计算累计上榜次数。
        """
        if not fields:
            raise ValueError("用于计算上榜次数的 'fields' 列表不能为空")

        # 根据传入榜单的数量，决定最终输出的字段名
        if len(fields) == 1:
            key_label = AStockGlobalQuery.RANKLIST_KEY_LABEL_MAP.get(fields[0])
            if key_label is None:
                raise ValueError(f"请为榜单 {fields[0].__name__} 在 RANKLIST_KEY_LABEL_MAP 中配置输出名称")
        else:
            key_label = "ranklist_times"

        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)

        # 选出每日排名前50的股票
        top_50_queries = []
        for BaseTable in fields:
            sort_column = AStockGlobalQuery.RANKLIST_MAP.get(BaseTable)
            if sort_column is None:
                raise ValueError(f"不支持的榜单类型: {BaseTable.__name__}")

            order_col = getattr(BaseTable, sort_column.key)

            ranked_subquery = (
                select(
                    BaseTable.secucode,
                    BaseTable.date,
                    func.row_number().over(partition_by=BaseTable.date, order_by=order_col.desc()).label("rank"),
                )
                .where(
                    BaseTable.date.between(trade_start, trade_end),
                    BaseTable.secucode.isnot(None),
                    order_col.isnot(None),
                )
                .subquery()
            )

            top_50_query = select(ranked_subquery.c.secucode, ranked_subquery.c.date).where(
                ranked_subquery.c.rank <= 50
            )

            top_50_queries.append(top_50_query)

        # 合并所有榜单的Top50结果，然后按天分组并计数
        unioned_top_50 = union_all(*top_50_queries).subquery("unioned_top_50")

        daily_appearance_counts = (
            select(unioned_top_50.c.secucode, unioned_top_50.c.date, func.count().label("daily_count"))
            .group_by(unioned_top_50.c.secucode, unioned_top_50.c.date)
            .subquery("daily_appearance_counts")
        )

        secucode_subq = secucode_query.subquery() if hasattr(secucode_query, "subquery") else secucode_query
        dates_cte = (
            select(AStockMarket.date.label("date"))
            .where(AStockMarket.date.between(trade_start, trade_end))
            .distinct()
            .cte(f"dates_grid_{uuid.uuid4().hex[:6]}")
        )
        uniq_sec_subq = select(secucode_subq.c.secucode).distinct().subquery("uniq_sec")
        secucode_dates_grid_cte = select(
            uniq_sec_subq.c.secucode.label("secucode"), dates_cte.c.date.label("date")
        ).cte(f"secucode_dates_grid_{uuid.uuid4().hex[:6]}")

        # 将网格与每日上榜次数进行左连接
        joined = (
            select(
                secucode_dates_grid_cte.c.secucode,
                secucode_dates_grid_cte.c.date,
                func.coalesce(daily_appearance_counts.c.daily_count, 0).label("daily_count"),
            )
            .select_from(
                secucode_dates_grid_cte.outerjoin(
                    daily_appearance_counts,
                    and_(
                        secucode_dates_grid_cte.c.secucode == daily_appearance_counts.c.secucode,
                        secucode_dates_grid_cte.c.date == daily_appearance_counts.c.date,
                    ),
                )
            )
            .subquery("sec_date_with_counts")
        )

        # 使用窗口函数对“每日上榜次数”进行累计求和
        final_cte = select(
            joined.c.secucode,
            joined.c.date,
            func.sum(joined.c.daily_count)
            .over(partition_by=joined.c.secucode, order_by=joined.c.date)
            .label(key_label),
        ).cte(f"cte_ranklist_times_multi_{uuid.uuid4().hex[:6]}")

        return final_cte

    @staticmethod
    def calculate_in_ranklist_times(
        fields: Union[SQLModel, List[SQLModel]],
    ):
        """
        :功能:
            查询计算某一天或某一段时间单个股票上某个资金榜的累计次数。
            此方法返回一个元组，由 query_columns 方法解析以生成并加入一个计算上榜次数的 CTE。
        :参数:
            field: 要计算的榜单表模型 (例如 RanklistDuo1Level0, RanklistDuo3Level0, RanklistGan13Level0)。
        :量化解释:
            统计在指定日期范围内，每日按特定指标倒序排名前50的股票的累计上榜次数。
            - RanklistDuo1Level0 按 duor_1 排序
            - RanklistDuo3Level0 按 duor_3 排序
            - RanklistGan13Level0 按 ganr_13 排序
        """
        # 如果传入单个模型，则包装成列表
        if not isinstance(fields, list):
            fields = [fields]

        # 根据传入榜单的数量
        if len(fields) == 1:
            key_label = AStockGlobalQuery.RANKLIST_KEY_LABEL_MAP.get(fields[0])
            if key_label is None:
                raise ValueError(f"请为榜单 {fields[0].__name__} 在 RANKLIST_KEY_LABEL_MAP 中配置输出名称")
        else:
            key_label = "ranklist_times"

        return ("_calculate_in_ranklist_times", key_label, {"fields": fields})

    @staticmethod
    def _calculate_growth_metric(field: Field, start_date: str, end_date: str, secucode_query, mode: str = "yoy"):
        """
        计算增长率（同比 YOY / 环比 QOQ）。
        """
        BaseTable = field.class_
        unique_id = uuid.uuid4().hex[:6]

        trade_start = get_date(start_date, is_start=True)
        trade_end = get_date(end_date)

        suffix = "yoy" if mode == "yoy" else "qoq"
        key_label = f"{field.key}_{suffix}"

        # 当期数据
        if start_date == end_date:
            current_cte = (
                select(
                    BaseTable.secucode,
                    BaseTable.date,
                    field.label("current_value"),
                    func.row_number()
                    .over(partition_by=BaseTable.secucode, order_by=desc(BaseTable.date))
                    .label("row_num"),
                )
                .where(BaseTable.date <= trade_end, BaseTable.secucode.in_(secucode_query))
                .cte(f"cte_current_{suffix}_{unique_id}")
            )

            # 取最新的一条
            current_data = (
                select(current_cte.c.secucode, current_cte.c.date, current_cte.c.current_value)
                .where(current_cte.c.row_num == 1)
                .subquery(f"sub_current_{suffix}_{unique_id}")
            )
        else:
            current_data = (
                select(
                    BaseTable.secucode,
                    BaseTable.date,
                    field.label("current_value"),
                )
                .where(BaseTable.date.between(trade_start, trade_end), BaseTable.secucode.in_(secucode_query))
                .subquery(f"sub_current_{suffix}_{unique_id}")
            )

        # 对比期数据
        PreviousTable = aliased(BaseTable, name=f"prev_{suffix}_{unique_id}")
        prev_value_col = getattr(PreviousTable, field.key)

        # 连接当期和往期
        join_conditions = [current_data.c.secucode == PreviousTable.secucode]

        if mode == "yoy":
            # 同比: 1 年前
            join_conditions.extend(
                [
                    func.toYear(current_data.c.date) == func.toYear(PreviousTable.date) + 1,
                    func.toMonth(current_data.c.date) == func.toMonth(PreviousTable.date),
                    func.toDayOfMonth(current_data.c.date) == func.toDayOfMonth(PreviousTable.date),
                ]
            )
        elif mode == "qoq":
            # 环比:  3 个月前
            join_conditions.append(func.addMonths(PreviousTable.date, 3) == current_data.c.date)

        growth_cte = (
            select(
                current_data.c.secucode,
                current_data.c.date,
                case(
                    (func.coalesce(prev_value_col, 0) == 0, None),
                    (prev_value_col.is_(None), None),
                    else_=((current_data.c.current_value - prev_value_col) / func.abs(prev_value_col)),
                ).label(key_label),
            )
            .select_from(current_data.outerjoin(PreviousTable, and_(*join_conditions)))
            .cte(f"cte_{suffix}_{field.key}_{unique_id}")
        )

        return growth_cte


# ──────────── 模拟大模型输出（保留但未使用） ────────────
model_response = """
select(AStockMarket).where(
    AStockMarket.limit_up == True,
    AStockMarket.macd_cross == True,
    AStockMarket.sci_tech_board == True
)
""".strip()


# ──────────── SQL 生成函数 ────────────
def generate_sql(queryition):
    """
    将 SQLAlchemy 条件编译为纯 SQL。
    参数:
        queryition: SQLAlchemy 条件表达式
    返回:
        str: 编译后的 SQL 语句
    """
    engine = create_engine("sqlite://")
    stmt = select(AStockMarket.secucode).where(queryition)  # 只选股票代码，匹配参考 SQL
    compiled = stmt.compile(engine, compile_kwargs={"literal_binds": True})
    return str(compiled)


def contains_intersect_or_union(node: ast.AST) -> bool:
    """
    判断 AST 中是否有调用 intersect() 或 union() 的节点
    """

    class Finder(ast.NodeVisitor):
        def __init__(self):
            self.found = False

        def visit_Call(self, node):
            if isinstance(node.func, ast.Attribute) and node.func.attr in ["intersect", "union"]:
                self.found = True
            elif isinstance(node.func, ast.Name) and node.func.id in ["intersect", "union"]:
                self.found = True
            self.generic_visit(node)

    finder = Finder()
    finder.visit(node)
    return finder.found


# def rebuild_intersect_ast_preserving_structure(node: ast.expr) -> ast.expr:
#     """
#     把链式 .intersect() 调用展平为 intersect(a, b, c, d) 形式。
#     """
#     # 先拆尾部调用链
#     tail_chain = []
#     cur = node
#     while isinstance(cur, ast.Call) and isinstance(cur.func, ast.Attribute):
#         attr = cur.func.attr
#         if attr == "intersect":
#             break
#         tail_chain.append((attr, cur.args, cur.keywords))
#         cur = cur.func.value

#     # 收集所有链式 .intersect() 的参数，扁平化
#     def collect_chain(n: ast.expr):
#         parts = []
#         while (
#             isinstance(n, ast.Call)
#             and isinstance(n.func, ast.Attribute)
#             and n.func.attr == "intersect"
#         ):
#             parts.append(n.args[0])
#             n = n.func.value
#         parts.append(n)
#         return list(reversed(parts))

#     parts = collect_chain(cur)
#     parts = [
#         visit(part) if hasattr(visit, "__call__") else part for part in parts
#     ]  # 这里 visit 见后面

#     # 构造多参数 intersect(...)
#     intersect_call = ast.Call(
#         func=ast.Name(id="intersect", ctx=ast.Load()), args=parts, keywords=[]
#     )

#     # 把尾部调用接回去
#     for attr, args, keywords in reversed(tail_chain):
#         intersect_call = ast.Call(
#             func=ast.Attribute(value=intersect_call, attr=attr, ctx=ast.Load()),
#             args=args,
#             keywords=keywords,
#         )


#     return intersect_call
def rebuild_intersect_or_union_ast_preserving_structure(node: ast.expr) -> ast.expr:
    """
    把链式 .intersect() 或 .union() 调用展平为 intersect(a, b, c, d) / union(a, b, c, d) 形式。
    如果不是这两种调用，则递归处理其参数，保留外层结构。
    """
    # 先判断当前节点是否是 intersect/union 调用
    if not (
        isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("intersect", "union")
    ):
        if isinstance(node, ast.Call):
            node.args = [rebuild_intersect_or_union_ast_preserving_structure(arg) for arg in node.args]
            node.keywords = [
                ast.keyword(arg=kw.arg, value=rebuild_intersect_or_union_ast_preserving_structure(kw.value))
                for kw in node.keywords
            ]
        return node

    op_type = node.func.attr  # "intersect" 或 "union"

    # ===== 以下是 flatten 链的逻辑 =====
    tail_chain = []
    cur = node
    while isinstance(cur, ast.Call) and isinstance(cur.func, ast.Attribute):
        attr = cur.func.attr
        # 一旦遇到当前类型的链（intersect 或 union），开始收集
        if attr == op_type:
            break
        tail_chain.append((attr, cur.args, cur.keywords))
        cur = cur.func.value

    def collect_chain(n: ast.expr):
        parts = []
        while isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == op_type:
            parts.append(n.args[0])
            n = n.func.value
        parts.append(n)
        return list(reversed(parts))

    parts = collect_chain(cur)
    parts = [rebuild_intersect_or_union_ast_preserving_structure(part) for part in parts]

    # 构建 flatten 后的调用，如 intersect(a,b,c) 或 union(x,y,z)
    flat_call = ast.Call(func=ast.Name(id=op_type, ctx=ast.Load()), args=parts, keywords=[])

    # 把链后面其它调用重新包回去（例如 .filter() 之类）
    for attr, args, keywords in reversed(tail_chain):
        flat_call = ast.Call(
            func=ast.Attribute(value=flat_call, attr=attr, ctx=ast.Load()),
            args=args,
            keywords=keywords,
        )

    return flat_call


def visit(n: ast.expr) -> ast.expr:
    # 避免递归调用问题，定义在外面，改用递归遍历参数
    if isinstance(n, ast.Call):
        if isinstance(n.func, ast.Attribute) and n.func.attr == "intersect":
            # 这里不用递归构造二叉树，全部交给 collect_chain 扁平处理
            return n
        elif isinstance(n.func, ast.Name) and n.func.id == "intersect":
            # 函数式 intersect 多参数递归访问参数
            return ast.Call(
                func=ast.Name(id="intersect", ctx=ast.Load()),
                args=[visit(arg) for arg in n.args],
                keywords=[],
            )
    return n


def rewrite_intersect_or_union_code(code_str: str) -> str:
    """
    对含 intersect() 表达式的字符串进行 AST 结构化重写。
    """
    tree = ast.parse(code_str.strip(), mode="eval")  # Expression
    assert isinstance(tree, ast.Expression)

    if contains_intersect_or_union(tree.body):
        rebuilt = rebuild_intersect_or_union_ast_preserving_structure(tree.body)
        assert isinstance(rebuilt, ast.expr)
        tree.body = rebuilt

    return ast.unparse(ast.fix_missing_locations(tree))


def generate_sql_from_orm(orm_code: str) -> str:
    """
    把 ORM DSL 表达式（字符串）转换为 SQL。
    自动结构化 intersect 写法（链式或函数式）并生成 SQL。
    """
    # 延迟导入 StockNewApi 避免循环依赖
    import sys

    if "dao.interfaces_new_api" not in sys.modules:
        import importlib

        interfaces_new_api_module = importlib.import_module("dao.interfaces_new_api")
        StockNewApi = interfaces_new_api_module.StockNewApi
    else:
        StockNewApi = sys.modules["dao.interfaces_new_api"].StockNewApi

    orm_code = f"({orm_code})"
    engine = create_engine("sqlite://")
    safe_locals = {
        "AStockMarket": AStockMarket,
        "AStockFinancial": AStockFinancial,
        "AStockCashFlow": AStockCashFlow,
        "AStockStructure": AStockStructure,
        "AStockShareHolder": AStockShareHolder,
        "AStockDividenedDetail": AStockDividenedDetail,
        "AStockDividenedAddition": AStockDividenedAddition,
        "AStockDividenedAllotment": AStockDividenedAllotment,
        "AStockPositionSum": AStockPositionSum,
        "AStockProfitView": AStockProfitView,
        "AStockBasic": AStockBasic,
        "AStockMarketCur": AStockMarketCur,
        "Fund3cjLevel0": Fund3cjLevel0,
        "RanklistDuo1Level0": RanklistDuo1Level0,
        "RanklistDuo3Level0": RanklistDuo3Level0,
        "RanklistGan13Level0": RanklistGan13Level0,
        "DailyThreeMethodLevel0": DailyThreeMethodLevel0,
        "DailyCwDataLevel0": DailyCwDataLevel0,
        "DailyGzkjLevel0": DailyGzkjLevel0,
        "FundMoveFlagLevel0": FundMoveFlagLevel0,
        "StockFinancialPropertyBalanceView": StockFinancialPropertyBalanceView,
        "WzHoldingPoolsLevel15": WzHoldingPoolsLevel15,
        "AiForecastZdLevel20": AiForecastZdLevel20,
        "AiStockStrategyLevel20": AiStockStrategyLevel20,
        "GgjyHoldStatLevel15": GgjyHoldStatLevel15,
        "select": select,
        "intersect": AStockGlobalQuery._intersect,
        "union": AStockGlobalQuery._union,
        "date": datetime.date,
        "AStockGlobalQuery": AStockGlobalQuery,
        "DailyZjdxStatLevel5": DailyZjdxStatLevel5,
        "DailyRzrqStatLevel15": DailyRzrqStatLevel15,
        "DailyDzjmStockStatLevel15": DailyDzjmStockStatLevel15,
        "DailyDzjmStockDetailLevel15": DailyDzjmStockDetailLevel15,
        "ReportHotstocksLevel15": ReportHotstocksLevel15,
        "DailyGqzjLevel20": DailyGqzjLevel20,
        "XwlhSeatdetailLevel15": XwlhSeatdetailLevel15,
        "XwlhSeatStatLevel15": XwlhSeatStatLevel15,
        "get_days_ago_date": get_days_ago_date,
        "StockNewApi": StockNewApi,
        # 动态添加 StockNewApi 的所有静态方法
        **{
            name: getattr(StockNewApi, name)
            for name in dir(StockNewApi)
            if not name.startswith("_") and callable(getattr(StockNewApi, name))
        },
    }
    fixed_code = rewrite_intersect_or_union_code(orm_code)
    stmt = eval(fixed_code, {}, safe_locals)
    compiled = stmt.compile(engine, compile_kwargs={"literal_binds": True})
    return str(compiled)


# ──────────── 主逻辑 ────────────
async def main():
    sql = generate_sql_from_orm(
        """(AStockBasic.query_concept_sector(concept='存储芯片')
    .intersect(
        AStockGlobalQuery.query_dragon_leader_stock()
    )
    .intersect(
        AStockGlobalQuery.query_growth_stock()
    )
    .limit(5))"""
    )


# ──────────── 动态注入 StockNewApi 方法到 AStockGlobalQuery ────────────
from .interfaces_new_api import StockNewApi
import inspect

# 获取 StockNewApi 的所有 staticmethod 并动态添加到 AStockGlobalQuery
for name, method in inspect.getmembers(StockNewApi, predicate=inspect.isfunction):
    if name.startswith("query_") or ("get_"):
        # 将方法设置为 staticmethod 并添加到 AStockGlobalQuery
        setattr(AStockGlobalQuery, name, staticmethod(method))


if __name__ == "__main__":
    # 想实现计算的指标时，全局搜索get_d()查看示例
    # AStockGlobalQuery.query_columns([AStockMarketCur.get_secucode(),AStockBasic.get_secuname(),AStockMarket.get_date(),ValuationSpace.get_gzhigh()],'0d','0d',AStockGlobalQuery.query_price_in_range(low=ValuationSpace.get_gzhigh()))
    sql = generate_sql_from_orm(
        """AStockGlobalQuery.query_columns([AStockGlobalQuery.get_secucode(mode=1),AStockGlobalQuery.get_secuname(),AStockGlobalQuery.get_date(mode=1)],'0d','0d',AStockGlobalQuery.query_longtou('0d','0d',0), 'stock')"""
    )
    print(sql)
