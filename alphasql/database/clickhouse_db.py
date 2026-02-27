"""
ClickHouse数据库连接和执行模块
"""
import os
import clickhouse_connect
from typing import Dict, List, Tuple, Optional, Any
from loguru import logger
import threading

# ClickHouse配置
CLICKHOUSE_HOST = os.getenv("STOCK_CLICKHOUSE_HOST", "192.168.81.84")
CLICKHOUSE_PORT = int(os.getenv("STOCK_CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DBNAME = os.getenv("STOCK_CLICKHOUSE_DBNAME", "compass_ai")
CLICKHOUSE_USERNAME = os.getenv("STOCK_CLICKHOUSE_USERNAME", "default")
CLICKHOUSE_PASSWORD = os.getenv("STOCK_CLICKHOUSE_PASSWORD", "123456")

# ClickHouse客户端类型
ClickHouseClientType = clickhouse_connect.driver.Client

# ClickHouse默认设置
DEFAULT_CLICKHOUSE_SETTINGS = {
    "union_default_mode": "DISTINCT"
}


class ClickHouseClient:
    """ClickHouse客户端单例"""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._client = None
        return cls._instance

    def get_client(self):
        """获取ClickHouse客户端连接"""
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                database=CLICKHOUSE_DBNAME,
                username=CLICKHOUSE_USERNAME,
                password=CLICKHOUSE_PASSWORD,
                settings=DEFAULT_CLICKHOUSE_SETTINGS
            )
            logger.info(f"Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DBNAME} with settings: {DEFAULT_CLICKHOUSE_SETTINGS}")
        return self._client

    def close(self):
        """关闭连接"""
        if self._client is not None:
            self._client.close()
            self._client = None


# 全局客户端实例
_ch_client = ClickHouseClient()


def get_clickhouse_client():
    """获取ClickHouse客户端"""
    return _ch_client.get_client()


def get_table_names() -> List[str]:
    """获取所有表名（排除内部表）"""
    client = get_clickhouse_client()
    tables = client.query('SHOW TABLES').result_rows
    return [t[0] for t in tables if not t[0].startswith('.inner')]


def get_table_columns(table_name: str) -> List[Tuple[str, str]]:
    """获取表的列名和类型"""
    client = get_clickhouse_client()
    result = client.query(f'DESCRIBE TABLE `{table_name}`')
    return [(row[0], row[1]) for row in result.result_rows]


def get_table_descriptions() -> Dict[str, Any]:
    """获取所有表和它们的列信息（用于构建schema）"""
    table_names = get_table_names()
    schema_dict = {"tables": {}}

    for table_name in table_names:
        columns = get_table_columns(table_name)
        schema_dict["tables"][table_name] = {
            "table_name": table_name,
            "columns": {}
        }
        for col_name, col_type in columns:
            schema_dict["tables"][table_name]["columns"][col_name] = {
                "original_column_name": col_name,
                "column_type": col_type,
                "primary_key": False,
                "foreign_keys": [],
                "referenced_by": [],
                "expanded_column_name": "",
                "column_description": "",
                "value_description": "",
                "value_examples": []
            }

    return schema_dict


def execute_query(query: str, timeout: int = 60) -> Tuple[bool, List[str], List[List], str]:
    """
    执行ClickHouse查询

    Args:
        query: SQL查询语句
        timeout: 超时时间（秒）

    Returns:
        (success, columns, rows, error_message)
    """
    client = get_clickhouse_client()
    try:
        result = client.query(query)
        columns = [col for col in result.column_names]
        rows = [list(row) for row in result.result_rows]
        return True, columns, rows, ""
    except Exception as e:
        return False, [], [], str(e)


def execute_sql_with_timeout(db_path: str, query: str, timeout: int = 60):
    """
    模拟SQLite的execute_sql_with_timeout接口，用于兼容现有代码

    Args:
        db_path: 数据库路径（ClickHouse忽略此参数）
        query: SQL查询语句
        timeout: 超时时间

    Returns:
        SQLExecutionResult对象
    """
    from alphasql.database.sql_execution import SQLExecutionResult, SQLExecutionResultType

    # 替换 get_date() 函数为具体日期
    processed_query = replace_get_date_in_sql(query)

    success, columns, rows, error = execute_query(processed_query, timeout)

    if success:
        return SQLExecutionResult(
            db_path=db_path,
            sql=query,  # 保留原始SQL用于参考
            result_type=SQLExecutionResultType.SUCCESS,
            result_cols=columns,
            result=rows,
            error_message=None
        )
    elif "timeout" in error.lower() or "timeout" in error.lower():
        return SQLExecutionResult(
            db_path=db_path,
            sql=query,
            result_type=SQLExecutionResultType.TIMEOUT,
            result_cols=None,
            result=None,
            error_message=error
        )
    else:
        return SQLExecutionResult(
            db_path=db_path,
            sql=query,
            result_type=SQLExecutionResultType.ERROR,
            result_cols=None,
            result=None,
            error_message=error
        )


def load_value_examples(table_name: str, column_name: str, max_num_examples: int = 3) -> List[str]:
    """获取列的值示例"""
    client = get_clickhouse_client()
    try:
        query = f"SELECT DISTINCT `{column_name}` FROM `{table_name}` WHERE `{column_name}` IS NOT NULL AND `{column_name}` != '' LIMIT {max_num_examples}"
        result = client.query(query)
        return [str(row[0]) for row in result.result_rows]
    except Exception as e:
        logger.warning(f"Failed to load value examples for {table_name}.{column_name}: {e}")
        return []


# 默认的核心表列表（用于 interfaces_new_api.py）
DEFAULT_CORE_TABLES = [
    "hq_basic_view",      # 股票基本信息（概念、代码、名称等）
    "hq_current_view",    # 当前行情
    "hq_history_agg_view", # 历史行情
    "hot_info_view",      # 热门股票信息
]


def get_schema_dict_for_table(table_name: str) -> Dict[str, Any]:
    """
    获取单个表的schema字典
    返回与load_database_schema_dict兼容的格式

    Args:
        table_name: 表名。如果为'all'，则返回所有表的schema
    """
    client = get_clickhouse_client()

    # 如果是获取所有表
    if table_name == 'all':
        all_table_names = get_table_names()
        schema_dict = {
            "db_id": "clickhouse_all",
            "db_directory": "clickhouse",
            "tables": {}
        }
        for tn in all_table_names:
            columns = get_table_columns(tn)
            table_schema = {
                "table_name": tn,
                "columns": {}
            }
            for col_name, col_type in columns:
                value_examples = load_value_examples(tn, col_name)
                table_schema["columns"][col_name] = {
                    "original_column_name": col_name,
                    "column_type": col_type,
                    "primary_key": False,
                    "foreign_keys": [],
                    "referenced_by": [],
                    "expanded_column_name": "",
                    "column_description": "",
                    "value_description": "",
                    "value_examples": value_examples
                }
            schema_dict["tables"][tn] = table_schema
        return schema_dict

    # 获取单个表的schema
    # 先检查表是否存在，如果不存在则返回默认的核心表
    all_tables = get_table_names()
    if table_name not in all_tables:
        logger.warning(f"Table {table_name} not found in ClickHouse, returning default core tables: {DEFAULT_CORE_TABLES}")
        schema_dict = {
            "db_id": table_name,
            "db_directory": "clickhouse",
            "tables": {}
        }
        # 只加载默认的核心表，而不是所有表
        for tn in DEFAULT_CORE_TABLES:
            if tn not in all_tables:
                logger.warning(f"Core table {tn} not found in ClickHouse, skipping")
                continue
            columns = get_table_columns(tn)
            table_schema = {
                "table_name": tn,
                "columns": {}
            }
            for col_name, col_type in columns:
                value_examples = load_value_examples(tn, col_name)
                table_schema["columns"][col_name] = {
                    "original_column_name": col_name,
                    "column_type": col_type,
                    "primary_key": False,
                    "foreign_keys": [],
                    "referenced_by": [],
                    "expanded_column_name": "",
                    "column_description": "",
                    "value_description": "",
                    "value_examples": value_examples
                }
            schema_dict["tables"][tn] = table_schema
        return schema_dict

    columns = get_table_columns(table_name)

    table_schema = {
        "table_name": table_name,
        "columns": {}
    }

    for col_name, col_type in columns:
        value_examples = load_value_examples(table_name, col_name)

        table_schema["columns"][col_name] = {
            "original_column_name": col_name,
            "column_type": col_type,
            "primary_key": False,
            "foreign_keys": [],
            "referenced_by": [],
            "expanded_column_name": "",
            "column_description": "",
            "value_description": "",
            "value_examples": value_examples
        }

    # 返回与load_database_schema_dict兼容的格式
    return {
        "db_id": table_name,
        "db_directory": "clickhouse",
        "tables": {
            table_name: table_schema
        }
    }


import re
import datetime
from dateutil.relativedelta import relativedelta

# 交易日历基准代码（与 interfaces.py 一致）
BENCHMARK_CODE = "SHHQ000001"


# ==================== 日期处理函数（直接输出 SQL，与 interfaces.py 等价） ====================

def get_days_ago_date(days_ago):
    """
    获取第N个交易日前的日期（如days_ago=3表示3个交易日前）
    等效于SQL: LIMIT (days_ago-1), 1
    返回: SQL 子查询字符串
    """
    return f"(SELECT date FROM `hq_history_agg_view` WHERE secucode = '{BENCHMARK_CODE}' ORDER BY date DESC LIMIT 1 OFFSET {days_ago - 1})"


def get_report_ago_date(days_ago):
    """
    获取第N个报告期前的日期（如days_ago=3表示3个报告期前）
    返回: SQL 子查询字符串
    """
    return f"(SELECT date FROM `stock_financial_indicator` GROUP BY date ORDER BY date DESC LIMIT 1 OFFSET {days_ago - 1})"


def apply_lookback_sql(date_sql: str, is_start: bool, lookback_days: int) -> str:
    """
    应用回溯天数的 SQL
    """
    if is_start and lookback_days > 0:
        return f"(SELECT date FROM `hq_history_agg_view` WHERE secucode = '{BENCHMARK_CODE}' AND date < ({date_sql}) ORDER BY date DESC LIMIT 1 OFFSET {lookback_days})"
    return date_sql


def parse_composite_date(date_str: str, is_start: bool, lookback_days: int) -> str:
    """
    解析复合日期格式（2025y6m15d 或 6m15d）
    返回: SQL 子查询字符串或具体日期
    """
    today = datetime.datetime.today().date()

    # 解析年份（如果有）
    year_match = re.match(r"^(\d{4})y", date_str)
    if year_match:
        year = int(year_match.group(1))
        remaining_str = date_str[5:]  # 去掉年份部分
    else:
        year = today.year
        remaining_str = date_str

    # 解析月份和日
    month_day_match = re.match(r"^(\d{1,2})m(\d{1,2})d$", remaining_str)
    if not month_day_match:
        raise ValueError("复合日期格式错误，应为2025y6m15d或6m15d格式")

    month = int(month_day_match.group(1))
    day = int(month_day_match.group(2))

    # 创建日期字符串
    result_date_str = f"'{year:04d}-{month:02d}-{day:02d}'"

    # 应用回溯天数
    return apply_lookback_sql(result_date_str, is_start, lookback_days)


def get_report_date(date_str: str, is_start: bool = False) -> str:
    """
    根据日期字符串获取财报报告期日期。
    支持格式:
    - 绝对年份季度: '2025y1q' (2025年第一季度)
    - 相对年份季度: '-1y4q' (去年第四季度)
    - 相对季度: '0q' (最新季度), '-1q' (上一个季度), '-2q' (上上个季度)
    返回: SQL 子查询字符串或具体日期
    """
    date_str = date_str.strip()
    today = datetime.datetime.today().date()
    QUARTER_ENDS = [(3, 31), (6, 30), (9, 30), (12, 31)]

    def get_quarter_end_date_sql(year: int, quarter: int) -> str:
        """给定年份和季度，返回该季度末日期的 SQL"""
        if not 1 <= quarter <= 4:
            raise ValueError("季度必须在1到4之间")
        month, day = QUARTER_ENDS[quarter - 1]
        return f"'{year:04d}-{month:02d}-{day:02d}'"

    # 模式1：匹配绝对年份+季度, e.g., '2024y1q'
    match_abs_year = re.match(r"^(\d{4})y([1-4])q$", date_str)
    if match_abs_year:
        year = int(match_abs_year.group(1))
        quarter = int(match_abs_year.group(2))
        result_date_sql = get_quarter_end_date_sql(year, quarter)
        result_date = datetime.date(year, QUARTER_ENDS[quarter - 1][0], QUARTER_ENDS[quarter - 1][1])
        if result_date > today:
            return get_report_ago_date(1)
        return result_date_sql

    # 模式2：匹配相对年份+季度, e.g., '-1y2q'
    match_rel_year = re.match(r"^([+-]\d+)y([1-4])q$", date_str)
    if match_rel_year:
        year_offset = int(match_rel_year.group(1))
        quarter = int(match_rel_year.group(2))
        target_year = today.year + year_offset
        result_date_sql = get_quarter_end_date_sql(target_year, quarter)
        result_date = datetime.date(target_year, QUARTER_ENDS[quarter - 1][0], QUARTER_ENDS[quarter - 1][1])
        if result_date > today:
            return get_report_ago_date(1)
        return result_date_sql

    # 模式3：匹配仅相对季度, e.g., '-1q', '0q'
    match_quarter_only = re.match(r"^([+-]?\d+)q$", date_str)
    if match_quarter_only:
        offset_str = match_quarter_only.group(1)
        offset = int(offset_str)

        if not offset_str.startswith(("+", "-")) and 1 <= offset <= 4:
            # 直接拼接成当前年份形式，如 "2025y1q"
            year = today.year
            return get_report_date(f"{year}y{offset}q", is_start=is_start)

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

            return get_quarter_end_date_sql(new_year, new_quarter)

    raise ValueError(f"无效的报告期日期格式: {date_str}")


def get_date(date_str: str, is_start: bool = False, lookback_days: int = 0):
    """
    根据日期字符串获取日期对象，支持绝对日期格式和相对日期格式
    与 interfaces.py 中的 get_date 逻辑完全一致，但直接输出 SQL 字符串
    """
    if "q" in date_str:
        return get_report_date(date_str, is_start)
    if "m" in date_str and "d" in date_str:
        date_str = date_str.replace("-", "")
    if re.fullmatch(r"\d{8}", date_str):  # 匹配 yyyymmdd 格式
        year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
        date_str = f"{year}y{int(month)}m{int(day)}d"
    if not isinstance(date_str, str) or len(date_str) < 2:
        raise ValueError("日期参数格式错误，应为nX格式（如1d、-2m等）")

    if date_str == "近期" and is_start:
        date_str = "-10d"
    if date_str == "近期" and not is_start:
        date_str = "0d"

    # 处理绝对日期格式（yyyy-mm-dd）
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        result_date_str = f"'{date_str}'"
        return apply_lookback_sql(result_date_str, is_start, lookback_days)

    # 处理复合日期格式（2025y6m15d 或 6m15d）
    if re.match(r"^(\d{4}y)?\d{1,2}m\d{1,2}d$", date_str):
        return parse_composite_date(date_str, is_start, lookback_days)

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
            result_date_str = f"'{result_date}'"
        elif unit == "m":  # 0m = 本月
            if is_start:
                result_date = today.replace(day=1)  # 本月第一天
            else:
                next_month = today.replace(day=28) + datetime.timedelta(days=4)
                result_date = next_month.replace(day=1) - datetime.timedelta(days=1)  # 本月最后一天
            result_date_str = f"'{result_date}'"
        elif unit == "y":  # 0y = 今年
            if is_start:
                result_date = today.replace(month=1, day=1)  # 年初
            else:
                result_date = today.replace(month=12, day=31)  # 年末
            result_date_str = f"'{result_date}'"
        else:
            raise ValueError(f"无效的时间单位: {unit}")

        return apply_lookback_sql(result_date_str, is_start, lookback_days)

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
        elif unit == "y":  # 年
            result_date = today + relativedelta(years=value)
        else:
            raise ValueError(f"无效的时间单位: {unit}")

        result_date_str = f"'{result_date}'"
        return apply_lookback_sql(result_date_str, is_start, lookback_days)

    # 单独处理d单位
    if value > 0:
        # 未来日期直接计算自然日
        result_date = today + datetime.timedelta(days=value)
        result_date_str = f"'{result_date}'"
        return apply_lookback_sql(result_date_str, is_start, lookback_days)
    else:
        # 过去日期使用交易日查询
        back_days = abs(value) + 1
        back_days += lookback_days if is_start and lookback_days > 0 else 0
        return get_days_ago_date(back_days)


def replace_get_date_in_sql(sql: str) -> str:
    """
    替换 SQL 中的 get_date() / GET_DATE() 函数调用
    与 interfaces.py 中的 get_date 函数等价，但直接输出 SQL 字符串
    """
    # 匹配 get_date(...) 函数调用，支持大小写
    # 支持 get_date('...'), get_date('...', is_start=True), get_date('...', is_start=True, lookback_days=5)
    # 也支持 GET_DATE('...'), GET_DATE('...', is_start = TRUE) 等变体
    pattern = r"GET_DATE\s*\(\s*['\"]([^'\"]+)['\"]\s*(?:,\s*is_start\s*=\s*(True|False|TRUE|FALSE))?\s*(?:,\s*lookback_days\s*=\s*(\d+))?\s*\)"

    def replace_func(match):
        date_str = match.group(1)
        is_start_raw = match.group(2)
        is_start = is_start_raw in ("True", "TRUE") if is_start_raw else False
        lookback_days = int(match.group(3)) if match.group(3) else 0

        # 调用 get_date 获取 SQL 表达式
        result = get_date(date_str, is_start, lookback_days)
        return result

    # 替换 GET_DATE
    result = re.sub(pattern, replace_func, sql, flags=re.IGNORECASE)

    # 替换 TODATE(...) 为 toDate(...) - ClickHouse 内置函数
    # TODATE(column) -> toDate(column)
    result = re.sub(r'TODATE\s*\(', 'toDate(', result, flags=re.IGNORECASE)

    return result


if __name__ == "__main__":
    # 测试连接
    client = get_clickhouse_client()
    print("Tables:", get_table_names()[:10])
    print("\nTable schema example:")
    print(get_schema_dict_for_table("basic_stockinfo"))
