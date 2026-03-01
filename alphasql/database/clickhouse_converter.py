"""
ClickHouse SQL 关键字和函数名转换器
用于将标准/其他数据库的SQL语法转换为ClickHouse兼容的SQL
"""
import re
from typing import Dict, Tuple, Callable

# 定义转换规则: (pattern, replacement) 或 (pattern, replacement, flags)
# 格式: (正则模式, 替换后字符串, 可选的正则标志)
FunctionConversion = Tuple[str, str, int]
SimpleConversion = Tuple[str, str]


def get_clickhouse_conversions() -> list:
    """
    获取ClickHouse函数名转换规则列表
    返回: [(pattern, replacement), ...] 格式的列表
    """
    conversions = [
        # ========== 日期时间函数 ==========
        # GET_DATE -> 实际日期值
        (r"GET_DATE\s*\(\s*['\"]([^'\"]+)['\"]\s*(?:,\s*is_start\s*=\s*(True|False|TRUE|FALSE))?\s*(?:,\s*lookback_days\s*=\s*(\d+))?\s*\)", r"GET_DATE_REPLACED"),

        # TODATE -> toDate
        (r'\bTODATE\s*\(', 'toDate('),

        # TOYEAR -> toYear
        (r'\bTOYEAR\s*\(', 'toYear('),

        # TOMONTH -> toMonth
        (r'\bTOMONTH\s*\(', 'toMonth('),

        # TODAYOFMONTH -> toDayOfMonth
        (r'\bTODAYOFMONTH\s*\(', 'toDayOfMonth('),

        # TODATETIME -> toDateTime
        (r'\bTODATETIME\s*\(', 'toDateTime('),

        # TOYYYYMMDD -> toYYYYMMDD
        (r'\bTOYYYYMMDD\s*\(', 'toYYYYMMDD('),

        # TOSTARTYEAR -> toStartOfYear
        (r'\bTOSTARTYEAR\s*\(', 'toStartOfYear('),

        # TOSTARTMONTH -> toStartOfMonth
        (r'\bTOSTARTMONTH\s*\(', 'toStartOfMonth('),

        # TOSTARTOFDAY -> toStartOfDay
        (r'\bTOSTARTOFDAY\s*\(', 'toStartOfDay('),

        # ========== 窗口函数 ==========
        # ROWNUMBER -> row_number
        (r'\bROWNUMBER\s*\(', 'row_number('),

        # ROWNUMBERINPARTITION -> row_number
        (r'\bROWNUMBERINPARTITION\s*\(', 'row_number('),

        # ROWNUMBERINFRAME -> row_number
        (r'\bROWNUMBERINFRAME\s*\(', 'row_number('),

        # ROWNUMBERINALL -> row_number
        (r'\bROWNUMBERINALL\s*\(', 'row_number('),

        # ========== 条件函数 ==========
        # IIF -> if
        (r'\bIIF\s*\(', 'if('),

        # IFNULL -> coalesce
        (r'\bIFNULL\s*\(', 'coalesce('),

        # NVL -> coalesce
        (r'\bNVL\s*\(', 'coalesce('),

        # ========== 字符串函数 ==========
        # INSTR -> position
        # 注意: INSTR(str, substr) -> position(substr IN str)
        (r'INSTR\s*\(\s*([^,]+),\s*([^)]+)\)\s*', r'position(\2 IN \1)'),

        # SUBSTRING -> substring
        (r'\bSUBSTRING\s*\(', 'substring('),

        # CONCAT -> concat
        (r'\bCONCAT\s*\(', 'concat('),

        # ========== 聚合函数 ==========
        # LAG -> lagInFrame
        (r'\bLAG\s*\(', 'lagInFrame('),

        # LEAD -> leadInFrame
        (r'\bLEAD\s*\(', 'leadInFrame('),

        # LAGINFRAME -> lagInFrame
        (r'\bLAGINFRAME\s*\(', 'lagInFrame('),

        # LEADINFRAME -> leadInFrame
        (r'\bLEADINFRAME\s*\(', 'leadInFrame('),

        # GROUP_ARRAY -> groupArray
        (r'\bGROUP_ARRAY\s*\(', 'groupArray('),

        # ========== 类型转换 ==========
        # PARSEDATETIMEBESTEFFORTORNULL -> toDate
        (r'PARSEDATETIMEBESTEFFORTORNULL', 'toDate'),

        # CAST -> CAST (ClickHouse语法略有不同，但基本兼容)
        # ========== 其他 ==========
        # LIMITATION: ClickHouse不支持和SQL Server类似的TOP语法
    ]
    return conversions


def apply_clickhouse_conversions(sql: str, get_date_replacement_func: Callable = None) -> str:
    """
    应用ClickHouse关键字转换

    Args:
        sql: 输入的SQL语句
        get_date_replacement_func: 可选的GET_DATE替换函数

    Returns:
        转换后的SQL语句
    """
    result = sql

    # 1. 首先处理GET_DATE (需要特殊处理)
    if get_date_replacement_func:
        # 匹配 get_date(...) 函数调用
        pattern = r"GET_DATE\s*\(\s*['\"]([^'\"]+)['\"]\s*(?:,\s*is_start\s*=\s*(True|False|TRUE|FALSE))?\s*(?:,\s*lookback_days\s*=\s*(\d+))?\s*\)"

        def replace_get_date(match):
            date_str = match.group(1)
            is_start_raw = match.group(2)
            is_start = is_start_raw in ("True", "TRUE") if is_start_raw else False
            lookback_days = int(match.group(3)) if match.group(3) else 0
            return get_date_replacement_func(date_str, is_start, lookback_days)

        result = re.sub(pattern, replace_get_date, result, flags=re.IGNORECASE)

    # 2. 应用其他转换规则
    conversions = get_clickhouse_conversions()

    for conversion in conversions:
        pattern = conversion[0]
        replacement = conversion[1]
        flags = conversion[2] if len(conversion) > 2 else re.IGNORECASE

        # 跳过GET_DATE处理，因为已经在上面处理了
        if 'GET_DATE' in pattern:
            continue

        result = re.sub(pattern, replacement, result, flags=flags)

    # 3. 将双引号替换为反引号 (ClickHouse使用反引号)
    result = re.sub(r'(?<!`)"([^"]+)"', r'`\1`', result)

    return result


def get_sqlserver_to_clickhouse_keywords() -> Dict[str, str]:
    """
    获取SQL Server到ClickHouse的关键字映射
    (用于参考)
    """
    return {
        # 数据类型
        "DATETIME": "DateTime",
        "DATE": "Date",
        "INT": "Int32",
        "BIGINT": "Int64",
        "SMALLINT": "Int16",
        "TINYINT": "UInt8",
        "VARCHAR": "String",
        "NVARCHAR": "String",
        "TEXT": "String",
        "DECIMAL": "Decimal",
        "NUMERIC": "Decimal",
        "FLOAT": "Float64",
        "REAL": "Float32",
        "BIT": "UInt8",
        "IMAGE": "String",

        # 关键字
        "TOP": "LIMIT",  # 注意: 语法不同
        "GETDATE()": "now()",
        "GETUTCDATE()": "nowUTC()",
        "NEWID()": "generateUUIDv4()",
        "SCOPE_IDENTITY()": "last_insert_id()",  # 不完全等价

        # 函数
        "ISNULL": "coalesce",
        "IIF": "if",
        "LAG": "lagInFrame",
        "LEAD": "leadInFrame",
        "ROW_NUMBER": "row_number",
        "RANK": "rank",
        "DENSE_RANK": "dense_rank",
        "NTILE": "ntile",
        "SUBSTRING": "substring",
        "CHARINDEX": "position",
        "PATINDEX": "position",
        "LEN": "length",
        "DATALENGTH": "length",
    }


def get_mysql_to_clickhouse_keywords() -> Dict[str, str]:
    """
    获取MySQL到ClickHouse的关键字映射
    (用于参考)
    """
    return {
        # 函数
        "IFNULL": "coalesce",
        "NVL": "coalesce",
        "CONV": "convert",
        "DATE_ADD": "dateAdd",
        "DATE_SUB": "dateSub",
        "DATEDIFF": "dateDiff",
        "GROUP_CONCAT": "groupArray",
        "FIND_IN_SET": "has",
        "INSTR": "position",
        "ELT": "if",
        "FIELD": "indexOf",

        # 数据类型
        "TINYINT": "Int8",
        "SMALLINT": "Int16",
        "MEDIUMINT": "Int32",
        "INT": "Int32",
        "BIGINT": "Int64",
    }


if __name__ == "__main__":
    # 测试转换器
    test_sqls = [
        "SELECT ROWNUMBER() OVER (ORDER BY id) FROM t",
        "SELECT TODATE('2024-01-01') FROM t",
        "SELECT IIF(x > 0, 1, 0) FROM t",
        "SELECT LAG(col) FROM t",
        "SELECT INSTR('hello', 'll') FROM t",
    ]

    for sql in test_sqls:
        print(f"Original: {sql}")
        print(f"Converted: {apply_clickhouse_conversions(sql)}")
        print("-" * 50)
