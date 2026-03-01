import sqlite3
import threading
import os
from enum import Enum
from typing import Optional, List, Tuple
from functools import lru_cache
from prettytable import PrettyTable
import sqlglot

# 数据库类型
DB_TYPE = os.getenv("ALPHASQL_DB_TYPE", "sqlite")

class SQLExecutionResultType(Enum):
    """
    Type of the result of a SQL query execution.
    
    Attributes:
        SUCCESS: The query is executed successfully.
        TIMEOUT: The query execution timed out.
        ERROR: The query execution failed.
    """
    SUCCESS = "success"
    TIMEOUT = "timeout"
    ERROR = "error"
    
class SQLExecutionResult:
    """
    Result of a SQL query execution.
    """
    def __init__(self, db_path: str, sql: str, result_type: SQLExecutionResultType, result_cols: Optional[List[str]], result: Optional[List[Tuple]], error_message: Optional[str]) -> None:
        self.db_path = db_path
        self.sql = sql
        self.result_type = result_type
        self.result_cols = result_cols
        self.result = result
        self.error_message = error_message
        
    def to_dict(self) -> dict:
        return {
            "db_path": self.db_path,
            "sql": self.sql,
            "result_type": self.result_type.value,
            "result_cols": self.result_cols,
            "result": self.result,
            "error_message": self.error_message
        }

class ExecuteSQLThread(threading.Thread):
    """
    Thread to execute a SQL query.
    """
    def __init__(self, db_path: str, query: str, timeout: int) -> None:
        super().__init__()
        self.db_path = db_path
        self.query = query
        self.timeout = timeout
        self.result_cols = None
        self.result = None
        self.exception = None
        
        self.stop_event = threading.Event()
        
    def run(self) -> None:
        def check_stop():
            if self.stop_event.is_set():
                raise Exception("Query execution cancelled")
        
        try:
            # Enforce to read-only mode, to prevent accidental modification of the database
            with sqlite3.connect(f'file:{self.db_path}?mode=ro', uri=True) as conn:
                conn.text_factory = lambda x: str(x, 'utf-8', errors='replace')  # Add error handling for UTF-8 decoding
                conn.set_progress_handler(check_stop, 1000)
                cursor = conn.cursor()
                cursor.execute(self.query)
                self.result_cols = [col[0] for col in cursor.description]
                self.result = cursor.fetchall()
        except Exception as e:
            self.exception = e

def execute_sql_with_timeout(db_path: str, query: str, timeout: int = 60) -> SQLExecutionResult:
    """
    Execute a SQL query synchronously with a timeout.

    Args:
        db_path: The path to the database.
        query: The SQL query to execute.
        timeout: The timeout.
    Returns:
        The result of the SQL query.
    """
    if DB_TYPE == "clickhouse":
        # 使用ClickHouse执行查询
        from alphasql.database import clickhouse_db
        return clickhouse_db.execute_sql_with_timeout(db_path, query, timeout)

    # 使用SQLite执行查询
    thread = ExecuteSQLThread(db_path, query, timeout)
    thread.daemon = True
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        thread.stop_event.set()
        thread.join(1)
        error_message = f"SQL execution timed out after {timeout} seconds"
        return SQLExecutionResult(db_path, query, SQLExecutionResultType.TIMEOUT, None, None, error_message)
    if thread.exception:
        error_message = str(thread.exception)
        return SQLExecutionResult(db_path, query, SQLExecutionResultType.ERROR, None, None, error_message)
    return SQLExecutionResult(db_path, query, SQLExecutionResultType.SUCCESS, thread.result_cols, thread.result, None)

def execute_sql_without_timeout(db_path: str, query: str) -> SQLExecutionResult:
    """
    Execute a SQL query without a timeout.

    Args:
        db_path: The path to the database.
        query: The SQL query to execute.
    Returns:
        The result of the SQL query.
    """
    if DB_TYPE == "clickhouse":
        # 使用ClickHouse执行查询
        from alphasql.database import clickhouse_db
        return clickhouse_db.execute_sql_with_timeout(db_path, query)

    # 使用SQLite执行查询
    try:
        with sqlite3.connect(f'file:{db_path}?mode=ro', uri=True) as conn:
            conn.text_factory = lambda x: str(x, 'utf-8', errors='replace')  # Add error handling for UTF-8 decoding
            cursor = conn.cursor()
            cursor.execute(query)
            result_cols = [col[0] for col in cursor.description]
            result = cursor.fetchall()
            return SQLExecutionResult(db_path, query, SQLExecutionResultType.SUCCESS, result_cols, result, None)
    except Exception as e:
        return SQLExecutionResult(db_path, query, SQLExecutionResultType.ERROR, None, None, str(e))

# def normalize_sql(sql: str) -> str:
#     """
#     Normalize a SQL query.
    
#     Args:
#         sql: The SQL query to normalize.
#     Returns:
#         The normalized SQL query.
#     """
#     if sql.startswith("```sql") and sql.endswith("```"):
#         sql = sql[6:-3]
#     norm_sql = sql.replace(";", "").replace("\n", " ").replace("\t", " ").replace("\\n", " ")
#     while "  " in norm_sql:
#         norm_sql = norm_sql.replace("  ", " ")
#     return norm_sql.strip()

def normalize_sql(sql: str) -> str:
    """
    Normalize a SQL query.
    
    Args:
        sql: The SQL query to normalize.
    Returns:
        The normalized SQL query.
    """
    sql = sql.strip()
    if sql.startswith("```sql") and sql.endswith("```"):
        sql = sql[6:-3]
    try:
        parsed = sqlglot.parse_one(sql, dialect="sqlite")
        return parsed.sql(dialect="sqlite", normalize=True, pretty=False, comments=False)
    except Exception as e:
        return sql

@lru_cache(maxsize=10000)
def _cached_execute_sql_with_timeout(db_path: str, sql_query: str) -> SQLExecutionResult:
    result = execute_sql_with_timeout(db_path, sql_query)
    return result

def cached_execute_sql_with_timeout(db_path: str, sql_query: str) -> SQLExecutionResult:
    # sql_query = normalize_sql(sql_query)
    # ClickHouse: 转换 SQL 函数名
    if DB_TYPE == "clickhouse":
        import re
        from alphasql.database.clickhouse_db import replace_get_date_in_sql
        # 1. 替换 get_date() 函数
        sql_query = replace_get_date_in_sql(sql_query)
        # 2. 将双引号替换为反引号
        sql_query = re.sub(r'(?<!`)"([^"]+)"', r'`\1`', sql_query)
        # 3. 将 INSTR 函数替换为 position
        sql_query = re.sub(r'INSTR\s*\(\s*([^,]+),\s*([^)]+)\)\s*',
                          r'position(\2 IN \1)', sql_query, flags=re.IGNORECASE)
        # 4. 将大写的函数名转换为小写
        sql_query = re.sub(r'PARSEDATETIMEBESTEFFORTORNULL', 'toDate', sql_query, flags=re.IGNORECASE)

        # 5. 替换其他大写函数名
        sql_query = re.sub(r'LAGINFRAME\s*\(', 'lagInFrame(', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'LEADINFRAME\s*\(', 'leadInFrame(', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'IIF\s*\(', 'if(', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'\bLAG\s*\(', 'lagInFrame(', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'\bLEAD\s*\(', 'leadInFrame(', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'\bTOMONTH\s*\(', 'toMonth(', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'\bTODAYOFMONTH\s*\(', 'toDayOfMonth(', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'\bTOYEAR\s*\(', 'toYear(', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'\bTODATE\s*\(', 'toDate(', sql_query, flags=re.IGNORECASE)
        # 6. 替换 ROWNUMBER -> row_number()
        sql_query = re.sub(r'\bROWNUMBER\s*\(', 'row_number()', sql_query, flags=re.IGNORECASE)
        # 7. 替换 ROWNUMBERINPARTITION -> row_number()
        sql_query = re.sub(r'\bROWNUMBERINPARTITION\s*\(', 'row_number()', sql_query, flags=re.IGNORECASE)
        # 8. 替换 ROWNUMBERINFRAME -> row_number()
        sql_query = re.sub(r'\bROWNUMBERINFRAME\s*\(', 'row_number()', sql_query, flags=re.IGNORECASE)
        # 9. 替换 ROWNUMBERINALL -> row_number()
        sql_query = re.sub(r'\bROWNUMBERINALL\s*\(', 'row_number()', sql_query, flags=re.IGNORECASE)
        # 10. 替换 TOYYYYMMDD -> toYYYYMMDD
        sql_query = re.sub(r'\bTOYYYYMMDD\s*\(', 'toYYYYMMDD(', sql_query, flags=re.IGNORECASE)
        # 11. 替换 TODATETIME -> toDateTime
        sql_query = re.sub(r'\bTODATETIME\s*\(', 'toDateTime(', sql_query, flags=re.IGNORECASE)
        # 12. 替换 GROUP_ARRAY -> groupArray
        sql_query = re.sub(r'\bGROUP_ARRAY\s*\(', 'groupArray(', sql_query, flags=re.IGNORECASE)
        # 13. SUMIF 警告（ClickHouse不支持）
        if re.search(r'\bSUMIF\s*\(', sql_query, flags=re.IGNORECASE):
            logger.warning("SUMIF is not supported in ClickHouse. Please use CASE WHEN + SUM instead.")

    result = _cached_execute_sql_with_timeout(db_path, sql_query)
    return result

def is_valid_execution_result(result: SQLExecutionResult) -> bool:
    if result.result_type is not SQLExecutionResultType.SUCCESS:
        return False
    return any(any(col is not None for col in row) for row in result.result)
    # return True

def format_execution_result(result: SQLExecutionResult, row_limit: int = 3, val_length_limit: int = 100) -> str:
    if result.result_type == SQLExecutionResultType.SUCCESS:
        table = PrettyTable()
        # if the result_cols has non-unique values, add a suffix to the column name
        # since table.field_names cannot have duplicate values
        if len(result.result_cols) != len(set(result.result_cols)):
            result.result_cols = [col + "_" + str(i) for i, col in enumerate(result.result_cols)]
        table.field_names = result.result_cols
        truncated_result = []
        for row in result.result[:row_limit]:
            truncated_row = []
            for i, val in enumerate(row):
                if isinstance(val, str) and len(val) > val_length_limit:
                    truncated_row.append(val[:val_length_limit] + "...")
                else:
                    truncated_row.append(val)
            truncated_result.append(truncated_row)
        table.add_rows(truncated_result)
        return str(table)
    else:
        return result.error_message
