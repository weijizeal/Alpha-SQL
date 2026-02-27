"""
ClickHouse 数据库模块测试

运行方式:
    ALPHASQL_DB_TYPE=clickhouse STOCK_CLICKHOUSE_HOST=192.168.81.84 \
    STOCK_CLICKHOUSE_PORT=8123 STOCK_CLICKHOUSE_DBNAME=compass_ai \
    STOCK_CLICKHOUSE_USERNAME=default STOCK_CLICKHOUSE_PASSWORD=123456 \
    python -m pytest tests/test_clickhouse.py -v
"""
import os

# 设置环境变量（在导入模块之前）
os.environ["ALPHASQL_DB_TYPE"] = "clickhouse"
os.environ["STOCK_CLICKHOUSE_HOST"] = "192.168.81.84"
os.environ["STOCK_CLICKHOUSE_PORT"] = "8123"
os.environ["STOCK_CLICKHOUSE_DBNAME"] = "compass_ai"
os.environ["STOCK_CLICKHOUSE_USERNAME"] = "default"
os.environ["STOCK_CLICKHOUSE_PASSWORD"] = "123456"

import pytest


class TestClickHouseConnection:
    """测试 ClickHouse 连接"""

    def test_get_client(self):
        """测试获取客户端连接"""
        from alphasql.database.clickhouse_db import get_clickhouse_client
        client = get_clickhouse_client()
        assert client is not None

    def test_get_table_names(self):
        """测试获取表名"""
        from alphasql.database.clickhouse_db import get_table_names
        tables = get_table_names()
        assert len(tables) > 0
        assert "hq_basic_view" in tables
        assert "hot_info_view" in tables

    def test_get_table_columns(self):
        """测试获取表列信息"""
        from alphasql.database.clickhouse_db import get_table_columns
        columns = get_table_columns("hq_basic_view")
        assert len(columns) > 0
        # 检查列名和类型
        col_names = [c[0] for c in columns]
        assert "secucode" in col_names
        assert "secuname" in col_names


class TestClickHouseSchema:
    """测试 ClickHouse Schema 加载"""

    def test_get_schema_dict_for_single_table(self):
        """测试获取单个表的 schema"""
        from alphasql.database.clickhouse_db import get_schema_dict_for_table
        schema = get_schema_dict_for_table("hq_basic_view")
        assert "tables" in schema
        assert "hq_basic_view" in schema["tables"]
        table_schema = schema["tables"]["hq_basic_view"]
        assert "columns" in table_schema
        assert len(table_schema["columns"]) > 0

    def test_get_schema_dict_for_all(self):
        """测试获取所有表的 schema"""
        from alphasql.database.clickhouse_db import get_schema_dict_for_table
        schema = get_schema_dict_for_table("all")
        assert "tables" in schema
        assert len(schema["tables"]) > 100  # 应该有很多表

    def test_get_schema_dict_for_nonexistent_table(self):
        """测试获取不存在的表（应返回所有表）"""
        from alphasql.database.clickhouse_db import get_schema_dict_for_table
        schema = get_schema_dict_for_table("nonexistent_table_xyz")
        assert "tables" in schema
        # 应该返回所有表
        assert len(schema["tables"]) > 100


class TestClickHouseSQLExecution:
    """测试 ClickHouse SQL 执行"""

    def test_execute_simple_query(self):
        """测试执行简单查询"""
        from alphasql.database.clickhouse_db import execute_query
        success, columns, rows, error = execute_query("SELECT 1 as test")
        assert success is True
        assert len(rows) > 0
        assert rows[0][0] == 1

    def test_execute_table_query(self):
        """测试执行表查询"""
        from alphasql.database.clickhouse_db import execute_query
        success, columns, rows, error = execute_query("SELECT secucode, secuname FROM hq_basic_view LIMIT 3")
        assert success is True
        assert "secucode" in columns
        assert "secuname" in columns
        assert len(rows) == 3

    def test_execute_sql_with_timeout(self):
        """测试带超时的 SQL 执行"""
        from alphasql.database.sql_execution import execute_sql_with_timeout
        result = execute_sql_with_timeout("", "SELECT * FROM hq_basic_view LIMIT 5")
        assert result.result_type.value == "success"
        assert len(result.result) > 0


class TestDatabaseManager:
    """测试 DatabaseManager 集成"""

    def test_get_database_schema(self):
        """测试 DatabaseManager 获取 schema"""
        from alphasql.database.database_manager import DatabaseManager
        schema = DatabaseManager.get_database_schema("hq_basic_view", "")
        assert schema is not None
        assert len(schema.tables) > 0
        assert "hq_basic_view" in schema.tables

    def test_get_database_schema_with_stock_data(self):
        """测试使用 stock_data 作为 db_id"""
        from alphasql.database.database_manager import DatabaseManager
        schema = DatabaseManager.get_database_schema("stock_data", "")
        assert schema is not None
        # 应该返回所有表
        assert len(schema.tables) > 100
        assert "hq_basic_view" in schema.tables

    def test_cache_functionality(self):
        """测试缓存功能"""
        from alphasql.database.database_manager import DatabaseManager
        # 清除缓存
        DatabaseManager.CACHED_DATABASE_SCHEMA.clear()
        # 第一次加载
        schema1 = DatabaseManager.get_database_schema("hq_basic_view", "")
        # 第二次应该从缓存获取
        schema2 = DatabaseManager.get_database_schema("hq_basic_view", "")
        assert schema1 is schema2  # 应该是同一个对象


class TestValueExamples:
    """测试值示例加载"""

    def test_load_string_examples(self):
        """测试加载字符串列的值示例"""
        from alphasql.database.clickhouse_db import load_value_examples
        examples = load_value_examples("hq_basic_view", "secuname", max_num_examples=3)
        assert isinstance(examples, list)

    def test_load_numeric_examples(self):
        """测试加载数值列的值示例"""
        from alphasql.database.clickhouse_db import load_value_examples
        # 数值列可能返回空列表（因为有null值问题）
        examples = load_value_examples("hq_basic_view", "idstk", max_num_examples=3)
        assert isinstance(examples, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
