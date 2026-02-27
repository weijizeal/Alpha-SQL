from typing import Dict, List, Tuple
from threading import Lock
from collections import defaultdict
import os

from alphasql.database.schema import DatabaseSchema
from alphasql.database.utils import load_database_schema_dict, build_table_ddl_statement
from alphasql.database import clickhouse_db


class DatabaseManager:
    """
    A class for managing databases.

    Attributes:
        CACHED_DATABASE_SCHEMA (Dict[str, DatabaseSchema]): A dictionary mapping database ids to database schemas.
    """
    CACHED_DATABASE_SCHEMA: Dict[str, DatabaseSchema] = {}
    CACHED_DATABASE_SCHEMA_REPRESENTATION: Dict[str, str] = {}
    _lock = Lock()  # Add class-level lock

    # 数据库类型: 'sqlite' 或 'clickhouse'
    # 注意: 使用函数动态获取，以便在运行时可以修改
    @staticmethod
    def get_db_type():
        return os.getenv("ALPHASQL_DB_TYPE", "sqlite")

    @classmethod
    def get_database_schema(cls, db_id: str, database_root_dir: str) -> DatabaseSchema:
        """
        Get the database schema for a given database.

        Args:
            db_id (str): The id of the database.
            database_root_dir (str): The root directory of the database.

        Returns:
            DatabaseSchema: The database schema.
        """
        db_type = cls.get_db_type()
        cache_key = f"{db_type}:{db_id}"

        if cache_key not in cls.CACHED_DATABASE_SCHEMA:
            with cls._lock:  # Add lock protection
                # Double-check pattern to prevent unnecessary loading
                if cache_key not in cls.CACHED_DATABASE_SCHEMA:
                    if db_type == "clickhouse":
                        # 从ClickHouse加载schema
                        # 如果 db_id 是 "stock_data" 或其他非表名，加载所有表
                        database_schema_dict = clickhouse_db.get_schema_dict_for_table(db_id)
                        # 如果返回的 tables 为空，尝试加载所有表
                        if not database_schema_dict.get("tables"):
                            database_schema_dict = clickhouse_db.get_schema_dict_for_table("all")
                        database_schema_dict["db_id"] = db_id
                        database_schema_dict["db_directory"] = "clickhouse"
                    else:
                        # 从SQLite加载schema
                        database_schema_dict = load_database_schema_dict(
                            db_id=db_id,
                            database_root_dir=database_root_dir
                        )
                    cls.CACHED_DATABASE_SCHEMA[cache_key] = DatabaseSchema.from_database_schema_dict(database_schema_dict)
        return cls.CACHED_DATABASE_SCHEMA[cache_key]
    
    @classmethod
    def get_primary_keys(cls, database_schema: DatabaseSchema) -> Dict[str, List[str]]:
        """
        Get the primary keys for each table in the database schema.
        
        Args:
            database_schema (DatabaseSchema): The database schema.
        
        Returns:
            Dict[str, List[str]]: A dictionary mapping table names to a list of primary keys.
        """
        primary_keys = {}
        for table_schema in database_schema.tables.values():
            table_name = table_schema.table_name
            primary_keys[table_name] = [column_name for column_name, column_schema in table_schema.columns.items() if column_schema.primary_key]
        return primary_keys
    
    @classmethod
    def get_foreign_keys(cls, database_schema: DatabaseSchema) -> Dict[Tuple[str, str], List[Tuple[str, str]]]:
        """
        Get the foreign keys for each table in the database schema.
        
        Args:
            database_schema (DatabaseSchema): The database schema.
        
        Returns:
            Dict[Tuple[str, str], List[Tuple[str, str]]]: A dictionary mapping table and column name pairs to a list of foreign key tuples, where each tuple contains the target table name and the target column name.
        """
        foreign_keys = defaultdict(list)
        for table_schema in database_schema.tables.values():
            table_name = table_schema.table_name
            for column_name, column_schema in table_schema.columns.items():
                for foreign_key in column_schema.foreign_keys:
                    foreign_keys[(table_name, column_name)].append(foreign_key)
        return foreign_keys

    @classmethod
    def get_database_schema_representation(cls, 
                                           database_schema: DatabaseSchema,
                                           add_expanded_column_name: bool = True,
                                           add_column_description: bool = True,
                                           add_value_description: bool = True,
                                           add_value_examples: bool = True) -> str:
        """
        Get the representation of the database schema.
        
        Args:
            database_schema (DatabaseSchema): The database schema.
            add_expanded_column_name (bool): Whether to add the expanded column name.
            add_column_description (bool): Whether to add the column description.
            add_value_description (bool): Whether to add the value description.
            add_value_examples (bool): Whether to add the value examples.
        
        Returns:
            str: The representation of the database schema.
        """
        db_id = database_schema.db_id
        if db_id not in cls.CACHED_DATABASE_SCHEMA_REPRESENTATION:
            cls.CACHED_DATABASE_SCHEMA_REPRESENTATION[db_id] = "\n".join([build_table_ddl_statement(
                table_schema_dict=table_schema.to_dict(),
                add_expanded_column_name=add_expanded_column_name,
                add_column_description=add_column_description,
                add_value_description=add_value_description,
                add_value_examples=add_value_examples
            ) for table_schema in database_schema.tables.values()])
        return cls.CACHED_DATABASE_SCHEMA_REPRESENTATION[db_id]