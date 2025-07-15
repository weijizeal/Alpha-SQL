from functools import partial
import multiprocessing
import pickle
import sqlite3
import sys
from datasketch import MinHash, MinHashLSH
from pathlib import Path
from typing import Dict, List
from collections import defaultdict
from nltk.util import ngrams
from tqdm import tqdm
from typing import Tuple, Any
import shutil
from functools import lru_cache

from alphasql.database.schema import DatabaseSchema
from alphasql.database.sql_execution import execute_sql_without_timeout


class LSHIndex:
    """
    A class for creating and querying a LSH index for a database schema.
    
    Attributes:
        QUERY_DISTINCT_VALUES_SQL (str): The SQL query to get the unique values for a column.
        CACHED_LSH_INDEX (Dict[str, Tuple[MinHashLSH, Dict[str, Tuple[MinHash, str, str, int, str]]]]): A dictionary mapping database ids to LSH indexes.
    """
    QUERY_DISTINCT_VALUES_SQL = "SELECT DISTINCT `{column_name}` FROM `{table_name}` WHERE `{column_name}` IS NOT NULL"
    
    CACHED_LSH_INDEX: Dict[str, Tuple[MinHashLSH, Dict[str, Tuple[MinHash, str, str, int, str]]]] = {}
    
    @classmethod
    def get_unique_database_values(cls, database_schema: DatabaseSchema, ignore_primary_keys: bool = True, ignore_non_text_columns: bool = True) -> Dict[str, Dict[str, List[str]]]:
        """
        Get the unique values for each column in the database schema.
        
        Args:
            database_schema (DatabaseSchema): The database schema to get the unique values for.
            ignore_primary_keys (bool): Whether to ignore primary keys.
            ignore_non_text_columns (bool): Whether to ignore non-text columns.
        Returns:
            Dict[str, Dict[str, List[str]]]: A dictionary containing the unique values for each column.
        """
        unique_values: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
        db_path = Path(database_schema.db_directory) / f"{database_schema.db_id}.sqlite"
        
        for table_name, table_schema in database_schema.tables.items():
            for column_name, column_schema in table_schema.columns.items():
                if ignore_primary_keys and column_schema.primary_key:
                    continue
                if ignore_non_text_columns and column_schema.column_type.lower() != "text":
                    continue
                query = cls.QUERY_DISTINCT_VALUES_SQL.format(column_name=column_name, table_name=table_name)
                execution_result = execute_sql_without_timeout(db_path, query)
                unique_values[table_name][column_name] = [str(row[0]) for row in execution_result.result]
                
        return unique_values
        
    @classmethod
    def create_minhash(cls, string: str, signature_size: int = 64, n_gram: int = 3) -> MinHash:
        """
        Create a MinHash for a string.
        
        Args:
            string (str): The string to create a MinHash for.
            signature_size (int): The size of the signature, defaults to 64.
            n_gram (int): The size of the n-gram, defaults to 5.
        Returns:
            MinHash: A MinHash for the string.
        """
        minhash = MinHash(num_perm=signature_size)
        for d in ngrams(string, n_gram):
            minhash.update("".join(d).encode('utf8'))
        return minhash
    
    @classmethod
    def create_lsh_index(cls, database_schema: DatabaseSchema, threshold: float = 0.5, 
                        signature_size: int = 64, n_gram: int = 3, 
                        batch_size: int = 1500, num_workers: int = None) -> None:
        """
        创建LSH索引并将MinHash数据存储在SQLite数据库中
        """
        # 获取数据库中的唯一值
        unique_values = cls.get_unique_database_values(database_schema)
        
        # 创建LSH索引
        lsh_index = MinHashLSH(threshold=threshold, num_perm=signature_size)
        
        # 准备SQLite数据库存储MinHash数据
        lsh_index_dir_path = Path(database_schema.db_directory) / "lsh_index"
        if lsh_index_dir_path.exists():
            shutil.rmtree(lsh_index_dir_path)
        lsh_index_dir_path.mkdir(parents=True)
        
        # 创建SQLite数据库
        db_path = lsh_index_dir_path / "minhashes.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 创建表结构
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS minhashes (
            id TEXT PRIMARY KEY,
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            value TEXT NOT NULL,
            minhash BLOB NOT NULL
        )
        ''')
        
        # 准备要处理的项目
        items_to_process = []
        for table_name, table_values in unique_values.items():
            for column_name, column_values in table_values.items():
                for value_idx, value in enumerate(column_values):
                    items_to_process.append((table_name, column_name, value_idx, value))
        
        total_unique_values_count = len(items_to_process)
        
        # 设置多进程
        num_workers = num_workers or multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=num_workers)
        
        # 处理批次
        process_batch_fn = partial(
            cls._process_batch,
            signature_size=signature_size,
            n_gram=n_gram
        )
        
        with tqdm(total=total_unique_values_count, 
                desc=f"Creating LSH index for database: {database_schema.db_id}") as pbar:
            
            # 分批处理
            for batch_idx in range(0, total_unique_values_count, batch_size):
                batch = items_to_process[batch_idx:batch_idx + batch_size]
                
                # 并行处理批次
                batch_results = pool.map(process_batch_fn, batch)
                
                # 更新索引和数据库
                for result in batch_results:
                    minhash_key, minhash, table_name, column_name, value = result
                    
                    # 将MinHash序列化
                    minhash_bytes = pickle.dumps(minhash)
                    
                    # 插入数据库
                    cursor.execute('''
                    INSERT INTO minhashes (id, table_name, column_name, value, minhash)
                    VALUES (?, ?, ?, ?, ?)
                    ''', (minhash_key, table_name, column_name, value, minhash_bytes))
                    
                    # 添加到LSH索引
                    lsh_index.insert(minhash_key, minhash)
                    pbar.update(1)
                
                # 提交当前批次
                conn.commit()
        
        pool.close()
        pool.join()
        conn.close()
        
        # 保存LSH索引
        lsh_index_path = lsh_index_dir_path / "lsh_index.pkl"
        with open(lsh_index_path, "wb") as f:
            pickle.dump(lsh_index, f)

    @classmethod
    def _process_batch(cls, item, signature_size: int, n_gram: int) -> tuple:
        """
        Helper method to process a single item in a batch.
        
        Args:
            item: Tuple of (table_name, column_name, value_idx, value)
            signature_size: Size of the MinHash signature
            n_gram: Size of n-grams to use
            
        Returns:
            Tuple containing (minhash_key, minhash, table_name, column_name, value)
        """
        table_name, column_name, value_idx, value = item
        minhash = cls.create_minhash(value, signature_size, n_gram)
        minhash_key = f"{table_name}_{column_name}_{value_idx}"
        return (minhash_key, minhash, table_name, column_name, value)
    
    @classmethod
    @lru_cache(maxsize=1)
    def _get_cached_lsh_index(cls, db_directory: str):
        """从SQLite数据库加载缓存的LSH索引"""
        lsh_index_dir_path = Path(db_directory) / "lsh_index"
        lsh_index_path = lsh_index_dir_path / "lsh_index.pkl"
        
        # 加载LSH索引
        with open(lsh_index_path, "rb") as f:
            lsh_index = pickle.load(f)
            
        # 不再需要加载所有minhashes到内存
        # 我们将在query_lsh_index中按需从SQLite加载
        
        return lsh_index, str(lsh_index_dir_path / "minhashes.db")  # 返回数据库路径
    
    @classmethod
    def query_lsh_index(cls, database_schema: DatabaseSchema, query: str, top_k: int = 10, 
                       signature_size: int = 64, n_gram: int = 3) -> List[Dict[str, Any]]:
        """
        查询LSH索引，从SQLite数据库按需加载MinHash数据
        """
        # 获取缓存中的LSH索引和数据库路径
        lsh_index, db_path = cls._get_cached_lsh_index(str(database_schema.db_directory))
        # print(f"LSH索引大小: {sys.getsizeof(lsh_index)} bytes")

        # 创建查询的minhash
        query_minhash = cls.create_minhash(query, signature_size, n_gram)
        # print(f"MinHash对象大小: {sys.getsizeof(query_minhash)} bytes")

        # 查询LSH索引获取匹配的键
        result_keys = lsh_index.query(query_minhash)
        
        # 连接到SQLite数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        similar_items = []
        for result_key in result_keys:
            # 从数据库加载minhash
            cursor.execute('''
            SELECT table_name, column_name, value, minhash 
            FROM minhashes 
            WHERE id = ?
            ''', (result_key,))
            
            row = cursor.fetchone()
            if row:
                table_name, column_name, value, minhash_bytes = row
                minhash = pickle.loads(minhash_bytes)
                score = minhash.jaccard(query_minhash)
                similar_items.append((result_key, score, table_name, column_name, value))
        
        conn.close()
        
        # 排序并返回top_k结果
        similar_items = sorted(similar_items, key=lambda x: x[1], reverse=True)[:top_k]
        # print(f"结果键数量: {len(result_keys)}")
        return [
            {
                "query": query,
                "lsh_score": score,
                "table_name": table_name,
                "column_name": column_name,
                "value": value
            }
            for _, score, table_name, column_name, value in similar_items
        ]
        