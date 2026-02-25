#!/usr/bin/env python
"""调试 preprocessor 的错误"""

import json
import sys
sys.path.insert(0, '/home/weiji/git_proj/Alpha-SQL')

from alphasql.runner.task import Task
from alphasql.runner.preprocessor import Preprocessor
from alphasql.database.database_manager import DatabaseManager
import traceback

# 加载数据
data = json.load(open('data/bird/dev/dev1.json'))
db_root = 'data/bird/dev/dev_databases'

# 初始化 preprocessor
preprocessor = Preprocessor(
    data_file_path='data/bird/dev/dev1.json',
    database_root_dir=db_root,
    lsh_threshold=0.5,
    lsh_signature_size=128,
    lsh_n_gram=3,
    lsh_top_k=20,
    edit_similarity_threshold=0.3,
    embedding_similarity_threshold=0.6,
    save_root_dir='data/preprocessed/bird/dev1'
)

# 测试出错的任务
failed_tasks = [3, 4, 6, 7, 15, 17, 19, 20, 23, 28, 29, 30, 32, 33, 37, 38, 40, 42, 44, 45]

for task_idx in failed_tasks[:3]:  # 只测试前3个
    print(f"\n{'='*50}")
    print(f"测试 Task {task_idx}")
    print(f"{'='*50}")

    task_data = data[task_idx]
    task = Task(
        question_id=task_idx,
        db_id=task_data['db_id'],
        question=task_data['question'],
        evidence=task_data.get('evidence', ''),
        sql=task_data.get('SQL', '')
    )

    print(f"DB: {task.db_id}")
    print(f"Question: {task.question[:80]}...")

    try:
        # 步骤1: 获取 keywords
        print("\n[Step 1] 获取 keywords...")
        keywords = preprocessor.get_keywords_for_task(task)
        print(f"  Keywords: {keywords[:5] if keywords else 'None'}...")

        if not keywords:
            print("  ❌ keywords 为空!")
            continue

        # 步骤2: LSH 查询
        print("\n[Step 2] LSH 查询...")
        from alphasql.database.lsh_index import LSHIndex
        lsh_candidate_values = []
        for keyword in keywords[:3]:  # 只测试前3个关键词
            results = LSHIndex.query_lsh_index(
                DatabaseManager.get_database_schema(task.db_id, db_root),
                keyword,
                top_k=20,
                signature_size=128,
                n_gram=3
            )
            lsh_candidate_values.extend(results)
        print(f"  LSH 结果数量: {len(lsh_candidate_values)}")

        # 步骤3: edit similarity 过滤
        print("\n[Step 3] Edit similarity 过滤...")
        edit_candidates = preprocessor.filter_candidate_values_by_edit_similarity(
            lsh_candidate_values, 0.3
        )
        print(f"  Edit 过滤后数量: {len(edit_candidates)}")

        # 步骤4: embedding similarity 过滤
        print("\n[Step 4] Embedding similarity 过滤...")
        if edit_candidates:
            embed_candidates = preprocessor.filter_candidate_values_by_embedding_similarity(
                edit_candidates, 0.6
            )
            print(f"  Embedding 过滤后数量: {len(embed_candidates)}")
        else:
            print("  ❌ edit_candidates 为空，跳过 embedding")

        # 步骤5: 完整流程
        print("\n[Step 5] 完整流程...")
        result = preprocessor.get_relevant_values_for_task(task)
        print(f"  ✅ 最终结果: {len(result)} 个 (table, column) 对")

    except Exception as e:
        print(f"\n❌ 错误: {e}")
        traceback.print_exc()
