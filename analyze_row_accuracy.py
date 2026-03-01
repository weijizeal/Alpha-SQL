#!/usr/bin/env python3
"""
分析SQL预测准确率
统计：
1. 完全准确：行数相同 + 内容完全相同
2. 行相同但列有问题：行数相同，但列不完全匹配（包括多列、少列、列不同）
3. 行相同但内容不同：行数相同，但列匹配或不完全匹配，内容不同
"""

import json
import os
import sys
import re
from typing import Any, Dict, List, Tuple

current_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_path)

def load_json(file_path: str) -> Any:
    with open(file_path, 'r') as f:
        return json.load(f)

def execute_sql(sql: str, db_path: str, timeout: float = 30.0) -> Tuple[List, str]:
    """执行SQL并返回结果和错误信息"""
    from alphasql.database import clickhouse_db

    try:
        result = clickhouse_db.execute_sql_with_timeout(db_path, sql, timeout)
        if result.result_type.value == "success":
            return result.result if result.result else [], ""
        else:
            return [], result.error_message or "Query execution failed"
    except Exception as e:
        return [], str(e)

def extract_columns(sql: str) -> List[str]:
    """提取SQL中的SELECT列"""
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)

    match = re.search(r'SELECT\s+(.+?)\s+(?:FROM|ORDER|GROUP|WHERE|$)', sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return []

    cols_text = match.group(1).strip()

    for sep in ['UNION', 'INTERSECT', 'EXCEPT']:
        if sep in cols_text.upper():
            cols_text = re.split(r'\b' + sep + r'\b', cols_text, flags=re.IGNORECASE)[0]

    if cols_text.upper().startswith('WITH'):
        return []

    cols = []
    for col in cols_text.split(','):
        col = col.strip()
        if '(' in col and ')' in col:
            inner = re.search(r'(.+?)\((.+?)\)', col)
            if inner:
                col = inner.group(2)
        if ' AS ' in col.upper():
            col = re.split(r'\bAS\b', col, flags=re.IGNORECASE)[0].strip()
        col = col.replace('`', '').replace('"', '')
        if '.' in col:
            col = col.split('.')[-1]
        col = col.replace('DISTINCT', '').strip()
        col = re.sub(r'^(COUNT|SUM|AVG|MAX|MIN)\(', '', col, flags=re.IGNORECASE).rstrip(')')

        if col and col not in ['FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT']:
            cols.append(col)

    return [c for c in cols if c]

def check_partial_column_match(gt_result: List, pred_result: List) -> Tuple[bool, List[int]]:
    """检查是否有至少一列完全匹配"""
    if not gt_result or not pred_result:
        return False, []

    gt_cols = len(gt_result[0]) if gt_result else 0
    pred_cols = len(pred_result[0]) if pred_result else 0

    if gt_cols == 0 or pred_cols == 0:
        return False, []

    matched_cols = []
    for col_idx in range(min(gt_cols, pred_cols)):
        gt_vals = set(row[col_idx] for row in gt_result if len(row) > col_idx)
        pred_vals = set(row[col_idx] for row in pred_result if len(row) > col_idx)
        if gt_vals == pred_vals and len(gt_vals) > 0:
            matched_cols.append(col_idx)

    return len(matched_cols) > 0, matched_cols

def main():
    print("加载数据...")
    pred_sqls = load_json('pred_clickhouse_sqls.json')
    dev_data = load_json('data/stock/dev/dev1.json')

    gt_map = {}
    for item in dev_data:
        qid = str(item['question_id'])
        gt_map[qid] = {
            "question": item['question'],
            "ground_truth": item['SQL'],
            "evidence": item.get('evidence', ''),
            "difficulty": item.get('difficulty', '')
        }

    db_path = "stock_data"

    exact_match_ids = []
    row_match_partial_match_ids = []  # 行相同+至少有1列匹配
    row_match_no_match_ids = []  # 行相同+完全没有列匹配

    results = []

    print(f"开始评估 {len(pred_sqls)} 个问题...")

    for qid, pred_sql in pred_sqls.items():
        if qid not in gt_map:
            continue

        gt_sql = gt_map[qid]['ground_truth']
        question = gt_map[qid]['question']

        pred_result, pred_error = execute_sql(pred_sql, db_path)
        gt_result, gt_error = execute_sql(gt_sql, db_path)

        if pred_error or gt_error:
            results.append({
                "qid": qid,
                "question": question,
                "category": "sql_error"
            })
            continue

        pred_count = len(pred_result)
        gt_count = len(gt_result)

        pred_set = set(tuple(row) for row in pred_result)
        gt_set = set(tuple(row) for row in gt_result)

        is_row_match = (pred_count == gt_count)
        is_exact_match = (pred_set == gt_set)

        if is_exact_match:
            exact_match_ids.append(int(qid))
            category = "exact_match"
        elif is_row_match:
            # 行相同，检查是否有至少一列内容相同
            has_partial, matched_cols = check_partial_column_match(gt_result, pred_result)

            if has_partial:
                row_match_partial_match_ids.append(int(qid))
                category = "row_match_partial_match"
            else:
                row_match_no_match_ids.append(int(qid))
                category = "row_match_no_match"
        else:
            category = "row_count_diff"

        # 提取列信息
        gt_cols = extract_columns(gt_sql)
        pred_cols = extract_columns(pred_sql)

        results.append({
            "qid": qid,
            "question": question,
            "ground_truth": gt_sql,
            "prediction": pred_sql,
            "gt_count": gt_count,
            "pred_count": pred_count,
            "category": category,
            "gt_columns": gt_cols,
            "pred_columns": pred_cols,
            "gt_result": gt_result[:5],
            "pred_result": pred_result[:5]
        })

        if int(qid) % 10 == 0:
            print(f"已处理 {qid} 个问题...")

    exact_match_ids.sort()
    row_match_partial_match_ids.sort()
    row_match_no_match_ids.sort()

    print("\n" + "="*60)
    print("统计结果")
    print("="*60)
    print(f"\n完全准确: {len(exact_match_ids)}")
    print(f"  IDs: {exact_match_ids}")

    print(f"\n行相同但至少有1列内容相同: {len(row_match_partial_match_ids)}")
    print(f"  IDs: {row_match_partial_match_ids}")

    print(f"\n行相同但完全没有列相同: {len(row_match_no_match_ids)}")
    print(f"  IDs: {row_match_no_match_ids}")

    # 保存到文件
    output = {
        "exact_match": {
            "count": len(exact_match_ids),
            "ids": exact_match_ids
        },
        "row_match_partial_match": {
            "count": len(row_match_partial_match_ids),
            "ids": row_match_partial_match_ids
        },
        "row_match_no_match": {
            "count": len(row_match_no_match_ids),
            "ids": row_match_no_match_ids
        },
        "all_results": results
    }

    with open('accuracy_analysis_result.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n结果已保存到 accuracy_analysis_result.json")

if __name__ == '__main__':
    main()
