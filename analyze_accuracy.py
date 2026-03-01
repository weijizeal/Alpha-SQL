#!/usr/bin/env python3
"""
分析真实准确率和行准确率的差异
"""

import json
import re
from collections import defaultdict

def extract_select_columns(sql):
    """提取 SELECT 语句中的列名"""
    # 简单提取 SELECT 和 FROM 之间的内容
    match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
    if match:
        cols_text = match.group(1).strip()
        # 处理 UNION, INTERSECT 等
        if 'UNION' in cols_text.upper() or 'INTERSECT' in cols_text.upper():
            cols_text = re.split(r'\b(UNION|INTERSECT)\b', cols_text, flags=re.IGNORECASE)[0]
        # 提取列名
        cols = []
        for col in cols_text.split(','):
            col = col.strip()
            # 去除别名
            if ' AS ' in col.upper():
                col = re.split(r'\bAS\b', col, flags=re.IGNORECASE)[0].strip()
            # 去除函数
            col = re.sub(r'^\w+\(', '', col).rstrip(')')
            cols.append(col.strip('`').strip('"'))
        return [c for c in cols if c]
    return []

def has_aggregation(sql):
    """检查是否有聚合函数"""
    agg_patterns = ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'GROUP BY', 'ORDER BY']
    sql_upper = sql.upper()
    return any(p in sql_upper for p in agg_patterns)

def main():
    # 读取分析对比文件
    with open('analysis_comparison.json', 'r') as f:
        data = json.load(f)

    # 分类统计
    stats = {
        'total': len(data),
        'has_prediction': 0,
        'no_prediction': 0,
        'exact_match': 0,  # SQL 完全相同
        'different_columns': 0,  # 列不同
        'both_have_agg': 0,  # 两者都有聚合
        'only_pred_has_agg': 0,  # 只有预测有聚合
        'only_gt_has_agg': 0,  # 只有真实有聚合
        'neither_has_agg': 0,  # 两者都没有聚合
    }

    column_issues = []
    agg_issues = []

    for qid, item in data.items():
        if item.get('prediction') is None:
            stats['no_prediction'] += 1
            continue

        stats['has_prediction'] += 1

        gt_sql = item['ground_truth']
        pred_sql = item['prediction']

        # 检查是否完全匹配
        if gt_sql.strip() == pred_sql.strip():
            stats['exact_match'] += 1

        # 提取列名
        gt_cols = extract_select_columns(gt_sql)
        pred_cols = extract_select_columns(pred_sql)

        # 检查列差异
        if set(gt_cols) != set(pred_cols):
            stats['different_columns'] += 1
            column_issues.append({
                'qid': qid,
                'question': item['question'][:50],
                'gt_columns': gt_cols,
                'pred_columns': pred_cols
            })

        # 检查聚合差异
        gt_has_agg = has_aggregation(gt_sql)
        pred_has_agg = has_aggregation(pred_sql)

        if gt_has_agg and pred_has_agg:
            stats['both_have_agg'] += 1
        elif gt_has_agg and not pred_has_agg:
            stats['only_gt_has_agg'] += 1
            agg_issues.append({
                'qid': qid,
                'question': item['question'][:50],
                'type': 'missing_agg',
                'gt_sql': gt_sql[:100],
                'pred_sql': pred_sql[:100]
            })
        elif not gt_has_agg and pred_has_agg:
            stats['only_pred_has_agg'] += 1
            agg_issues.append({
                'qid': qid,
                'question': item['question'][:50],
                'type': 'extra_agg',
                'gt_sql': gt_sql[:100],
                'pred_sql': pred_sql[:100]
            })
        else:
            stats['neither_has_agg'] += 1

    print("=" * 60)
    print("SQL 结构分析统计")
    print("=" * 60)
    print(f"总问题数: {stats['total']}")
    print(f"有预测的问题数: {stats['has_prediction']}")
    print(f"无预测的问题数: {stats['no_prediction']}")
    print(f"SQL 完全匹配数: {stats['exact_match']}")
    print()
    print("列差异分析:")
    print(f"  列不同的问题数: {stats['different_columns']}")
    print()
    print("聚合函数分析:")
    print(f"  两者都有聚合: {stats['both_have_agg']}")
    print(f"  只有真实有聚合: {stats['only_gt_has_agg']}")
    print(f"  只有预测有聚合: {stats['only_pred_has_agg']}")
    print(f"  两者都没有聚合: {stats['neither_has_agg']}")

    print()
    print("=" * 60)
    print("列差异详情 (前10个)")
    print("=" * 60)
    for issue in column_issues[:10]:
        print(f"\n问题ID: {issue['qid']}")
        print(f"问题: {issue['question']}...")
        print(f"真实列: {issue['gt_columns']}")
        print(f"预测列: {issue['pred_columns']}")

    print()
    print("=" * 60)
    print("聚合差异详情 (前10个)")
    print("=" * 60)
    for issue in agg_issues[:10]:
        print(f"\n问题ID: {issue['qid']}")
        print(f"问题: {issue['question']}...")
        print(f"类型: {issue['type']}")
        print(f"真实SQL: {issue['gt_sql']}...")
        print(f"预测SQL: {issue['pred_sql']}...")

    # 保存详细分析结果
    analysis_result = {
        'stats': stats,
        'column_issues': column_issues,
        'agg_issues': agg_issues
    }
    with open('accuracy_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(analysis_result, f, ensure_ascii=False, indent=2)

    print("\n详细分析结果已保存到 accuracy_analysis.json")

if __name__ == '__main__':
    main()
