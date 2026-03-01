#!/usr/bin/env python3
"""
生成分析对比文件 analysis_comparison.json
将 dev1.json (真实SQL) 和 pred_clickhouse_sqls.json (预测SQL) 合并
"""

import json

def main():
    # 读取 dev1.json (ground truth)
    with open('data/stock/dev/dev1.json', 'r') as f:
        dev_data = json.load(f)

    # 读取 pred_clickhouse_sqls.json (predictions)
    with open('pred_clickhouse_sqls.json', 'r') as f:
        pred_data = json.load(f)

    # 创建 question_id 到 ground_truth 的映射
    gt_map = {}
    for item in dev_data:
        qid = str(item['question_id'])
        gt_map[qid] = {
            "question": item['question'],
            "ground_truth": item['SQL'],
            "evidence": item.get('evidence', ''),
            "difficulty": item.get('difficulty', '')
        }

    # 合并数据
    result = {}
    for qid in gt_map:
        if qid in pred_data:
            result[qid] = {
                "question": gt_map[qid]['question'],
                "ground_truth": gt_map[qid]['ground_truth'],
                "prediction": pred_data[qid],
                "evidence": gt_map[qid]['evidence'],
                "difficulty": gt_map[qid]['difficulty']
            }
        else:
            # 如果预测中没有这个 question_id，仍然保留 ground_truth
            result[qid] = {
                "question": gt_map[qid]['question'],
                "ground_truth": gt_map[qid]['ground_truth'],
                "prediction": None,
                "evidence": gt_map[qid]['evidence'],
                "difficulty": gt_map[qid]['difficulty']
            }

    # 保存结果
    with open('analysis_comparison.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Generated analysis_comparison.json with {len(result)} entries")
    print(f"  - Questions with predictions: {sum(1 for v in result.values() if v['prediction'] is not None)}")
    print(f"  - Questions without predictions: {sum(1 for v in result.values() if v['prediction'] is None)}")

if __name__ == '__main__':
    main()
