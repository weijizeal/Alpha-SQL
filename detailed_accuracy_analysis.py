#!/usr/bin/env python3
"""
详细分析真实准确率和行准确率的差异
"""

import json

def main():
    with open('pred_accuracy_check.json', 'r') as f:
        data = json.load(f)

    print("=" * 70)
    print("真实准确率 vs 行准确率 差异分析")
    print("=" * 70)

    total = len(data)
    correct = sum(1 for v in data.values() if v.get('correct'))
    row_match = sum(1 for v in data.values() if v.get('gt_count') == v.get('pred_count'))

    print(f"\n总体统计:")
    print(f"  总问题数: {total}")
    print(f"  真实准确 (完全匹配): {correct} ({correct/total*100:.2f}%)")
    print(f"  行数匹配: {row_match} ({row_match/total*100:.2f}%)")
    print(f"  差异: {row_match - correct} ({row_match - correct}个问题行数对但内容不对)")

    print(f"\n差异来源分析:")

    # 分类统计
    gt_empty_pred_not_empty = []
    pred_empty_gt_not_empty = []
    row_match_content_diff = []
    row_diff = []
    both_empty = []

    for qid, v in data.items():
        gt_count = v.get('gt_count', 0)
        pred_count = v.get('pred_count', 0)
        correct = v.get('correct', False)

        if correct:
            continue
        elif gt_count == 0 and pred_count == 0:
            both_empty.append((qid, v))
        elif gt_count == pred_count:
            row_match_content_diff.append((qid, v))
        elif gt_count == 0 and pred_count > 0:
            gt_empty_pred_not_empty.append((qid, v))
        elif gt_count > 0 and pred_count == 0:
            pred_empty_gt_not_empty.append((qid, v))
        else:
            row_diff.append((qid, v))

    print(f"\n1. 行数匹配但内容不同: {len(row_match_content_diff)} 个")
    print(f"   -> 这是真实准确率与行准确率差异的主要原因")

    print(f"\n2. GT为空但预测有结果: {len(gt_empty_pred_not_empty)} 个")
    print(f"   -> 预测了不存在的或多余的数据")

    print(f"\n3. 预测为空但GT有结果: {len(pred_empty_gt_not_empty)} 个")
    print(f"   -> 漏掉了真实存在的数据")

    print(f"\n4. 行数不同: {len(row_diff)} 个")
    print(f"   -> 预测的数量与真实数量不符")

    print(f"\n5. 都为空: {len(both_empty)} 个")

    print("\n" + "=" * 70)
    print("详细案例分析")
    print("=" * 70)

    print("\n--- 行数匹配但内容不同的案例 ---")
    for qid, v in row_match_content_diff[:5]:
        print(f"\n问题 {qid}: {v.get('question', '')[:50]}")
        print(f"  GT行数: {v.get('gt_count')}, Pred行数: {v.get('pred_count')}")
        missing = v.get('missing', [])
        extra = v.get('extra', [])
        print(f"  缺失: {missing[:3]}...")
        print(f"  多余: {extra[:3]}...")

    print("\n\n--- 预测为空但GT有结果的案例 ---")
    for qid, v in pred_empty_gt_not_empty[:5]:
        print(f"\n问题 {qid}: {v.get('question', '')[:50]}")
        print(f"  GT行数: {v.get('gt_count')}, Pred行数: {v.get('pred_count')}")
        gt_result = v.get('gt_result', [])
        print(f"  真实结果: {gt_result[:3]}...")

    print("\n\n--- GT为空但预测有结果的案例 ---")
    for qid, v in gt_empty_pred_not_empty[:5]:
        print(f"\n问题 {qid}: {v.get('question', '')[:50]}")
        print(f"  GT行数: {v.get('gt_count')}, Pred行数: {v.get('pred_count')}")
        pred_result = v.get('pred_result', [])
        print(f"  预测结果: {pred_result[:3]}...")

if __name__ == '__main__':
    main()
