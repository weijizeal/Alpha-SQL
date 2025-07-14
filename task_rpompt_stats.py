import json
import numpy as np
from pathlib import Path
import argparse

def analyze_stats(stats_file: Path):
    with open(stats_file, 'r') as f:
        results = json.load(f)
    
    if not results:
        print("No task results found")
        return
    
    # 计算统计指标
    stats = {
        "summary": {
            "任务总数": len(results),
            "任务平均调用次数": sum(r['call_count'] for r in results) / len(results),
            "任务大模型返回内容数": sum(r['n_content_count'] for r in results) / len(results),
            "任务平均token数": sum(r['total_tokens'] for r in results) / len(results),
        },
        "percentiles": {
            "调用次数": {
                "p50": np.percentile([r['call_count'] for r in results], 50),
                "p90": np.percentile([r['call_count'] for r in results], 90),
            },
            "token消耗": {
                "p50": np.percentile([r['total_tokens'] for r in results], 50),
                "p90": np.percentile([r['total_tokens'] for r in results], 90),
            }
        }
    }
    
    # 保存统计结果
    output_file = stats_file.parent / "analysis_report.json"
    with open(output_file, 'w') as f:
        json.dump(stats, f, indent=4, ensure_ascii=False)
    
    print(f"Analysis report saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("stats_file", help="Path to task stats JSON file", required=False, default="task_stats.json")
    args = parser.parse_args()
    
    analyze_stats(Path(args.stats_file))