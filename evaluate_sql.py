import sys
import json
import argparse
import sqlite3
import multiprocessing as mp
from func_timeout import func_timeout, FunctionTimedOut
import os
import statistics
from typing import List, Dict, Tuple, Any
import pickle
from collections import defaultdict


current_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_path)
from alphasql.algorithm.mcts.mcts_node import MCTSNode

def load_json(file_path: str) -> Any:
    """加载JSON文件"""
    with open(file_path, 'r') as f:
        return json.load(f)

def load_pickle(file_path: str) -> Any:
    """加载Pickle文件"""
    with open(file_path, 'rb') as f:
        return pickle.load(f)

def save_json(data: Any, file_path: str) -> None:
    """保存数据到JSON文件"""
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def execute_sql(sql: str, db_path: str) -> List[Tuple]:
    """执行SQL查询并返回结果"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

def compare_sql_results(predicted_sql: str, ground_truth_sql: str, db_path: str) -> int:
    """比较预测SQL和真实SQL的执行结果"""
    try:
        pred_res = execute_sql(predicted_sql, db_path)
        gt_res = execute_sql(ground_truth_sql, db_path)
        return 1 if set(pred_res) == set(gt_res) else 0
    except Exception:
        return 0

def execute_model(predicted_sql: str, ground_truth_sql: str, db_path: str, idx: int, 
                  timeout: float = 30.0) -> Dict[str, Any]:
    """带超时的SQL执行模型"""
    try:
        res = func_timeout(timeout, compare_sql_results, 
                          args=(predicted_sql, ground_truth_sql, db_path))
    except (FunctionTimedOut, KeyboardInterrupt):
        res = 0
    except Exception as e:
        res = 0
    
    return {'sql_idx': idx, 'res': res}

def result_callback(result: Dict[str, Any]) -> None:
    """多进程回调函数"""
    exec_result.append(result)

def run_sqls_parallel(sql_pairs: List[Tuple[str, str]], db_paths: List[str], 
                     num_cpus: int = 1, timeout: float = 30.0) -> None:
    """并行执行SQL查询"""
    pool = mp.Pool(processes=num_cpus)
    for i, (pred_sql, gt_sql) in enumerate(sql_pairs):
        pool.apply_async(execute_model, 
                        args=(pred_sql, gt_sql, db_paths[i], i, timeout),
                        callback=result_callback)
    pool.close()
    pool.join()

def compute_accuracy(exec_results: List[Dict[str, Any]], 
                    difficulty_data: List[Dict[str, Any]]) -> Tuple[List[float], List[int], List[Dict[str, Any]]]:
    """计算不同难度级别的准确率"""
    difficulty_map = {'simple': [], 'moderate': [], 'challenging': []}
    
    for i, res in enumerate(exec_results):
        difficulty = difficulty_data[i]['difficulty']
        difficulty_map[difficulty].append(res['res'])
    
    simple_acc = sum(difficulty_map['simple']) / len(difficulty_map['simple']) * 100 if difficulty_map['simple'] else 0
    moderate_acc = sum(difficulty_map['moderate']) / len(difficulty_map['moderate']) * 100 if difficulty_map['moderate'] else 0
    challenging_acc = sum(difficulty_map['challenging']) / len(difficulty_map['challenging']) * 100 if difficulty_map['challenging'] else 0
    total_acc = sum(res['res'] for res in exec_results) / len(exec_results) * 100
    
    counts = [
        len(difficulty_map['simple']),
        len(difficulty_map['moderate']),
        len(difficulty_map['challenging']),
        len(exec_results)
    ]

    difficulty_idx_map = { 'simple': [], 'moderate': [], 'challenging': [] }
    for i, res in enumerate(exec_results):
        difficulty = difficulty_data[i]['difficulty']
        difficulty_idx_map[difficulty].append({
            'sql_idx': i,
            'is_right': res['res'],
        })
    
    return [simple_acc, moderate_acc, challenging_acc, total_acc], counts, difficulty_idx_map

def print_results(accuracies: List[float], counts: List[int], time_stats: Dict[str, float]) -> None:
    """打印评估结果"""
    levels = ['Simple', 'Moderate', 'Challenging', 'Total']
    print("\n{:<15} {:<15} {:<15} {:<15} {:<15}".format("", *levels))
    print("{:<15} {:<15} {:<15} {:<15} {:<15}".format('Count', *counts))
    
    print('\n' + '='*30 + ' ACCURACY ' + '='*30)
    print("{:<15} {:<15.2f}% {:<15.2f}% {:<15.2f}% {:<15.2f}%".format('Accuracy', *accuracies))
    
    print('\n' + '='*30 + ' TIME STATS ' + '='*30)
    print("Max: {:.2f}s, Min: {:.2f}s, Avg: {:.2f}s, Median: {:.2f}s".format(
        time_stats['max'], time_stats['min'], time_stats['avg'], time_stats['median']))

def get_action_choose_count(action_paths_dict: Dict[str, List[str]]):
    action_path_count = defaultdict(int)
    for action_path in action_paths_dict.values():
        for action in action_path:
            action_path_count[action] += 1

    action_path_count = sorted(action_path_count.items(), key=lambda x: x[1], reverse=True)
    return action_path_count

def get_action_path_count(action_paths_dict: Dict[str, List[str]]):
    action_path_count = defaultdict(int)
    for action_path in action_paths_dict.values():
        action_path_str = "->".join(action_path)
        action_path_count[action_path_str] += 1

    action_path_count = sorted(action_path_count.items(), key=lambda x: (x[1], len(x[0])), reverse=True)
    return action_path_count

def count_for_path(action_paths_dict, gt_data):
    # all data analysis
    all_action_path_count = get_action_path_count(action_paths_dict)
    all_action_choose_count = get_action_choose_count(action_paths_dict)

    # right data analysis
    right_data_action_dict = {}
    for i, res in enumerate(exec_result):
        question_id = gt_data[i]['question_id']
        if res['res'] == 1:
            right_data_action_dict[str(question_id)] = action_paths_dict[str(question_id)]
    
    right_data_action_path_count = get_action_path_count(right_data_action_dict)
    right_data_action_choose_count = get_action_choose_count(right_data_action_dict)

    # right data analysis
    simple_right_data_action_dict = {}
    moderate_right_data_action_dict = {}
    challenging_right_data_action_dict = {}
    for i, res in enumerate(exec_result):
        question_id = gt_data[i]['question_id']
        if res['res'] == 1 and gt_data[i]['difficulty'] == 'simple':
            simple_right_data_action_dict[str(question_id)] = action_paths_dict[str(question_id)]
        elif res['res'] == 1 and gt_data[i]['difficulty'] =='moderate':
            moderate_right_data_action_dict[str(question_id)] = action_paths_dict[str(question_id)]
        elif res['res'] == 1 and gt_data[i]['difficulty'] == 'challenging':
            challenging_right_data_action_dict[str(question_id)] = action_paths_dict[str(question_id)]
    
    simple_right_data_action_path_count = get_action_path_count(simple_right_data_action_dict)
    simple_right_data_action_choose_count = get_action_choose_count(simple_right_data_action_dict)
    moderate_right_data_action_path_count = get_action_path_count(moderate_right_data_action_dict)
    moderate_right_data_action_choose_count = get_action_choose_count(moderate_right_data_action_dict)
    challenging_right_data_action_path_count = get_action_path_count(challenging_right_data_action_dict)
    challenging_right_data_action_choose_count = get_action_choose_count(challenging_right_data_action_dict)

    # 打印结果
    count_dict = {
        '所有结果(正确/错误)': {
            "数据长度": len(gt_data),
            '所有结果最优SQL动作路径': all_action_path_count,
            '所有结果最优SQL动作选择次数': all_action_choose_count,
        },
        '所有正确结果': {
            "数据长度": len(right_data_action_dict),
            '所有正确结果最优SQL动作路径': right_data_action_path_count,
            '所有正确结果最优SQL动作选择次数': right_data_action_choose_count,
        },
       '正确的简单结果': {
            "数据长度": len(simple_right_data_action_dict),
           '正确的简单结果最优SQL动作路径': simple_right_data_action_path_count,
           '正确的简单结果最优SQL动作选择次数': simple_right_data_action_choose_count,
        },
       '正确的中等结果': {
            "数据长度": len(moderate_right_data_action_dict),
           '正确的中等结果最优SQL动作路径': moderate_right_data_action_path_count,
           '正确的中等结果最优SQL动作选择次数': moderate_right_data_action_choose_count,
        },
        '正确的困难结果': {
            "数据长度": len(challenging_right_data_action_dict),
            '正确的困难结果最优SQL动作路径': challenging_right_data_action_path_count,
            '正确的困难结果最优SQL动作选择次数': challenging_right_data_action_choose_count,
        }
    }
    save_json(count_dict, 'count_dict.json')

def get_single_node_data(node: MCTSNode) -> Dict[str, Any]:
    """获取单个节点的完整数据"""
    return {
        "node_id": id(node),
        "node_type": str(node.node_type),
        "depth": node.depth,
        "visit_count": node.N,
        "total_reward": node.Q,
        "parent_action": str(node.parent_action.__class__.__name__) if node.parent_action else None,
        "parent_node_id": id(node.parent_node) if node.parent_node else None,
        "node_content": {
            "rephrased_question": node.rephrased_question,
            "selected_schema": node.selected_schema_context,
            "identified_values": node.identified_column_values,
            "identified_functions": node.identified_column_functions,
            "generated_sql": node.sql_query,
            "revised_sql": node.revised_sql_query,
            "final_sql": node.final_sql_query,
            "is_valid": node.is_valid_sql_query,
            "consistency_score": node.consistency_score
        },
        "node_prompts": {
            "prompts": node.prompt if node.prompt else None,
            "responses": node.response if node.response else None
        }
    }

def save_path_node_info(save_path_node_path, path_node_pkl, action_paths_dict):
    """保存路径节点信息"""
    os.makedirs(save_path_node_path, exist_ok=True)
    for question_id, pkl_list in path_node_pkl.items():
        action_path = action_paths_dict[question_id]
        action_path_str = "->".join(action_path)
        save_path_node_file_path = os.path.join(save_path_node_path, str(question_id) + ".json")
        node_info_list = [get_single_node_data(node) for node in pkl_list]
        save_path_node_dict = {
            "question_id": question_id,
            "action_path": action_path_str,
            "node_info_list": node_info_list
        }
        save_json(save_path_node_dict, save_path_node_file_path)

def evaluate(pred_sql_path: str, gt_data_path: str, db_root_path: str, action_path_json_path: str,
        path_node_pkl_path: str, save_path_node_path: str, num_cpus: int = 1, timeout: float = 30.0) -> None:
    """主评估函数"""
    global exec_result
    exec_result = []
    
    # 加载数据
    gt_data = load_json(gt_data_path)
    pred_sqls = load_json(pred_sql_path)
    
    # 准备SQL对和数据库路径
    sql_pairs = []
    db_paths = []
    time_spans = []
    
    for item in gt_data:
        question_id = str(item['question_id'])
        if question_id in pred_sqls:
            sql_pairs.append((pred_sqls[question_id], item['SQL']))
            db_paths.append(os.path.join(db_root_path, item['db_id'], item['db_id'] + '.sqlite'))
            if 'time_span' in item:
                time_spans.append(item['time_span'])
    
    # 并行执行SQL比较
    run_sqls_parallel(sql_pairs, db_paths, num_cpus, timeout)
    exec_result = sorted(exec_result, key=lambda x: x['sql_idx'])
    
    # 计算准确率和时间统计
    accuracies, counts, difficulty_idx_map = compute_accuracy(exec_result, gt_data)
    
    time_stats = {
        'max': max(time_spans) if time_spans else 0,
        'min': min(time_spans) if time_spans else 0,
        'avg': sum(time_spans)/len(time_spans) if time_spans else 0,
        'median': statistics.median(time_spans) if time_spans else 0
    }
    
    # 打印结果
    print_results(accuracies, counts, time_stats)
    
    # 保存详细结果
    detailed_results = {
        'accuracy': accuracies,
        'counts': counts,
        'time_stats': time_stats,
        'individual_results': exec_result
    }
    save_json(detailed_results, 'evaluation_results.json')
    action_paths_dict = load_json(action_path_json_path)
    count_for_path(action_paths_dict, gt_data)
    
    path_node_pkl = load_pickle(path_node_pkl_path)
    save_path_node_info(save_path_node_path, path_node_pkl, action_paths_dict)






    
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SQL Evaluation Script')
    pred_sql_path = "/home/weiji/Git-projects/Alpha-SQL/pred_sqls.json"
    gt_data_path = "/home/weiji/Git-projects/Alpha-SQL/data/bird/dev/dev_100.json"
    db_root_path = "/home/weiji/Git-projects/Alpha-SQL/data/bird/dev/dev_databases"
    action_path_json_path = "/home/weiji/Git-projects/Alpha-SQL/action_paths.json"
    path_node_pkl_path = "/home/weiji/Git-projects/Alpha-SQL/paths_node.pkl"
    save_path_node_path = "/home/weiji/Git-projects/Alpha-SQL/path_node_info"
    parser.add_argument('--save_path_node_path', type=str, default=save_path_node_path, required=False,
                       help='Path to save path node info')
    parser.add_argument('--action_path_json_path', type=str, default=action_path_json_path, required=False,
                       help='Path to action path JSON file')
    parser.add_argument('--path_node_pkl_path', type=str, default=path_node_pkl_path, required=False,
                        help='Path to path node pickle file')
    parser.add_argument('--pred_sql_path', type=str, default=pred_sql_path, required=False,
                       help='Path to predicted SQL JSON file')
    parser.add_argument('--gt_data_path', type=str, default=gt_data_path, required=False,
                       help='Path to ground truth JSON file')
    parser.add_argument('--db_root_path', type=str, default=db_root_path, required=False, 
                       help='Root path to database directories')
    parser.add_argument('--num_cpus', type=int, default=1, 
                       help='Number of CPUs for parallel processing')
    parser.add_argument('--timeout', type=float, default=30.0, 
                       help='Timeout for SQL execution in seconds')
    
    args = parser.parse_args()    
    evaluate(
        pred_sql_path=args.pred_sql_path,
        gt_data_path=args.gt_data_path,
        db_root_path=args.db_root_path,
        action_path_json_path=args.action_path_json_path,
        path_node_pkl_path=args.path_node_pkl_path,
        save_path_node_path=args.save_path_node_path,
        num_cpus=args.num_cpus,
        timeout=args.timeout
    )