from typing import List
from alphasql.database.sql_execution import cached_execute_sql_with_timeout, is_valid_execution_result
from alphasql.algorithm.selection.utils import measure_sql_execution_time
import pickle
import glob
import json
import argparse
from tqdm import tqdm
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import sqlparse
from alphasql.algorithm.mcts.mcts_action import *

EXECUTION_TIME_REPEAT = 20

def select_final_sql_query(results_file_path: str, db_root_dir: str):
    question_id = int(results_file_path.split("/")[-1].split(".")[0])
    with open(results_file_path, "rb") as f:
        results = pickle.load(f)
    db_id = results[0][0].db_id
    db_path = f"{db_root_dir}/{db_id}/{db_id}.sqlite"
    result_groups = defaultdict(list)
    result_groups_with_invalid_result = defaultdict(list)
    beautified_sql_file = f"{results_file_path}.beautified.sql"
    beautify_list = []
    for idx, result in tqdm(enumerate(results), desc=f"Processing results for {question_id}"):
        sql_query = result[-1].final_sql_query
        beautified_sql = sqlparse.format(sql_query, reindent=True, keyword_case='upper')
        beautify_list.append(beautified_sql)
        answer = cached_execute_sql_with_timeout(db_path, sql_query)
        if answer.result_type.value == "success":
            if is_valid_execution_result(answer):
                result_groups[frozenset(answer.result)].append(idx)
            result_groups_with_invalid_result[frozenset(answer.result)].append(idx)
    with open(beautified_sql_file, "w") as f:
        f.write("\n\n".join(beautify_list))
        
    if len(result_groups) == 0:
        final_selected_sql_query = "ERROR"
        
        if len(result_groups_with_invalid_result) > 0:
            path_idx_with_sc_score = []
            for answer, path_indices in result_groups_with_invalid_result.items():
                sc_score = len(path_indices) / sum([len(v) for v in result_groups_with_invalid_result.values()])
                execution_time = measure_sql_execution_time(db_path, results[path_indices[0]][-1].final_sql_query, repeat=EXECUTION_TIME_REPEAT)
                # for path_idx in path_indices:
                #     consistency_score[path_idx] = (sc_score, execution_time)
                path_idx_with_sc_score.append((path_indices[0], sc_score, execution_time))
            path_idx_with_sc_score.sort(key=lambda x: (x[1], -x[2]), reverse=True)
            # Group paths by their scores
            # score_to_paths = {}
            # for path_idx, score in consistency_score.items():
            #     if score not in score_to_paths:
            #         score_to_paths[score] = []
            #     score_to_paths[score].append(path_idx)
            
            # Sort scores in descending order
            # sorted_scores = sorted(score_to_paths.keys(), reverse=True)
            path_idx = path_idx_with_sc_score[0][0]
            final_selected_sql_query = results[path_idx][-1].final_sql_query
            action_paths = path_to_action_paths(results[path_idx])

        return {
            "question_id": question_id,
            "sql": final_selected_sql_query,
            "action_paths": action_paths,
            "paths_node": results[path_idx]
        }

    # consistency_score = {}
    # for answer, path_indices in result_groups.items():
    #     for path_idx in path_indices:
    #         consistency_score[path_idx] = len(path_indices) / sum([len(v) for v in result_groups.values()])
    # Group paths by their scores
    # score_to_paths = {}
    # for path_idx, score in consistency_score.items():
    #     if score not in score_to_paths:
    #         score_to_paths[score] = []
    #     score_to_paths[score].append(path_idx)
    
    # sorted_scores = sorted(score_to_paths.keys(), reverse=True)
    # path_idx = score_to_paths[sorted_scores[0]][0]
    # final_selected_sql_query = results[path_idx][-1].final_sql_query

    path_idx_with_sc_score = []
    for answer, path_indices in result_groups.items():
        sc_score = len(path_indices) / sum([len(v) for v in result_groups.values()])
        execution_time = measure_sql_execution_time(db_path, results[path_indices[0]][-1].final_sql_query, repeat=EXECUTION_TIME_REPEAT)
        path_idx_with_sc_score.append((path_indices[0], sc_score, execution_time))
    path_idx_with_sc_score.sort(key=lambda x: (x[1], -x[2]), reverse=True)
    path_idx = path_idx_with_sc_score[0][0]
    final_selected_sql_query = results[path_idx][-1].final_sql_query
    action_paths = path_to_action_paths(results[path_idx])
    return {
        "question_id": question_id,
        "sql": final_selected_sql_query,
        "action_paths": action_paths,
        "paths_node": results[path_idx]
    }

def path_to_action_paths(path_nodes: List["MCTSNode"]):
    action_paths: List[str] = []

    for path_node in path_nodes:
        if isinstance(path_node.parent_action, RaphraseQuestionAction):
            action_paths.append("A1")
        if isinstance(path_node.parent_action, IdentifyColumnValuesAction):
            action_paths.append("A2")
        if isinstance(path_node.parent_action, IdentifyColumnFunctionsAction):
            action_paths.append("A3")
        if isinstance(path_node.parent_action, SchemaSelectionAction):
            action_paths.append("A4")
        if isinstance(path_node.parent_action, SQLGenerationAction):
            action_paths.append("A5")
        if isinstance(path_node.parent_action, SQLRevisionAction):
            action_paths.append("A6")
        if isinstance(path_node.parent_action, EndAction):
            action_paths.append("A7")

    return action_paths

def main(args):
    final_pred_sqls = {}
    action_paths_dict = {}
    paths_node_dict = {}

    with ProcessPoolExecutor(max_workers=args.process_num) as executor:
        result_paths = glob.glob(args.results_dir + "/*.pkl")
        future_to_path = {executor.submit(select_final_sql_query, path, args.db_root_dir): path for path in result_paths}
        
        for future in tqdm(as_completed(future_to_path), total=len(future_to_path), desc="Processing results"):
            selected_item = future.result()
            final_pred_sqls[str(selected_item["question_id"])] = selected_item["sql"]
            action_paths_dict[str(selected_item["question_id"])] = selected_item["action_paths"]
            paths_node_dict[str(selected_item["question_id"])] = selected_item["paths_node"]

    with open(args.output_path, "w", encoding='utf-8') as f:
        json.dump(final_pred_sqls, f, indent=4, ensure_ascii=False)

    # 在args.output_path的同级目录下生成action_paths.json文件记录所有sql的action路径
    output_dir = "/".join(args.output_path.split("/")[:-1])
    action_paths_file_path = f"{output_dir}/action_paths.json"
    with open(action_paths_file_path, "w", encoding='utf-8') as f:
        json.dump(action_paths_dict, f, indent=4, ensure_ascii=False)
    
    # 在args.output_path的同级目录下生成paths_node.json文件记录所有sql的路径节点
    paths_node_file_path = f"{output_dir}/paths_node.pkl"
    with open(paths_node_file_path, "wb") as f:
        pickle.dump(paths_node_dict, f)

    # 统计每个路径的选择次数
    action_path_count = defaultdict(int)
    for action_path in action_paths_dict.values():
        for action in action_path:
            action_path_count[action] += 1
    print(action_path_count)

    # 统计每条路径的出现频率
    action_path_count = defaultdict(int)
    for action_path in action_paths_dict.values():
        action_path_str = "->".join(action_path)
        action_path_count[action_path_str] += 1
    print(action_path_count)
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--results_dir", type=str, required=True)
    parser.add_argument("--db_root_dir", type=str, required=True)
    parser.add_argument("--process_num", type=int, default=32)
    parser.add_argument("--output_path", type=str, required=True)
    args = parser.parse_args()
    main(args)
