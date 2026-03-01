#!/usr/bin/env python3
"""
Alpha-SQL 批量问题预测脚本

- 一次处理所有问题到一个临时JSON文件
- 并发运行12个预测
- 生成 analysis_comparison.json 格式的分析文件
"""

import os
import sys
import json
import pickle
import tempfile
import concurrent.futures
from pathlib import Path
from dotenv import load_dotenv
from copy import deepcopy
from typing import List, Dict
from tqdm import tqdm

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# 加载环境变量
load_dotenv(os.path.join(current_dir, '.env'), override=True)


# ============================================================
# 配置
# ============================================================
TEST_QUESTION_IDS = list(range(1, 101))  # 运行问题1-100
MAX_CONCURRENT = 12  # 并发数
# ============================================================


def evaluate_sql_准确(predicted_sql: str, ground_truth_sql: str, db_config: dict = None) -> Dict:
    """评估SQL是否正确"""
    import clickhouse_connect

    if db_config is None:
        db_config = {
            "host": "192.168.81.84",
            "port": 8123,
            "database": "compass_ai",
            "username": "default",
            "password": "123456"
        }

    client = clickhouse_connect.get_client(
        host=db_config["host"],
        port=db_config["port"],
        database=db_config["database"],
        username=db_config["username"],
        password=db_config["password"],
        settings={"union_default_mode": "DISTINCT"}
    )

    result = {
        "is_correct": False,
        "predicted_rows": None,
        "ground_truth_rows": None,
        "predicted_error": None,
        "ground_truth_error": None,
        "error_type": None
    }

    pred_set = None
    gt_set = None

    try:
        pred_res = client.query(predicted_sql)
        result["predicted_rows"] = len(pred_res.result_rows)
        pred_set = set(tuple(row) for row in pred_res.result_rows)
    except Exception as e:
        result["predicted_error"] = str(e)[:500]
        result["error_type"] = "predicted_error"

    try:
        gt_res = client.query(ground_truth_sql)
        result["ground_truth_rows"] = len(gt_res.result_rows)
        gt_set = set(tuple(row) for row in gt_res.result_rows)
    except Exception as e:
        result["ground_truth_error"] = str(e)[:500]
        if result["error_type"] is None:
            result["error_type"] = "ground_truth_error"

    if pred_set is not None and gt_set is not None:
        if pred_set == gt_set:
            result["is_correct"] = True
            result["error_type"] = None
        else:
            result["error_type"] = "result_mismatch"

    client.close()
    return result


class InMemoryPreprocessor:
    """内存化预处理器，不生成任何缓存文件"""

    def __init__(self, data_file_path: str):
        self.data_file_path = data_file_path
        self.data = json.load(open(data_file_path, "r", encoding="utf-8"))

        self.preprocess_api_key = os.getenv('PREPROCESS_API_KEY', os.getenv('OPENAI_API_KEY', ''))
        self.preprocess_base_url = os.getenv('PREPROCESS_BASE_URL', os.getenv('OPENAI_BASE_URL', 'http://172.17.160.41:8080/v1'))

        from alphasql.runner.task import Task
        self.tasks = [
            Task(
                question_id=data_item.get("question_id", question_id),
                db_id=data_item["db_id"],
                question=data_item["question"],
                evidence=data_item["evidence"],
                sql=data_item.get("SQL", None),
                difficulty=data_item.get("difficulty", None)
            ) for question_id, data_item in enumerate(self.data)
        ]

    def preprocess(self) -> List:
        """运行完整的预处理流程"""
        print("开始预处理...")
        print("  [1/2] 获取相关值...")
        relevant_values_list = self.get_relevant_values_for_all_tasks()

        print("  [2/2] 生成 schema context...")
        tasks_with_context = self.preprocess_schema_context_for_all_tasks(relevant_values_list)

        print("预处理完成!")
        return tasks_with_context

    def get_relevant_values_for_all_tasks(self):
        results = []
        for i, task in enumerate(tqdm(self.tasks, desc="处理任务")):
            result = self.get_relevant_values_for_task(task)
            results.append(result)
        return results

    def get_relevant_values_for_task(self, task) -> Dict:
        from alphasql.database.database_manager import DatabaseManager
        from alphasql.database.lsh_index import LSHIndex
        from alphasql.runner.preprocessor import call_openai, get_prompt, MODEL_NAME, TEMPERATURE, COST_RECORDER

        keywords = self.get_keywords_for_task(task)
        db_schema = DatabaseManager.get_database_schema(task.db_id, "")

        lsh_candidate_values = []
        for keyword in keywords:
            results = LSHIndex.query_lsh_index(
                db_schema,
                keyword,
                top_k=20,
                signature_size=64,
                n_gram=3
            )
            lsh_candidate_values.extend(results)

        from collections import defaultdict
        candidate_dict = defaultdict(set)
        for item in lsh_candidate_values:
            table_name = item.get("table_name")
            column_name = item.get("column_name")
            value = item.get("value")
            if table_name and column_name and value:
                candidate_dict[(table_name, column_name)].add(value)

        from alphasql.database.clickhouse_db import load_value_examples
        final_candidate_values = {}
        for (table_name, column_name), values in tqdm(candidate_dict.items(), desc="获取值示例"):
            try:
                value_examples = load_value_examples(table_name, column_name, max_num_examples=10)
                if value_examples:
                    final_candidate_values[(table_name, column_name)] = value_examples
            except Exception as e:
                print(f"获取 {table_name}.{column_name} 值示例失败: {e}")
                continue

        return final_candidate_values

    def get_keywords_for_task(self, task) -> List[str]:
        import re
        from alphasql.runner.preprocessor import call_openai, get_prompt, MODEL_NAME, TEMPERATURE, COST_RECORDER

        max_retries = 10
        retry_count = 0

        while retry_count < max_retries:
            try:
                raw_keywords_str = call_openai(
                    get_prompt("keywords_extraction", {"QUESTION": task.question, "HINT": task.evidence}),
                    MODEL_NAME,
                    TEMPERATURE,
                    base_url=self.preprocess_base_url,
                    api_key=self.preprocess_api_key,
                    cost_recorder=COST_RECORDER
                )[0]

                pattern = r"```python\s*\[(.*?)\]\s*```"
                match = re.search(pattern, raw_keywords_str, re.DOTALL)

                if match:
                    raw_keywords_str = f"[{match.group(1)}]"
                    raw_keywords = eval(raw_keywords_str)
                    keywords = []
                    for keyword in raw_keywords:
                        keyword: str
                        keywords.append(keyword.strip())
                        keywords.append(keyword.replace("/", "-").strip('"').strip("'"))
                        keywords.extend(keyword.replace("=", " ").replace("(", " ").replace(")", " ").replace("_", " ").split(" "))
                        keywords.extend(keyword.strip("'").replace("=", " ").replace("(", " ").replace(")", " ").replace("_", " ").split(" "))
                        keywords.extend(keyword.strip('"').replace("=", " ").replace("(", " ").replace(")", " ").replace("_", " ").split(" "))
                    keywords = list(set(keyword.strip() for keyword in keywords))
                    return keywords
                else:
                    print(f"警告: 无法从响应中提取关键词列表，重试 {retry_count + 1}/{max_retries}")
                    retry_count += 1
                    continue

            except Exception as e:
                print(f"获取关键词出错: {e}, 重试 {retry_count + 1}/{max_retries}")
                retry_count += 1
                if retry_count >= max_retries:
                    raise e

        return []

    def preprocess_schema_context_for_all_tasks(self, relevant_values_list: List[Dict]) -> List:
        from alphasql.database.database_manager import DatabaseManager
        from alphasql.database.utils import build_table_ddl_statement

        tasks_with_schema_context = []
        for task, relevant_values_one_task in tqdm(zip(self.tasks, relevant_values_list), desc="生成 schema context"):
            task = deepcopy(task)
            database_schema = DatabaseManager.get_database_schema(task.db_id, "")
            table_schema_dict = deepcopy(database_schema.tables)

            for table_name, column_name in relevant_values_one_task.keys():
                if table_name in table_schema_dict and column_name in table_schema_dict[table_name].columns:
                    table_schema_dict[table_name].columns[column_name].value_examples = relevant_values_one_task[(table_name, column_name)][:3]

            schema_context = "\n".join([
                build_table_ddl_statement(
                    table_schema_dict[table_name].to_dict(),
                    add_value_description=True,
                    add_column_description=True,
                    add_value_examples=True,
                    add_expanded_column_name=True
                ) for table_name in table_schema_dict
            ])
            task.schema_context = schema_context
            task.table_schema_dict = table_schema_dict
            tasks_with_schema_context.append(task)

        return tasks_with_schema_context


def create_all_questions_json(question_ids: List[int]) -> str:
    """从dev1.json中提取所有问题，生成一个临时json文件"""
    source_file = os.path.join(current_dir, "data/stock/dev/dev1.json")

    with open(source_file, 'r', encoding='utf-8') as f:
        all_data = json.load(f)

    # 找到对应的问题
    selected_data = []
    for item in all_data:
        if item.get('question_id') in question_ids:
            selected_data.append(item)

    if not selected_data:
        raise ValueError(f"找不到question_id={question_ids}的问题")

    # 保存到一个临时文件
    temp_fd, temp_file = tempfile.mkstemp(suffix='.json', prefix='alphasql_all_', dir=current_dir)
    os.close(temp_fd)

    with open(temp_file, 'w', encoding='utf-8') as f:
        json.dump(selected_data, f, ensure_ascii=False, indent=2)

    print(f"已生成临时文件: {temp_file}, 包含 {len(selected_data)} 个问题")
    return temp_file


def run_single_prediction(args):
    """运行单个问题的预测"""
    question_id, tasks, config, save_dir = args

    from alphasql.runner.mcts_runner import MCTSRunner
    from alphasql.runner.sql_selection import select_final_sql_query

    # 找到对应的问题
    task = None
    for t in tasks:
        if t.question_id == question_id or (hasattr(t, 'original_question') and t.original_question):
            task = t
            break

    if task is None:
        return {
            "success": False,
            "question_id": question_id,
            "error": f"找不到question_id={question_id}的预处理数据",
            "question": None
        }

    print(f"[{question_id}] 开始MCTS预测...")

    try:
        runner = MCTSRunner(config)
        runner.run_one_task(task)

        result_file = Path(save_dir) / f"{question_id}.pkl"
        if not result_file.exists():
            result_file = Path(save_dir) / "0.pkl"

        if not result_file.exists():
            return {
                "success": False,
                "question_id": question_id,
                "error": f"结果文件不存在: {result_file}",
                "question": task.question if hasattr(task, 'question') else None
            }

        with open(result_file, 'rb') as f:
            results = pickle.load(f)

        final_result = select_final_sql_query(str(result_file), config.db_root_dir)
        predicted_sql = final_result.get('sql', '')

        evaluation = evaluate_sql_准确(
            predicted_sql=predicted_sql,
            ground_truth_sql=task.sql
        )

        print(f"[{question_id}] 预测完成: {'✓' if evaluation['is_correct'] else '✗'}")

        return {
            "success": True,
            "question_id": question_id,
            "question": task.question,
            "predicted_sql": predicted_sql,
            "ground_truth_sql": task.sql,
            "evaluation": evaluation
        }
    except Exception as e:
        print(f"[{question_id}] 预测失败: {e}")
        return {
            "success": False,
            "question_id": question_id,
            "error": str(e),
            "question": task.question if hasattr(task, 'question') else None
        }


def main():
    import yaml
    from alphasql.config.mcts_config import MCTSConfig

    question_ids = TEST_QUESTION_IDS

    if not isinstance(question_ids, list):
        question_ids = [question_ids]

    print(f"开始批量处理问题ID: {question_ids}")
    print(f"共 {len(question_ids)} 个问题，并发数: {MAX_CONCURRENT}")
    print("=" * 60)

    # 加载配置
    config_path = os.path.join(current_dir, 'config/clickhouse_stock.yaml')
    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)

    config_dict['save_root_dir'] = "results/batch_run"
    config_dict['mcts_model_kwargs']['n'] = 1
    config_dict['show_process_view'] = False
    config_dict['show_total_time_statistics'] = False

    config = MCTSConfig.model_validate(config_dict)

    # 创建临时文件（包含所有问题）
    temp_json = create_all_questions_json(question_ids)

    # 预处理
    preprocessor = InMemoryPreprocessor(temp_json)
    tasks = preprocessor.preprocess()

    # 并发运行预测
    print(f"\n开始并发预测 ({MAX_CONCURRENT}个并发)...")
    save_dir = "results/batch_run"

    # 准备参数
    args_list = [(qid, tasks, config, save_dir) for qid in question_ids]

    detailed_results = []
    correct_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        futures = {executor.submit(run_single_prediction, args): args[0] for args in args_list}

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="预测进度"):
            result = future.result()

            if result["success"]:
                eval_result = result["evaluation"]
                detailed_results.append({
                    "question_id": result["question_id"],
                    "question": result.get("question", ""),
                    "predicted_sql": result.get("predicted_sql", ""),
                    "ground_truth_sql": result.get("ground_truth_sql", ""),
                    "is_correct": eval_result.get("is_correct", False),
                    "predicted_rows": eval_result.get("predicted_rows"),
                    "ground_truth_rows": eval_result.get("ground_truth_rows"),
                    "predicted_error": eval_result.get("predicted_error"),
                    "ground_truth_error": eval_result.get("ground_truth_error")
                })

                if eval_result.get('is_correct'):
                    correct_count += 1
            else:
                print(f"  问题{result['question_id']}失败: {result.get('error', '未知错误')}")
                detailed_results.append({
                    "question_id": result["question_id"],
                    "question": result.get("question", ""),
                    "error": result.get('error', '未知错误')
                })

    # 清理临时文件
    if os.path.exists(temp_json):
        os.remove(temp_json)

    # 汇总结果
    total_count = len(question_ids)
    print(f"\n{'=' * 60}")
    print(f"汇总结果")
    print(f"{'=' * 60}")
    print(f"总数: {total_count}")
    print(f"正确: {correct_count}")
    print(f"准确率: {correct_count/total_count*100:.1f}%")

    # 生成 analysis_comparison.json
    analysis_comparison = {}
    for result in detailed_results:
        if "question_id" in result and "question" in result:
            qid = str(result["question_id"])
            analysis_comparison[qid] = {
                "question": result.get("question", ""),
                "ground_truth": result.get("ground_truth_sql", ""),
                "prediction": result.get("predicted_sql", ""),
                "evidence": "",
                "is_correct": result.get("is_correct", False),
                "predicted_rows": result.get("predicted_rows"),
                "ground_truth_rows": result.get("ground_truth_rows"),
                "predicted_error": result.get("predicted_error"),
                "ground_truth_error": result.get("ground_truth_error")
            }

    # 从dev1.json获取evidence
    try:
        dev_file = os.path.join(current_dir, "data/stock/dev/dev1.json")
        with open(dev_file, 'r', encoding='utf-8') as f:
            dev_data = json.load(f)
        for item in dev_data:
            qid = str(item.get("question_id"))
            if qid in analysis_comparison:
                analysis_comparison[qid]["evidence"] = item.get("evidence", "")
    except Exception as e:
        print(f"警告: 无法加载evidence: {e}")

    # 保存 analysis_comparison.json
    comparison_file = os.path.join(current_dir, "analysis_comparison.json")
    with open(comparison_file, 'w', encoding='utf-8') as f:
        json.dump(analysis_comparison, f, ensure_ascii=False, indent=2)
    print(f"\n分析对比结果已保存到: {comparison_file}")

    return 0 if correct_count == total_count else 1


if __name__ == "__main__":
    sys.exit(main())
