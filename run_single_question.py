#!/usr/bin/env python3
"""
Alpha-SQL 单问题预测脚本

直接在 SAMPLE_QUESTION 中填写问题
使用内存化预处理器，不生成任何缓存文件
"""

import os
import sys
import json
import pickle
import shutil
import tempfile
from pathlib import Path
import clickhouse_connect
from dotenv import load_dotenv
from copy import deepcopy
from typing import List, Dict, Tuple, Optional
from tqdm import tqdm

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# 加载环境变量
load_dotenv(os.path.join(current_dir, '.env'), override=True)


# ============================================================
# 在这里直接填写要测试的问题ID，可以是单个或多个
# ============================================================
TEST_QUESTION_IDS = [25]  # 例如: [1, 2, 3] 或 [9] 或 list(range(1, 11))
# ============================================================


class InMemoryPreprocessor:
    """
    内存化预处理器，不生成任何缓存文件
    """

    def __init__(self, data_file_path: str):
        self.data_file_path = data_file_path
        self.data = json.load(open(data_file_path, "r", encoding="utf-8"))

        # 从环境变量读取API配置
        self.preprocess_api_key = os.getenv('PREPROCESS_API_KEY', os.getenv('OPENAI_API_KEY', ''))
        self.preprocess_base_url = os.getenv('PREPROCESS_BASE_URL', os.getenv('OPENAI_BASE_URL', 'http://172.17.160.41:8080/v1'))

        # 创建Task对象
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
        """运行完整的预处理流程，返回处理后的Task列表"""
        print("开始预处理...")

        # 1. 为所有任务生成 keywords 和 relevant values
        print("  [1/2] 获取相关值...")
        relevant_values_list = self.get_relevant_values_for_all_tasks()

        # 2. 生成 schema context
        print("  [2/2] 生成 schema context...")
        tasks_with_context = self.preprocess_schema_context_for_all_tasks(relevant_values_list)

        print("预处理完成!")
        return tasks_with_context

    def get_relevant_values_for_all_tasks(self):
        """为所有任务获取相关值（内存版本）"""
        results = []
        for i, task in enumerate(tqdm(self.tasks, desc="处理任务")):
            result = self.get_relevant_values_for_task(task)
            results.append(result)
        return results

    def get_relevant_values_for_task(self, task) -> Dict[Tuple[str, str], List[str]]:
        """为单个任务获取相关值"""
        from alphasql.database.database_manager import DatabaseManager
        from alphasql.database.lsh_index import LSHIndex
        from alphasql.runner.preprocessor import call_openai, get_prompt, MODEL_NAME, TEMPERATURE, COST_RECORDER

        # 获取关键词
        keywords = self.get_keywords_for_task(task)

        # 获取数据库schema
        db_schema = DatabaseManager.get_database_schema(task.db_id, "")

        # 查询 LSH 索引（每个关键词分别查询）
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

        # 合并结果
        from collections import defaultdict
        candidate_dict = defaultdict(set)
        for item in lsh_candidate_values:
            table_name = item.get("table_name")
            column_name = item.get("column_name")
            value = item.get("value")
            if table_name and column_name and value:
                candidate_dict[(table_name, column_name)].add(value)

        # 获取值示例
        from alphasql.database.clickhouse_db import load_value_examples
        final_candidate_values = {}
        for (table_name, column_name), values in tqdm(candidate_dict.items(), desc="获取值示例"):
            # 对于每个 (table, column)，获取数据库中实际存在的值
            all_values = list(values)
            # 查询数据库获取匹配的示例
            try:
                value_examples = load_value_examples(table_name, column_name, max_num_examples=10)
                if value_examples:
                    final_candidate_values[(table_name, column_name)] = value_examples
            except Exception as e:
                print(f"获取 {table_name}.{column_name} 值示例失败: {e}")
                continue

        return final_candidate_values

    def get_keywords_for_task(self, task) -> List[str]:
        """获取任务的关键词"""
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

                # 使用正则提取 ```python [...] ``` 中的内容
                pattern = r"```python\s*\[(.*?)\]\s*```"
                match = re.search(pattern, raw_keywords_str, re.DOTALL)

                if match:
                    raw_keywords_str = f"[{match.group(1)}]"
                    raw_keywords = eval(raw_keywords_str)
                    keywords = []
                    for keyword in raw_keywords:
                        keyword: str
                        keywords.append(keyword.strip())
                        # 处理日期格式
                        keywords.append(keyword.replace("/", "-").strip("\"").strip("'"))
                        # 分割关键词
                        keywords.extend(keyword.replace("=", " ").replace("(", " ").replace(")", " ").replace("_", " ").split(" "))
                        keywords.extend(keyword.strip("'").replace("=", " ").replace("(", " ").replace(")", " ").replace("_", " ").split(" "))
                        keywords.extend(keyword.strip("\"").replace("=", " ").replace("(", " ").replace(")", " ").replace("_", " ").split(" "))
                    # 去重
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
        """生成 schema context（内存版本）"""
        from alphasql.database.database_manager import DatabaseManager
        from alphasql.database.utils import build_table_ddl_statement

        tasks_with_schema_context = []
        for task, relevant_values_one_task in tqdm(zip(self.tasks, relevant_values_list), desc="生成 schema context"):
            task = deepcopy(task)
            relevant_values_one_task: Dict[Tuple[str, str], List[str]]
            database_schema = DatabaseManager.get_database_schema(task.db_id, "")
            table_schema_dict = deepcopy(database_schema.tables)

            # 添加相关的值示例
            for table_name, column_name in relevant_values_one_task.keys():
                if table_name in table_schema_dict and column_name in table_schema_dict[table_name].columns:
                    table_schema_dict[table_name].columns[column_name].value_examples = relevant_values_one_task[(table_name, column_name)][:3]

            # 构建 schema context
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


def create_single_question_json(question_id):
    """从dev1.json中提取单个问题，生成临时json文件"""
    source_file = os.path.join(current_dir, "data/stock/dev/dev1.json")

    with open(source_file, 'r', encoding='utf-8') as f:
        all_data = json.load(f)

    # 找到对应的问题
    single_data = None
    for item in all_data:
        if item.get('question_id') == question_id:
            single_data = [item]
            break

    if single_data is None:
        raise ValueError(f"找不到question_id={question_id}的问题")

    # 保存到临时文件
    temp_fd, temp_file = tempfile.mkstemp(suffix='.json', prefix='alphasql_q', dir=current_dir)
    os.close(temp_fd)

    with open(temp_file, 'w', encoding='utf-8') as f:
        json.dump(single_data, f, ensure_ascii=False, indent=2)

    print(f"已生成临时文件: {temp_file}")
    return temp_file


def run_prediction(question_id, save_dir="results/single_run"):
    """运行预测流程"""
    from alphasql.runner.mcts_runner import MCTSRunner
    from alphasql.config.mcts_config import MCTSConfig
    from alphasql.runner.sql_selection import select_final_sql_query
    import yaml

    print(f"=" * 60)
    print(f"加载预处理数据: question_id={question_id}")
    print(f"=" * 60)

    # 1. 使用内存化预处理器
    temp_json = create_single_question_json(question_id)
    preprocessor = InMemoryPreprocessor(temp_json)
    tasks = preprocessor.preprocess()

    # 找到对应的问题
    task = None
    for t in tasks:
        if t.question_id == question_id or t.question_id == 0:
            task = t
            break

    if task is None:
        return {
            "success": False,
            "error": f"找不到question_id={question_id}的预处理数据",
            "question": None
        }

    print(f"[1/3] Task加载完成: {task.question_id}")
    print(f"     问题: {task.question[:30]}...")

    # 2. 运行MCTS预测
    config_path = os.path.join(current_dir, 'config/clickhouse_stock.yaml')
    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)

    config_dict['save_root_dir'] = save_dir
    config_dict['mcts_model_kwargs']['n'] = 1
    config = MCTSConfig.model_validate(config_dict)
    runner = MCTSRunner(config)

    print(f"[2/3] 开始MCTS预测...")
    runner.run_one_task(task)
    print(f"       预测完成")

    # 3. 获取结果
    print(f"[3/3] 获取预测结果...")

    result_file = Path(save_dir) / f"{task.question_id}.pkl"
    if not result_file.exists():
        # 尝试用 0 作为 question_id
        result_file = Path(save_dir) / "0.pkl"

    if not result_file.exists():
        return {
            "success": False,
            "error": f"结果文件不存在: {result_file}",
            "question": task.question
        }

    with open(result_file, 'rb') as f:
        results = pickle.load(f)

    final_result = select_final_sql_query(str(result_file), config.db_root_dir)
    predicted_sql = final_result.get('sql', '')

    evaluation = evaluate_sql(
        predicted_sql=predicted_sql,
        ground_truth_sql=task.sql
    )

    # 清理临时json文件
    if os.path.exists(temp_json):
        try:
            os.remove(temp_json)
            print(f"[清理] 已删除临时文件: {temp_json}")
        except Exception as e:
            print(f"[警告] 无法删除临时文件: {e}")

    return {
        "success": True,
        "question_id": task.question_id,
        "question": task.question,
        "predicted_sql": predicted_sql,
        "ground_truth_sql": task.sql,
        "evaluation": evaluation
    }


def evaluate_sql(predicted_sql, ground_truth_sql):
    """评估SQL是否正确"""
    client = clickhouse_connect.get_client(
        host="192.168.81.84",
        port=8123,
        database="compass_ai",
        username="default",
        password="123456",
        settings={"union_default_mode": "DISTINCT"}
    )

    result = {
        "is_correct": False,
        "predicted_rows": None,
        "ground_truth_rows": None,
        "predicted_error": None,
        "ground_truth_error": None
    }

    try:
        pred_res = client.query(predicted_sql)
        result["predicted_rows"] = len(pred_res.result_rows)
        pred_set = set(tuple(row) for row in pred_res.result_rows)
    except Exception as e:
        result["predicted_error"] = str(e)[:200]

    try:
        gt_res = client.query(ground_truth_sql)
        result["ground_truth_rows"] = len(gt_res.result_rows)
        gt_set = set(tuple(row) for row in gt_res.result_rows)
    except Exception as e:
        result["ground_truth_error"] = str(e)[:200]

    if result["predicted_rows"] is not None and result["ground_truth_rows"] is not None:
        if pred_set == gt_set:
            result["is_correct"] = True

    client.close()
    return result


def main():
    question_ids = TEST_QUESTION_IDS

    # 支持单个ID或多个ID
    if not isinstance(question_ids, list):
        question_ids = [question_ids]

    print(f"开始处理问题ID: {question_ids}")
    print(f"共 {len(question_ids)} 个问题")
    print("=" * 60)

    all_results = []
    correct_count = 0
    total_count = len(question_ids)

    # 保存详细结果到文件
    detailed_results = []

    for i, question_id in enumerate(question_ids):
        print(f"\n[{i+1}/{total_count}] 处理问题ID: {question_id}")
        print("-" * 40)

        result = run_prediction(question_id, save_dir="results/single_run")

        if result["success"]:
            eval_result = result["evaluation"]
            print(f"  问题: {result['question'][:50]}...")
            print(f"  是否正确: {'✓ 是' if eval_result['is_correct'] else '✗ 否'}")
            print(f"  预测行数: {eval_result.get('predicted_rows', 'N/A')}, 实际行数: {eval_result.get('ground_truth_rows', 'N/A')}")
            if eval_result['predicted_error']:
                print(f"  预测错误: {eval_result['predicted_error'][:80]}")

            # 保存详细信息
            detailed_results.append({
                "question_id": question_id,
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
            print(f"  预测失败: {result.get('error', '未知错误')}")
            detailed_results.append({
                "question_id": question_id,
                "question": result.get("question", ""),
                "error": result.get('error', '未知错误')
            })

        all_results.append(result)

        # 清理临时json文件
        temp_json_files = [f for f in os.listdir(current_dir) if f.startswith('alphasql_q') and f.endswith('.json')]
        for f in temp_json_files:
            try:
                os.remove(os.path.join(current_dir, f))
            except Exception as e:
                pass

    # 汇总结果
    print(f"\n{'=' * 60}")
    print(f"汇总结果")
    print(f"{'=' * 60}")
    print(f"总数: {total_count}")
    print(f"正确: {correct_count}")
    print(f"准确率: {correct_count/total_count*100:.1f}%")

    # 保存详细结果到文件
    result_file = os.path.join(current_dir, "results", "single_run", "detailed_results.json")
    os.makedirs(os.path.dirname(result_file), exist_ok=True)
    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump(detailed_results, f, ensure_ascii=False, indent=2)
    print(f"\n详细结果已保存到: {result_file}")

    return 0 if correct_count == total_count else 1


if __name__ == "__main__":
    sys.exit(main())
