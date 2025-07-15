from alphasql.algorithm.mcts.mcts import MCTSSolver
from alphasql.algorithm.mcts.reward import MajorityVoteRewardModel
from alphasql.runner.task import Task
from alphasql.config.mcts_config import MCTSConfig
from pathlib import Path
from typing import Union
import pickle
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import yaml
from alphasql.llm_call.cost_recoder import CostRecorder
import json
import random
from dotenv import load_dotenv  
import os
import traceback
import numpy as np

load_dotenv(override=True)

try:
    import weave
    if "WANDB_API_KEY" in os.environ:
        weave.init("mcts_runner")
    else:
        print("WANDB_API_KEY is not set in environment variables, will not use it")
except ImportError:
    print("Weave is not installed, will not use it")

class MCTSRunner:
    def __init__(self, config: Union[MCTSConfig, str]):
        if isinstance(config, str):
            config_path = Path(config)
            assert config_path.exists(), f"Config file {config_path} does not exist"
            if config_path.suffix == ".json":
                self.config = MCTSConfig.model_validate_json(config_path.read_text())
            elif config_path.suffix == ".yaml":
                self.config = MCTSConfig.model_validate(yaml.safe_load(config_path.read_text()))
            else:
                raise ValueError(f"Unsupported config file extension: {config_path.suffix}")
        else:
            self.config = config
            
        if not Path(self.config.save_root_dir).exists():
            Path(self.config.save_root_dir).mkdir(parents=True, exist_ok=True)

        random.seed(self.config.random_seed)

        self.all_task_results = []  # 新增：存储所有任务结果
        self.stats_file = Path(self.config.save_root_dir) / "task_rpompt_stats.json"
        
    def run_one_task(self, task: Task) -> None:
        task_recorder = CostRecorder(model=self.config.mcts_model_kwargs.get("model", "gpt-3.5-turbo"))
        mcts_solver = MCTSSolver(
            db_root_dir=self.config.db_root_dir,
            task=task,
            max_rollout_steps=self.config.max_rollout_steps,
            max_depth=self.config.max_depth,
            exploration_constant=self.config.exploration_constant,
            save_root_dir=self.config.save_root_dir,
            llm_kwargs={**self.config.mcts_model_kwargs, "cost_recorder": task_recorder},
            reward_model=MajorityVoteRewardModel(self.config.reward_model_kwargs),
            show_total_time_statistics=self.config.show_total_time_statistics
        )
        try:
            mcts_solver.solve()
        except Exception as e:
            print("-" * 100)
            print(f"Error solving task {task.question_id}: {e}")
            traceback.print_exc()
            print(f"The task {task.question_id} has been given up")
            print("-" * 100)
        
        task_stats = {
            "question_id": str(task.question_id),
            **task_recorder.to_dict(),
            "avg_tokens_per_call": task_recorder.total_tokens / max(1, task_recorder.call_count)
        }
        self._append_task_result(task_stats)

    def run_all_tasks(self):
        with open(self.config.tasks_file_path, "rb") as f:
            tasks = pickle.load(f)
            
        if self.config.subset_file_path:
            print(f"Using subset file {self.config.subset_file_path} to filter tasks")
            with open(self.config.subset_file_path, "r") as f:
                subset_data = json.load(f)
                subset_ids = [item["question_id"] for item in subset_data]
                tasks = [task for task in tasks if task.question_id in subset_ids]
            print(f"Filtered {len(tasks)} tasks from {len(tasks)} tasks")
        
        done_task_ids = []
        for pkl_file in Path(self.config.save_root_dir).glob("*.pkl"):
            done_task_ids.append(int(pkl_file.stem))
        print(f"Ignore done task ids: {done_task_ids}")
        tasks = [task for task in tasks if task.question_id not in done_task_ids]
        
        if len(done_task_ids) == 0:
            with open(self.stats_file, 'w') as f:
                json.dump([], f)  # 每次没有完成的任务,即一开始初始化为空列表

        with open(Path(self.config.save_root_dir) / "config.json", "w") as f:
            print(f"Saving config to {Path(self.config.save_root_dir) / 'config.json'}")
            json.dump(self.config.model_dump(), f, indent=4)

        print(f"There are {len(tasks)} tasks to solve")
        with ProcessPoolExecutor(max_workers=self.config.n_processes) as executor:
            list(tqdm(executor.map(self.run_one_task, tasks), total=len(tasks), desc="Solving tasks"))

    def _append_task_result(self, task_stats: dict):
        """追加单个任务结果到文件"""
        # 使用临时文件避免写入冲突
        temp_path = self.stats_file.with_suffix('.tmp')
        
        try:
            # 读取现有数据
            if self.stats_file.exists():
                with open(self.stats_file, 'r') as f:
                    existing_data = json.load(f)
            else:
                existing_data = []
            
            # 追加新结果
            existing_data.append(task_stats)
            
            # 写入临时文件
            with open(temp_path, 'w') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=4)
            
            # 原子替换原文件
            os.replace(temp_path, self.stats_file)
            
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            print(f"Error appending task result: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m alphasql.runner.mcts_runner <config_path>")
        sys.exit(1)
    config_path = sys.argv[1]
    runner = MCTSRunner(config=config_path)
    runner.run_all_tasks()
