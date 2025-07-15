import time
from datetime import datetime
from alphasql.algorithm.mcts.mcts_node import *
from alphasql.algorithm.mcts.mcts_action import *
from alphasql.algorithm.mcts.mcts_tree_visualizer import MCTSTreeVisualizer
from alphasql.algorithm.mcts.reward import *
from alphasql.runner.task import Task
from alphasql.algorithm.mcts.mcts_action import MCTSNodeType
import math
import random
from pathlib import Path
from typing import Dict, Any, List
import pickle
import os
from graphviz import Digraph
from pathlib import Path

class MCTSSolver:
    def __init__(self,
                 db_root_dir: str,
                 task: Task, 
                 max_rollout_steps: int,
                 max_depth: int,
                 exploration_constant: float,
                 save_root_dir: str,
                 llm_kwargs: Dict[str, Any],
                 reward_model: RewardModel,
                 show_total_time_statistics: bool = False):
        self.llm_kwargs = llm_kwargs
        self.reward_model = reward_model
        self.task = task
        self.db_root_dir = db_root_dir
        self.max_rollout_steps = max_rollout_steps
        self.max_depth = max_depth
        self.exploration_constant = exploration_constant
        self.save_root_dir = save_root_dir
        self.visualizer = MCTSTreeVisualizer(task, show_total_time_statistics)
    
    def select(self, node: MCTSNode) -> MCTSNode:
        current = node
        while current.children and not current.is_terminal():
            if not all(child.N > 0 for child in current.children):
                return next(child for child in current.children if child.N == 0)
            
            current = max(current.children, key=lambda child: (child.Q / child.N) + self.exploration_constant * math.sqrt(math.log(current.N) / child.N))
        return current
    
    def expand(self, node: MCTSNode) -> List[MCTSNode]:
        assert node.children == [], f"Children nodes of node {node.node_type} before expansion is not empty"
        valid_action_space = get_valid_action_space_for_node(node)
        for action in valid_action_space:
            action_nodes = action.create_children_nodes(node, self.llm_kwargs)
            node.children.extend(action_nodes)
        
        random.shuffle(node.children)
        
        # three special actions: EndAction, SQLGenerationAction, SQLRevisionAction
        # they only generate one child node each time
        # n_special_actions = len([action for action in valid_action_space if isinstance(action, (EndAction, SQLGenerationAction, SQLRevisionAction))])
        # n_not_special_actions = len([action for action in valid_action_space if not isinstance(action, (EndAction, SQLGenerationAction, SQLRevisionAction))])
        # assert len(node.children) == \
        #     n_not_special_actions * self.llm_kwargs.get("n", 1) + \
        #     n_special_actions * 1, \
        #     f"Number of children nodes is not expected, expected: {n_not_special_actions * self.llm_kwargs.get('n', 1) + n_special_actions * 1}, actual: {len(node.children)}"

    def simulate(self, node: MCTSNode) -> MCTSNode:
        assert node.children == [], f"Node before simulation have non-empty children"
        current = node
        while not current.is_terminal():
            self.expand(current)

            while len(current.children) > 0:
                current = random.choice(current.children)

                if not current._is_initialized:
                    action_class = type(current.parent_action)
                    action_class.initialize_node(current, self.llm_kwargs)
                    current._is_initialized = True
                    
                if current._is_pruned:
                    current.parent_node.children.remove(current)
                    continue
                else:
                    break
                
        return current

    def backpropagate(self, node: MCTSNode):
        print("Backpropagate, Final SQL Query: ", node.final_sql_query)
        current = node
        if current.N == 0:
            reward = self.reward_model.get_reward(current)
        else:
            reward = current.Q / current.N
        while current is not None:
            current.N += 1
            current.Q += reward
            current = current.parent_node
    
    def find_all_end_nodes(self, node: MCTSNode) -> List[MCTSNode]:
        if node.node_type == MCTSNodeType.END:
            return [node]
        else:
            end_nodes = []
            for child in node.children:
                end_nodes.extend(self.find_all_end_nodes(child))
            return end_nodes
    
    def find_all_valid_reasoning_paths(self, node: MCTSNode) -> List[List[MCTSNode]]:
        end_nodes = self.find_all_end_nodes(node)
        reasoning_paths = []
        for end_node in end_nodes:
            reasoning_paths.append(end_node.path_nodes)
        return reasoning_paths

    def solve(self):
        # 初始化时间统计结构
        time_stats = {
            'question_id': self.task.question_id,
            'start_time': datetime.now().isoformat(),
            'total_time': 0,
            'schema_build_time': 0,
            'mcts_phases': [],  # 存储每一轮的阶段时间
            'find_paths_time': 0,
            'save_results_time': 0,
            'end_time': None
        }

        # 1. 总计时开始
        total_start = time.time()

        # 2. Schema构建阶段计时
        schema_start = time.time()
        schema_context = "\n".join([build_table_ddl_statement(
            self.task.table_schema_dict[table_name].to_dict(), 
            add_value_description=True,
            add_column_description=True,
            add_value_examples=True,
            add_expanded_column_name=True
        ) for table_name in self.task.table_schema_dict])
        time_stats['schema_build_time'] = time.time() - schema_start

        # 3. 初始化根节点
        root_node = MCTSNode(
            MCTSNodeType.ROOT,
            parent_node=None,
            parent_action=None,
            depth=0,
            db_id=self.task.db_id,
            db_root_dir=self.db_root_dir,
            original_question=self.task.question,
            hint=self.task.evidence,
            schema_context=schema_context,
            table_schema_dict=self.task.table_schema_dict
        )
        root_node.path_nodes = [root_node]

        # 4. MCTS主循环
        for rollout_step in range(self.max_rollout_steps):
            # 初始化当前轮次的时间记录
            phase_times = {'step': rollout_step + 1,'select_time': 0,'expand_time': 0,'simulate_time': 0,'backprop_time': 0,'total_time': 0}
            rollout_start = time.time()

            print(f"Question ID: {self.task.question_id}, Rollout step {rollout_step + 1}/{self.max_rollout_steps}")

            # 创建本轮次的专属文件夹
            step_dir = Path(self.save_root_dir) / f"{self.task.question_id}" / f"step_{rollout_step + 1}"
            os.makedirs(step_dir, exist_ok=True)
            
            # 记录本轮初始树状态
            self.visualizer.visualize_tree(root_node=root_node,rollout_step=rollout_step + 1,phase="initial",num=1,step_dir=step_dir)

            # 选择阶段计时
            select_start = time.time()
            
            if rollout_step == 0:
                leaf_node = self.select(root_node)
            else:
                while len(leaf_node.parent_node.children) > 0:
                    leaf_node = self.select(root_node)

                    if not leaf_node._is_initialized:
                        action_class = type(leaf_node.parent_action)
                        action_class.initialize_node(leaf_node, self.llm_kwargs)
                        leaf_node._is_initialized = True

                    if leaf_node._is_pruned:
                        leaf_node.parent_node.children.remove(leaf_node)
                        continue
                    else:
                        break
            
            phase_times['select_time'] = time.time() - select_start

            # 记录选择后的树状态
            self.visualizer.visualize_tree(root_node=root_node, rollout_step=rollout_step + 1, phase="select", num=2, step_dir=step_dir)

            if leaf_node.is_terminal():
                # 回传阶段计时
                backprop_start = time.time()
                self.backpropagate(leaf_node)
                phase_times['backprop_time'] = time.time() - backprop_start

                # 记录回传后的树状态
                self.visualizer.visualize_tree(root_node=root_node,rollout_step=rollout_step + 1,phase="backprop",num=3,step_dir=step_dir)
            else:
                # 扩展阶段计时
                expand_start = time.time()
                self.expand(leaf_node)
                phase_times['expand_time'] = time.time() - expand_start

                # 记录扩展后的树状态
                self.visualizer.visualize_tree(root_node=root_node,rollout_step=rollout_step + 1,phase="expand",num=3,step_dir=step_dir)

                while len(leaf_node.children) > 0:
                    leaf_node = random.choice(leaf_node.children)

                    if not leaf_node._is_initialized:
                        action_class = type(leaf_node.parent_action)
                        action_class.initialize_node(leaf_node, self.llm_kwargs)
                        leaf_node._is_initialized = True
                        
                    if leaf_node._is_pruned:
                        leaf_node.parent_node.children.remove(leaf_node)
                        continue
                    else:
                        break

                # 模拟阶段计时
                simulate_start = time.time()
                end_node = self.simulate(leaf_node)
                phase_times['simulate_time'] = time.time() - simulate_start

                # 记录模拟后的树状态
                self.visualizer.visualize_tree(root_node=root_node,rollout_step=rollout_step + 1,phase="simulate",num=4,step_dir=step_dir)

                # 回传阶段计时
                backprop_start = time.time()
                self.backpropagate(end_node)
                phase_times['backprop_time'] = time.time() - backprop_start

                 # 记录回传后的树状态
                self.visualizer.visualize_tree(root_node=root_node,rollout_step=rollout_step + 1,phase="backprop",num=5,step_dir=step_dir)

            # 记录当前轮次总时间
            phase_times['total_time'] = time.time() - rollout_start
            time_stats['mcts_phases'].append(phase_times)

        # 5. 查找有效路径计时
        find_paths_start = time.time()
        all_valid_reasoning_paths = self.find_all_valid_reasoning_paths(root_node)
        time_stats['find_paths_time'] = time.time() - find_paths_start

        # 6. 保存结果计时
        save_start = time.time()
        result_path = Path(self.save_root_dir) / f"{self.task.question_id}.pkl"
        with open(result_path, "wb") as f:
            pickle.dump(all_valid_reasoning_paths, f)
        time_stats['save_results_time'] = time.time() - save_start

        # 7. 完成总计时
        time_stats['total_time'] = time.time() - total_start
        time_stats['end_time'] = datetime.now().isoformat()

        # 8. 保存时间统计到独立文件
        stats_path = Path(self.save_root_dir) / f"{self.task.question_id}_time_stats.json"
        with open(stats_path, "w") as f:
            json.dump(time_stats, f, ensure_ascii=False, indent=4)

        return all_valid_reasoning_paths

        