
import json
import os
from pathlib import Path
from typing import Any, Dict, List
from alphasql.algorithm.mcts.mcts_action import MCTSNodeType
from alphasql.algorithm.mcts.mcts_node import MCTSNode
from alphasql.runner.task import Task
from graphviz import Digraph
from datetime import datetime

class MCTSTreeVisualizer:
    def __init__(self, task: Task, show_total_time_statistics: bool = False):
        self.task = task
        self.show_total_time_statistics = show_total_time_statistics
        
        # 动作类型到边样式的映射
        self.action_style = {
            'RaphraseQuestionAction': {'color': 'darkgreen', 'fontcolor': 'darkgreen'},
            'IdentifyColumnValuesAction': {'color': 'orange', 'fontcolor': 'orange'},
            'IdentifyColumnFunctionsAction': {'color': 'darkorange', 'fontcolor': 'darkorange'},
            'SchemaSelectionAction': {'color': 'gold', 'fontcolor': 'gold3'},
            'SQLGenerationAction': {'color': 'deeppink', 'fontcolor': 'deeppink3'},
            'SQLRevisionAction': {'color': 'purple', 'fontcolor': 'purple3'},
            'EndAction': {'color': 'firebrick', 'fontcolor': 'firebrick'}
        }

        # 节点类型到颜色的映射
        self.node_color = {
            MCTSNodeType.ROOT: {'fillcolor': 'lightblue', 'color': 'darkblue'},
            MCTSNodeType.REPHRASE_QUESTION: {'fillcolor': 'palegreen', 'color': 'darkgreen'},
            MCTSNodeType.SCHEMA_SELECTION: {'fillcolor': 'lightyellow', 'color': 'gold3'},
            MCTSNodeType.IDENTIFY_COLUMN_VALUES: {'fillcolor': 'moccasin', 'color': 'darkorange'},
            MCTSNodeType.IDENTIFY_COLUMN_FUNCTIONS: {'fillcolor': 'bisque', 'color': 'chocolate'},
            MCTSNodeType.SQL_GENERATION: {'fillcolor': 'lightpink', 'color': 'deeppink3'},
            MCTSNodeType.SQL_REVISION: {'fillcolor': 'thistle', 'color': 'purple3'},
            MCTSNodeType.END: {'fillcolor': 'lightcoral', 'color': 'firebrick'}
        }

    def visualize_tree(self, root_node: MCTSNode, rollout_step: int, num: int, phase: str, step_dir: str):
        """
        可视化MCTS树结构
        :param root_node: 根节点
        :param rollout_step: 当前轮次
        :param phase: 阶段名称
        :return: 包含图片路径和节点ID映射的字典
        """
        if not self.show_total_time_statistics:
            return

        self.save_root_dir = step_dir
        dot = Digraph(
            name=f'MCTS_Tree_Step_{rollout_step}_{phase}',
            format='png',
            graph_attr={
                'rankdir': 'TB',
                'splines': 'ortho',
                'nodesep': '0.2',
                'ranksep': '0.3'
            },
            node_attr={'style': 'rounded,filled', 'fontname': 'Helvetica'}
        )

        node_counter = 0
        node_id_map = {}  # 映射节点对象到唯一ID

        # 添加所有节点和边
        stack = [(root_node, None)]
        while stack:
            node, parent_id = stack.pop()
            
            if node not in node_id_map:
                node_id_map[node] = f"N{node_counter}"
                node_counter += 1
            
            node_id = node_id_map[node]
            
            # 创建节点标签
            node_label = [
                f"ID: {node_id}",
                f"Type: {node.node_type.name}",
                f"Q: {node.Q:.2f}",
                f"N: {node.N}"
            ]
            node.id = node_id
            
            # 添加节点
            dot.node(
                name=node_id,
                label='\n'.join(node_label),
                **self.node_color.get(node.node_type, {'fillcolor': 'white', 'color': 'black'}),
                fontsize='10'
            )
            
            # 添加边（如果是子节点）
            if parent_id is not None and node.parent_action:
                action_name = node.parent_action.__class__.__name__
                style = self.action_style.get(action_name, {'color': 'gray', 'fontcolor': 'gray'})
                
                dot.edge(
                    tail_name=parent_id,
                    head_name=node_id,
                    label=action_name,
                    fontsize='9',
                    **style
                )
            
            # 将子节点加入栈（反向添加以保证从左到右的顺序）
            for child in reversed(node.children):
                stack.append((child, node_id))

        # 确保目录存在
        os.makedirs(self.save_root_dir, exist_ok=True)
        
        # 保存图片
        output_path = Path(self.save_root_dir) / f"step_{num}_{phase}"
        dot.render(
            filename=output_path,
            cleanup=True
        )
        
        # 保存节点详细信息
        self.save_node_details(root_node, rollout_step, phase, num, node_id_map)

    def save_node_details(self, root_node: MCTSNode, rollout_step: int, phase: str, num: int, node_id_map: Dict[MCTSNode, str]):
        """
        保存节点详细信息和树结构到JSON文件
        """
        # 收集所有节点信息
        nodes_data = self.collect_all_nodes_data(root_node, node_id_map)
        
        # 准备完整的数据结构
        data = {
            "metadata": {
                "rollout_step": rollout_step,
                "phase": phase,
                "timestamp": datetime.now().isoformat(),
                "total_nodes": len(nodes_data),
                "node_types": self.count_node_types(nodes_data)
            },
            "initial_state": self.get_initial_state(),
            "nodes": nodes_data,
            "tree_structure": self.extract_tree_structure(root_node)
        }

        # 保存到文件
        file_path = Path(self.save_root_dir) / f"step_{num}_{phase}_details.json"
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def get_initial_state(self) -> Dict[str, Any]:
        """获取初始状态信息"""
        return {
            "task_configuration": {

                "db_id": self.task.db_id,
                "original_question": self.task.question,
                "evidence": self.task.evidence,
                "gold_sql": self.task.sql,
                "difficulty": self.task.difficulty,
                "tables_in_schema": list(self.task.table_schema_dict.keys()) if self.task.table_schema_dict else []
            },
            "initial_timestamp": datetime.now().isoformat()
        }

    def collect_all_nodes_data(self, root_node: MCTSNode, node_id_map: Dict[MCTSNode, str]) -> List[Dict[str, Any]]:
        """收集所有节点的详细信息"""
        nodes_data = []
        stack = [root_node]
        
        while stack:
            node = stack.pop()
            nodes_data.append(self.get_single_node_data(node, node_id_map))
            
            # 添加子节点到栈
            for child in node.children:
                stack.append(child)
        
        return nodes_data

    def get_single_node_data(self, node: MCTSNode, node_id_map: Dict[MCTSNode, str]) -> Dict[str, Any]:
        """获取单个节点的完整数据"""
        return {
            "ID": node_id_map[node],
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
                "sql_query": node.sql_query,
                "revised_sql": node.revised_sql_query,
                "final_sql": node.final_sql_query,
                "is_valid": node.is_valid_sql_query,
                "consistency_score": node.consistency_score
            },
            "node_prompts": {
                "prompts": node.prompt,
                "responses": node.response
            }
        }

    def extract_tree_structure(self, root_node: MCTSNode) -> Dict[str, Any]:
        """提取树的结构信息"""
        def build_node_structure(node):
            return {
                "node_id": id(node),
                "node_type": str(node.node_type),
                "children": [build_node_structure(child) for child in node.children]
            }
        
        return build_node_structure(root_node)

    def count_node_types(self, nodes_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """统计各类节点的数量"""
        type_counts = {}
        for node in nodes_data:
            node_type = node["node_type"]
            type_counts[node_type] = type_counts.get(node_type, 0) + 1
        return type_counts