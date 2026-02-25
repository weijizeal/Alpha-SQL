#!/usr/bin/env python3
import json
import re
from collections import defaultdict

# 读取JSON文件
with open('/home/weiji/git_proj/Alpha-SQL/results/Qwen2.5-Coder-32B-Instruct-temp/bird/dev 2/1/step_24/step_5_backprop_details.json', 'r') as f:
    data = json.load(f)

# 动作到A1-A6的映射
action_to_label = {
    "RaphraseQuestionAction": "A1",
    "IdentifyColumnValuesAction": "A2",
    "IdentifyColumnFunctionsAction": "A3",
    "SchemaSelectionAction": "A4",
    "SQLGenerationAction": "A5",
    "SQLRevisionAction": "A6",
    "EndAction": "A7"
}

# 收集所有节点信息
nodes = {}  # node_id -> node_info
edges = []  # (from_node, to_node, action_label)

for node in data['nodes']:
    node_id = node['ID']
    parent_action = node.get('parent_action')
    parent_node_id = node.get('parent_node_id')

    nodes[node_id] = {
        'node_type': node.get('node_type'),
        'depth': node.get('depth')
    }

    # 只有当有父节点时才添加边
    if parent_action and parent_node_id:
        # 找到父节点的ID
        parent_node_id_str = str(parent_node_id)
        # 在nodes中查找对应的ID
        parent_id = None
        for nid, ninfo in nodes.items():
            # 需要通过另一种方式找 parent_id
            pass

# 重新处理：正确建立映射
# 首先建立 node_id 到 ID 的映射
node_id_to_id = {}
for node in data['nodes']:
    node_id_to_id[str(node['node_id'])] = node['ID']

# 重新收集边
edges = []
for node in data['nodes']:
    node_id = node['ID']
    parent_action = node.get('parent_action')
    parent_node_id = node.get('parent_node_id')

    if parent_action and parent_node_id:
        parent_node_id_str = str(parent_node_id)
        parent_id = node_id_to_id.get(parent_node_id_str)

        if parent_id:
            action_label = action_to_label.get(parent_action)
            if action_label:  # 包含A7
                edges.append((parent_id, node_id, action_label))

# 生成Graphviz图形
graphviz_code = """digraph MCTSGraph {
    rankdir=TB;
    node [shape=box, style="rounded,filled", fontname="Arial"];

"""

# 添加节点
for node_id in nodes:
    graphviz_code += f'    "{node_id}" [label="{node_id}"];\n'

graphviz_code += "\n"

# 添加边
for from_node, to_node, action_label in edges:
    graphviz_code += f'    "{from_node}" -> "{to_node}" [label="{action_label}", fontcolor="blue"];\n'

graphviz_code += "}\n"

# 保存图形代码
output_path = '/home/weiji/git_proj/Alpha-SQL/results/Qwen2.5-Coder-32B-Instruct-temp/bird/dev 2/1/step_24/mcts_graph.dot'
with open(output_path, 'w') as f:
    f.write(graphviz_code)

print(f"Graphviz DOT file saved to: {output_path}")
print(f"Total nodes: {len(nodes)}")
print(f"Total edges (A1-A6): {len(edges)}")

# 统计各类动作的数量
action_counts = defaultdict(int)
for _, _, action_label in edges:
    action_counts[action_label] += 1
print(f"\nAction counts: {dict(action_counts)}")
