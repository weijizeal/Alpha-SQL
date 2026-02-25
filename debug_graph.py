#!/usr/bin/env python3
import json
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
    "EndAction": "A7"  # 排除A7
}

# 首先建立 node_id 到 ID 的映射
node_id_to_id = {}
for node in data['nodes']:
    node_id_to_id[str(node['node_id'])] = node['ID']

# 检查一些特定的节点映射
print("检查节点映射:")
print(f"N294 对应的 node_id: 126967715675984 -> {node_id_to_id.get('126967715675984')}")
print(f"N295 的 parent_node_id: 126967715675984")

# 收集边
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
            if action_label and action_label != "A7":  # 排除A7
                edges.append((parent_id, node_id, action_label))

# 检查 N294 -> N295 的边是否存在
print("\n检查边:")
print(f"N294 -> N295 的边: {('N294', 'N295', 'A6') in edges}")

# 统计
print(f"\n总边数: {len(edges)}")

# 打印一些边的例子
print("\n前10条边:")
for e in edges[:10]:
    print(e)
