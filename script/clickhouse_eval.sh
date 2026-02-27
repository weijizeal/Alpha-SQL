#!/bin/bash

# ClickHouse SQL Evaluation Script

# 激活 conda 环境
source ~/miniconda/etc/profile.d/conda.sh
conda activate alphasql

cd /home/weiji/git_proj/Alpha-SQL
export ALPHASQL_DB_TYPE=clickhouse

python evaluate_sql.py \
    --pred_sql_path pred_clickhouse_sqls.json \
    --gt_data_path data/stock/dev/dev1.json \
    --db_root_path "" \
    --action_path_json_path "" \
    --path_node_pkl_path "" \
    --num_cpus 1 \
    --timeout 30
