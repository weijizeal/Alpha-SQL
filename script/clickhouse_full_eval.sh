#!/bin/bash

# 激活 conda 环境
source ~/miniconda/etc/profile.d/conda.sh
conda activate alphasql

# 设置 ClickHouse 环境变量
export ALPHASQL_DB_TYPE=clickhouse

# 1. 先运行 sql_selection 选择最终SQL
echo "Selecting SQLs..."
python -m alphasql.runner.sql_selection \
    --results_dir results/clickhouse_stock_test \
    --db_root_dir "" \
    --process_num 8 \
    --output_path pred_clickhouse_sqls.json

# 2. 运行评估
echo "Evaluating..."
python evaluate_sql.py \
    --pred_sql_path pred_clickhouse_sqls.json \
    --gt_data_path data/stock/dev/dev1.json \
    --db_root_path "" \
    --num_cpus 1 \
    --timeout 30
