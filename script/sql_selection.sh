#! /bin/bash

DB_ROOT_DIR="data/bird/dev/dev_databases"
PROCESS_NUM=8

RESULTS_DIR="results/clickhouse_stock_test"
OUTPUT_PATH="./pred_clickhouse_sqls.json"

echo "Selecting SQLs..."
python -m alphasql.runner.sql_selection \
    --results_dir $RESULTS_DIR \
    --db_root_dir $DB_ROOT_DIR \
    --process_num $PROCESS_NUM \
    --output_path $OUTPUT_PATH