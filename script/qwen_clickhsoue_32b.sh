#! /bin/bash

# 设置 ClickHouse 环境变量
export ALPHASQL_DB_TYPE=clickhouse

start_time=$(date +%s)

echo "Running MCTS..."
python -m alphasql.runner.mcts_runner config/clickhouse_stock.yaml

end_time=$(date +%s)
echo "Time taken: $((end_time - start_time)) seconds"


