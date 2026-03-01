#!/bin/bash

# ClickHouse 数据集预处理脚本

# 激活 conda 环境
source ~/miniconda/etc/profile.d/conda.sh
conda activate alphasql

export ALPHASQL_DB_TYPE=clickhouse
# 设置参数
PYTHON_CMD="python -m alphasql.runner.preprocessor \
    --data_file_path data/stock/dev/dev1.json \
    --database_root_dir data/stock/dev/databases \
    --save_root_dir data/preprocessed/stock \
    --lsh_threshold 0.5 \
    --lsh_signature_size 64 \
    --lsh_n_gram 3 \
    --lsh_top_k 20 \
    --edit_similarity_threshold 0.3 \
    --embedding_similarity_threshold 0.6 \
    --n_parallel_processes 1 \
    --max_dataset_samples=-1"

# 持续运行直到收到完成信号
while true; do
    # 使用临时文件捕获输出
    TEMP_LOG=$(mktemp)
    $PYTHON_CMD | tee "$TEMP_LOG"
    exit_code=$?
    
    # 解析输出内容（严格匹配Python的输出标记）
    if grep -q "✅ All batches completed!" "$TEMP_LOG"; then
        echo "✅ 所有批次处理完成！"
        rm "$TEMP_LOG"
        break
    elif grep -q "⏳ Batch .* completed" "$TEMP_LOG"; then
        batch_info=$(grep -o "⏳ Batch .* completed" "$TEMP_LOG")
        echo "⚡ ${batch_info}，准备下一批次..."
        rm "$TEMP_LOG"
        sleep 1
    else
        echo "❌ 发生错误！"
        tail -n 10 "$TEMP_LOG"
        rm "$TEMP_LOG"
        exit 1
    fi
done

echo "开始预处理 ClickHouse 数据集..."
echo "数据文件: data/stock/dev/dev1.json"
echo "保存目录: data/preprocessed/stock"
echo ""

$PYTHON_CMD
