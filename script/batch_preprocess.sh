#!/bin/bash

BATCH_SIZE=20
PYTHON_CMD="python -m alphasql.runner.preprocessor  --data_file_path data/stock/dev/dev.json  --database_root_dir data/stock/dev/dev_databases  --save_root_dir data/preprocessed/stock/dev  --lsh_threshold 0.5  --lsh_signature_size 128  --lsh_n_gram 3  --lsh_top_k 20  --edit_similarity_threshold 0.3  --embedding_similarity_threshold 0.6  --n_parallel_processes 1  --max_dataset_samples -1"

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
        tail -n 10 "$TEMP_LOG"
        rm "$TEMP_LOG"
        exit 1
    fi
done