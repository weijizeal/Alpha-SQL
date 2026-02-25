#!/usr/bin/env python
"""测试 embedding API 是否可用"""

from openai import OpenAI
import numpy as np
import os

client = OpenAI(
    api_key=os.getenv("API_KEY", "sk-33c92c76842f4c4f83716a2339b7d17f"),
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
)

try:
    # 测试单文本
    response = client.embeddings.create(
        model="text-embedding-v2",
        input="Hello world",
        encoding_format="float"
    )
    embedding = np.array(response.data[0].embedding, dtype=np.float32)
    print("✅ embedding API 正常工作!")
    print(f"   向量维度: {len(embedding)}")
    print(f"   前5个值: {embedding[:5]}")

    # 测试批量
    response2 = client.embeddings.create(
        model="text-embedding-v2",
        input=["test1", "test2", "test3"],
        encoding_format="float"
    )
    print(f"   批量测试: 成功生成 {len(response2.data)} 个嵌入向量")

except Exception as e:
    print(f"❌ embedding API 调用失败: {e}")
