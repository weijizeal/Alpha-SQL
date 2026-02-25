#!/usr/bin/env python
"""测试 deepseek-chat API 是否可用"""

from openai import OpenAI
import dotenv
import os

dotenv.load_dotenv(override=True)

# 使用 preprocessor.py 中的配置
client = OpenAI(
    api_key=os.getenv("API_KEY", "sk-e68a6a508fae4a3aa10963376ded8544"),
    base_url="https://api.deepseek.com"
)

try:
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role": "user", "content": "Say 'Hello' in one word"}],
        temperature=0.0,
        max_tokens=10
    )
    print("✅ deepseek-chat API 正常工作!")
    print(f"   回复: {response.choices[0].message.content}")
    print(f"   使用 token 数: {response.usage.total_tokens}")
except Exception as e:
    print(f"❌ deepseek-chat API 调用失败: {e}")
