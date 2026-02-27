import os
from pathlib import Path
from typing import Dict

# 根据数据库类型选择模板目录
DB_TYPE = os.getenv("ALPHASQL_DB_TYPE", "sqlite")
if DB_TYPE == "clickhouse":
    TEMPLATE_DIR = Path("alphasql/templates_clickhouse")
else:
    TEMPLATE_DIR = Path("alphasql/templates")

TEMPLATE_DICT = {}

for template_file in TEMPLATE_DIR.glob("*.txt"):
    with open(template_file, "r", encoding="utf-8") as f:
        TEMPLATE_DICT[template_file.stem] = f.read()

def get_prompt(template_name: str, template_args: Dict[str, str]) -> str:
    if template_name not in TEMPLATE_DICT:
        # 如果当前模板目录没有，回退到默认模板
        default_dir = Path("alphasql/templates")
        template_file = default_dir / f"{template_name}.txt"
        if template_file.exists():
            with open(template_file, "r", encoding="utf-8") as f:
                TEMPLATE_DICT[template_name] = f.read()

    template = TEMPLATE_DICT[template_name]
    return template.format(**template_args)
