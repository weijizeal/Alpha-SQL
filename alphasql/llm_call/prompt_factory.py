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

# 加载补充规则文件
RULES_CONTENT = ""
rules_file = TEMPLATE_DIR / "sql_generation_rules.md"
if rules_file.exists():
    with open(rules_file, "r", encoding="utf-8") as f:
        RULES_CONTENT = f.read()

def get_prompt(template_name: str, template_args: Dict[str, str]) -> str:
    if template_name not in TEMPLATE_DICT:
        # 如果当前模板目录没有，回退到默认模板
        default_dir = Path("alphasql/templates")
        template_file = default_dir / f"{template_name}.txt"
        if template_file.exists():
            with open(template_file, "r", encoding="utf-8") as f:
                TEMPLATE_DICT[template_name] = f.read()

    template = TEMPLATE_DICT[template_name]

    # 对于 sql_generation 模板，添加补充规则内容
    if template_name == "sql_generation" and "RULES_CONTENT" not in template_args:
        template_args = dict(template_args)
        template_args["RULES_CONTENT"] = RULES_CONTENT

    return template.format(**template_args)
