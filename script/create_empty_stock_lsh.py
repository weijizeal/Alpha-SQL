#!/usr/bin/env python3
"""
为 stock_data 创建一个空壳数据库并生成 LSH 索引
用于开发/测试环境，避免存储大量数据
"""

import sqlite3
import os
from pathlib import Path

# 配置
STOCK_DATA_DIR = Path("data/stock/dev/dev_databases/stock_data")
OUTPUT_SQLITE = STOCK_DATA_DIR / "stock_data.sqlite"

# 建表语句 (只保留表结构，没有数据)
CREATE_TABLES = """
CREATE TABLE "hq_basic_view" ("idstk" TEXT, "secucode" TEXT, "secuname" TEXT, "type" TEXT, "market" TEXT, "trade" TEXT, "concept" TEXT, "area" TEXT, "date_int" TEXT, "date" TEXT, "is_hszb" TEXT, "is_kcb" TEXT, "is_cyb" TEXT, "is_tp" TEXT, "is_ts" TEXT, "is_xingu" TEXT, "is_cixingu" TEXT, "is_cwfx" TEXT, "is_qzts" TEXT, "is_tsfx" TEXT, "category" TEXT, "detail_company_name" TEXT, "detail_english_name" TEXT, "detail_before_name" TEXT, "detail_region" TEXT, "detail_industry" TEXT, "detail_board_name" TEXT, "detail_website" TEXT, "detail_main_business" TEXT, "detail_product_name" TEXT, "detail_ctrl_shareholder" TEXT, "detail_actual" TEXT, "detail_ultimate" TEXT, "detail_chairman" TEXT, "detail_secretary" TEXT, "detail_legal" TEXT, "detail_general_manager" TEXT, "detail_regist_capital" TEXT, "detail_staff_num" TEXT, "detail_telephone" TEXT, "detail_fax" TEXT, "detail_zip_code" TEXT, "detail_office_address" TEXT, "detail_company_profile" TEXT, "detail_regist_address" TEXT, "issue_establish_date" TEXT, "issue_price" TEXT, "issue_list_date" TEXT, "issue_pe" TEXT, "issue_estimate" TEXT, "issue_open_price" TEXT, "issue_winning_rate" TEXT, "issue_actual" TEXT, "issue_lead_underwriter" TEXT, "issue_listing_sponsor" TEXT, "issue_nums" TEXT, "roa" TEXT, "roic" TEXT);

CREATE TABLE "hq_current_view" ("idstk" TEXT, "date" TEXT, "date_int" TEXT, "secucode" TEXT, "zhu_13" TEXT, "zhu_1" TEXT, "gan_1" TEXT, "duo_1" TEXT, "zhu_3" TEXT, "gan_3" TEXT, "duo_3" TEXT, "pe_ratio_static" TEXT, "three_lock" TEXT, "long_policy" TEXT, "bd_policy" TEXT, "ma" TEXT, "chip" TEXT, "r_price" TEXT, "r_rate" TEXT, "s_rate" TEXT, "s_price" TEXT, "hot_value" TEXT, "hot_industry_trade" TEXT, "hot_industry_concept" TEXT, "hot_word" TEXT, "open" TEXT, "low" TEXT, "high" TEXT, "close" TEXT, "zdf" TEXT, "volume" TEXT, "amount" TEXT, "toratio" TEXT, "amplitude" TEXT, "macd_jincha" TEXT, "macd_sicha" TEXT, "macd_dingbeili" TEXT, "kdj_dibeili" TEXT, "kdj_dingbeili" TEXT, "kdj_jincha" TEXT, "kdj_sicha" TEXT, "rsi6_80" TEXT, "rsi6_20" TEXT, "rsi12_80" TEXT, "rsi12_20" TEXT, "cci_100" TEXT, "cci_ne100" TEXT, "cci_over_bought" TEXT, "cci_over_sold" TEXT, "wr_80" TEXT, "wr_20" TEXT, "wr_over_bought" TEXT, "wr_over_sold" TEXT, "ma_dtpl" TEXT, "ma_ktpl" TEXT, "ma_jincha" TEXT, "ma_sicha" TEXT, "cyc_dtpl" TEXT, "cyc_ktpl" TEXT, "cyc_jincha" TEXT, "cyc_sicha" TEXT, "cd_90" TEXT, "cd_70" TEXT, "kline_cxyx" TEXT, "kline_lyt" TEXT, "kline_jjyyt" TEXT, "kline_sstd" TEXT, "kline_cyzp" TEXT, "kline_jztd" TEXT, "kline_hsb" TEXT, "kline_wygd" TEXT, "kline_zczx" TEXT, "kline_xrds" TEXT, "kline_vxfz" TEXT, "kline_mrj" TEXT, "kline_zthmq" TEXT, "kline_dfp" TEXT, "kline_yycsx" TEXT, "opinions_month3" TEXT, "organ_grade_month3_buy" TEXT, "organ_grade_month3_overweight" TEXT, "organ_grade_month3_neutral" TEXT, "organ_grade_month3_reduction" TEXT, "organ_grade_month3_sell" TEXT, "violation_month3" TEXT, "exchange_record_month3_overweight" TEXT, "exchange_record_month3_reduction" TEXT, "sales_restrictions_lifted_future" TEXT, "sales_restrictions_lifted_over" TEXT, "private_placement" TEXT, "assets_reorganization" TEXT, "performance_forecast" TEXT, "organ_hold_total" TEXT, "fund_hold_num" TEXT, "qs_hold_num" TEXT, "qfll_hold_num" TEXT, "insure_hold_num" TEXT, "social_security_hold_ratio" TEXT, "qfll_hold_ratio" TEXT, "social_security_hold_num" TEXT, "trust_hold_num" TEXT, "organ_hold_ratio_total" TEXT, "fund_hold_ratio" TEXT, "qs_hold_ratio" TEXT, "insure_hold_ratio" TEXT, "trust_hold_ratio" TEXT, "sb_hold_2quarter" TEXT, "trade_seat_times_1month" TEXT, "zt" TEXT, "dt" TEXT, "rise" TEXT, "all_time_high" TEXT, "all_time_low" TEXT, "period_high" TEXT, "period_low" TEXT, "near_all_time_high" TEXT, "near_all_time_low" TEXT, "qrr" TEXT, "chg_day5" TEXT, "chg_day20" TEXT, "zf_month" TEXT, "pe_ratio" TEXT, "pe_ratio_ttm" TEXT, "pb_ratio" TEXT, "market_cap" TEXT, "market_value" TEXT, "tq" TEXT, "cir_equity" TEXT, "chg_day240" TEXT, "lz_day_count" TEXT, "zhu" TEXT, "duo" TEXT, "gan" TEXT, "dividend_yield" TEXT);

CREATE TABLE "hq_history_agg_view" ("secucode" TEXT, "date" TEXT, "close" TEXT, "date_int" TEXT, "open" TEXT, "low" TEXT, "high" TEXT, "amount" TEXT, "volume" TEXT, "marketvalue" TEXT, "zt" TEXT, "dt" TEXT, "duo" TEXT, "zhu" TEXT, "gan" TEXT, "toratio" TEXT, "zdf" TEXT, "ma_dtpl" TEXT, "ma_ktpl" TEXT, "ma_jincha" TEXT, "ma_sicha" TEXT, "pe_dynamic" TEXT, "pe_static" TEXT, "pe_ttm" TEXT);

CREATE TABLE "f10_main_indicator_view" ("secucode" TEXT, "date" TEXT, "date_int" TEXT, "parent_net_profit" TEXT, "parent_net_profit_yoy" TEXT, "non_net_profit" TEXT, "non_net_profit_yoy" TEXT, "income_total" TEXT, "income_total_yoy" TEXT, "basic_eps" TEXT, "common_fund" TEXT, "un_profit" TEXT, "share_opera_cash" TEXT, "gross_margin" TEXT, "sales_margin" TEXT, "roe" TEXT, "roe_diluted" TEXT, "business_cycle" TEXT, "inventory_turn_rate" TEXT, "inventory_turn_days" TEXT, "account_turn_days" TEXT, "current_ratio" TEXT, "quick_ratio" TEXT, "con_quick_ratio" TEXT, "equity_ratio" TEXT, "share_net_asset" TEXT, "assets_and_liability" TEXT, "total_assets_turnover_rate" TEXT);

CREATE TABLE "f10_shareholder_nums_view" ("date" TEXT, "secucode" TEXT, "date_int" TEXT, "holders_a_total" TEXT, "holders_b_total" TEXT, "holders_h_total" TEXT, "total_people_num" TEXT, "tradable_share_avg" TEXT, "industry_share_avg" TEXT, "cir_a_avg_share" TEXT, "cir_a_avg_share_change" TEXT);

CREATE TABLE "hot_info_view" ("date" TEXT, "type" TEXT, "data_code" TEXT, "date_int" TEXT, "sortid" TEXT);

CREATE TABLE "daily_zjdx_stat_level5_view" ("secucode" TEXT, "date" TEXT, "duo" TEXT, "duor" TEXT, "zhu" TEXT, "zhur" TEXT, "gan" TEXT, "ganr" TEXT, "date_int" TEXT);

CREATE TABLE "fund_move_flag_level0_view" ("idstk" TEXT, "secucode" TEXT, "date_int" TEXT, "date" TEXT, "zhu_flag" TEXT, "duo_flag" TEXT, "gan_flag" TEXT);

CREATE TABLE "daily_qlld_level5_view" ("secucode" TEXT, "date" TEXT, "ldmarket" TEXT, "ldtrade" TEXT, "ld0z" TEXT, "state" TEXT, "ldindex" TEXT, "ld_20" TEXT, "ld_60" TEXT, "ld_120" TEXT, "ld_240" TEXT, "date_int" TEXT);

CREATE TABLE "daily_ldpools_level5_view" ("filter_conditions" TEXT, "date" TEXT, "stock_pools" TEXT, "date_int" TEXT);

CREATE TABLE "daily_qlstock_basic_level5_view" ("secucode" TEXT, "date" TEXT, "lanchou" TEXT, "ismultild" TEXT, "ma_90_480" TEXT, "zldd" TEXT, "market" TEXT, "tradecore" TEXT, "prepare" TEXT, "multild" TEXT, "bd_ykcsl" TEXT, "stock_attribute" TEXT, "state" TEXT, "is_new" TEXT, "date_int" TEXT);
"""


def create_empty_database():
    """创建空壳数据库"""
    # 删除已存在的文件
    if OUTPUT_SQLITE.exists():
        OUTPUT_SQLITE.unlink()

    # 创建数据库和表
    conn = sqlite3.connect(OUTPUT_SQLITE)
    cursor = conn.cursor()

    for stmt in CREATE_TABLES.strip().split(';'):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)

    conn.commit()
    conn.close()
    print(f"✓ 已创建空壳数据库: {OUTPUT_SQLITE}")


def generate_lsh_index():
    """为 stock_data 生成 LSH 索引"""
    from alphasql.database.database_manager import DatabaseManager
    from alphasql.database.lsh_index import LSHIndex

    # 参数
    lsh_threshold = 0.5
    lsh_signature_size = 128
    lsh_n_gram = 3

    # 获取数据库模式
    db_schema = DatabaseManager.get_database_schema('stock_data', 'data/stock/dev/dev_databases')

    # 创建 LSH 索引
    print(f"开始为 stock_data 生成 LSH 索引...")
    LSHIndex.create_lsh_index(
        db_schema,
        threshold=lsh_threshold,
        signature_size=lsh_signature_size,
        n_gram=lsh_n_gram
    )
    print(f"✓ LSH 索引生成完成!")

    # 检查索引文件大小
    lsh_dir = Path(db_schema.db_directory) / "lsh_index"
    if lsh_dir.exists():
        total_size = sum(f.stat().st_size for f in lsh_dir.rglob('*') if f.is_file())
        print(f"  索引目录大小: {total_size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    print("=" * 50)
    print("Step 1: 创建空壳数据库")
    print("=" * 50)
    create_empty_database()

    print()
    print("=" * 50)
    print("Step 2: 生成 LSH 索引")
    print("=" * 50)
    generate_lsh_index()

    print()
    print("=" * 50)
    print("完成!")
    print("=" * 50)
