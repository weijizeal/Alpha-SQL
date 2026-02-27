from dao.interfaces import *


class StockNewApi:
    @staticmethod
    def query_beidou_navigation(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选北斗导航股票
        """
        return AStockGlobalQuery.query_concept("BR01B02106", start_date, end_date)

    @staticmethod
    def query_military_industry(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选军工概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02164", start_date, end_date)

    @staticmethod
    def query_natural_gas(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选天燃气概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02467", start_date, end_date)

    @staticmethod
    def query_uav(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选无人机概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02160", start_date, end_date)

    @staticmethod
    def query_military_ai(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选军工AI概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            and_(
                AStockBasic.concept.like(f"%BR01B02164%"),  # 军工
                AStockBasic.concept.like(f"%BR01B02191%"),  # 人工智能
            )
        )

    @staticmethod
    def query_hot_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选热门股票
        """
        return AStockGlobalQuery.query_hot_stock_info(start_date, end_date)

    @staticmethod
    def query_main_capital_inflow(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主力资金流入的股票
        """
        # 1. 获取所有A股股票作为基础查询（过滤停牌）
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)

        # 2. 调用 _calculate_diff 获取1日主力资金流入 CTE
        zhu_in_cte = AStockGlobalQuery._calculate_diff(
            field=DailyZjdxStatLevel5.zhu,
            days=1,
            start_date=start_date,
            end_date=end_date,
            secucode_query=all_stocks_query,
            calculation_mode="positive",
            filter_positive=True,
        )

        # 3. 返回结果（filter_positive=True 已确保所有值 > 0）
        return select(zhu_in_cte.c.secucode)

    @staticmethod
    def query_good_performing_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选表现好的股票
        """
        return AStockGlobalQuery.query_good_performance(start_date, end_date)

    @staticmethod
    def query_ultra_high_voltage_industry_chain_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选特高压产业链相关的龙头股
        """
        concept_query = AStockGlobalQuery.query_concept("BR01B02292", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_star_market(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选科创板的股票
        """
        return AStockGlobalQuery.query_is_kcb(1, start_date, end_date)

    @staticmethod
    def query_chinext(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选创业板的股票
        """
        return AStockGlobalQuery.query_is_cyb(1, start_date, end_date)

    @staticmethod
    def query_annual_report_high_growth_companies(start_date: str = "0q", end_date: str = "0q"):
        """
        term_explanation: 筛选年报高增长的企业

        quantitative_definition: 归母净利润同比增长率 ≥ 30% 且 营业收入同比增长率 ≥ 30%
        """
        start = get_date(start_date, is_start=True)
        end = get_date(end_date)

        return (
            select(AStockFinancial.secucode)
            .where(
                AStockFinancial.date.between(start, end),
                AStockFinancial.parent_net_profit_yoy >= 0.30,
                AStockFinancial.income_total_yoy >= 0.30,
            )
            .distinct()
        )

    @staticmethod
    def query_shanghai_local_consumer_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选上海本地消费股
        """
        return select(AStockBasic.secucode).where(
            and_(
                AStockBasic.concept.like(f"%BR01B02198%"),  # 上证消费
                AStockBasic.area == "BR01B03002",  # 上海板块
            )
        )

    @staticmethod
    def query_humanoid_robot(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选人形机器人
        """
        return AStockGlobalQuery.query_concept("BR01B02528", start_date, end_date)

    @staticmethod
    def query_obtain_large_orders(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选获得大批订单
        """
        ...

    @staticmethod
    def query_carbon_fiber_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选碳纤维龙头股
        """
        concept_query = AStockGlobalQuery.query_concept("BR01B02573", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_semiconductor(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选半导体概念板块的股票
        """
        return AStockGlobalQuery.query_trade("BR01B01031", start_date, end_date)

    @staticmethod
    def query_consumer_electronics(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选消费电子概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02546", start_date, end_date)

    @staticmethod
    def query_companies_producing_silver(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选生产白银的公司
        """
        return AStockGlobalQuery.query_concept("BR01B02717", start_date, end_date)

    @staticmethod
    def query_stocks_listed_in_star_market_within_specified_date_range(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选指定日期范围内上市的科创板
        """
        return (
            select(AStockBasic.secucode)
            .where(
                AStockBasic.is_kcb == 1,
                AStockBasic.issue_list_date.isnot(None),
                func.toDate(AStockBasic.issue_list_date).between(
                    get_date(start_date, is_start=True), get_date(end_date)
                ),
            )
            .order_by(AStockBasic.issue_list_date.desc())
        )

    @staticmethod
    def query_chuangye_board_stocks_listed_within_specified_date_range(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选指定日期范围内上市的创业板
        """
        return (
            select(AStockBasic.secucode)
            .where(
                AStockBasic.is_cyb == 1,
                AStockBasic.issue_list_date.isnot(None),
                func.toDate(AStockBasic.issue_list_date).between(
                    get_date(start_date, is_start=True), get_date(end_date)
                ),
            )
            .order_by(AStockBasic.issue_list_date.desc())
        )

    @staticmethod
    def query_carbon_fiber_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选碳纤维股票
        """
        return AStockGlobalQuery.query_concept("BR01B02573", start_date, end_date)

    @staticmethod
    def query_dividend_preferred_stock_pool(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选红利优选股份池
        """
        return AStockGlobalQuery.query_stock_attribute_512(1, start_date, end_date)

    @staticmethod
    def query_undervalued_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选被低估的股票
        """
        return AStockGlobalQuery.query_low_valuation_pe("0d", "0d")

    @staticmethod
    def query_volume_cooperation(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选成交量配合

        quantitative_definition:
            量比 > 1
            或
            近5日平均成交量 > 过去20日平均成交量
        """
        start = get_date(start_date, is_start=True)
        end = get_date(end_date)

        # 条件1: 量比 > 1
        condition_1 = (
            select(AStockMarketCur.secucode)
            .where(
                AStockMarketCur.qrr > 1,
                AStockMarketCur.date.between(start, end),
            )
            .subquery()
        )

        # 条件2: 近5日平均成交量 > 过去20日平均成交量
        # 使用窗口函数计算移动平均成交量
        volume_with_avg = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                func.avg(AStockMarket.volume)
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date, rows=(-4, 0))
                .label("avg_volume_5"),
                func.avg(AStockMarket.volume)
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date, rows=(-19, 0))
                .label("avg_volume_20"),
            )
            .where(AStockMarket.date.between(get_date("-20d", is_start=True), end))
            .subquery()
        )

        condition_2 = (
            select(volume_with_avg.c.secucode)
            .where(
                volume_with_avg.c.avg_volume_5 > volume_with_avg.c.avg_volume_20,
                volume_with_avg.c.date.between(start, end),
            )
            .subquery()
        )

        # 使用 union 组合两个条件 (OR 关系)
        return union(select(condition_1.c.secucode), select(condition_2.c.secucode))

    def query_stocks_that_can_rise_and_breakthrough(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选可以上涨突破的股票

        quantitative_definition:
            股价向上突破重要技术位
            规则：

            筛选条件：
            - 收盘价突破技术位（前60日高点/平台/均线）
            - 突破幅度 > 3%
            - 突破日量比 > 1.5
            - 突破后3日未回落至原位
            数据来源： K线数据、成交量数据
        """
        ...

    @staticmethod
    def query_volume_ratio_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选量比在指定范围

        quantitative_definition:
            量比在指定范围内的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 直接调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_qrr(), interval, start_date, end_date)

    @staticmethod
    def query_bullish_arrangement(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选多头排列
        """
        return AStockGlobalQuery.query_ma_dtpl(1, start_date, end_date)

    @staticmethod
    def query_non_st(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选非ST的股票
        """
        return AStockGlobalQuery.query_st(0, start_date, end_date)

    @staticmethod
    def query_listed_companies(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选上市的公司
        """
        return AStockGlobalQuery.query_is_ts(0, start_date, end_date)

    @staticmethod
    def query_long_short_fund_inflow(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选多空资金流入
        """
        # 1. 获取所有A股股票作为基础查询（过滤停牌）
        all_stocks_query = select(AStockBasic.secucode)

        # 2. 调用 _calculate_diff 获取1日主力资金流入 CTE
        duo_in_cte = AStockGlobalQuery._calculate_diff(
            field=DailyZjdxStatLevel5.duo,
            days=1,
            start_date=start_date,
            end_date=end_date,
            secucode_query=all_stocks_query,
            calculation_mode="positive",
            filter_positive=True,
        )

        # 3. 返回结果（filter_positive=True 已确保所有值 > 0）
        return select(duo_in_cte.c.secucode)

    def query_semiconductor_equipment(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选半导体设备概念板块的股票
        """
        return AStockGlobalQuery.query_trade("BR01B01031", start_date, end_date)

    @staticmethod
    def query_high_quality_performance(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选优质业绩
        """
        return AStockGlobalQuery.query_good_performance(start_date, end_date)

    @staticmethod
    def query_limit_up_sealing_ratio_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选涨停封成比在指定范围
        """
        ...

    @staticmethod
    def query_dde_large_orders_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选DDE大单在指定范围
        """
        ...

    @staticmethod
    def query_dde_large_orders_top_n(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选dde大单前n名
        """
        ...

    def query_turnover_rate_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选换手率在指定范围

        quantitative_definition:
            换手率在指定范围内的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 直接调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_toratio(), interval, start_date, end_date)

    @staticmethod
    def query_volume_surge_rise(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选放量上涨的股票

        quantitative_definition: 量比＞2，涨幅＞2%
        """
        # 筛选 A股 且 非ST
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)

        zdf_cte = AStockGlobalQuery._get_zdf_days(
            start_date=start_date, end_date=end_date, secucode_query=all_stocks_query, days=1
        )

        start = get_date(start_date, is_start=True)
        end = get_date(end_date)

        return (
            select(AStockMarketCur.secucode)
            .select_from(zdf_cte)
            .join(
                AStockMarketCur,
                and_(AStockMarketCur.secucode == zdf_cte.c.secucode, AStockMarketCur.date == zdf_cte.c.date),
            )
            .where(
                AStockMarketCur.qrr > 2,
                zdf_cte.c.zdf_1 > 0.02,
                AStockMarketCur.date.between(start, end),
            )
        )

    @staticmethod
    def query_wanxing_technology(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选万兴科技股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ300624")

    def query_a_shares(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选A股的股票
        """
        return AStockGlobalQuery.query_A_market_stocks(start_date, end_date)

    @staticmethod
    def query_aero_engine_power(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选航发动力股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600893")

    def query_main_board(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主板的股票
        """
        return AStockGlobalQuery.query_is_hszb(1, start_date, end_date)

    @staticmethod
    def query_artificial_intelligence_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选人工智能板块
        """
        return AStockGlobalQuery.query_concept("BR01B02191", start_date, end_date)

    @staticmethod
    def query_avic_optoelectronics(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选中航光电股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002179")

    @staticmethod
    def query_avic_shenyang_aircraft(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选中航沈飞股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600760")

    @staticmethod
    def query_avic_xi_aircraft(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选中航西飞股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ000768")

    def query_stocks_with_completed_position_building(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选建好仓的股票
        """
        ...

    @staticmethod
    def query_stocks_ready_to_rise(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选准备拉升的股票
        """
        ...

    @staticmethod
    def query_stocks_currently_building_positions(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选正在建仓的股票

        quantitative_definition:
            主力资金在低位秘密买入股票的阶段
            规则：

            筛选条件：
            - 近20日股价波动幅度 < 15%
            - 近10日累计涨幅 < 10%
            - 换手率 BETWEEN 3 AND 8
            - 股东人数较上期减少 > 5%（季报数据）
        """
        # 获取日期范围
        end = get_date(end_date)
        start_current = get_date(start_date, is_start=True)
        start_20 = get_date(start_date, is_start=True, lookback_days=20)

        # 条件1: 近20日股价波动幅度 < 15%
        price_range_20 = (
            select(
                AStockMarket.secucode,
                func.max(AStockMarket.high).label("highest_20"),
                func.min(AStockMarket.low).label("lowest_20"),
            )
            .where(AStockMarket.date.between(start_20, end))
            .group_by(AStockMarket.secucode)
            .having((func.max(AStockMarket.high) - func.min(AStockMarket.low)) / func.min(AStockMarket.low) < 0.15)
            .subquery()
        )

        # 条件2: 近10日累计涨幅 < 10% + 条件3: 换手率 BETWEEN 3 AND 8
        # 获取今日收盘价和换手率
        today_data = (
            select(
                AStockMarket.secucode,
                AStockMarket.close.label("today_close"),
                AStockMarket.toratio.label("turnover"),
            )
            .where(AStockMarket.date.between(start_current, end))
            .subquery()
        )

        # 获取10日前收盘价（使用窗口函数）
        start_10 = get_date(start_date, is_start=True, lookback_days=10)
        close_10_days_ago = (
            select(
                AStockMarket.secucode,
                AStockMarket.close.label("close_10d_ago"),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.date.between(start_10, end))
            .subquery()
        )

        # 筛选rn=10的记录（10个交易日前的收盘价）
        close_10d = (
            select(close_10_days_ago.c.secucode, close_10_days_ago.c.close_10d_ago)
            .where(close_10_days_ago.c.rn == 10)
            .subquery()
        )

        # 组合所有条件
        query = (
            select(price_range_20.c.secucode)
            .select_from(price_range_20)
            .join(today_data, price_range_20.c.secucode == today_data.c.secucode)
            .join(close_10d, price_range_20.c.secucode == close_10d.c.secucode)
            .where(
                and_(
                    # 条件2: 近10日累计涨幅 < 10%
                    (today_data.c.today_close - close_10d.c.close_10d_ago) / close_10d.c.close_10d_ago < 0.10,
                    # 条件3: 换手率 BETWEEN 3% AND 8% (即 0.03 AND 0.08)
                    today_data.c.turnover.between(0.03, 0.08),
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_net_profit_growth_rate_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选净利润增长率在指定范围的股票

        quantitative_definition:
            归母净利润同比增长率在指定范围内的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 直接调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(
            AStockFinancial.parent_net_profit_yoy, interval, start_date, end_date
        )

    @staticmethod
    def query_high_volume_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选放量的个股

        quantitative_definition: 量比＞2
        """

        return select(AStockMarketCur.secucode).where(AStockMarketCur.qrr > 2)

    @staticmethod
    def query_rising_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选上涨的股票
        """
        # 1. 获取所有股票作为基础查询
        all_stocks_query = select(AStockMarket.secucode)

        # 2. 调用 _get_zdf_days 获取1日涨跌幅 CTE
        zdf_cte = AStockGlobalQuery._get_zdf_days(
            start_date=start_date, end_date=end_date, secucode_query=all_stocks_query, days=1
        )

        # 3. 筛选条件：1日涨跌幅 > 0
        condition = zdf_cte.c.zdf_1 > 0

        # 4. 返回结果
        return select(zdf_cte.c.secucode).where(condition)

    @staticmethod
    def query_close_price_equal_to_low_price(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选收盘价等于最低价的个股
        """
        # 获取日期范围
        start = get_date(start_date, is_start=True)
        end = get_date(end_date)

        # 获取所有A股的收盘价和最低价数据，并添加行号
        market_with_rn = (
            select(
                AStockMarket.secucode,
                AStockMarket.close,
                AStockMarket.low,
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .join(AStockBasic, AStockMarket.secucode == AStockBasic.secucode)
            .where(
                AStockBasic.type == "A股",
                AStockBasic.is_ts == 0,
                AStockMarket.date.between(start, end),
            )
            .subquery("market_with_rn")
        )

        # 筛选收盘价等于最低价的股票（只选择最新一天）
        query = (
            select(market_with_rn.c.secucode)
            .where(
                and_(
                    market_with_rn.c.rn == 1,  # 只选择最新一天
                    market_with_rn.c.close == market_with_rn.c.low,  # 收盘价 == 最低价
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_remove_limit_up_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选剔除涨停的个股
        """
        return AStockGlobalQuery.query_zt(0, start_date, end_date)

    @staticmethod
    def query_remove_limit_down_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选剔除跌停的个股
        """
        return AStockGlobalQuery.query_dt(0, start_date, end_date)

    @staticmethod
    def query_ai_applications(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选AI应用概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ601985",  # 中国核电：研发主体中核集团的核心上市平台
                    "SHHQ600111",  # 北方稀土：离子注入机核心耗材/磁体相关
                    "SHHQ600641",  # 万业企业：旗下凯世通是国内离子注入机商业化龙头
                    "SHHQ688012",  # 中微公司：半导体设备大平台
                    "SHHQ688037",  # 芯源微：半导体前道设备关键厂商
                    "SZHQ300722",  # 新莱应材：半导体设备真空系统/气体管路（离子注入机关键配套）
                    "SHHQ688126",  # 沪硅产业：大硅片（离子注入的下游应用载体）
                    "SHHQ688521",  # 芯海科技：高精度模拟芯片
                    "SHHQ603690",  # 至纯科技：高纯工艺系统及半导体设备
                    "SZHQ300327",  # 中颖电子：锂电/功率半导体应用
                    "SZHQ002049",  # 紫光国微：特种集成电路（高能离子注入重要应用领域）
                    "SHHQ600501",  # 航天晨光：涉及高能物理/真空压力容器配套
                ]
            )
        )

    @staticmethod
    def query_stocks_standing_above_n_day_line(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选站上n日线的股票
        """
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)
        ma_cte = AStockGlobalQuery._get_ma_days(start_date, end_date, all_stocks_query, n=n)

        close_query = select(AStockMarket.secucode, AStockMarket.close).where(
            AStockMarket.date.between(get_date(start_date, is_start=True), get_date(end_date))
        )

        close_cte = close_query.cte("close_cte")

        query = (
            select(close_cte.c.secucode)
            .join(ma_cte, close_cte.c.secucode == ma_cte.c.secucode)
            .where(close_cte.c.close > ma_cte.c[f"ma_{n}"])
            .distinct()
        )

        return query

    @staticmethod
    def query_bullish_upward(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选多头向上
        """
        return AStockGlobalQuery.query_ma_dtpl(1, start_date, end_date)

    @staticmethod
    def query_yanshan_technology(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选盐山科技股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002195")

    @staticmethod
    def query_robot_concept_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选机器人概念龙头股
        """
        concept_query = AStockGlobalQuery.query_concept("BR01B02551", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_semiconductor_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选半导体龙头
        """
        concept_query = AStockGlobalQuery.query_trade("BR01B01031", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_growth_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选成长期股票

        quantitative_definition: 市盈率0-50，归母净利润同比增长率≥30%，营业收入同比增长率≥30%
        """
        # 获取最新财务数据
        latest_financial = (
            select(
                AStockFinancial.secucode,
                AStockFinancial.parent_net_profit_yoy,
                AStockFinancial.income_total_yoy,
                func.row_number()
                .over(partition_by=AStockFinancial.secucode, order_by=desc(AStockFinancial.date))
                .label("rn"),
            )
            .where(
                AStockFinancial.parent_net_profit_yoy.isnot(None),
                AStockFinancial.income_total_yoy.isnot(None),
            )
            .subquery()
        )

        # 获取最新行情数据
        latest_market = (
            select(
                AStockMarketCur.secucode,
                AStockMarketCur.pe_ratio_static,
                AStockMarketCur.date,
            )
            .where(AStockMarketCur.pe_ratio_static.isnot(None))
            .subquery()
        )

        # JOIN 两个子查询，筛选符合条件的数据
        return (
            select(latest_financial.c.secucode)
            .join(
                latest_market,
                and_(
                    latest_financial.c.secucode == latest_market.c.secucode,
                    latest_financial.c.rn == 1,  # 最新财务数据
                ),
            )
            .where(
                latest_market.c.pe_ratio_static.between(0, 50),  # 市盈率0-50
                latest_financial.c.parent_net_profit_yoy >= 0.30,  # 归母净利润同比增长率≥30%
                latest_financial.c.income_total_yoy >= 0.30,  # 营业收入同比增长率≥30%
            )
        )

    @staticmethod
    def query_low_stock_index_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选低股指股票
        """
        return AStockGlobalQuery.query_low_valuation_pe(start_date, end_date)

    @staticmethod
    def query_deducted_net_profit_growth_rate_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选扣非净利润同比增长率在指定范围

        quantitative_definition:
            扣非净利润同比增长率在指定范围内的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 直接调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(
            AStockFinancial.non_net_profit_yoy, interval, start_date, end_date
        )

    @staticmethod
    def query_revenue_growth_rate_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选营收同比增长率在指定范围

        quantitative_definition:
            营业总收入同比增长率在指定范围内的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 直接调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(AStockFinancial.income_total_yoy, interval, start_date, end_date)

    @staticmethod
    def query_deducted_non_recurring_net_profit_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选扣非净利润在指定范围

        quantitative_definition:
            扣非净利润在指定范围内的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 直接调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(AStockFinancial.non_net_profit, interval, start_date, end_date)

    @staticmethod
    def query_desai_xiwei(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选德赛西威股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002920")

    def query_stocks_with_highest_capital_inflow(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选资金流入最多的股票
        """
        return AStockGlobalQuery.query_top_n_by_field(
            1, "stock", start_date, end_date, field=AStockGlobalQuery.get_zhu_in_days(1)
        )

    @staticmethod
    def query_stocks_with_good_fundamentals(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选基本面好的股票

        quantitative_definition:
            公司基本面（盈利能力、成长性、财务健康度）表现优秀，同时资金面（主力资金流入、机构持仓）呈现积极状态，是基本面和资金面双重驱动的优质标的。
            数据来源：财务报表数据 + 资金流向数据 + 股东持股数据
            选股规则：

            基本面好：
            ROE ≥ 15%
            且
            归母净利润同比增长率 ≥ 20%
            且
            营业收入同比增长率 ≥ 15%
            且
            销售毛利率 ≥ 行业平均水平
        """
        # 条件1: ROE ≥ 15%（获取最新一期财报）
        roe_ranked = (
            select(
                AStockFinancial.secucode,
                AStockFinancial.roe,
                func.row_number()
                .over(partition_by=AStockFinancial.secucode, order_by=desc(AStockFinancial.date))
                .label("rn"),
            )
            .where(AStockFinancial.roe.isnot(None))
            .subquery("roe_ranked")
        )
        roe_high = select(roe_ranked.c.secucode).where(roe_ranked.c.rn == 1, roe_ranked.c.roe >= 0.15)

        # 条件2: 归母净利润同比增长率 ≥ 20%（获取最新一期财报）
        profit_yoy_ranked = (
            select(
                AStockFinancial.secucode,
                AStockFinancial.parent_net_profit_yoy,
                func.row_number()
                .over(partition_by=AStockFinancial.secucode, order_by=desc(AStockFinancial.date))
                .label("rn"),
            )
            .where(AStockFinancial.parent_net_profit_yoy.isnot(None))
            .subquery("profit_yoy_ranked")
        )
        profit_yoy_high = select(profit_yoy_ranked.c.secucode).where(
            profit_yoy_ranked.c.rn == 1, profit_yoy_ranked.c.parent_net_profit_yoy >= 0.20
        )

        # 条件3: 营业收入同比增长率 ≥ 15%（获取最新一期财报）
        income_yoy_ranked = (
            select(
                AStockFinancial.secucode,
                AStockFinancial.income_total_yoy,
                func.row_number()
                .over(partition_by=AStockFinancial.secucode, order_by=desc(AStockFinancial.date))
                .label("rn"),
            )
            .where(AStockFinancial.income_total_yoy.isnot(None))
            .subquery("income_yoy_ranked")
        )
        income_yoy_high = select(income_yoy_ranked.c.secucode).where(
            income_yoy_ranked.c.rn == 1, income_yoy_ranked.c.income_total_yoy >= 0.15
        )

        # 条件4: 销售毛利率 ≥ 行业平均水平（获取最新一期财报）
        gross_margin_with_industry = (
            select(
                AStockFinancial.secucode,
                AStockFinancial.gross_margin,
                AStockBasic.trade,
                func.row_number()
                .over(partition_by=AStockFinancial.secucode, order_by=desc(AStockFinancial.date))
                .label("rn"),
                func.avg(AStockFinancial.gross_margin)
                .over(partition_by=AStockBasic.trade)
                .label("industry_avg_gross_margin"),
            )
            .join(AStockBasic, AStockFinancial.secucode == AStockBasic.secucode)
            .where(AStockFinancial.gross_margin.isnot(None))
            .subquery("gross_margin_with_industry")
        )
        gross_margin_above_industry = select(gross_margin_with_industry.c.secucode).where(
            gross_margin_with_industry.c.rn == 1,
            gross_margin_with_industry.c.gross_margin >= gross_margin_with_industry.c.industry_avg_gross_margin,
        )

        # 使用 intersect 组合四个条件 (AND 关系)
        return intersect(roe_high, profit_yoy_high, income_yoy_high, gross_margin_above_industry)

    @staticmethod
    def query_stocks_with_good_technical_fundamentals(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选技术面好的股票

        quantitative_definition:
            股票技术形态表现良好，包括趋势向上、均线多头排列、成交量配合、突破关键技术位等。技术面好意味着当前股价走势健康，具备继续上涨的潜力。
            数据来源：日线OHLC数据 + 均线数据 + 成交数据
            选股规则：

            趋势向好：
            MA5 > MA10 > MA20 > MA60（均线多头排列）
            且
            MA20 > MA20昨日（20日均线向上）

            技术形态强势：
            今日收盘价 > MA20
            且
            近5日涨幅 > 0
            且
            近20日涨幅排名前30%

            成交量配合：
            量比 > 1
            或
            近5日平均成交量 > 过去20日平均成交量
        """
        # 获取所有A股作为基础查询
        base_query = select(AStockBasic.secucode)

        # ==================== 准备CTE数据 ====================

        # 获取MA5, MA10, MA20, MA60数据（需要60天历史数据）
        ma_5_cte = AStockGlobalQuery._get_ma_days(start_date, end_date, base_query, n=5)
        ma_10_cte = AStockGlobalQuery._get_ma_days(start_date, end_date, base_query, n=10)
        ma_20_cte = AStockGlobalQuery._get_ma_days(start_date, end_date, base_query, n=20)
        ma_60_cte = AStockGlobalQuery._get_ma_days(start_date, end_date, base_query, n=60)

        # 获取5日涨跌幅
        zdf_5_cte = AStockGlobalQuery._get_zdf_days(start_date, end_date, base_query, days=5)

        # 获取20日涨跌幅
        zdf_20_cte = AStockGlobalQuery._get_zdf_days(start_date, end_date, base_query, days=20)

        # ==================== 条件1：均线多头排列 (MA5 > MA10 > MA20 > MA60) ====================
        ma_bullish = (
            select(ma_5_cte.c.secucode)
            .select_from(ma_5_cte)
            .join(ma_10_cte, ma_5_cte.c.secucode == ma_10_cte.c.secucode)
            .join(ma_20_cte, ma_5_cte.c.secucode == ma_20_cte.c.secucode)
            .join(ma_60_cte, ma_5_cte.c.secucode == ma_60_cte.c.secucode)
            .where(
                ma_5_cte.c.ma_5 > ma_10_cte.c.ma_10,
                ma_10_cte.c.ma_10 > ma_20_cte.c.ma_20,
                ma_20_cte.c.ma_20 > ma_60_cte.c.ma_60,
            )
        )

        # ==================== 条件2：MA20向上 (MA20今日 > MA20昨日) ====================
        # 需要获取MA20的历史数据来比较
        # 使用row_number()获取最新的两条MA20数据
        ma_20_with_rn = (
            select(
                AStockMarket.secucode,
                func.avg(AStockMarket.close)
                .over(
                    partition_by=AStockMarket.secucode,
                    order_by=AStockMarket.date,
                    rows=(-19, 0),
                )
                .label("ma_20"),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.secucode.in_(select(base_query.c.secucode)))
            .where(
                AStockMarket.date.between(
                    get_date(start_date, is_start=True, lookback_days=20),
                    get_date(end_date),
                )
            )
            .subquery()
        )

        ma_20_today = ma_20_with_rn.alias("t1")
        ma_20_yesterday = ma_20_with_rn.alias("t2")

        ma20_rising = (
            select(ma_20_today.c.secucode)
            .select_from(ma_20_today)
            .join(
                ma_20_yesterday,
                and_(
                    ma_20_today.c.secucode == ma_20_yesterday.c.secucode,
                    ma_20_today.c.rn == 1,
                    ma_20_yesterday.c.rn == 2,
                ),
            )
            .where(ma_20_today.c.ma_20 > ma_20_yesterday.c.ma_20)
        )

        # ==================== 条件3：技术形态强势 ====================
        # 3.1 收盘价 > MA20
        price_above_ma20 = (
            select(ma_20_cte.c.secucode)
            .select_from(ma_20_cte)
            .join(
                AStockMarket,
                and_(
                    AStockMarket.secucode == ma_20_cte.c.secucode,
                    AStockMarket.date == ma_20_cte.c.date,
                ),
            )
            .where(AStockMarket.close > ma_20_cte.c.ma_20)
        )

        # 3.2 近5日涨幅 > 0
        zdf_5_positive = select(zdf_5_cte.c.secucode).where(zdf_5_cte.c.zdf_5 > 0)

        # 3.3 近20日涨幅排名前30%
        # 计算20日涨幅并排名
        zdf_20_with_rank = (
            select(
                zdf_20_cte.c.secucode,
                zdf_20_cte.c.zdf_20,
                func.percent_rank().over(order_by=zdf_20_cte.c.zdf_20.desc()).label("pct_rank"),
            )
            .where(zdf_20_cte.c.zdf_20.isnot(None))
            .subquery()
        )

        # 前30%意味着排名 >= 0.70 (percent_rank从0到1)
        zdf_20_top30 = select(zdf_20_with_rank.c.secucode).where(zdf_20_with_rank.c.pct_rank >= 0.70)

        # ==================== 条件4：成交量配合 ====================
        # 4.1 量比 > 1
        volume_ratio_high = select(AStockMarketCur.secucode).where(
            AStockMarketCur.date.between(get_date(start_date), get_date(end_date)),
            AStockMarketCur.qrr > 1,
        )

        # 4.2 近5日平均成交量 > 过去20日平均成交量
        vol_with_avg = (
            select(
                AStockMarket.secucode,
                func.avg(AStockMarket.volume)
                .over(
                    partition_by=AStockMarket.secucode,
                    order_by=AStockMarket.date,
                    rows=(-4, 0),
                )
                .label("avg_vol_5"),
                func.avg(AStockMarket.volume)
                .over(
                    partition_by=AStockMarket.secucode,
                    order_by=AStockMarket.date,
                    rows=(-19, 0),
                )
                .label("avg_vol_20"),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.secucode.in_(select(base_query.c.secucode)))
            .where(
                AStockMarket.date.between(
                    get_date(start_date, is_start=True, lookback_days=20),
                    get_date(end_date),
                )
            )
            .subquery()
        )

        volume_increasing = select(vol_with_avg.c.secucode).where(
            vol_with_avg.c.rn == 1, vol_with_avg.c.avg_vol_5 > vol_with_avg.c.avg_vol_20
        )

        # ==================== 组合条件 ====================
        # 成交量配合：量比>1 OR 成交量递增
        volume_condition = union(volume_ratio_high, volume_increasing)

        # 技术形态强势：收盘价>MA20 AND 5日涨幅>0 AND 20日涨幅前30%
        technical_strength = intersect(price_above_ma20, zdf_5_positive, zdf_20_top30)

        # 趋势向好：均线多头排列 AND MA20向上
        trend_good = intersect(ma_bullish, ma20_rising)

        # 最终结果：趋势向好 AND 技术形态强势 AND 成交量配合
        result = intersect(trend_good, technical_strength, volume_condition)

        return result

    @staticmethod
    def query_stock_price_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选收盘价(股价)在指定范围

        quantitative_definition:
            收盘价(股价)在指定范围内的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 直接调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_close(), interval, start_date, end_date)

    @staticmethod
    def query_storage_chip_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选存储芯片概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02523", start_date, end_date)

    @staticmethod
    def query_market_cap_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选市值在指定范围

        quantitative_definition:
            总市值在指定范围内的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_market_cap(), interval, start_date, end_date
        )

    @staticmethod
    def query_limit_up_times_within_n_days_in_specified_range(
        n: int,
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选n日内涨停次数在指定范围
        """
        # 获取所有A股股票作为基础查询（过滤停牌）
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)

        # 调用 _get_zt_days 获取N日涨停次数 CTE
        zt_count_cte = AStockGlobalQuery._get_zt_days(
            start_date=start_date, end_date=end_date, secucode_query=all_stocks_query, days=n
        )

        # 构建范围条件
        if low_closed:
            low_condition = zt_count_cte.c[f"zt_{n}d_count"] >= low
        else:
            low_condition = zt_count_cte.c[f"zt_{n}d_count"] > low

        if high_closed:
            high_condition = zt_count_cte.c[f"zt_{n}d_count"] <= high
        else:
            high_condition = zt_count_cte.c[f"zt_{n}d_count"] < high

        return select(zt_count_cte.c.secucode).where(and_(low_condition, high_condition))

    @staticmethod
    def query_limit_up(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选涨停
        """
        return AStockGlobalQuery.query_zt(1, start_date, end_date)

    @staticmethod
    def query_non_delisted(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选非退市的股票
        """
        return AStockGlobalQuery.query_is_ts(0, start_date, end_date)

    @staticmethod
    def query_golden_cross(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选金叉的股票
        """
        return AStockGlobalQuery.query_ma_jincha(1, start_date, end_date)

    @staticmethod
    def query_golden_pit_oversold_stock_pool(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选黄金坑超跌选股池出现的股票
        """
        return AStockGlobalQuery.query_oversold_pool(start_date, end_date)

    @staticmethod
    def query_golden_pit_strategy_selected_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选黄金坑战法入选股票

        quantitative_definition:
            入选日线的轮动战法、黄金坑战法下
        """
        start = get_date(start_date, is_start=True)
        end = get_date(end_date)

        # 条件1：轮动股票（ldmarket、ldtrade、ld0z 任一不为0）
        rotation_stocks = (
            select(DailyQlldLevel5.secucode)
            .where(
                and_(
                    or_(
                        DailyQlldLevel5.ldmarket.in_([1, 2, 3, 4, 5]),
                        DailyQlldLevel5.ldtrade.in_([1, 2, 3, 4, 5]),
                        DailyQlldLevel5.ld0z.in_([1, 2, 3, 4, 5]),
                    ),
                    DailyQlldLevel5.date.between(start, end),
                )
            )
            .subquery()
        )

        # 条件2：超跌选股池
        oversold_subquery = (
            select(
                func.arrayJoin(func.splitByChar(",", func.coalesce(DailyHjkpoolsLevel5.stock_pools, ""))).label(
                    "secucode"
                ),
            )
            .where(
                and_(
                    DailyHjkpoolsLevel5.filter_conditions == "0:1:1:0",
                    DailyHjkpoolsLevel5.date.between(start, end),
                )
            )
            .subquery()
        )

        # 条件3：突破选股池
        breakthrough_subquery = (
            select(
                func.arrayJoin(func.splitByChar(",", func.coalesce(DailyHjkpoolsLevel5.stock_pools, ""))).label(
                    "secucode"
                ),
            )
            .where(
                and_(
                    DailyHjkpoolsLevel5.filter_conditions == "0:3:1:0",
                    DailyHjkpoolsLevel5.date.between(start, end),
                )
            )
            .subquery()
        )

        # 使用 UNION 合并三个条件
        return union(
            select(rotation_stocks.c.secucode),
            select(oversold_subquery.c.secucode),
            select(breakthrough_subquery.c.secucode),
        )

    def query_start_rotation_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选开始轮动的股票
        """
        # 获取三个维度的开始轮动股票
        ldmarket_query = AStockGlobalQuery.query_ldmarket_start_rotation(start_date, end_date)
        ldtrade_query = AStockGlobalQuery.query_ldtrade_start_rotation(start_date, end_date)
        ld0z_query = AStockGlobalQuery.query_ld0z_start_rotation(start_date, end_date)

        # 使用union合并三个查询
        return ldmarket_query.union(ldtrade_query, ld0z_query)

    @staticmethod
    def query_hot_concepts(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选热点概念
        """
        ...

    @staticmethod
    def query_short_position_opening(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选短线开仓
        """
        return AStockGlobalQuery.query_short_policy_status(1, start_date, end_date)

    @staticmethod
    def query_band_opening_position(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选波段开仓
        """
        return AStockGlobalQuery.query_op_policy_status(1, start_date, end_date)

    @staticmethod
    def query_continuous_capital_inflow(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选资金持续流入
        """
        ...

    @staticmethod
    def query_high_control(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选高控盘

        quantitative_definition:
            主力资金持有大量流通筹码，对股价走势有较强控制力
            数据来源：股东持股比例数据 + 换手率数据 + 机构持股数据
            选股规则：

            前10大流通股东持股比例 > 60%  ⚠️ 数据未提供，暂时未使用
            且
            近20日平均换手率 < 5%
            且
            机构持股比例 > 30%
        """
        # 计算日期范围：需要回溯20-1=19天来计算20日平均
        start = get_date(start_date, is_start=True, lookback_days=19)
        end = get_date(end_date)

        # ==================== 条件1：近20日平均换手率 < 5% ====================
        # 计算近20日平均换手率
        avg_turnover = func.avg(AStockMarket.toratio).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-19, 0),  # ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )

        turnover_data = (
            select(
                AStockMarket.secucode,
                cast(avg_turnover, Float).label("avg_turnover_20"),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=desc(AStockMarket.date))
                .label("rn"),
            )
            .where(AStockMarket.date.between(start, end))
            .subquery()
        )

        # 筛选最新一天的数据，平均换手率 < 5%
        condition_1 = select(turnover_data.c.secucode).where(
            turnover_data.c.rn == 1, turnover_data.c.avg_turnover_20 < 0.05
        )

        # ==================== 条件2：机构持股比例 > 30% ====================
        condition_2 = select(AStockMarketCur.secucode).where(AStockMarketCur.organ_hold_ratio_total > 0.30)

        # ==================== 组合条件：AND 关系 ====================
        return intersect(condition_1, condition_2)

    @staticmethod
    def query_china_first_strategic_high_energy_hydrogen_ion_implanter_related(
        start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选中国首台战略型高能氢离子注入机相关
        """
        ...

    @staticmethod
    def query_strong_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选强势股
        """
        return AStockGlobalQuery.query_strong_stock(start_date, end_date)

    @staticmethod
    def query_china_life_0051_001_sh(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选中国人寿0051一001沪
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ600320",
                    "SHHQ600031",
                    "SHHQ603338",
                    "SHHQ601233",
                    "SHHQ600760",
                ]
            )
        )

    @staticmethod
    def query_china_satellite(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选中国卫星股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600118")

    def query_positions_increased(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选增仓的股票
        """
        # 获取所有A股股票作为基础查询（过滤停牌）
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)

        # 调用 _calculate_diff 获取1日主力资金增仓 CTE
        zhur_in_cte = AStockGlobalQuery._calculate_diff(
            field=DailyZjdxStatLevel5.zhur,
            days=1,
            start_date=start_date,
            end_date=end_date,
            secucode_query=all_stocks_query,
            calculation_mode="positive",
            filter_positive=True,
        )

        # 返回增仓的股票（filter_positive=True 已确保所有值 > 0）
        return select(zhur_in_cte.c.secucode)

    @staticmethod
    def query_positive_dde_large_orders(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选DDE大单为正
        """
        ...

    def query_serial_high_energy_hydrogen_ion_implantation_machine_related(
        start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选串列型高能氢离子注入机相关的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ600641",
                    "SZHQ002371",
                    "SHHQ688120",
                    "BJHQ920402",
                    "SZHQ300260",
                    "SZHQ300666",
                    "SHHQ688409",
                    "SHHQ688396",
                    "SHHQ600460",
                    "SHHQ603290",
                ]
            )
        )

    @staticmethod
    def query_fifteen_five_bullish_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选十五五利好股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ600550",
                    "SZHQ300882",
                    "SZHQ002498",
                ]
            )
        )

    @staticmethod
    def query_power_grid(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电网概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02080", start_date, end_date)

    @staticmethod
    def query_shandong(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选山东板块的股票
        """
        return AStockGlobalQuery.query_area("BR01B03023", start_date, end_date)

    @staticmethod
    def query_heating_power_generation(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选供热发电
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ001376",
                    "SZHQ001210",
                    "SHHQ600052",
                    "SHHQ603071",
                    "SHHQ605162",
                    "SZHQ300335",
                    "SZHQ002479",
                    "SHHQ600167",
                    "SHHQ600226",
                    "SHHQ600475",
                    "SHHQ600982",
                    "SHHQ600149",
                    "SZHQ000692",
                    "SHHQ600719",
                    "SZHQ002893",
                    "SHHQ605011",
                    "SHHQ605580",
                    "SHHQ605028",
                ]
            )
        )

    @staticmethod
    def query_technological_innovation_and_transformation(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选技术创新和改造的股票
        """
        ...

    @staticmethod
    def query_power_transmission_and_distribution_equipment_leading_enterprises(
        start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选输变电设备龙头企业
        """
        concept_query = AStockGlobalQuery.query_trade("BR01B01022", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_technology_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选科技板块的股票
        """
        return select(AStockBasic.secucode).where(
            or_(
                AStockBasic.concept.like(f"%BR01B02191%"),  # 人工智能
                AStockBasic.concept.like(f"%BR01B01031%"),  # 半导体
                AStockBasic.concept.like(f"%BR01B02512%"),  # CPO
                AStockBasic.concept.like(f"%BR01B02567%"),  # 需要查找
                AStockBasic.concept.like(f"%BR01B02579%"),  # 商业航天
                AStockBasic.concept.like(f"%BR01B02024%"),  # 航天军工
                AStockBasic.concept.like(f"%BR01B02551%"),  # 机器人
                AStockBasic.concept.like(f"%BR01B02182%"),  # 需要查找
            )
        )

    @staticmethod
    def query_annual_report_forecast_increase(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选年报预增的股票
        """
        # 获取业绩预告盈利大增和盈利略增的股票
        big_increase_query = AStockGlobalQuery.query_performance_forecast_big_increase(start_date, end_date)
        small_increase_query = AStockGlobalQuery.query_performance_forecast_small_increase(start_date, end_date)

        # 使用union合并两个查询
        return big_increase_query.union(small_increase_query)

    @staticmethod
    def query_main_control_line_and_attack_line_gold_cross(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主力控盘线与攻击线金叉的股票
        """
        ...

    @staticmethod
    def query_5_day_10_day_20_day_moving_average_triple_golden_cross(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选5日、10日、20日均线三线金叉的股票

        quantitative_definition:
            昨天：MA5 < MA10 < MA20（空头排列）
            今天：MA5 > MA10 > MA20（多头排列）
            （三线同时从空头转为多头）
        """
        # 需要回溯20-1=19天来计算MA20，再加1天用于昨天数据
        start = get_date(start_date, is_start=True, lookback_days=20)
        end = get_date(end_date)

        # 计算MA5、MA10、MA20
        ma5_value = func.avg(AStockMarket.close).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-4, 0),  # ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )

        ma10_value = func.avg(AStockMarket.close).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-9, 0),  # ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        )

        ma20_value = func.avg(AStockMarket.close).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-19, 0),  # ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )

        # 创建带rn的MA数据CTE
        ma_cte = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                cast(ma5_value, Float).label("ma5"),
                cast(ma10_value, Float).label("ma10"),
                cast(ma20_value, Float).label("ma20"),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.date.between(start, end))
            .subquery()
        )

        # 创建别名进行自连接
        t1 = ma_cte.alias("t1")  # 今天 (rn=1)
        t2 = ma_cte.alias("t2")  # 昨天 (rn=2)

        # 检测三线金叉：昨天空头，今天多头
        # 明确指定：t1=rn=1（今天），t2=rn=2（昨天）
        triple_golden_cross = (
            select(t1.c.secucode)
            .select_from(t1)
            .join(
                t2,
                and_(
                    t1.c.secucode == t2.c.secucode,
                    t1.c.rn == 1,  # t1必须是rn=1（今天）
                    t2.c.rn == 2,  # t2必须是rn=2（昨天）
                ),
            )
            .where(
                and_(
                    # 昨天：空头排列 MA5 < MA10 < MA20
                    t2.c.ma5 < t2.c.ma10,
                    t2.c.ma10 < t2.c.ma20,
                    # 今天：多头排列 MA5 > MA10 > MA20
                    t1.c.ma5 > t1.c.ma10,
                    t1.c.ma10 > t1.c.ma20,
                )
            )
            .distinct()
        )

        return triple_golden_cross

    @staticmethod
    def query_yunnan_chengtou(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选云南城投股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600239")

    @staticmethod
    def query_zhejiang_culture(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选浙数文化股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600633")

    def query_trend_upward_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选趋势向上的股票
        """
        return AStockGlobalQuery.query_ma_dtpl(1, start_date, end_date)

    @staticmethod
    def query_liquid_cooling_concept(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选液冷概念
        """
        return AStockGlobalQuery.query_concept("BR01B02524", start_date, end_date)

    @staticmethod
    def query_high_dividend_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选高股息个股

        quantitative_definition: 股息率≥0.04
        """
        # 筛选 A股 且 非退市
        all_stocks_query = select(AStockBasic.secucode)

        start = get_date(start_date, is_start=True)
        end = get_date(end_date)

        # 获取股息率大于0.04的股票
        return (
            select(AStockMarketCur.secucode)
            .select_from(all_stocks_query)
            .join(
                AStockMarketCur,
                and_(
                    AStockMarketCur.secucode == all_stocks_query.c.secucode,
                    AStockMarketCur.date.between(start, end),
                ),
            )
            .where(AStockMarketCur.dividend_yield >= 0.04)
            .distinct()
        )

    @staticmethod
    def query_stock_price_at_bottom(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选股价处于底部
        """
        return AStockGlobalQuery.query_low_price_pool(start_date, end_date)

    @staticmethod
    def query_stocks_selected_by_moving_average_pattern(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选按均线形态选股的股票
        """
        ...

    @staticmethod
    def query_n_day_moving_average_upward(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选n日均线上移的股票

        quantitative_definition:
            移动平均线数值呈现上升趋势，表明股价中长期趋势向上。均线上移是趋势向好的重要信号，不同周期的均线上移对应不同级别的上涨趋势。
            数据来源：均线数据（MA5、MA10、MA20、MA60等）
            选股规则：

            MA今日 > MA昨日
            且
            连续3日MA数值递增
        """
        # 参考 AStockGlobalQuery._get_ma_days 的实现模式
        # lookback_days=n+2 确保有足够的数据进行3日连续比较
        start = get_date(start_date, is_start=True, lookback_days=n + 2)
        end = get_date(end_date)

        # 计算n日均线值（与_get_ma_days相同的窗口函数逻辑）
        ma_value = func.avg(AStockMarket.close).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-(n - 1), 0),
        )

        # 创建MA CTE（与_get_ma_days类似，但保留多日数据用于比较）
        # _get_ma_days只返回rn=1（最新一天），这里需要多天进行连续比较
        ma_cte = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                cast(ma_value, Float).label("ma_value"),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.date.between(start, end))
            .subquery()
        )

        # 创建别名用于自连接
        t1 = ma_cte.alias("t1")
        t2 = ma_cte.alias("t2")
        t3 = ma_cte.alias("t3")

        # 自连接：比较最新3日的MA值（rn=1,2,3）
        # 连接条件确保t1=rn=1, t2=rn=2, t3=rn=3
        ma_trend = (
            select(t1.c.secucode)
            .select_from(t1)
            .join(
                t2,
                and_(
                    t1.c.secucode == t2.c.secucode,
                    t1.c.rn == 1,  # t1必须是rn=1（最新日）
                    t2.c.rn == 2,  # t2必须是rn=2（昨日）
                ),
            )
            .join(
                t3,
                and_(
                    t1.c.secucode == t3.c.secucode,
                    t3.c.rn == 3,  # t3必须是rn=3（前日）
                ),
            )
            .where(
                t1.c.ma_value > t2.c.ma_value,  # 今日>昨日
                t2.c.ma_value > t3.c.ma_value,  # 昨日>前日
            )
            .distinct()
        )

        return ma_trend

    @staticmethod
    def query_kdj_gold_cross(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选KDJ金叉的股票
        """
        return AStockGlobalQuery.query_kdj_jincha(1, start_date, end_date)

    @staticmethod
    def query_macd_gold_cross(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选MACD金叉的股票
        """
        return AStockGlobalQuery.query_macd_jincha(1, start_date, end_date)

    @staticmethod
    def query_beijing_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选北京板块的股票
        """
        return AStockGlobalQuery.query_area("BR01B03001", start_date, end_date)

    @staticmethod
    def query_jiangsu_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选江苏板块
        """
        return AStockGlobalQuery.query_area("BR01B03018", start_date, end_date)

    @staticmethod
    def query_shanghai_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选上海板块的股票
        """
        return AStockGlobalQuery.query_area("BR01B03002", start_date, end_date)

    @staticmethod
    def query_zhejiang_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选浙江地域板块的股票
        """
        return AStockGlobalQuery.query_area("BR01B03030", start_date, end_date)

    @staticmethod
    def query_fujian_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选福建板块的股票
        """
        return AStockGlobalQuery.query_area("BR01B03007", start_date, end_date)

    @staticmethod
    def query_funds_top_n_by_ten_year_return(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选基金十年收益排名前n名
        """
        ...

    def query_guangdong_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选广东板块的股票
        """
        return AStockGlobalQuery.query_area("BR01B03009", start_date, end_date)

    @staticmethod
    def query_guide_infrared(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选高德红外
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002414")

    def query_shenzhen_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选深圳板块的股票
        """
        return AStockGlobalQuery.query_area("BR01B03004", start_date, end_date)

    @staticmethod
    def query_main_capital_inflow_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选主力资金流入在指定范围
        """
        # 1. 获取所有A股股票作为基础查询
        all_stocks_query = select(AStockBasic.secucode)

        # 2. 调用 _calculate_diff 获取1日主力资金流入 CTE
        zhu_in_cte = AStockGlobalQuery._calculate_diff(
            field=DailyZjdxStatLevel5.zhu,
            days=1,
            start_date=start_date,
            end_date=end_date,
            secucode_query=all_stocks_query,
            calculation_mode="positive",
            filter_positive=False,
        )

        # 3. 构建范围条件
        zhu_field = zhu_in_cte.c.zhu_in_1d

        if low_closed:
            low_condition = zhu_field >= low
        else:
            low_condition = zhu_field > low

        if high_closed:
            high_condition = zhu_field <= high
        else:
            high_condition = zhu_field < high

        # 4. 组合条件
        condition = (
            low_condition
            & high_condition
            & DailyZjdxStatLevel5.date.between(get_date(start_date, is_start=True), get_date(end_date))
        )

        # 5. 返回结果
        return select(zhu_in_cte.c.secucode).where(condition)

    @staticmethod
    def query_stocks_with_price_change_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选涨幅在指定范围
        """
        # 1. 获取所有股票作为基础查询
        all_stocks_query = select(AStockMarket.secucode)

        # 2. 调用 _get_zdf_days 获取N日涨跌幅 CTE
        zdf_cte = AStockGlobalQuery._get_zdf_days(
            start_date=start_date, end_date=end_date, secucode_query=all_stocks_query, days=1
        )

        # 3. 构建范围条件 - 动态字段名 zdf_{days}
        zdf_field = getattr(zdf_cte.c, f"zdf_1")

        if low_closed:
            low_condition = zdf_field >= low
        else:
            low_condition = zdf_field > low

        if high_closed:
            high_condition = zdf_field <= high
        else:
            high_condition = zdf_field < high

        # 4. 组合条件
        condition = (
            low_condition
            & high_condition
            & AStockMarket.date.between(get_date(start_date, is_start=True), get_date(end_date))
        )

        # 5. 返回结果
        return select(zdf_cte.c.secucode).where(condition)

    @staticmethod
    def query_non_bj_exchange_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选非北交所股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.market != "BJHQ")

    @staticmethod
    def query_non_star_market_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选非科创板股票
        """
        return AStockGlobalQuery.query_is_kcb(0, start_date, end_date)

    @staticmethod
    def query_solid_state_transformer(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选固态变压器概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ601179",
                    "SZHQ002885",
                    "SZHQ600550",
                    "SHHQ002335",
                    "SZHQ002335",
                    "SZHQ603097",
                    "SHHQ688187",
                    "SHHQ600406",
                    "SHHQ688676",
                    "SHHQ600089",
                ]
            )
        )

    @staticmethod
    def query_hot_money_entering_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选游资进入的股票
        """
        ...

    @staticmethod
    def query_stocks_related_to_anthropic(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选与anthropic相关的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ003032",
                    "SHHQ600845",
                ]
            )
        )

    @staticmethod
    def query_n_day_price_change_within_specified_range(
        n: int,
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选n日涨跌幅在指定范围
        """
        # 1. 获取所有股票作为基础查询
        all_stocks_query = select(AStockMarket.secucode)

        # 2. 调用 _get_zdf_days 获取N日涨跌幅 CTE
        zdf_cte = AStockGlobalQuery._get_zdf_days(
            days=n,
            start_date=start_date,
            end_date=end_date,
            secucode_query=all_stocks_query,
        )

        # 3. 构建范围条件 - 动态字段名 zdf_{n}
        zdf_field = getattr(zdf_cte.c, f"zdf_{n}")

        if low_closed:
            low_condition = zdf_field >= low
        else:
            low_condition = zdf_field > low

        if high_closed:
            high_condition = zdf_field <= high
        else:
            high_condition = zdf_field < high

        # 4. 组合条件（不添加 > 0 过滤，因为涨跌幅可以是负值）
        condition = (
            low_condition
            & high_condition
            & AStockMarket.date.between(get_date(start_date, is_start=True), get_date(end_date))
        )

        # 5. 返回结果
        return select(zdf_cte.c.secucode).where(condition)

    @staticmethod
    def query_n_optimal_etfs(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选n支最优的ETF
        """
        ...

    def query_stocks_with_code_starting_with_688(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选股票代码688开头的上市公司
        """
        prefixes = ["SZHQ688%", "SHHQ688%", "BJHQ688%"]

        return select(AStockBasic.secucode).where(or_(*(AStockBasic.secucode.like(p) for p in prefixes)))

    @staticmethod
    def query_stocks_with_code_starting_with_30(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选股票代码30开头的上市公司
        """
        prefixes = ["SZHQ30%", "SHHQ30%", "BJHQ30%"]

        return select(AStockBasic.secucode).where(or_(*(AStockBasic.secucode.like(p) for p in prefixes)))

    @staticmethod
    def query_multiple_status_rotation_not_yet_started_rising(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选多个状态轮动还没开始上涨的股票

        quantitative_definition:
            对于0z，行业，战区至少2个状态属于轮动中或强势轮动中的股票
            且
            还没开始上涨的股票(涨幅小)
        """
        # 条件1：获取各个维度的轮动中或强势轮动中的股票
        ld0z_rotation = AStockGlobalQuery.query_ld0z_in_rotation(start_date, end_date)
        ld0z_strong_rotation = AStockGlobalQuery.query_ld0z_in_strong_rotation(start_date, end_date)
        ldtrade_rotation = AStockGlobalQuery.query_ldtrade_in_rotation(start_date, end_date)
        ldtrade_strong_rotation = AStockGlobalQuery.query_ldtrade_in_strong_rotation(start_date, end_date)
        ldmarket_rotation = AStockGlobalQuery.query_ldmarket_in_rotation(start_date, end_date)
        ldmarket_strong_rotation = AStockGlobalQuery.query_ldmarket_in_strong_rotation(start_date, end_date)

        # 合并每个维度的"轮动中"和"强势轮动中"
        ld0z_any = union(ld0z_rotation, ld0z_strong_rotation)
        ldtrade_any = union(ldtrade_rotation, ldtrade_strong_rotation)
        ldmarket_any = union(ldmarket_rotation, ldmarket_strong_rotation)

        # 条件2：至少2个维度满足条件 (使用 intersect 组合)
        # (ld0z AND ldtrade) OR (ld0z AND ldmarket) OR (ldtrade AND ldmarket)
        rotation_0z_trade = intersect(ld0z_any, ldtrade_any)
        rotation_0z_market = intersect(ld0z_any, ldmarket_any)
        rotation_trade_market = intersect(ldtrade_any, ldmarket_any)

        rotation_any = union(rotation_0z_trade, rotation_0z_market, rotation_trade_market)

        # 条件3：还没开始上涨（涨幅小）
        small_increase = AStockGlobalQuery.query_small_increase(start_date, end_date)

        # 使用 intersect 组合轮动条件和涨幅条件（AND 关系）
        return intersect(rotation_any, small_increase)

    @staticmethod
    def query_embodied_intelligence_related_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选具身智能相关股票
        """
        return AStockGlobalQuery.query_concept("BR01B02528", start_date, end_date)

    @staticmethod
    def query_emerging_equipment(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选新兴装备概念板块的股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002933")

    @staticmethod
    def query_profit_growth_rate_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选利润增速在指定范围
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(
            AStockFinancial.parent_net_profit_yoy, interval, start_date, end_date
        )

    @staticmethod
    def query_ai_business_ratio_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选AI业务占比在指定范围
        """
        ...

    @staticmethod
    def query_dalian_dian_ci(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选大连电瓷股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002606")

    @staticmethod
    def query_high_institutional_holdings(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选机构持仓量高的股票
        """
        ...

    @staticmethod
    def query_aerospace_and_military_sector_absolute_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选航天军工板块绝对龙头股
        """
        ...

    @staticmethod
    def query_aerospace_and_military_sector_monopoly_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选航天军工板块垄断龙头股
        """
        ...

    @staticmethod
    def query_aerospace_electronics(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选航天电子股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600879")

    @staticmethod
    def query_aerospace_macrotics(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选航天宏图股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ688066")

    def query_excluding_chinext(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选除创业板以外的股票
        """
        return AStockGlobalQuery.query_is_cyb(0, start_date, end_date)

    @staticmethod
    def query_top_n_hot_stocks(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选前n名热门股
        """
        return AStockGlobalQuery.query_hot_stock_info(start_date, end_date).limit(n)

    @staticmethod
    def query_roe_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选ROE在指定范围
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_roe(), interval, start_date, end_date)

    @staticmethod
    def query_huashengtiancheng(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选华胜天成股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600410")

    @staticmethod
    def query_inovance_technology(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选汇川技术股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ300124")

    @staticmethod
    def query_changdian_technology(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选长电科技股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600584")

    @staticmethod
    def query_chaojie_shares(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选超捷股份股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ301005")

    def query_potential_dragon_head_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选潜伏龙头股

        quantitative_definition:
            ROE > 15%
            且
            净利润增长率 > 20%
            且
            市值50-200亿
            且
            近60日涨幅 < 20%
            且
            连续10日的1日主力资金净流入
        """
        # 条件1: ROE > 15% 且 净利润增长率 > 20%
        roe_condition = AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_roe(), "(0.15,inf)", start_date, end_date
        )
        profit_growth_condition = AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_yoy(AStockGlobalQuery.get_parent_net_profit()),
            "(0.20,inf)",
            start_date,
            end_date,
        )
        financial_condition = intersect(roe_condition, profit_growth_condition)

        # 条件2: 市值50-200亿
        market_cap_condition = AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_market_cap(), "[5e9,2e10]", start_date, end_date
        )

        # 条件3: 近60日涨幅 0% < zdf < 20%
        zdf_60_cte = AStockGlobalQuery._get_zdf_days(start_date, end_date, select(AStockMarket.secucode), days=60)
        zdf_60_query = select(zdf_60_cte.c.secucode).select_from(zdf_60_cte).where(text("zdf_60 > 0 AND zdf_60 < 0.20"))

        # 条件4: 连续10日的1日主力资金净流入
        _, start_fund, end_fund, _ = AStockGlobalQuery._get_aftermarket_date_range(
            DailyZjdxStatLevel5, start_date, end_date, lookback_days=10
        )

        consecutive_main_fund = (
            select(
                DailyZjdxStatLevel5.secucode,
                func.count(case((DailyZjdxStatLevel5.zhu > 0, 1)))
                .over(
                    partition_by=DailyZjdxStatLevel5.secucode,
                    order_by=DailyZjdxStatLevel5.date,
                    rows=(-9, 0),  # 最近10天（包括今天）
                )
                .label("consecutive_positive_days"),
            )
            .where(DailyZjdxStatLevel5.date.between(start_fund, end_fund))
            .subquery()
        )

        fund_condition = select(consecutive_main_fund.c.secucode).where(
            consecutive_main_fund.c.consecutive_positive_days >= 10
        )

        # 使用intersect组合所有条件（AND逻辑）
        return intersect(financial_condition, market_cap_condition, zdf_60_query, fund_condition)

    @staticmethod
    def query_future_breakthrough_n_day_line_stocks(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选未来突破n日线的个股
        """
        ...

    @staticmethod
    def query_langchao_information(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选浪潮信息股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ000977")

    @staticmethod
    def query_kunlun_wanwei(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选昆仑万维股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ300418")

    @staticmethod
    def query_consumption_recovery_concept(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选消费复苏概念
        """
        ...

    @staticmethod
    def query_main_force_lurking(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主力潜伏的股票

        quantitative_definition:
            主力资金在股价未大涨时悄然建仓，温和吸筹
            数据来源：资金流向数据 + 日线行情数据 + 成交量数据
            选股规则：

            连续10日的1日主力资金连续净流入 > 0
            且
            近20日涨幅 < 15%
            且
            1 < 量比 < 2
            且
            换手率 3%-8%
        """

        # ==================== 条件1：连续10日的1日主力资金连续净流入 > 0 ====================
        condition_1 = StockNewApi.query_continuous_capital_inflow_for_n_days(10, start_date, end_date)

        # ==================== 条件2：近20日涨幅 0% < 涨幅 < 15% ====================
        base_query = select(AStockBasic.secucode)
        zdf_20_cte = AStockGlobalQuery._get_zdf_days(start_date, end_date, base_query, days=20)
        condition_2 = select(zdf_20_cte.c.secucode).where(zdf_20_cte.c.zdf_20 > 0, zdf_20_cte.c.zdf_20 < 0.15)

        # ==================== 条件3：1 < 量比 < 2 ====================
        condition_3 = select(AStockMarketCur.secucode).where(
            AStockMarketCur.date.between(get_date(start_date), get_date(end_date)),
            AStockMarketCur.qrr > 1,
            AStockMarketCur.qrr < 2,
        )

        # ==================== 条件4：换手率 3%-8% ====================
        condition_4 = select(AStockMarketCur.secucode).where(
            AStockMarketCur.date.between(get_date(start_date), get_date(end_date)),
            AStockMarketCur.toratio >= 0.03,
            AStockMarketCur.toratio <= 0.08,
        )

        # ==================== 组合条件：所有条件都要满足 ====================
        result = intersect(condition_1, condition_2, condition_3, condition_4)

        return result

    @staticmethod
    def query_main_force_lurking_condition_1(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 主力潜伏 - 条件1：连续10日的1日主力资金连续净流入 > 0

        quantitative_definition:
            连续10个交易日，每天的1日主力资金净流入都 > 0
        """
        return StockNewApi.query_continuous_capital_inflow_for_n_days(10, start_date, end_date)

    @staticmethod
    def query_sanbian_technology(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选三变科技股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002112")

    @staticmethod
    def query_main_companies_in_power_grid_with_investment_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选电网的主要公司筛选投资在指定范围主要股票公司
        """
        ...

    @staticmethod
    def query_huachen_equipment(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选华辰装备股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ300809")

    @staticmethod
    def query_rotating_sectors(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选轮动的板块
        """
        # 获取四个轮动状态的板块
        in_rotation_query = AStockGlobalQuery.query_ld_in_rotation(start_date, end_date)
        in_strong_rotation_query = AStockGlobalQuery.query_ld_in_strong_rotation(start_date, end_date)
        start_strong_rotation_query = AStockGlobalQuery.query_ld_start_strong_rotation(start_date, end_date)
        start_rotation_query = AStockGlobalQuery.query_ld_start_rotation(start_date, end_date)

        # 使用 union 合并四个查询
        return in_rotation_query.union(in_strong_rotation_query, start_strong_rotation_query, start_rotation_query)

    @staticmethod
    def query_power_grid_equipment_concept_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电网设备概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02647", start_date, end_date)

    @staticmethod
    def query_a_share_main_board(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选A股主榜
        """
        return AStockGlobalQuery.query_is_hszb(1, start_date, end_date)

    @staticmethod
    def query_supply_and_demand_funds_large_inflow(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选供求资金大量进入的股票
        """
        ...

    @staticmethod
    def query_stocks_with_similar_trend_to_jiaoda_angli(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选与交大昂立相似走势的股票
        """
        ...

    @staticmethod
    def query_stocks_with_ma_in_name(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选名字中含马字的股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secuname.like("%马%"))

    @staticmethod
    def query_robot_concept_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选机器人概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02551", start_date, end_date)

    @staticmethod
    def query_main_force_highly_controlled_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主力高度控盘的股票
        """
        return StockNewApi.query_high_control(start_date, end_date)

    @staticmethod
    def query_stocks_not_significantly_pulled_up(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选未大幢拉升的股票
        """
        return AStockGlobalQuery.query_small_increase(start_date, end_date)

    @staticmethod
    def query_sectors_with_highest_fund_flow_in_recent_n_days(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选近n日资金流最多的板块
        """
        return AStockGlobalQuery.query_top_n_by_field(
            1, "board", start_date, end_date, field=AStockGlobalQuery.get_zhu_in_days(n)
        )

    @staticmethod
    def query_securities_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选券商板块的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    # 头部券商
                    "SHHQ600030",  # 中信证券
                    "SHHQ601066",  # 中信建投
                    "SHHQ601688",  # 华泰证券
                    "SHHQ601211",  # 国泰海通
                    "SHHQ600837",  # 海通证券
                    "SZHQ000166",  # 申万宏源
                    "SHHQ600999",  # 招商证券
                    "SZHQ300059",  # 东方财富
                    "SZHQ002736",  # 国信证券
                    "SHHQ601881",  # 中国银河
                    # 中大型券商
                    "SZHQ000776",  # 广发证券
                    "SHHQ601995",  # 中金公司
                    "SHHQ601788",  # 光大证券
                    "SZHQ000783",  # 长江证券
                    "SHHQ601901",  # 方正证券
                    "SHHQ600958",  # 东方证券
                    # 中小型券商
                    "SHHQ601136",  # 首创证券
                    "SHHQ601059",  # 信达证券
                    "SHHQ601198",  # 东兴证券
                    "SHHQ600095",  # 湘财股份
                    "SZHQ002670",  # 国盛证券
                    "SZHQ002939",  # 长城证券
                    "SZHQ000686",  # 东北证券
                    "SZHQ000750",  # 国海证券
                    "SHHQ601099",  # 太平洋
                    "SZHQ002500",  # 山西证券
                    "SZHQ002673",  # 西部证券
                    "SZHQ002797",  # 第一创业
                    "SHHQ601236",  # 红塔证券
                    "SHHQ601878",  # 浙商证券
                    "SHHQ600909",  # 华安证券
                    "SHHQ601108",  # 财通证券
                    "SHHQ601990",  # 南京证券
                    "SZHQ002926",  # 华西证券
                    "SHHQ601456",  # 国联民生
                    # 补充券商股
                    "SHHQ601377",  # 兴业证券
                    "SHHQ600061",  # 国投资本
                    "SHHQ601375",  # 中原证券
                    "SHHQ600621",  # 华鑫股份
                    "SHHQ600109",  # 国金证券
                    "SHHQ600918",  # 中泰证券
                    "SZHQ000728",  # 国元证券
                    "SHHQ601696",  # 中银证券
                    "SHHQ601162",  # 天风证券
                    "SHHQ601555",  # 东吴证券
                    "SHHQ600906",  # 财达证券
                    "SHHQ600864",  # 哈投股份
                    "SZHQ000712",  # 锦龙股份
                    "SZHQ002945",  # 华林证券
                    "SHHQ600155",  # 华创云信
                    "SHHQ600369",  # 西南证券
                ]
            )
        )

    @staticmethod
    def query_stocks_with_highest_cash_flow_in_recent_n_days(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选近n日资金流最多的个股
        """
        return AStockGlobalQuery.query_top_n_by_field(
            1, "stock", start_date, end_date, field=AStockGlobalQuery.get_zhu_in_days(n)
        )

    @staticmethod
    def query_aerospace_science_and_technology(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选航天科技股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ000901")

    @staticmethod
    def query_breakthrough_infinite_cost_average(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选突破无穷成本均线的个股

        quantitative_definition:
            无穷成本均线是历史上所有持仓筹码的平均成本线，突破该线意味着股价越过市场平均持仓成本，通常预示着趋势反转或加速上涨，是重要的技术信号。
            数据来源：成交数据（历史成交量和成交价格）+ 均线数据
            选股规则：

            无穷成本均线 = SUM(成交价 × 成交量) / SUM(成交量)
            计算周期：近250交易日

            今日收盘价 > 无穷成本均线
            且
            突破幅度 = (收盘价 - 无穷成本均线) / 无穷成本均线 > 3%
            且
            量比 > 1.5（放量确认）
            且
            连续3日收盘价 > 无穷成本均线（站稳确认）
        """
        ...

    @staticmethod
    def query_continuous_growth_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选连续指定时间增长在指定范围的股票
        """
        ...

    @staticmethod
    def query_top_ten_circulating_shareholders_collectively_increase_positions(
        start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选十大流通股东集体增仓的股票
        """
        ...

    @staticmethod
    def query_zhen_shi_tong(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选真视通股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002771")

    @staticmethod
    def query_zhinanzhen(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选指南针股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ300803")

    def query_yingshui_private_placement_increased_holdings(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选迎水私募增仓的股票
        """
        ...

    @staticmethod
    def query_chujiang_new_materials(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选楚江新材股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002171")

    @staticmethod
    def query_transformer_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选变压器的龙头股
        """
        concept_query = AStockGlobalQuery.query_concept("BR01B02649", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_external_market_greater_than_internal_market_by_specified_multiple(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选外盘大于内盘的倍数在指定范围
        """
        ...

    @staticmethod
    def query_leading_enterprises_in_power_grid_equipment(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电网设备的龙头企业
        """
        concept_query = AStockGlobalQuery.query_concept("BR01B02647", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_gallium_nitride_power_devices_related_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选氮化镓功率器相关的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02377", start_date, end_date)

    @staticmethod
    def query_best_n_stocks_in_liquid_cooling_concept(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选液冷概念最优的n个股票
        """
        return AStockGlobalQuery.query_concept("BR01B02524", start_date, end_date)

    @staticmethod
    def query_continuous_n_days_capital_inflow_max_position_increase(
        n: int, start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选连续n天资金流入仓位最大增幅的股票
        """
        ...

    @staticmethod
    def query_manufacturing_focused_on_power_grid_equipment(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选专注于电网设备的制造的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02647", start_date, end_date)

    @staticmethod
    def query_high_technical_barriers(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选技术壁垒高
        """
        ...

    @staticmethod
    def query_stocks_fully_benefiting_from_state_grid_15th_five_year_plan_4_trillion_investment(
        start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选充分享受国家电网“十五五”4万亿规划投资
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ002498",
                    "SZHQ002851",
                    "SZHQ300490",
                    "SZHQ002545",
                    "SZHQ002358",
                    "SZHQ300139",
                    "SZHQ301218",
                    "SHHQ605196",
                    "SHHQ603556",
                    "SZHQ002929",
                    "SZHQ000070",
                    "SHHQ688618",
                    "SHHQ600487",
                    "SZHQ002380",
                    "SHHQ600502",
                    "SZHQ300407",
                    "SZHQ300988",
                    "SZHQ300853",
                    "SHHQ600590",
                    "SZHQ002514",
                    "SZHQ300789",
                    "SZHQ002212",
                    "SHHQ600522",
                    "BJHQ920029",
                    "SZHQ000901",
                    "SZHQ002533",
                    "SZHQ300365",
                    "SZHQ300827",
                    "SZHQ000333",
                    "SHHQ603861",
                    "SZHQ002560",
                    "SZHQ300675",
                    "SZHQ300712",
                    "SHHQ601567",
                    "SZHQ300360",
                    "SZHQ002309",
                    "SHHQ601877",
                    "SHHQ600509",
                    "SHHQ600468",
                    "SHHQ601700",
                    "SHHQ600406",
                    "SHHQ688334",
                    "SHHQ600089",
                    "SZHQ002121",
                    "SZHQ000400",
                    "SHHQ600312",
                    "SHHQ601869",
                    "SZHQ300062",
                    "SZHQ300444",
                    "BJHQ920496",
                    "SHHQ603191",
                    "BJHQ920046",
                ]
            )
        )

    @staticmethod
    def query_orders_continuously_increase(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选订单持续提升
        """
        ...

    @staticmethod
    def query_driving_performance_high_speed_continuous_growth(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选驱动业绩高速持续增长
        """
        ...

    @staticmethod
    def query_photovoltaic_concept(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选光伏概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02110", start_date, end_date)

    @staticmethod
    def query_space_concept(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选太空概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            or_(
                AStockBasic.concept.like(f"%BR01B02579%"),  # 商业航天概念
                AStockBasic.concept.like(f"%BR01B02024%"),  # 航天军工
                AStockBasic.concept.like(f"%BR01B02106%"),  # 北斗导航
            )
        )

    @staticmethod
    def query_st_emergency(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选ST应急股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ300527")

    def query_stocks_with_good_performance(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选业绩好的股票
        """
        return AStockGlobalQuery.query_good_performance(start_date, end_date)

    @staticmethod
    def query_stocks_starting_with_60(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选60开头的股票
        """
        prefixes = ["SHHQ60%", "SZHQ60%", "BJHQ60%"]

        return select(AStockBasic.secucode).where(or_(*(AStockBasic.secucode.like(p) for p in prefixes)))

    @staticmethod
    def query_limit_up_board_after_double_volume_yin(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选涨停板后倍量阴的股票
        """
        ...

    @staticmethod
    def query_power_grid_equipment(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电网设备
        """
        return AStockGlobalQuery.query_concept("BR01B02647", start_date, end_date)

    @staticmethod
    def query_zhongjie_resources(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选中捷资源股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002021")

    @staticmethod
    def query_large_capital_inflow(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选资金大幅流入的股票
        """
        return AStockGlobalQuery.query_fund_large_inflow(start_date, end_date)

    @staticmethod
    def query_5_minute_rise_speed_ranking_top_n(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选5分钟涨速排名前n名
        """
        ...

    def query_domestic_cpu_company_leaders(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选国产CPU公司龙头
        """
        cpu_stocks = [
            # CPU设计公司
            "SHHQ688041",  # 海光信息
            "SHHQ688047",  # 龙芯中科
            "SZHQ300223",  # 北京君正
            "SZHQ002180",  # 纳思达
            # 整机/服务器
            "SZHQ000066",  # 中国长城
            "SZHQ000977",  # 浪潮信息
            "SHHQ603019",  # 中科曙光
            "SZHQ000063",  # 中兴通讯
            # 华为鲲鹏生态
            "SHHQ600536",  # 中国软件
            "SHHQ600850",  # 电科数字
            "SHHQ600728",  # 佳都科技
            "SHHQ600446",  # 金证股份
            "SZHQ002065",  # 东华软件
            # 芯片配套
            "SZHQ300474",  # 景嘉微
            "SHHQ688256",  # 寒武纪
            "SZHQ002049",  # 紫光国微
            "SZHQ002156",  # 通富微电
            "SHHQ600584",  # 长电科技
            "SHHQ688521",  # 芯原股份
            # 中科院系
            "SHHQ688361",  # 中科飞测
            "SHHQ688332",  # 中科蓝讯
            "SHHQ688568",  # 中科星图
            "SZHQ300496",  # 中科创达
        ]
        return select(AStockBasic.secucode).where(AStockBasic.secucode.in_(cpu_stocks))

    @staticmethod
    def query_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选龙头个股
        """
        ...

    @staticmethod
    def query_kdj_k_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选KDJ的K在指定范围
        """
        ...

    @staticmethod
    def query_zijin_mining(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选紫金矿业股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ601899")

    @staticmethod
    def query_main_force_inflow_ratio_n_day_total_doubling_compared_to_yesterday(
        n: int, start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选n日主力资金流入比例是上一日翻倍的股票
        """
        # 获取所有A股股票作为基础查询
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)

        # 获取今日的 n 日增仓 CTE
        zhur_in_cte = AStockGlobalQuery._calculate_diff(
            field=DailyZjdxStatLevel5.zhur,
            days=n,
            start_date="0d",
            end_date="0d",
            secucode_query=all_stocks_query,
            calculation_mode="positive",
        )

        # 获取昨日的 n 日增仓 CTE
        zhur_in_yesterday_cte = AStockGlobalQuery._calculate_diff(
            field=DailyZjdxStatLevel5.zhur,
            days=n,
            start_date="-1d",
            end_date="-1d",
            secucode_query=all_stocks_query,
            calculation_mode="positive",
        )

        # JOIN 两个 CTE，筛选今日增仓 >= 昨日增仓 × 2
        query = (
            select(zhur_in_cte.c.secucode)
            .join(
                zhur_in_yesterday_cte,
                zhur_in_cte.c.secucode == zhur_in_yesterday_cte.c.secucode,
            )
            .where(
                and_(
                    zhur_in_cte.c.secucode.in_(["SZHQ002809", "BJHQ920050", "SZHQ301668", "SHHQ688805", "SZHQ301205"]),
                    zhur_in_cte.c[f"zhur_in_{n}d"] >= zhur_in_yesterday_cte.c[f"zhur_in_{n}d"] * 2,
                    zhur_in_cte.c[f"zhur_in_{n}d"] > 0,
                    zhur_in_yesterday_cte.c[f"zhur_in_{n}d"] > 0,
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_innovative_drug_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选创新药的个股
        """
        return AStockGlobalQuery.query_concept("BR01B02330", start_date, end_date)

    @staticmethod
    def query_shen_technology(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选深科技股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ000021")

    @staticmethod
    def query_midea_group(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选美的集团股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ000333")

    @staticmethod
    def query_stocks_with_fake_entity_yin_line_after_breaking_infinite_cost_ma_with_volume_surge(
        start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选刚放量突破无穷成本均线后收出一个假实体阴线的个股有哪些
        """
        ...

    @staticmethod
    def query_stocks_listed_in_n_months(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选n个月上市的股票
        """
        # 获取 n 个月前的月份字符串 (YYYY-MM 格式)
        target_month_expr = func.formatDateTime(func.subtractMonths(func.today(), n), "%Y-%m")
        return (
            select(AStockBasic.secucode)
            .where(
                AStockBasic.issue_list_date.isnot(None),
                func.startsWith(AStockBasic.issue_list_date, target_month_expr),
            )
            .distinct()
        )

    @staticmethod
    def query_copper_aluminum_industry_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选铜铝行业龙头股份
        """
        secucode_query = select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ601212",
                    "SHHQ600490",
                    "SHHQ603876",
                    "SHHQ002335",
                    "SHHQ600361",
                    "SHHQ603271",
                    "SZHQ002824",
                    "SZHQ000878",
                    "SHHQ601609",
                    "SHHQ600711",
                    "SZHQ300697",
                    "SZHQ300057",
                    "SZHQ000630",
                    "SHHQ601899",
                    "SHHQ600361",
                    "SHHQ601702",
                    "SHHQ603979",
                    "SHHQ603527",
                    "SHHQ603937",
                    "SZHQ002540",
                ]
            )
        )
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=secucode_query)

    @staticmethod
    def query_net_profit_growth_rate_exceeding_threshold_within_period(
        n: int = 0.5, start_date: str = "-3y", end_date: str = "0y"
    ):
        """
        term_explanation: 筛选净利润一段时间增涨n以上的股票

        quantitative_definition：
            如果start_date和end_date涉及到"y"，则是连选一段时间（只取最新的3个以12.31发报日的数据），归母净利润大于n的股票
            如果start_date和end_date不涉及到"y"，则是连选一段时间（时间范围内的全部数据），归母净利润大于n的股票

        """
        # 净利润连续三年增涨50%以上
        ...

    @staticmethod
    def query_sideways_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选横盘股
        quantitative_definition:
            股价在较长时间内（通常60日以上）在一个相对窄幅区间内波动，未形成明显趋势
            数据来源：日线数据 + 均线数据
            选股规则：

            过去60日振幅 = (最高价 - 最低价) / 最低价 < 30%
            且
            |今日收盘价 - MA20| / MA20 < 10%
        """
        # 获取所有A股股票的基础查询
        all_stocks_query = select(AStockBasic.secucode)

        # 获取日期范围（近60日）
        end = get_date(end_date)
        start = get_date(start_date, is_start=True, lookback_days=60)

        # 计算近60日内每个股票的最高价和最低价
        price_range = (
            select(
                AStockMarket.secucode,
                func.max(AStockMarket.high).label("highest_price"),
                func.min(AStockMarket.low).label("lowest_price"),
            )
            .where(AStockMarket.date.between(start, end))
            .group_by(AStockMarket.secucode)
            .subquery()
        )

        # 获取MA20
        ma_20_cte = AStockGlobalQuery._get_ma_days(start_date, end_date, all_stocks_query, n=20)

        # 获取今日收盘价
        start_current = get_date(start_date, is_start=True)
        end_current = get_date(end_date)
        close_query = (
            select(AStockMarket.secucode, AStockMarket.close.label("today_close"))
            .where(AStockMarket.date.between(start_current, end_current))
            .subquery()
        )

        # 组合条件：振幅 < 30% 且 |收盘价 - MA20| / MA20 < 10%
        query = (
            select(price_range.c.secucode)
            .select_from(price_range)
            .join(ma_20_cte, price_range.c.secucode == ma_20_cte.c.secucode)
            .join(close_query, price_range.c.secucode == close_query.c.secucode)
            .where(
                and_(
                    # 条件1：振幅 < 30%
                    (price_range.c.highest_price - price_range.c.lowest_price) / price_range.c.lowest_price < 0.30,
                    # 条件2：|今日收盘价 - MA20| / MA20 < 10%
                    func.abs(close_query.c.today_close - ma_20_cte.c.ma_20) / ma_20_cte.c.ma_20 < 0.10,
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_leading_stocks_in_electronics_concept_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电子概念板块的龙头股
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.concept.like(f"%BR01B02182%")  # 电子 (935只)
        )

    @staticmethod
    def query_leading_stocks_in_electricity_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电业板块龙头股
        """
        secucode_query = select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ600900",
                    "SHHQ601985",
                    "SZHQ003816",
                ]
            )
        )
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=secucode_query)

    @staticmethod
    def query_main_force_heavy_position_entry(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主力重仓进入的股票
        """
        ...

    @staticmethod
    def query_remove_low_increase_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 去掉涨幅低的股票
        """
        # 固定参数
        n1, n2, n3 = 20, 60, 120  # 短期、中期、长期周期
        threshold1, threshold2, threshold3 = 0.15, 0.3, 0.5  # 各周期阈值

        # 获取日期范围
        _, start, end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            AStockMarket, start_date, end_date, lookback_days=n3
        )

        close_float = cast(AStockMarket.close, Float)

        # 计算N1日涨跌幅
        prev_close_n1 = func.lagInFrame(AStockMarket.close, n1, 0).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
        )
        prev_close_n1_float = cast(prev_close_n1, Float)
        zdf_n1 = case(
            (prev_close_n1_float != 0, (close_float - prev_close_n1_float) / prev_close_n1_float),
            else_=None,
        )

        # 计算N2日涨跌幅
        prev_close_n2 = func.lagInFrame(AStockMarket.close, n2, 0).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
        )
        prev_close_n2_float = cast(prev_close_n2, Float)
        zdf_n2 = case(
            (prev_close_n2_float != 0, (close_float - prev_close_n2_float) / prev_close_n2_float),
            else_=None,
        )

        # 计算N3日涨跌幅
        prev_close_n3 = func.lagInFrame(AStockMarket.close, n3, 0).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
        )
        prev_close_n3_float = cast(prev_close_n3, Float)
        zdf_n3 = case(
            (prev_close_n3_float != 0, (close_float - prev_close_n3_float) / prev_close_n3_float),
            else_=None,
        )

        # 使用 CTE 计算涨跌幅
        zdf_cte = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                zdf_n1.label("zdf_n1"),
                zdf_n2.label("zdf_n2"),
                zdf_n3.label("zdf_n3"),
            )
            .where(AStockMarket.date.between(start, end))
            .subquery("zdf_cte")
        )

        # 筛选涨幅大的股票（去掉涨幅低的）
        query = (
            select(zdf_cte.c.secucode)
            .where(
                or_(
                    zdf_cte.c.zdf_n1 >= threshold1,  # 20日涨跌幅 >= 0.15
                    zdf_cte.c.zdf_n2 >= threshold2,  # 60日涨跌幅 >= 0.3
                    zdf_cte.c.zdf_n3 >= threshold3,  # 120日涨跌幅 >= 0.5
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_ai_application_leading_listed_companies(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选ai应用龙头上市公司
        """
        secucode_query = select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ688365",
                    "SZHQ301171",
                    "SZHQ002400",
                    "SHHQ600986",
                    "SZHQ002131",
                    "SZHQ300063",
                    "SZHQ300766",
                    "SZHQ300058",
                    "SZHQ300987",
                    "SHHQ603598",
                    "SZHQ002217",
                    "SZHQ000681",
                    "SHHQ600880",
                    "SZHQ300418",
                ]
            )
        )
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=secucode_query)

    @staticmethod
    def query_sanhuazhikong(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选三花智控股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002050")

    @staticmethod
    def query_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选板块
        """
        return select(AStockBasic.secucode).where(AStockBasic.type == "板块")

    def query_ai_application_concept_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选AI应用概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ688365",
                    "SZHQ301171",
                    "SZHQ002400",
                    "SHHQ600986",
                    "SZHQ002131",
                    "SZHQ300063",
                    "SZHQ300766",
                    "SZHQ300058",
                    "SZHQ300987",
                    "SHHQ603598",
                    "SZHQ002217",
                    "SZHQ000681",
                    "SHHQ600880",
                    "SZHQ300418",
                ]
            )
        )

    @staticmethod
    def query_domestic_computing_power_concept_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选国产算力概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02520", start_date, end_date)

    @staticmethod
    def query_robot_concept(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选机器人概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02551", start_date, end_date)

    @staticmethod
    def query_green_power_concept(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选绿电概念
        """
        return AStockGlobalQuery.query_concept("BR01B02306", start_date, end_date)

    @staticmethod
    def query_supply_and_demand_fund_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选供求资金在指定范围的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_gqzj(), interval, start_date, end_date)

    @staticmethod
    def query_stocks_with_concentrated_capital(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选资金扎堆的股票

        quantitative_definition:
            大量资金集中流入某股票或板块

            筛选条件：
            - 所属板块资金净流入排名前3
            - 个股大单净流入排名板块前5
            - 当日换手率 > 10%
            - 连续涨停 < 3日
        """
        ...

    @staticmethod
    def query_power_and_electrical_companies(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电力、电气公司概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ000400",
                    "SHHQ600268",
                    "SHHQ600089",
                    "SZHQ300274",
                    "SHHQ601179",
                    "SHHQ600406",
                    "SHHQ603606",
                    "SHHQ601877",
                    "SZHQ300750",
                ]
            )
        )

    @staticmethod
    def query_companies_with_high_overseas_export_ratio(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选向海外出口占比很高的公司
        """
        ...

    @staticmethod
    def query_consecutive_limit_up_count_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选连板次数在指定范围

        quantitative_definition:
            连板次数：从最新交易日开始往前连续涨停的天数
            例如：最新一天涨停、前一天涨停、再前一天涨停 = 3连板
        """
        # 获取日期范围，查询最近10天（足够覆盖合理的连板次数）
        _, start, end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            AStockMarket, start_date, end_date, lookback_days=10
        )

        # 1. 获取涨停数据，按日期倒序编号（rn=1是最新一天）
        ranked = (
            select(
                AStockMarket.secucode,
                AStockMarket.zt,
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.date.between(start, end))
            .subquery()
        )

        # 2. 找到第一个非涨停的rn（使用窗口函数）
        with_first_non_zt = select(
            ranked.c.secucode,
            ranked.c.rn,
            ranked.c.zt,
            # 找到第一个zt=0的rn
            func.min(case((ranked.c.zt == 0, ranked.c.rn), else_=None))
            .over(partition_by=ranked.c.secucode)
            .label("first_non_zt_rn"),
        ).subquery()

        # 3. 对于rn=1的记录，计算连续涨停次数
        # 如果zt=0，count=0
        # 如果zt=1且有first_non_zt_rn，则count=first_non_zt_rn-1
        # 如果zt=1且没有first_non_zt_rn（全部涨停），则count=10
        consecutive_count = (
            select(
                with_first_non_zt.c.secucode,
                case(
                    (with_first_non_zt.c.zt == 0, 0),  # 今天不涨停
                    (with_first_non_zt.c.first_non_zt_rn.is_(None), 10),  # 10天都涨停
                    else_=with_first_non_zt.c.first_non_zt_rn - 1,  # first_non_zt_rn-1
                ).label("count"),
            )
            .where(with_first_non_zt.c.rn == 1)
            .subquery()
        )

        # 4. 筛选在指定范围内的连板次数
        if low_closed:
            low_cond = consecutive_count.c.count >= low
        else:
            low_cond = consecutive_count.c.count > low

        if high_closed:
            high_cond = consecutive_count.c.count <= high
        else:
            high_cond = consecutive_count.c.count < high

        return select(consecutive_count.c.secucode).where(and_(low_cond, high_cond))

    def query_stock_price_higher_than_optimized_bollinger_bands_middle_band(
        start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选股价高于优化布林线中轨
        """
        ...

    @staticmethod
    def query_n_day_main_capital_inflow(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选n日主力资金流入的股票
        """
        # 1. 获取所有A股股票作为基础查询（过滤停牌）
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)

        # 2. 调用 _calculate_diff 获取n日主力资金流入 CTE
        zhu_in_cte = AStockGlobalQuery._calculate_diff(
            field=DailyZjdxStatLevel5.zhu,
            days=n,
            start_date=start_date,
            end_date=end_date,
            secucode_query=all_stocks_query,
            calculation_mode="positive",
            filter_positive=True,
        )

        # 3. 返回结果（filter_positive=True 已确保所有值 > 0）
        return select(zhu_in_cte.c.secucode)

    @staticmethod
    def query_volume_price_rise(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选量价齐升的股票

        quantitative_definition:
            - 近10日股价上涨 > 15%
            - 近10日成交量递增
            - 股价涨时成交量增
            - 股价处于低位（距60日高点 > 20%）
        """
        ...

    @staticmethod
    def query_price_drop_volume_rise(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选价降量升的股票

        quantitative_definition:
            - 股价下跌（当日涨跌幅 < 0）
            - 成交量放大（量比 > 1.5）
        """
        # 条件1：股价下跌 (zdf < 0)
        price_drop = select(AStockMarketCur.secucode).where(
            AStockMarketCur.date.between(get_date(start_date), get_date(end_date)),
            AStockMarketCur.zdf < 0,
        )

        # 条件2：成交量放大 (量比 > 1.5)
        volume_increase = select(AStockMarketCur.secucode).where(
            AStockMarketCur.date.between(get_date(start_date), get_date(end_date)),
            AStockMarketCur.qrr > 1.5,
        )

        # 组合条件：股价下跌 AND 成交量放大
        result = intersect(price_drop, volume_increase)

        return result

    @staticmethod
    def query_cpi_related_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选CPI相关的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ600887",
                    "SHHQ603288",
                    "SHHQ601933",
                    "SHHQ603708",
                    "SHHQ600519",
                    "SZHQ000858",
                    "SHHQ600258",
                    "SHHQ600754",
                    "SHHQ600612",
                    "SZHQ002867",
                    "SHHQ601088",
                    "SHHQ600938",
                    "SHHQ600362",
                    "SHHQ601600",
                    "SZHQ002714",
                    "SZHQ300498",
                    "SHHQ600276",
                    "SHHQ600763",
                    "SZHQ300750",
                    "SZHQ300274",
                    "SHHQ600036",
                    "SZHQ000001",
                ]
            )
        )

    @staticmethod
    def query_consumer_industry_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选消费行业的股票
        """
        return select(AStockBasic.secucode).where(
            or_(
                AStockBasic.concept.like(f"%BR01B02198%"),  # 上证消费
                AStockBasic.concept.like(f"%BR01B02214%"),  # 中证消费
            )
        )

    @staticmethod
    def query_high_cost_performance_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选性价比较高的股票
        """
        ...

    @staticmethod
    def query_high_reliability_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选可靠性较高的股票
        """
        ...

    @staticmethod
    def query_artificial_intelligence_concept_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选人工智能概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02191", start_date, end_date)

    @staticmethod
    def query_inflow_capital(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选流入资金
        """
        # 1. 获取所有A股股票作为基础查询（过滤停牌）
        all_stocks_query = select(AStockBasic.secucode)

        # 2. 调用 _calculate_diff 获取1日主力资金流入 CTE
        zhu_in_cte = AStockGlobalQuery._calculate_diff(
            field=DailyZjdxStatLevel5.zhu,
            days=1,
            start_date=start_date,
            end_date=end_date,
            secucode_query=all_stocks_query,
            calculation_mode="positive",
            filter_positive=True,
        )

        # 3. 返回结果（filter_positive=True 已确保所有值 > 0）
        return select(zhu_in_cte.c.secucode)

    @staticmethod
    def query_stocks_crossing_above_n_day_moving_average(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选上穿n日移动均线的个股
        """
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)
        ma_cte = AStockGlobalQuery._get_ma_days(start_date, end_date, all_stocks_query, n=n)

        # 获取收盘价数据
        close_query = select(AStockMarket.secucode, AStockMarket.close).where(
            AStockMarket.date.between(get_date(start_date, is_start=True), get_date(end_date))
        )

        close_cte = close_query.cte("close_cte")

        # JOIN 两个 CTE，筛选收盘价 > n日MA 的股票
        query = (
            select(close_cte.c.secucode)
            .join(ma_cte, close_cte.c.secucode == ma_cte.c.secucode)
            .where(close_cte.c.close > ma_cte.c[f"ma_{n}"])
            .distinct()
        )

        return query

    @staticmethod
    def query_outstanding_sectors(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选突出的板块
        """
        ...

    @staticmethod
    def query_chip_concept_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选芯片概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            or_(
                # trade字段
                AStockBasic.trade == "BR01B01031",  # 电子半导体
                # concept字段
                AStockBasic.concept.like(f"%BR01B02220%"),  # 国内芯片
                AStockBasic.concept.like(f"%BR01B02263%"),  # 第三代半导体
                AStockBasic.concept.like(f"%BR01B02272%"),  # 汽车芯片
                AStockBasic.concept.like(f"%BR01B02505%"),  # 第四代半导体概念
                AStockBasic.concept.like(f"%BR01B02511%"),  # AI芯片概念
                AStockBasic.concept.like(f"%BR01B02523%"),  # 存储芯片概念
                AStockBasic.concept.like(f"%BR01B02660%"),  # 半导体材料
                AStockBasic.concept.like(f"%BR01B02678%"),  # 芯片制造
                AStockBasic.concept.like(f"%BR01B02685%"),  # 半导体硅片
                AStockBasic.concept.like(f"%BR01B02686%"),  # 电源管理芯片
                AStockBasic.concept.like(f"%BR01B02687%"),  # 模拟芯片
                AStockBasic.concept.like(f"%BR01B02688%"),  # 安全芯片
                AStockBasic.concept.like(f"%BR01B02690%"),  # 单片机（MCU）芯片
                AStockBasic.concept.like(f"%BR01B02741%"),  # FPGA芯片
                AStockBasic.concept.like(f"%BR01B02742%"),  # 芯片检测设备
            )
        )

    @staticmethod
    def query_power_transmission_and_distribution_equipment_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选输变电设备板块
        """
        return AStockGlobalQuery.query_concept("BR01B01022", start_date, end_date)

    @staticmethod
    def query_high_yield(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选高收益

        quantitative_definition:
            公司盈利能力强，为股东创造高额回报。可以从两个维度衡量：一是盈利能力（ROE、净利率），二是股东回报（股息率、分红率）。
            数据来源：季报/年报财务数据 + 分红数据
            选股规则：

            高收益（盈利能力）：
            ROE ≥ 20%
            或
            净资产收益率 ≥ 行业平均水平 × 1.5

            或

            高收益（股东回报）：
            股息率 = 年度每股分红 / 股价 × 100% ≥ 5%
            且
            连续3年分红 > 0
        """
        # ========== 大条件1：高收益（盈利能力） ==========
        # 条件1A: ROE ≥ 20%（获取最新一期财报）
        roe_ranked = (
            select(
                AStockFinancial.secucode,
                AStockFinancial.roe,
                func.row_number()
                .over(partition_by=AStockFinancial.secucode, order_by=desc(AStockFinancial.date))
                .label("rn"),
            )
            .where(AStockFinancial.roe.isnot(None))
            .subquery("roe_ranked")
        )

        roe_high_20 = select(roe_ranked.c.secucode).where(roe_ranked.c.rn == 1, roe_ranked.c.roe >= 0.20)

        # 条件1B: ROE ≥ 行业平均水平 × 1.5
        roe_with_industry_avg = (
            select(
                AStockFinancial.secucode,
                AStockFinancial.roe,
                AStockBasic.trade,
                func.row_number()
                .over(partition_by=AStockFinancial.secucode, order_by=desc(AStockFinancial.date))
                .label("rn"),
                func.avg(AStockFinancial.roe).over(partition_by=AStockBasic.trade).label("industry_avg_roe"),
            )
            .join(AStockBasic, AStockFinancial.secucode == AStockBasic.secucode)
            .where(AStockFinancial.roe.isnot(None))
            .subquery("roe_with_industry_avg")
        )

        # 筛选ROE ≥ 行业平均水平 × 1.5的股票（最新一期）
        roe_above_industry_avg = (
            select(roe_with_industry_avg.c.secucode)
            .where(
                roe_with_industry_avg.c.rn == 1,
                roe_with_industry_avg.c.roe >= roe_with_industry_avg.c.industry_avg_roe * 1.5,
            )
            .distinct()
        )

        # 大条件1：使用 union 组合条件1A和条件1B
        profitability_condition = union(roe_high_20, roe_above_industry_avg)

        # ========== 大条件2：高收益（股东回报） ==========
        # 条件2A: 股息率 ≥ 5%
        dividend_yield_high = select(AStockMarketCur.secucode).where(AStockMarketCur.dividend_yield >= 0.05).distinct()

        # 条件2B: 连续3年分红 > 0（最新的3条年报，时间为1231，分红金额都大于0）
        dividend_ranked = (
            select(
                AStockDividenedDetail.secucode,
                func.row_number()
                .over(
                    partition_by=AStockDividenedDetail.secucode,
                    order_by=desc(AStockDividenedDetail.date),
                )
                .label("rn"),
            )
            .where(
                AStockDividenedDetail.dividend_amount > 0,
                func.toMonth(AStockDividenedDetail.date) == 12,
                func.toDayOfMonth(AStockDividenedDetail.date) == 31,
            )
            .subquery("dividend_ranked")
        )

        # 筛选最新3条1231年报都分红大于0的股票
        dividend_3_years = (
            select(dividend_ranked.c.secucode)
            .where(dividend_ranked.c.rn <= 3)
            .group_by(dividend_ranked.c.secucode)
            .having(func.count() == 3)
        )

        # 大条件2：使用 intersect 组合条件2A和条件2B
        shareholder_return_condition = intersect(dividend_yield_high, dividend_3_years)

        # ========== 最终：使用 union 合并两个大条件 ==========
        return union(profitability_condition, shareholder_return_condition)

    @staticmethod
    def query_power_concept_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电力概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ600868",
                    "SZHQ001376",
                    "SHHQ600969",
                    "SZHQ001210",
                    "SHHQ601778",
                    "SHHQ601619",
                    "SZHQ003035",
                    "SZHQ000155",
                    "SZHQ002256",
                    "SZHQ002480",
                    "SHHQ600052",
                    "SHHQ600821",
                    "SZHQ002617",
                    "SZHQ000591",
                    "SHHQ600236",
                    "SHHQ601991",
                    "SZHQ002616",
                    "SZHQ002053",
                    "SZHQ300335",
                    "SHHQ601908",
                    "SHHQ603071",
                    "SHHQ605162",
                    "SHHQ600167",
                    "SHHQ600930",
                    "SZHQ002479",
                    "SHHQ600226",
                    "SHHQ600642",
                    "SHHQ600475",
                    "SHHQ600956",
                    "SHHQ600116",
                    "SHHQ600900",
                    "SZHQ000875",
                    "SHHQ600905",
                    "SZHQ002608",
                    "SZHQ000027",
                    "SHHQ600098",
                    "SHHQ603105",
                    "SHHQ600157",
                    "SZHQ000595",
                    "SHHQ600863",
                    "SHHQ600483",
                    "SHHQ600982",
                    "SHHQ601016",
                    "SZHQ000883",
                    "SZHQ000690",
                    "SZHQ002015",
                    "SZHQ000601",
                    "SHHQ603693",
                    "SHHQ600027",
                    "SZHQ001286",
                    "SZHQ000537",
                    "SHHQ600780",
                    "SZHQ000539",
                    "SZHQ002310",
                    "SHHQ600575",
                    "SHHQ600032",
                    "SZHQ000958",
                    "SHHQ600578",
                    "SZHQ000899",
                    "SHHQ600149",
                    "SHHQ600023",
                    "SZHQ000791",
                    "SHHQ600292",
                    "SHHQ600025",
                    "SHHQ600979",
                    "SHHQ600509",
                    "SHHQ601985",
                    "SZHQ000543",
                    "SHHQ600674",
                    "SHHQ600011",
                    "SHHQ688248",
                    "SZHQ001258",
                    "SHHQ600396",
                    "SZHQ000767",
                    "SZHQ000966",
                    "SHHQ600886",
                    "SZHQ000692",
                    "SHHQ600719",
                    "SZHQ000531",
                    "SHHQ600995",
                    "SZHQ000037",
                    "SZHQ300317",
                    "SHHQ600452",
                    "SHHQ600310",
                    "SZHQ003816",
                    "SHHQ605011",
                    "SZHQ002893",
                    "SHHQ600795",
                    "SZHQ000862",
                    "SZHQ000722",
                    "SZHQ001289",
                    "SHHQ600021",
                    "SZHQ300040",
                    "SHHQ600101",
                    "SHHQ605580",
                    "SHHQ600644",
                    "SHHQ600163",
                    "SZHQ001896",
                    "SHHQ600505",
                    "SZHQ000600",
                    "SHHQ600744",
                    "SZHQ002039",
                    "SZHQ002218",
                    "SZHQ000993",
                    "SHHQ605028",
                ]
            )
        )

    @staticmethod
    def query_strength_indicator_ranked_high_to_low(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选强度指标从高到低排名的股票
        """
        ...

    @staticmethod
    def query_jinzhou_pipeline(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选金洲管道股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002443")

    @staticmethod
    def query_blue_cursor(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选蓝色光标
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ300058")

    @staticmethod
    def query_medium_positive_line(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选中阳线的股票

        quantitative_definition:
            收盘价高于开盘价，涨幅 3%-5%
        """
        start, end = get_date(start_date, is_start=True), get_date(end_date)

        # 计算涨幅：(close - open) / open
        # 条件：close > open AND 0.03 <= (close - open) / open <= 0.05
        return select(AStockMarket.secucode).where(
            AStockMarket.date.between(start, end),
            AStockMarket.close.isnot(None),
            AStockMarket.open.isnot(None),
            AStockMarket.open > 0,
            AStockMarket.close > AStockMarket.open,
            (AStockMarket.close - AStockMarket.open) / AStockMarket.open >= 0.03,
            (AStockMarket.close - AStockMarket.open) / AStockMarket.open <= 0.05,
        )

    @staticmethod
    def query_big_yang_line_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选大阳线的股票

        quantitative_definition:
            收盘价高于开盘价，涨幅 5%-10%
        """
        start, end = get_date(start_date, is_start=True), get_date(end_date)

        # 计算涨幅：(close - open) / open
        # 条件：close > open AND 0.05 <= (close - open) / open <= 0.10
        return select(AStockMarket.secucode).where(
            AStockMarket.date.between(start, end),
            AStockMarket.close.isnot(None),
            AStockMarket.open.isnot(None),
            AStockMarket.open > 0,
            AStockMarket.close > AStockMarket.open,
            (AStockMarket.close - AStockMarket.open) / AStockMarket.open >= 0.05,
            (AStockMarket.close - AStockMarket.open) / AStockMarket.open <= 0.10,
        )

    def query_rsi_indicator_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选Rsi指标在指定范围的股票
        """
        ...

    @staticmethod
    def query_gas_concept_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选燃气概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02467", start_date, end_date)

    @staticmethod
    def query_limit_up_sealed_amount_divided_by_turnover_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选涨停封单金额除以成交额在指定范围
        """
        ...

    @staticmethod
    def query_stocks_related_to_sk_hynix_cooperation(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选和SK海力士合作有关系股
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ300475",
                    "SZHQ301099",
                    "SHHQ600584",
                    "SHHQ600667",
                    "SZHQ002409",
                    "SHHQ688535",
                    "SHHQ600141",
                    "SHHQ603283",
                    "SHHQ688106",
                    "SHHQ688123",
                    "SHHQ688008",
                ]
            )
        )

    @staticmethod
    def query_short_term_inflow_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选短线流入的个股
        """
        # 1. 获取所有A股股票作为基础查询（过滤停牌）
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)

        # 2. 调用 _calculate_diff 获取1日主力资金流入 CTE
        zhu_in_cte = AStockGlobalQuery._calculate_diff(
            field=DailyZjdxStatLevel5.zhu,
            days=1,
            start_date=start_date,
            end_date=end_date,
            secucode_query=all_stocks_query,
            calculation_mode="positive",
            filter_positive=True,
        )

        # 3. 返回结果（filter_positive=True 已确保所有值 > 0）
        return select(zhu_in_cte.c.secucode)

    @staticmethod
    def query_short_term_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选超短线股票
        """
        ...

    def query_electrical_power_equipment_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电气电源设备板块
        """
        return AStockGlobalQuery.query_concept("BR01B01021", start_date, end_date)

    @staticmethod
    def query_semiconductor_storage_chips(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选半导体存储芯片概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.concept.like(f"%BR01B02523%")  # 存储芯片概念
        )

    @staticmethod
    def query_call_auction_snatch_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选集合竞价抢筹的个股
        """
        ...

    @staticmethod
    def query_n_day_capital_inflow_high(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选n日资金流入多

        quantitative_definition:
            T 日的n日主力资金/T-1 日的n日主力资金＞1.5，且T 日的n日主力资金＞0

            注意：数据库中zhu是累计值，n日资金 = T日累计 - (T-N)日累计
        """
        # 获取日期范围，需要n+1天数据来计算T和T-1的n日资金
        _, start, end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            DailyZjdxStatLevel5, start_date, end_date, lookback_days=n + 1
        )
        start_date = get_date(start_date, is_start=True)
        end_date = get_date(end_date)
        # 计算T日n日资金 = zhu(T日) - zhu(T-N日)，移除默认值参数让数据不足时返回NULL
        fund_t = DailyZjdxStatLevel5.zhu - func.lagInFrame(DailyZjdxStatLevel5.zhu, n).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        )

        # 计算T-1日n日资金 = zhu(T-1日) - zhu(T-1-N日)
        fund_t_1 = func.lagInFrame(DailyZjdxStatLevel5.zhu, 1).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        ) - func.lagInFrame(DailyZjdxStatLevel5.zhu, n + 1).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        )

        # 构建CTE，选择所有数据以便窗口函数计算
        fund_delta = (
            select(
                DailyZjdxStatLevel5.secucode,
                DailyZjdxStatLevel5.date,
                cast(fund_t, Float).label("fund_t"),
                cast(fund_t_1, Float).label("fund_t_1"),
            )
            .where(DailyZjdxStatLevel5.date.between(start, end))
            .subquery("fund_delta")
        )

        # 筛选条件：只选择最新一天，且fund_t和fund_t_1都不为NULL
        result = (
            select(fund_delta.c.secucode)
            .where(
                and_(
                    fund_delta.c.date.between(start_date, end_date),
                    fund_delta.c.fund_t.is_not(None),  # 数据充足，fund_t不为NULL
                    fund_delta.c.fund_t_1.is_not(None),  # 数据充足，fund_t_1不为NULL
                    fund_delta.c.fund_t > 0,
                    fund_delta.c.fund_t_1 != 0,
                    (fund_delta.c.fund_t / fund_delta.c.fund_t_1) > 1.5,
                )
            )
            .distinct()
        )

        return result

    @staticmethod
    def query_n_day_increase_small(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选n日涨幅小
        """
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)
        zdf_cte = AStockGlobalQuery._get_zdf_days(start_date, end_date, all_stocks_query, days=n)

        return (
            select(zdf_cte.c.secucode)
            .where(
                and_(
                    zdf_cte.c[f"zdf_{n}"] > 0,
                    zdf_cte.c[f"zdf_{n}"] < 0.1,
                )
            )
            .distinct()
        )

    @staticmethod
    def query_n_day_main_inflow_high(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选n日主力流入多

        quantitative_definition:
            T 日的n日主力资金/T-1 日的n日主力资金＞1.5，且T 日的n日主力资金＞0
        """
        # 获取日期范围，需要n+1天数据来计算T和T-1的n日资金
        _, start, end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            DailyZjdxStatLevel5, start_date, end_date, lookback_days=n + 1
        )
        start_date = get_date(start_date, is_start=True)
        end_date = get_date(end_date)
        # 计算T日n日资金 = zhu(T日) - zhu(T-N日)，移除默认值参数让数据不足时返回NULL
        fund_t = DailyZjdxStatLevel5.zhu - func.lagInFrame(DailyZjdxStatLevel5.zhu, n).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        )

        # 计算T-1日n日资金 = zhu(T-1日) - zhu(T-1-N日)
        fund_t_1 = func.lagInFrame(DailyZjdxStatLevel5.zhu, 1).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        ) - func.lagInFrame(DailyZjdxStatLevel5.zhu, n + 1).over(
            partition_by=DailyZjdxStatLevel5.secucode,
            order_by=DailyZjdxStatLevel5.date,
        )

        # 构建CTE，选择所有数据以便窗口函数计算
        fund_delta = (
            select(
                DailyZjdxStatLevel5.secucode,
                DailyZjdxStatLevel5.date,
                cast(fund_t, Float).label("fund_t"),
                cast(fund_t_1, Float).label("fund_t_1"),
            )
            .where(DailyZjdxStatLevel5.date.between(start, end))
            .subquery("fund_delta")
        )

        # 筛选条件：只选择最新一天，且fund_t和fund_t_1都不为NULL
        result = (
            select(fund_delta.c.secucode)
            .where(
                and_(
                    fund_delta.c.date.between(start_date, end_date),
                    fund_delta.c.fund_t.is_not(None),  # 数据充足，fund_t不为NULL
                    fund_delta.c.fund_t_1.is_not(None),  # 数据充足，fund_t_1不为NULL
                    fund_delta.c.fund_t > 0,
                    fund_delta.c.fund_t_1 != 0,
                    (fund_delta.c.fund_t / fund_delta.c.fund_t_1) > 1.5,
                )
            )
            .distinct()
        )

        return result

    @staticmethod
    def query_n_day_increase_small(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选n日涨幅小
        """
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)
        zdf_cte = AStockGlobalQuery._get_zdf_days(start_date, end_date, all_stocks_query, days=n)

        return (
            select(zdf_cte.c.secucode)
            .where(
                and_(
                    zdf_cte.c[f"zdf_{n}"] > 0,
                    zdf_cte.c[f"zdf_{n}"] < 0.1,
                )
            )
            .distinct()
        )

    @staticmethod
    def query_leading_stocks_in_solid_state_battery_concept_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选固态电池概念板块的龙头股
        """
        concept_query = AStockGlobalQuery.query_concept("BR01B02401", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_stocks_with_highest_capital_inflow_in_recent_n_months(
        n: int, start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选近n月资金流入最多的个股
        """
        trading_days = n * 20
        return AStockGlobalQuery.query_top_n_by_field(
            n=1,
            filter_type="stock",
            start_date=start_date,
            end_date=end_date,
            subquery=None,
            field=AStockGlobalQuery.get_zhu_in_days(trading_days),
        )

    @staticmethod
    def query_stocks_with_highest_main_capital_inflow_in_recent_n_days(
        n: int, start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选近n日主力资金流入最多的个股
        """
        trading_days = n * 20
        return AStockGlobalQuery.query_top_n_by_field(
            n=1,
            filter_type="stock",
            start_date=start_date,
            end_date=end_date,
            subquery=None,
            field=AStockGlobalQuery.get_zhu_in_days(trading_days),
        )

    @staticmethod
    def query_limit_down_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选炸板的股票
        """
        ...

    @staticmethod
    def query_copper_related_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选和铜相关的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ601212",
                    "SHHQ600490",
                    "SZHQ000737",
                    "SHHQ002335",
                    "SZHQ000878",
                    "SZHQ300697",
                    "SHHQ601609",
                    "SZHQ000630",
                    "SHHQ603979",
                    "SHHQ600711",
                    "SHHQ603527",
                    "SHHQ601899",
                    "SZHQ002295",
                    "SHHQ603993",
                    "SHHQ601168",
                    "SHHQ600362",
                ]
            )
        )

    @staticmethod
    def query_performance_forecast_increase(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选业绩预增的股票
        """
        # 获取业绩预告盈利大增的股票
        big_increase_query = AStockGlobalQuery.query_performance_forecast_big_increase(start_date, end_date)

        # 获取业绩预告盈利略增的股票
        small_increase_query = AStockGlobalQuery.query_performance_forecast_small_increase(start_date, end_date)

        # 使用 union 合并两个查询（去重）
        query = union(big_increase_query, small_increase_query)

        return query

    @staticmethod
    def query_performance_pre_increase_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选业绩预增在指定范围的股票
        """
        ...

    @staticmethod
    def query_snapdragon_concept_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选骁龙战机概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ302132",
                ]
            )
        )

    @staticmethod
    def query_three_lines_convergence(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选三线合一的个股
        """
        ...

    @staticmethod
    def query_stocks_with_good_themes(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选题材好的个股
        """
        ...

    @staticmethod
    def query_ai_medical_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选Ai医疗的龙头股
        """
        concept_query = AStockGlobalQuery.query_concept("BR01B02665", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_solid_state_battery_equipment_concept_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选固态电池设备概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02401", start_date, end_date)

    @staticmethod
    def query_cpo_concept_sector_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选Cpo概念板块的龙头股
        """
        concept_query = AStockGlobalQuery.query_concept("BR01B02512", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_solid_state_battery_equipment_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选固态电池设备龙头股票
        """
        concept_query = AStockGlobalQuery.query_concept("BR01B02401", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_cpo_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选Cpo板块
        """
        return AStockGlobalQuery.query_concept("BR01B02512", start_date, end_date)

    @staticmethod
    def query_profitability_best_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选盈利情况最好的股票

        quantitative_definition:
            归母净利润最多的股票
        """
        return AStockGlobalQuery.query_top_n_by_field(
            n=1,
            filter_type="stock",
            start_date=start_date,
            end_date=end_date,
            subquery=None,
            field=AStockGlobalQuery.get_parent_net_profit(),
        )

    @staticmethod
    def query_main_business_is_solid_state_battery_equipment(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主营业务为固态电池设备的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02401")

    @staticmethod
    def query_hudian_shares(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选沪电股份股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002463")

    @staticmethod
    def query_power_grid_automation_leading_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电网自动化龙头股
        """
        secucode_query = select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ601616",
                    "SZHQ002112",
                    "BJHQ920046",
                ]
            )
        )
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=secucode_query)

    @staticmethod
    def query_double_volume_breakthrough_platform(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选倍量突破平台的股票
        """
        ...

    @staticmethod
    def query_shrinking_adjustment_for_n_days(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选缩量调整了n天的股票
        """
        ...

    @staticmethod
    def query_stocks_not_breaking_n_day_line(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选没破n日线的股票
        """
        ...

    @staticmethod
    def query_high_popularity(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选高人气
        """
        return AStockGlobalQuery.query_top_n_by_field(
            n=10,
            filter_type="stock",
            start_date=start_date,
            end_date=end_date,
            subquery=None,
            field=AStockGlobalQuery.get_hot_value(),
        )

    @staticmethod
    def query_n_stocks_with_highest_rating(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选评分最高的n只股票
        """
        ...

    @staticmethod
    def query_small_market_cap(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选小市值

        quantitative_definition:
            流通市值较小的公司
            规则：

            筛选条件：
            - 流通市值 BETWEEN 30亿 AND 50亿
            数据来源： 行情数据、股本数据
        """
        # 流通市值在30亿到50亿之间
        return select(AStockMarketCur.secucode).where(AStockMarketCur.market_value.between(3000000000, 5000000000))

    @staticmethod
    def query_hot_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选热门板块的股票

        量化定义:
            筛选行业、概念、地域字段中包含热门板块代码的股票
        """
        from sqlalchemy import func

        # 1. 获取热门板块代码
        hot_board_codes = AStockGlobalQuery.query_hot_board_info(start_date, end_date).subquery()

        # 2. 筛选 trade 字段包含热门板块代码的股票（使用 locate + 过滤空字符串）
        matched_by_trade = (
            select(AStockBasic.secucode)
            .select_from(AStockBasic)
            .join(
                hot_board_codes,
                and_(
                    func.locate(hot_board_codes.c.secucode, AStockBasic.trade) > 0,
                    AStockBasic.trade.isnot(None),
                    AStockBasic.trade != "",
                ),
            )
        )

        # 3. 筛选 concept 字段包含热门板块代码的股票
        matched_by_concept = (
            select(AStockBasic.secucode)
            .select_from(AStockBasic)
            .join(
                hot_board_codes,
                and_(
                    func.locate(hot_board_codes.c.secucode, AStockBasic.concept) > 0,
                    AStockBasic.concept.isnot(None),
                    AStockBasic.concept != "",
                ),
            )
        )

        # 4. 筛选 area 字段包含热门板块代码的股票
        matched_by_area = (
            select(AStockBasic.secucode)
            .select_from(AStockBasic)
            .join(
                hot_board_codes,
                and_(
                    func.locate(hot_board_codes.c.secucode, AStockBasic.area) > 0,
                    AStockBasic.area.isnot(None),
                    AStockBasic.area != "",
                ),
            )
        )

        # 5. UNION 三个结果并去重
        return union(matched_by_trade, matched_by_concept, matched_by_area)

    @staticmethod
    def query_hot_sectors(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选热门板块
        """
        return AStockGlobalQuery.query_hot_board_info(start_date, end_date)

    @staticmethod
    def query_coal_chemical_industry_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选煤化工股票
        """
        return AStockGlobalQuery.query_concept("BR01B02095", start_date, end_date)

    @staticmethod
    def query_book_value_ratio_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选市净率在指定范围
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_pb_ratio(), interval, start_date, end_date)

    @staticmethod
    def query_commercial_aerospace_sector_stocks_with_growth_potential(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选商业航天板块有增长潜力的股票
        """
        ...

    @staticmethod
    def query_chip_concentration_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选筹码集中度在指定范围
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 分别对 cd_90 和 cd_70 使用 query_field_in_range，然后取交集
        cd_90_query = AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_cd_90(), interval, start_date, end_date
        )
        cd_70_query = AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_cd_70(), interval, start_date, end_date
        )

        return cd_90_query.union(cd_70_query)

    @staticmethod
    def query_power_transmission_and_distribution_equipment_enterprises_supporting_automation_facilities_leading_stocks(
        start_date: str = "0d", end_date: str = "0d"
    ):
        """
        term_explanation: 筛选输变电企业配套的自动化设施龙头
        """
        secucode_query = select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ000155",
                    "SHHQ601877",
                    "SZHQ301012",
                ]
            )
        )
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=secucode_query)

    @staticmethod
    def query_power(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电力概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02306", start_date, end_date)

    @staticmethod
    def query_power_upstream_and_downstream_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选电力上下游的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ601179",
                    "SZHQ000400",
                    "SHHQ603606",
                    "SHHQ600011",
                    "SHHQ600886",
                    "SHHQ600268",
                    "SHHQ600550",
                    "SZHQ002028",
                    "SZHQ002498",
                    "SHHQ603530",
                    "SHHQ601616",
                ]
            )
        )

    @staticmethod
    def query_main_force_adjusted_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主力调整的股票
        """
        ...

    @staticmethod
    def query_institutional_continuous_position_increase(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选机构连续增仓的股票
        """
        ...

    @staticmethod
    def query_neutrino_component_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选中微子成分股
        """
        return select(
            AStockBasic.secucode
        ).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ300327",  # 中颖电子：高精度模拟芯片，涉及探测器信号处理
                    "SHHQ601179",  # 中国西电：为大科学装置提供特种变压器及输变电配套
                    "SHHQ600089",  # 特变电工：参与国家重大科学工程的电力保障系统
                    "SZHQ002465",  # 海格通信：涉及高精度授时与同步系统（中微子实验关键技术）
                    "SHHQ603259",  # 药明康德：旗下合全医药涉及特种化学品（部分闪烁液溶剂相关）
                    "SHHQ688012",  # 中微公司：虽主业为半导体，但作为精密真空镀膜/等离子体技术龙头，与高能物理设备有技术交叉
                    "SHHQ600501",  # 航天晨光：提供真空压力容器，曾参与大科学装置密封配套
                    "SZHQ300722",  # 新莱应材：超高真空管路系统，满足中微子探测器的严苛真空要求
                    "SHHQ601727",  # 上海电气：参与国家重大科研基础设施的精密机械部分
                    "SHHQ601618",  # 中国中冶：参与江门中微子实验地下 700 米实验大厅的土建与加固工程
                    "SHHQ600111",  # 北方稀土：稀土永磁材料用于部分物理实验的磁场环境构建
                ]
            )
        )

    @staticmethod
    def query_dragon_turnback_high_quality_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选龙回头优质股
        """
        ...

    @staticmethod
    def query_obvious_accumulation_pending_rise_high_quality_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选吸筹明显待涨优质股
        """
        ...

    @staticmethod
    def query_main_force_continuous_enhancement(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主力连续增强的股票
        """
        ...

    @staticmethod
    def query_stocks_with_patents_reducing_rocket_recovery_difficulty(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选有降低火箭回收难度专利的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ002342",
                    "SZHQ000987",
                    "SZHQ000917",
                    "SHHQ600343",
                    "SHHQ600879",
                    "SHHQ688102",
                    "SHHQ688333",
                    "SZHQ300065",
                    "SHHQ688282",
                ]
            )
        )

    @staticmethod
    def query_zhongke_aerospace_related_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选中科宇航相关的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ000987",
                    "SHHQ600571",
                    "SZHQ300129",
                    "SZHQ301005",
                    "SZHQ300900",
                    "SHHQ688102",
                    "SHHQ688239",
                    "SHHQ605123",
                    "SZHQ300053",
                    "SHHQ688539",
                    "SHHQ688066",
                    "SHHQ688333",
                ]
            )
        )

    @staticmethod
    def query_state_grid_concept_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选国家电网概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ002498",
                    "SZHQ002545",
                    "SZHQ000066",
                    "SZHQ002851",
                    "SHHQ601616",
                    "SZHQ300490",
                    "SZHQ300830",
                    "SZHQ301248",
                    "SZHQ002358",
                    "SZHQ300277",
                    "SZHQ002214",
                    "SZHQ002236",
                    "SZHQ002857",
                    "SHHQ601179",
                    "SZHQ301218",
                    "SZHQ300491",
                    "SHHQ688390",
                    "SZHQ003035",
                    "SZHQ301106",
                    "SZHQ300201",
                    "SZHQ300139",
                    "SZHQ000021",
                    "SHHQ688262",
                    "SZHQ002364",
                    "SZHQ001287",
                    "SHHQ601619",
                    "SZHQ301132",
                    "SZHQ002446",
                    "SZHQ003028",
                    "SHHQ605196",
                    "SZHQ300283",
                    "SHHQ603829",
                    "SHHQ603556",
                    "SHHQ603638",
                    "SHHQ688469",
                    "SZHQ000967",
                    "SHHQ603530",
                    "SHHQ603045",
                    "SZHQ003043",
                    "SHHQ688171",
                    "SHHQ688226",
                    "SZHQ002169",
                    "SHHQ600171",
                    "SHHQ688060",
                    "SZHQ002272",
                    "SHHQ688083",
                    "SZHQ300476",
                    "SZHQ300014",
                    "SZHQ002074",
                    "SZHQ000070",
                    "SHHQ600525",
                    "SZHQ000586",
                    "SZHQ002929",
                    "SHHQ688589",
                    "SZHQ300933",
                    "SZHQ301029",
                    "SHHQ688391",
                    "SHHQ600869",
                    "SHHQ688327",
                    "SHHQ688618",
                    "SZHQ002706",
                    "SHHQ600487",
                    "SHHQ688450",
                    "SHHQ688159",
                    "SHHQ688191",
                    "SHHQ600797",
                    "SHHQ688259",
                    "SZHQ300531",
                    "SHHQ688693",
                    "SZHQ301233",
                    "SZHQ300853",
                    "SZHQ002380",
                    "SZHQ300002",
                    "SZHQ300988",
                    "SZHQ300407",
                    "SZHQ002130",
                    "SZHQ300711",
                    "SHHQ688681",
                    "SHHQ600502",
                    "SZHQ002474",
                    "SZHQ300496",
                    "SHHQ603803",
                    "SZHQ301327",
                    "SHHQ688396",
                    "SHHQ603236",
                    "SHHQ600590",
                    "SZHQ001229",
                    "SZHQ300638",
                    "SZHQ300345",
                    "SHHQ600355",
                    "SZHQ300423",
                    "SHHQ603666",
                    "SZHQ301046",
                    "SHHQ688579",
                    "SHHQ600850",
                    "SHHQ600268",
                    "SHHQ600353",
                    "SZHQ300353",
                    "SHHQ603063",
                    "SZHQ002622",
                    "SHHQ688152",
                    "SZHQ301390",
                    "SZHQ000856",
                    "SZHQ301162",
                    "SHHQ603015",
                    "SHHQ600577",
                    "SHHQ688517",
                    "SZHQ002514",
                    "SZHQ300789",
                    "SZHQ300713",
                    "SHHQ600522",
                    "SZHQ300248",
                    "SZHQ301361",
                    "SZHQ002519",
                    "SZHQ301386",
                    "SZHQ002179",
                    "SHHQ603528",
                    "SHHQ603496",
                    "SZHQ300982",
                    "SHHQ688080",
                    "SZHQ002184",
                    "SZHQ002212",
                    "SZHQ300076",
                    "SZHQ300183",
                    "SZHQ300184",
                    "SZHQ300287",
                    "SHHQ688616",
                    "SHHQ688719",
                    "SHHQ603071",
                    "SHHQ603050",
                    "SZHQ300768",
                    "SHHQ688609",
                    "SZHQ000901",
                    "SZHQ000409",
                    "SZHQ002316",
                    "SZHQ300187",
                    "SZHQ300276",
                    "SHHQ605066",
                    "BJHQ920029",
                    "SZHQ300339",
                    "SHHQ688330",
                    "BJHQ920375",
                    "SZHQ300001",
                    "SZHQ002656",
                    "SZHQ003008",
                    "SZHQ300259",
                    "SZHQ002533",
                    "SZHQ300365",
                    "SZHQ002028",
                    "SZHQ301179",
                    "SZHQ002139",
                    "SZHQ000333",
                    "SZHQ300341",
                    "SZHQ002560",
                    "SZHQ300366",
                    "SHHQ688692",
                    "SHHQ600973",
                    "SZHQ300513",
                    "SZHQ002322",
                    "SHHQ601222",
                    "SHHQ603618",
                    "SZHQ301609",
                    "SZHQ300222",
                    "SZHQ300827",
                    "SZHQ002227",
                    "SZHQ300675",
                    "SZHQ001388",
                    "SHHQ603861",
                    "SZHQ301295",
                    "SHHQ688611",
                    "SZHQ002063",
                    "SZHQ300712",
                    "SZHQ002015",
                    "SZHQ002879",
                    "SZHQ300286",
                    "SZHQ002168",
                    "SHHQ600885",
                    "SZHQ300667",
                    "SHHQ601567",
                    "SZHQ002090",
                    "SZHQ300018",
                    "BJHQ920118",
                    "SZHQ301638",
                    "SZHQ002452",
                    "SZHQ001382",
                    "SZHQ002692",
                    "BJHQ920556",
                    "SZHQ300051",
                    "SHHQ600131",
                    "SHHQ601727",
                    "SZHQ300360",
                    "SHHQ601126",
                    "SZHQ002339",
                    "SZHQ301291",
                    "SHHQ688676",
                    "SHHQ688248",
                    "SHHQ601877",
                    "SZHQ300514",
                    "BJHQ920037",
                    "SHHQ603097",
                    "SHHQ688597",
                    "SZHQ002471",
                    "SHHQ600468",
                    "SZHQ300670",
                    "SHHQ600517",
                    "SHHQ688411",
                    "SHHQ600509",
                    "SZHQ000862",
                    "SZHQ000533",
                    "SHHQ688334",
                    "SZHQ002350",
                    "SZHQ002309",
                    "SZHQ300098",
                    "SHHQ600406",
                    "SHHQ688100",
                    "SHHQ600379",
                    "SZHQ300140",
                    "SZHQ002927",
                    "SZHQ300376",
                    "SHHQ601700",
                    "SZHQ002121",
                    "SZHQ300040",
                    "SZHQ002441",
                    "SHHQ600101",
                    "SHHQ600089",
                    "SZHQ002546",
                    "SZHQ002451",
                    "SZHQ300932",
                    "SZHQ301668",
                    "SZHQ000400",
                    "SZHQ300880",
                    "BJHQ920508",
                    "SZHQ002298",
                    "SZHQ300682",
                    "SZHQ300510",
                    "SHHQ600550",
                    "SHHQ600312",
                    "SHHQ600192",
                    "SZHQ000682",
                    "SZHQ300617",
                    "SZHQ300882",
                    "SZHQ002112",
                    "SHHQ603070",
                    "SZHQ300062",
                    "SHHQ601869",
                    "SZHQ300265",
                    "BJHQ920496",
                    "SZHQ300069",
                    "SHHQ603191",
                    "SHHQ601096",
                    "BJHQ920062",
                    "SZHQ301120",
                    "SHHQ603421",
                    "SZHQ300444",
                    "SZHQ000993",
                    "SZHQ300477",
                    "SZHQ300427",
                    "SZHQ300141",
                    "BJHQ920046",
                    "SZHQ300215",
                    "BJHQ920299",
                ]
            )
        )

    @staticmethod
    def query_liquid_cooling_concept_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选液冷概念的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02524", start_date, end_date)

    @staticmethod
    def query_ai_upstream_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选AI上游股票
        """
        return select(AStockBasic.secucode).where(
            or_(
                # 算力基础设施
                AStockBasic.concept.like(f"%BR01B02520%"),  # 算力概念
                AStockBasic.concept.like(f"%BR01B02659%"),  # 服务器
                AStockBasic.concept.like(f"%BR01B02512%"),  # CPO概念
                AStockBasic.concept.like(f"%BR01B02078%"),  # 云计算概念
                AStockBasic.concept.like(f"%BR01B02364%"),  # 边缘计算
                AStockBasic.concept.like(f"%BR01B02522%"),  # 光通信模块概念
                AStockBasic.concept.like(f"%BR01B01253%"),  # 网络与通信
                # 芯片相关
                AStockBasic.concept.like(f"%BR01B02511%"),  # AI芯片概念
                AStockBasic.concept.like(f"%BR01B02523%"),  # 存储芯片概念
                AStockBasic.concept.like(f"%BR01B02741%"),  # FPGA芯片
                AStockBasic.concept.like(f"%BR01B02220%"),  # 国内芯片
                AStockBasic.concept.like(f"%BR01B02678%"),  # 芯片制造
                AStockBasic.concept.like(f"%BR01B02742%"),  # 芯片检测设备
                AStockBasic.concept.like(f"%BR01B02660%"),  # 半导体材料
                AStockBasic.concept.like(f"%BR01B02685%"),  # 半导体硅片
                AStockBasic.concept.like(f"%BR01B02263%"),  # 第三代半导体
                AStockBasic.concept.like(f"%BR01B02505%"),  # 第四代半导体概念
                # 其他硬件
                AStockBasic.concept.like(f"%BR01B02354%"),  # PCB印制电路板
                AStockBasic.concept.like(f"%BR01B02524%"),  # 液冷概念
                # 数据相关
                AStockBasic.concept.like(f"%BR01B02572%"),  # AI语料概念
                AStockBasic.concept.like(f"%BR01B02521%"),  # 数据要素概念
                AStockBasic.concept.like(f"%BR01B02503%"),  # 数据确权概念
                AStockBasic.concept.like(f"%BR01B02515%"),  # 时空大数据概念
                AStockBasic.concept.like(f"%BR01B02657%"),  # 数据安全
            )
        )

    @staticmethod
    def query_oversold_reversal(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选超跌反转的股票
        """
        ...

    @staticmethod
    def query_ai_computing_power(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选AI算力概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            or_(
                AStockBasic.concept.like(f"%BR01B02520%"),  # 算力概念
                AStockBasic.concept.like(f"%BR01B02659%"),  # 服务器
                AStockBasic.concept.like(f"%BR01B02512%"),  # CPO概念
                AStockBasic.concept.like(f"%BR01B02078%"),  # 云计算概念
            )
        )

    @staticmethod
    def query_innovative_drug_leaders(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选创新药的龙头
        """
        concept_query = AStockGlobalQuery.query_concept("BR01B02330", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_large_cap_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选大盘票

        quantitative_definition:
            筛选标准（满足其一即可）：
            - 条件1：总市值 > 500亿
            - 条件2：流通市值 > 300亿

            附加条件：
            - 换手率 > 0.5%（保证流动性）
            - 近5日成交额 > 5亿（近5日成交额总和，保证可交易性）
            - 非 ST 股票
        """
        from dao.interfaces import AStockMarket

        # 获取日期范围
        start = get_date(start_date, is_start=True, lookback_days=7)
        end = get_date(end_date)

        # 筛选标准（OR关系 - union）
        # 条件1：总市值 > 500亿
        condition_1 = select(AStockMarketCur.secucode).where(AStockMarketCur.market_cap > 50000000000)

        # 条件2：流通市值 > 300亿
        condition_2 = select(AStockMarketCur.secucode).where(AStockMarketCur.market_value > 30000000000)

        # 使用 union() 组合筛选标准
        market_cap_filter = union(condition_1, condition_2)

        # 附加条件（AND关系 - intersect）
        # 附加条件1：换手率 > 0.5%
        turnover_filter = select(AStockMarketCur.secucode).where(AStockMarketCur.toratio > 0.005)

        # 附加条件2：近5日成交额 > 5亿
        amount_5d = (
            select(
                AStockMarket.secucode,
                func.sum(AStockMarket.amount)
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date, rows=(-4, 0))
                .label("amount_5d"),
            )
            .where(AStockMarket.date.between(start, end))
            .subquery()
        )

        amount_filter = select(amount_5d.c.secucode).where(amount_5d.c.amount_5d > 500000000)

        # intersect 组合附加条件
        additional_filter = intersect(turnover_filter, amount_filter)

        # 最终结果：筛选标准 AND 附加条件
        return intersect(market_cap_filter, additional_filter)

    @staticmethod
    def query_low_interest_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选低息的股票
        """
        ...

    @staticmethod
    def query_commercial_aerospace_concept_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选商业航天概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02579", start_date, end_date)

    @staticmethod
    def query_3d_printing_concept_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选3D打印概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02105", start_date, end_date)

    @staticmethod
    def query_humanoid_robot_concept_sector_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选人形机器人概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02528", start_date, end_date)

    @staticmethod
    def query_iflytek(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选科大讯飞
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002230")

    @staticmethod
    def query_stocks_with_performance_increase_in_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选业绩涨幅在指定范围的股票
        """
        ...

    @staticmethod
    def query_stocks_suitable_for_long_term(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选适合做长线的股票
        """
        ...

    @staticmethod
    def query_stocks_that_can_be_opened_for_position(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选可以建仓的股票
        """
        ...

    @staticmethod
    def query_good_news_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选好消息的个股
        """
        ...

    @staticmethod
    def query_dai_mei_shares(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选岱美股份股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ603730")

    @staticmethod
    def query_jiangsu_sector_main_board_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选江苏板块主板股票
        """
        return AStockGlobalQuery.query_area("BR01B03018", start_date, end_date)

    @staticmethod
    def query_extra_high_voltage_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选特高压的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02292", start_date, end_date)

    @staticmethod
    def query_innovative_drugs_with_good_overseas_sales(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选创新药出国销售好的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ688235",  # 百济神州-U：BTK抑制剂全球放量，海外收入核心支撑
                    "SHHQ688506",  # 百利天恒：凭借与BMS的巨额ADC授权协议，成为出海黑马
                    "SHHQ603259",  # 药明康德：CXO龙头，80%以上收入来自海外
                    "SHHQ600276",  # 恒瑞医药：创新药海外授权（BD）进入爆发期
                    "SZHQ002422",  # 科伦药业：子公司科伦博泰与默沙东的ADC合作是出海标杆
                    "SHHQ688331",  # 荣昌生物：泰它西普等品种具有极强的全球竞争力
                    "SHHQ688180",  # 君实生物-U：PD-1（特瑞普利单抗）率先在美国FDA获批上市
                    "SHHQ600521",  # 华海药业：国内特色原料药及制剂出口领军企业
                    "SZHQ300347",  # 泰格医药：临床CRO海外多中心业务增长迅速
                    "SZHQ002399",  # 海普瑞：肝素钠原料药及制剂全球份额领先
                    "SHHQ603456",  # 九洲药业：CDMO海外业务占比极高
                    "SHHQ600196",  # 复星医药：国际化程度极高，在非洲及欧美均有深度布局
                    "SHHQ603127",  # 昭衍新药：海外实验室产能利用率及订单增长较快
                    "SHHQ688166",  # 博瑞医药：高端仿制药及原料药在海外市场竞争力强
                    "SZHQ300012",  # 华测检测：海外检测认证业务持续扩张
                ]
            )
        )

    @staticmethod
    def query_chemical_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选化工股票概念板块的股票
        """
        return AStockGlobalQuery.query_concept("BR01B01082", start_date, end_date)

    @staticmethod
    def query_non_ferrous_metals_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选有色金属股票
        """
        return AStockGlobalQuery.query_concept("BR01B01284", start_date, end_date)

    @staticmethod
    def query_ai_media_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选AI传媒概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    # --- AI 语料与数字版权 (论文相关度最高) ---
                    "SHHQ601928",  # 凤凰传媒 (教育语料/语料库)
                    "SZHQ300364",  # 中文在线 (AI短剧/语料版权)
                    "SZHQ000681",  # 视觉中国 (AI图片版权/多模态素材)
                    "SHHQ601858",  # 中国科传 (专业论文/学术语料)
                    "SHHQ603000",  # 人民网 (官媒语料/数据要素)
                    "SHHQ603888",  # 新华网 (官媒语料/AIGC溯源)
                    "SHHQ601949",  # 中国出版 (优质图书语料)
                    "SZHQ300788",  # 中信出版 (经管类高质量语料)
                    "SZHQ000719",  # 中原传媒 (数字教育语料)
                    "SHHQ600373",  # 中文传媒 (游戏+出版语料)
                    "SHHQ603533",  # 掌阅科技 (AI阅读/网文语料)
                    "SZHQ301052",  # 果麦文化 (AI校对/版权)
                    # --- AI 视频生成、影视与互动娱乐 ---
                    "SZHQ300133",  # 华策影视 (AI剧本/AI视频生成)
                    "SZHQ300413",  # 芒果超媒 (AIGC导演系统/虚拟主持人)
                    "SZHQ300251",  # 光线传媒 (AI动画电影)
                    "SZHQ300182",  # 捷成股份 (AI版权保护/影视素材)
                    "SZHQ300459",  # 汤姆猫 (AI语音交互/电子宠物)
                    "SHHQ603598",  # 引力传媒 (AI视频生成/营销)
                    "SHHQ600088",  # 中视传媒 (高清视频素材)
                    "SZHQ300556",  # 丝路视觉 (AI视觉设计/CG生成)
                    "SZHQ301313",  # 凡拓数创 (AI数字人/数智展览)
                    # --- AI 数字营销、代理与 Agent 场景 ---
                    "SZHQ300058",  # 蓝色光标 (AI营销智能体/BlueFocus AI)
                    "SZHQ301171",  # 易点天下 (AI出海营销/KreadoAI)
                    "SZHQ300781",  # 因赛集团 (InsightGPT视频生成)
                    "SZHQ002400",  # 省广集团 (AI营销落地)
                    "SZHQ002131",  # 利欧股份 (AI算法投放)
                    "SZHQ002027",  # 分众传媒 (营销Agent/数字化梯媒)
                    "SHHQ600986",  # 浙文互联 (数字虚拟人/营销)
                    "SZHQ300612",  # 宣亚国际 (AI营销技术)
                    "SZHQ300766",  # 每日互动 (AI数据标签/智能推送)
                    "SZHQ300785",  # 值得买 (AI购物助手/消费数据)
                    "SZHQ002878",  # 元隆雅图 (AI IP开发)
                    "SZHQ002291",  # 遥望科技 (AI直播/虚拟偶像)
                    "SZHQ002354",  # 天娱数科 (AI数字人/虚拟竞技)
                    "SZHQ002712",  # 思美传媒 (营销语料)
                    # --- 智算、数据分析与智慧传媒 ---
                    "SHHQ600633",  # 浙数文化 (数据交易所/传播大脑)
                    "SHHQ600640",  # 国脉文化 (数字文化算力)
                    "SZHQ301169",  # 零点有数 (AI决策/金融舆情分析)
                    "SZHQ300688",  # 创业黑马 (企业服务AI大模型)
                    "SZHQ300987",  # 川网传媒 (智能政务/媒体AI)
                    "SZHQ301382",  # 蜂助手 (算力调度服务)
                    # --- AI 教育与知识服务 ---
                    "SZHQ300654",  # 世纪天鸿 (小鸿助教/教育Agent)
                    "SZHQ300192",  # 科德教育 (AI职业教育)
                    "SZHQ000526",  # 学大教育 (AI辅导)
                    "SHHQ600661",  # 昂立教育 (教育数字化)
                    # --- 北交所 AI 相关 ---
                    "BJHQ920090",  # 同辉信息 (VR/AR+AI数字视觉)
                ]
            )
        )

    @staticmethod
    def query_ai_game_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选AI游戏概念板块的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    # --- 核心游戏研发与应用层 ---
                    "SZHQ002555",  # 三七互娱
                    "SZHQ002624",  # 完美世界
                    "SZHQ002602",  # 世纪华通
                    "SZHQ002174",  # 游族网络
                    "SZHQ002558",  # 巨人网络
                    "SZHQ300418",  # 昆仑万维
                    "SZHQ002517",  # 恺英网络
                    "SHHQ603444",  # 吉比特
                    "SZHQ300315",  # 掌趣科技
                    "SZHQ300533",  # 冰川网络
                    "SZHQ300002",  # 神州泰岳
                    "SZHQ300494",  # 盛天网络
                    "SZHQ300043",  # 星辉娱乐
                    "SZHQ002131",  # 利欧股份
                    "SZHQ002354",  # 天娱数科
                    # --- AI 陪伴、IP 与虚拟现实 ---
                    "SZHQ300459",  # 汤姆猫
                    "SZHQ300364",  # 中文在线
                    "SZHQ002292",  # 奥飞娱乐
                    "SZHQ300081",  # 恒信东方
                    "SZHQ300299",  # 富春股份
                    "SZHQ300264",  # 佳创视讯
                    "SZHQ301358",  # 湖南裕能（注：部分软件关联AI游戏资产，选入需注意）
                    "SZHQ002148",  # 北巴传媒
                    # --- AI 营销与视频生成（游戏买量/互动剧） ---
                    "SZHQ300058",  # 蓝色光标
                    "SZHQ301171",  # 易点天下
                    "SZHQ300624",  # 万兴科技
                    "SZHQ300781",  # 因赛集团
                    "SHHQ603598",  # 引力传媒
                    "SZHQ300612",  # 宣亚国际
                    "SHHQ601928",  # 凤凰传媒（含游戏语料/IP）
                    # --- 云游戏算力与基础设施 ---
                    "SZHQ300113",  # 顺网科技
                    "SZHQ000063",  # 中兴通讯（底层算力）
                    "SZHQ300442",  # 润泽科技
                    "SZHQ000066",  # 中国长城
                    "SZHQ000681",  # 视觉中国（AI 游戏素材库）
                    "SZHQ300052",  # 中青宝
                    # --- 北交所 AI 游戏/创意相关 (BJHQ) ---
                    "BJHQ835338",  # 凯腾精工（精密掩膜，关联芯片/游戏硬件）
                    "BJHQ833509",  # 华英证券（部分AI游戏重组个股）
                    "BJHQ834599",  # 同享科技
                    "BJHQ870433",  # 阿为特
                ]
            )
        )

    @staticmethod
    def query_construction_machinery_with_good_overseas_sales(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选工程机械出国销售好的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ600031",  # 三一重工：海外收入占比已超55%，挖掘机/混凝土机械全球领先
                    "SZHQ000425",  # 徐工机械：起重机械全球第一，股权激励后盈利能力释放明显
                    "SZHQ000157",  # 中联重科：海外收入占比首次突破50%，高分红+海外全线产品放量
                    "SZHQ000528",  # 柳工：土方机械出口龙头，印度、拉美等新兴市场增长极强
                    "SHHQ688425",  # 铁建重工：高端盾构机出口领先，进入沙特NEOM等大项目
                    "SHHQ601100",  # 恒立液压：核心零部件出海，配套全球龙头（如卡特彼勒、小松）
                    "SHHQ603338",  # 浙江鼎利：高空作业平台出口占比极高，受益于北美及欧洲市场
                    "SZHQ002097",  # 山河智能：微型挖掘机在欧洲市场具有极强品牌号召力
                    "SZHQ000680",  # 山推股份：推土机出口保持高位增长，受益于资源型国家矿山建设
                    "SHHQ600710",  # 苏美达：虽然是综合类，但在海外机电工程和小型工程机械外贸中份额巨大
                ]
            )
        )

    @staticmethod
    def query_good_sales_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选销售好的股票
        """
        ...

    @staticmethod
    def query_stocks_involving_phosphorus(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选涉及磷的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SZHQ002539",
                    "SZHQ300505",
                    "SZHQ000422",
                    "SZHQ000902",
                    "SZHQ002895",
                    "SHHQ600141",
                    "SHHQ600096",
                ]
            )
        )

    @staticmethod
    def query_stocks_involving_fluorine(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选涉及氟的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ603505",
                    "SZHQ002326",
                    "SHHQ605020",
                    "SHHQ603379",
                    "SHHQ600378",
                    "SZHQ002407",
                    "SHHQ600160",
                ]
            )
        )

    @staticmethod
    def query_stocks_involving_silicon(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选涉及硅的股票
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ688432",
                    "SHHQ688155",
                    "SZHQ002129",
                    "SHHQ688303",
                    "SZHQ300821",
                    "SHHQ600596",
                    "SHHQ603260",
                ]
            )
        )

    @staticmethod
    def query_companies_with_consistent_buy_and_sell_quantities(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选买卖股票数量一致的公司
        """
        ...

    @staticmethod
    def query_shrinking_low_point_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选缩量低点的股票
        """
        ...

    @staticmethod
    def query_market_rotation(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选大盘轮动的股票
        """
        ...

    @staticmethod
    def query_three_locks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选三把锁的股票
        """
        return AStockGlobalQuery.query_three_lock_open_position(start_date, end_date)

    @staticmethod
    def query_top_group(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选拓普集团股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ601689")

    def query_large_order_amount_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选大单金额在指定范围
        """
        ...

    @staticmethod
    def query_n_day_ma5_golden_cross_ma60(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选n日内MA5金叉MA60

        quantitative_definition:
            在最新一天的前n天内，MA5从下方穿过MA60。
        """
        # 不使用start_date和end_date，固定使用最新一天
        # 需要回溯59+n天：59天用于计算MA60，n天用于检测n天内的金叉
        end = get_date("0d")
        start = get_date("0d", is_start=True, lookback_days=59 + n)

        # 计算MA5和MA60
        ma5_value = func.avg(AStockMarket.close).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-4, 0),  # ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )

        ma60_value = func.avg(AStockMarket.close).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-59, 0),  # ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        )

        # 创建带rn的MA数据CTE
        ma_cte = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                cast(ma5_value, Float).label("ma5"),
                cast(ma60_value, Float).label("ma60"),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.date.between(start, end))
            .subquery()
        )

        # 创建别名进行自连接
        t1 = ma_cte.alias("t1")  # 今天
        t2 = ma_cte.alias("t2")  # 昨天

        # 检测金叉：昨天MA5 < MA60，今天MA5 > MA60
        # 且这个交叉发生在n天内（t1.rn <= n，因为rn=1是最新日，rn=n是前n-1日）
        golden_cross = (
            select(t1.c.secucode)
            .select_from(t1)
            .join(
                t2,
                and_(
                    t1.c.secucode == t2.c.secucode,
                    t2.c.rn - t1.c.rn == 1,  # t2是t1的前一天（使用减法避免类型转换问题）
                ),
            )
            .where(
                and_(
                    t2.c.ma5 < t2.c.ma60,  # 昨天：MA5 < MA60
                    t1.c.ma5 > t1.c.ma60,  # 今天：MA5 > MA60
                    t1.c.rn <= n,  # 金叉发生在最近n天内
                )
            )
            .distinct()
        )

        return golden_cross

    @staticmethod
    def query_close_price_greater_than_ma5(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选收盘价大于MA5
        """
        # 获取所有A股股票的基础查询
        secucode_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)

        # 获取5日均线CTE
        ma_5_cte = AStockGlobalQuery._get_ma_days(start_date, end_date, secucode_query, n=5)

        # 获取日期范围
        start = get_date(start_date, is_start=True)
        end = get_date(end_date)

        # 将 AStockMarket 和 MA5 CTE 关联，筛选收盘价 > MA5 的股票
        market_with_rn = (
            select(
                AStockMarket.secucode,
                AStockMarket.close,
                ma_5_cte.c.ma_5,
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .join(ma_5_cte, AStockMarket.secucode == ma_5_cte.c.secucode)
            .where(AStockMarket.date.between(start, end))
            .subquery("market_with_rn")
        )

        # 筛选收盘价 > MA5 的股票
        query = (
            select(market_with_rn.c.secucode)
            .where(
                and_(
                    market_with_rn.c.rn == 1,  # 只选择最新一天
                    market_with_rn.c.close > market_with_rn.c.ma_5,  # 收盘价 > MA5
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_ma5_upward_angle_greater_than_n_degrees(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选MA5向上角度大于n度
        """
        ...

    @staticmethod
    def query_machinery_export_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选机械出口的公司
        """
        return select(AStockBasic.secucode).where(
            AStockBasic.secucode.in_(
                [
                    "SHHQ600031",
                    "SZHQ000425",
                    "SZHQ000157",
                    "SHHQ600761",
                    "SHHQ603298",
                    "SZHQ002444",
                    "SHHQ600320",
                    "SZHQ000338",
                    "SHHQ603699",
                    "SZHQ002747",
                ]
            )
        )

    @staticmethod
    def query_low_valuation(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选低估值
        """
        return AStockGlobalQuery.query_low_valuation_pe("0d", "0d")

    @staticmethod
    def query_main_position_increasing_upward(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主力仓位向上增加的股票
        """
        ...

    @staticmethod
    def query_lanzhou_bank(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选兰州银行股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ001227")

    @staticmethod
    def query_top_n_by_volume(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选成交量前n的股票
        """
        return AStockGlobalQuery.query_top_n_by_field(
            n, "stock", start_date, end_date, None, AStockGlobalQuery.get_volume()
        )

    @staticmethod
    def query_supply_and_demand_funds_continuous_position_increase(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选供求资金连续加仓的个股
        """
        ...

    @staticmethod
    def query_jinfengkeji_stock(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选金风科技股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002202")

    @staticmethod
    def query_jingjiawei(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选景嘉微股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ300474")

    @staticmethod
    def query_jinshan_office(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选金山办公股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ688111")

    def query_ningxia_sector(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选宁夏板块的股票
        """
        return AStockGlobalQuery.query_area("BR01B03021", start_date, end_date)

    @staticmethod
    def query_top_n_stocks_by_main_force_net_inflow(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主力资金净流入前n名的股票
        """
        return AStockGlobalQuery.query_top_n_by_field(
            n, "stock", start_date, end_date, None, AStockGlobalQuery.get_zhu_in_days(1)
        )

    @staticmethod
    def query_stocks_with_top_n_dividend_yield(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选股息率排前n的股票
        """
        return AStockGlobalQuery.query_top_n_by_field(
            n, "stock", start_date, end_date, None, AStockGlobalQuery.get_dividend_yield()
        )

    @staticmethod
    def query_chint_electronics(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选正泰电器股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ601877")

    @staticmethod
    def query_main_business_is_ultra_high_voltage_transformer(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选主营业务为特高压变压器的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02292")

    @staticmethod
    def query_continuous_capital_inflow_for_n_days(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选连续n日资金持续流入的股票

        quantitative_definition:
            最近n个交易日，每天的1日主力资金净流入都 > 0
            数据来源：资金流向数据（daily_zjdx_stat_level5_view）
            选股规则：
            计算每日主力资金净流入 = T日zhu - (T-1)日zhu
            检查最近n天每天的净流入都 > 0

        :参数:
            n: 连续天数，必须大于0
        """
        if n <= 0:
            raise ValueError(f"n must be greater than 0, current value: {n}")

        # 获取所有股票作为基础查询
        base_query = select(AStockBasic.secucode)

        # 获取每日主力资金净流入 = T日zhu - (T-1)日zhu
        # 需要n+1天数据才能计算n天的净流入
        start = get_date(start_date, is_start=True, lookback_days=n + 1)
        end = get_date(end_date)

        zhu_with_prev = (
            select(
                DailyZjdxStatLevel5.secucode,
                DailyZjdxStatLevel5.zhu,
                func.lagInFrame(DailyZjdxStatLevel5.zhu, 1, 0)
                .over(
                    partition_by=DailyZjdxStatLevel5.secucode,
                    order_by=DailyZjdxStatLevel5.date,
                )
                .label("zhu_prev"),
                func.row_number()
                .over(
                    partition_by=DailyZjdxStatLevel5.secucode,
                    order_by=DailyZjdxStatLevel5.date.desc(),
                )
                .label("rn"),
            )
            .where(DailyZjdxStatLevel5.secucode.in_(select(base_query.c.secucode)))
            .where(DailyZjdxStatLevel5.date.between(start, end))
            .subquery()
        )

        # 计算每日净流入并筛选最近n天每天都 > 0的股票
        zhu_daily_inflow = select(
            zhu_with_prev.c.secucode,
            (zhu_with_prev.c.zhu - zhu_with_prev.c.zhu_prev).label("zhu_inflow_1d"),
            zhu_with_prev.c.rn,
        ).subquery()

        continuous_inflow = (
            select(zhu_daily_inflow.c.secucode)
            .where(
                zhu_daily_inflow.c.rn <= n,  # 最近n天
                zhu_daily_inflow.c.zhu_inflow_1d > 0,  # 每日净流入 > 0
            )
            .group_by(zhu_daily_inflow.c.secucode)
            .having(func.count() == n)  # n天都满足
        )

        return continuous_inflow

    @staticmethod
    def query_incremental_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选增量的股票
        """
        ...

    @staticmethod
    def query_tbea(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选特变电工股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600089")

    @staticmethod
    def query_baijiu_industry_leaders(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选白酒行业龙头
        """
        concept_query = AStockGlobalQuery.query_concept("BR01B01241", start_date, end_date)
        return AStockGlobalQuery.query_longtou(start_date, end_date, trade_or_concept=1, subquery=concept_query)

    @staticmethod
    def query_top_n_by_turnover(n: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选成交额前n的股票
        """
        return AStockGlobalQuery.query_top_n_by_field(
            n, "stock", start_date, end_date, None, AStockGlobalQuery.get_amount()
        )

    @staticmethod
    def query_surge_in_performance(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选业绩暴涨的股票
        """
        ...

    @staticmethod
    def query_huawei_concept_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选华为概念的股票
        """
        return AStockGlobalQuery.query_concept("BR01B02232", start_date, end_date)

    @staticmethod
    def query_huayang_group(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选华阳集团股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002906")

    def query_low_position_just_started(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选低位启动
        """
        return AStockGlobalQuery.query_low_start_pool(start_date, end_date)

    @staticmethod
    def query_semiconductor_material_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选半导体材料股票
        """
        return AStockGlobalQuery.query_concept("BR01B02660", start_date, end_date)

    @staticmethod
    def query_guotai_ces_semiconductor_chip_industry_etf_connected_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选国泰ces半导体芯片行业etf连接的股票
        """
        ...

    @staticmethod
    def query_hangxin_technology(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选航新科技股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ300424")

    def query_capital_change(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选资金异动
        """
        zhu_in = AStockGlobalQuery.query_zhu_flow(1, start_date, end_date)
        zhu_out = AStockGlobalQuery.query_zhu_flow(2, start_date, end_date)
        duo_in = AStockGlobalQuery.query_duo_flow(1, start_date, end_date)
        duo_out = AStockGlobalQuery.query_duo_flow(2, start_date, end_date)
        gan_in = AStockGlobalQuery.query_gan_flow(1, start_date, end_date)
        gan_out = AStockGlobalQuery.query_gan_flow(2, start_date, end_date)

        return zhu_in.union(zhu_out, duo_in, duo_out, gan_in, gan_out)

    @staticmethod
    def query_capital_supporting_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选资金面配合的股票

        quantitative_definition:
            近5日的主力资金 > 0
            且
            机构持股比例 > 20%
        """
        # 1. 5日主力资金 > 0
        fund_stocks = AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_zhu_days(5), "(0,inf)", start_date, end_date
        )

        # 2. 机构持股比例 > 20%
        institution_stocks = AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_organ_hold_ratio_total(), "(0.2,inf)", start_date, end_date
        )

        # 两个条件取交集
        return intersect(fund_stocks, institution_stocks)

    def query_low_position_breakout(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选低位突破的股票
        """
        return AStockGlobalQuery.query_low_start_pool(start_date, end_date)

    def query_low_position(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选低位
        """
        return AStockGlobalQuery.query_low_price_pool(start_date, end_date)

    @staticmethod
    def query_long_term_sideways(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选长期横盘

        quantitative_definition:
            股价在较长时间内（通常60日以上）在一个相对窄幅区间内波动，未形成明显趋势
            数据来源：日线数据 + 均线数据
            选股规则：

            过去60日振幅 = (最高价 - 最低价) / 最低价 < 30%
            且
            |今日收盘价 - MA20| / MA20 < 10%
        """
        # 获取所有A股股票的基础查询
        all_stocks_query = select(AStockBasic.secucode)

        # 获取日期范围（近60日）
        end = get_date(end_date)
        start = get_date(start_date, is_start=True, lookback_days=60)

        # 计算近60日内每个股票的最高价和最低价
        price_range = (
            select(
                AStockMarket.secucode,
                func.max(AStockMarket.high).label("highest_price"),
                func.min(AStockMarket.low).label("lowest_price"),
            )
            .where(AStockMarket.date.between(start, end))
            .group_by(AStockMarket.secucode)
            .subquery()
        )

        # 获取MA20
        ma_20_cte = AStockGlobalQuery._get_ma_days(start_date, end_date, all_stocks_query, n=20)

        # 获取今日收盘价
        start_current = get_date(start_date, is_start=True)
        end_current = get_date(end_date)
        close_query = (
            select(AStockMarket.secucode, AStockMarket.close.label("today_close"))
            .where(AStockMarket.date.between(start_current, end_current))
            .subquery()
        )

        # 组合条件：振幅 < 30% 且 |收盘价 - MA20| / MA20 < 10%
        query = (
            select(price_range.c.secucode)
            .select_from(price_range)
            .join(ma_20_cte, price_range.c.secucode == ma_20_cte.c.secucode)
            .join(close_query, price_range.c.secucode == close_query.c.secucode)
            .where(
                and_(
                    # 条件1：振幅 < 30%
                    (price_range.c.highest_price - price_range.c.lowest_price) / price_range.c.lowest_price < 0.30,
                    # 条件2：|今日收盘价 - MA20| / MA20 < 10%
                    func.abs(close_query.c.today_close - ma_20_cte.c.ma_20) / ma_20_cte.c.ma_20 < 0.10,
                )
            )
            .distinct()
        )

        return query

    @staticmethod
    def query_first_board_limit_up(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选首板涨停

        quantitative_definition: 近 20 个交易日，股票出现首次涨停（20日内涨停一次且今日涨停）
        """
        # 获取所有A股股票作为基础查询（过滤停牌）
        all_stocks_query = select(AStockBasic.secucode).where(AStockBasic.type == "A股", AStockBasic.is_ts == 0)

        # 调用 _get_zt_days 获取20日内涨停次数 CTE
        zt_count_cte = AStockGlobalQuery._get_zt_days(
            start_date=start_date, end_date=end_date, secucode_query=all_stocks_query, days=20
        )

        # 获取今日涨停的股票
        _, start, end, _ = AStockGlobalQuery._get_aftermarket_date_range(
            AStockMarket, start_date, end_date, lookback_days=1
        )

        today_limit_up = (
            select(AStockMarket.secucode).where(
                and_(
                    AStockMarket.date == end,  # 今日
                    AStockMarket.zt == 1,  # 涨停
                )
            )
        ).subquery("today_limit_up")

        # 筛选条件：20日内涨停次数 = 1 且今日涨停
        query = (
            select(zt_count_cte.c.secucode)
            .join(today_limit_up, zt_count_cte.c.secucode == today_limit_up.c.secucode)
            .where(zt_count_cte.c.zt_20d_count == 1)
        )

        return query

    @staticmethod
    def query_four_creation_electronics(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选四创电子股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ600990")

    def query_significant_capital_inflow(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选资金明显流入的股票
        """
        return AStockGlobalQuery.query_fund_large_inflow(start_date, end_date)

    @staticmethod
    def query_low_volatility_stocks(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选低位震荡的股票
        """
        ...

    @staticmethod
    def query_lanchu_technology(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选澜起科技股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ688008")

    @staticmethod
    def query_china_aluminum(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选中国铝业股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ601600")

    @staticmethod
    def query_luxshare_precision(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选立讯精密
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002475")

    @staticmethod
    def query_weiming_environmental_protection(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选伟明环保股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ603568")

    @staticmethod
    def query_wu_xi_app_tec(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选药明康德股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ603259")

    @staticmethod
    def query_xinshida(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选新时达股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002527")

    def query_huahong_technology(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选华宏科技股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ002645")

    @staticmethod
    def query_riying_electronics(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选日盈电子股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SHHQ603286")

    @staticmethod
    def query_weichai_heavy_machinery(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选潍柴重机股票
        """
        return select(AStockBasic.secucode).where(AStockBasic.secucode == "SZHQ000880")

    @staticmethod
    def query_dmi_golden_cross(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选DMI金叉
        """
        ...

    @staticmethod
    def query_ma_cross_above(n: int, m: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选n日均线上穿m日均线的股票

        quantitative_definition:
            昨日n日MA < m日MA，今日n日MA > m日MA（金叉信号）
            数据来源：日线OHLC数据
            选股规则：
            1. 昨日：MA_n < MA_m
            2. 今日：MA_n > MA_m
        """
        # 需要回溯足够的天数：max(n,m)-1用于计算MA，再加1天用于昨日数据
        max_ma = max(n, m)
        start = get_date(start_date, is_start=True, lookback_days=max_ma)
        end = get_date(end_date)

        # 计算MA_n和MA_m
        ma_n_value = func.avg(AStockMarket.close).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-(n - 1), 0),  # ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW
        )

        ma_m_value = func.avg(AStockMarket.close).over(
            partition_by=AStockMarket.secucode,
            order_by=AStockMarket.date,
            rows=(-(m - 1), 0),  # ROWS BETWEEN m-1 PRECEDING AND CURRENT ROW
        )

        # 创建带rn的MA数据CTE
        ma_cte = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                cast(ma_n_value, Float).label("ma_n"),
                cast(ma_m_value, Float).label("ma_m"),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.date.between(start, end))
            .subquery()
        )

        # 创建别名进行自连接
        t1 = ma_cte.alias("t1")  # 今天 (rn=1)
        t2 = ma_cte.alias("t2")  # 昨天 (rn=2)

        # 检测上穿（金叉）：昨天MA_n < MA_m，今天MA_n > MA_m
        # 明确指定：t1=rn=1（今天），t2=rn=2（昨天）
        golden_cross = (
            select(t1.c.secucode)
            .select_from(t1)
            .join(
                t2,
                and_(
                    t1.c.secucode == t2.c.secucode,
                    t1.c.rn == 1,  # t1必须是rn=1（今天）
                    t2.c.rn == 2,  # t2必须是rn=2（昨天）
                ),
            )
            .where(
                and_(
                    t2.c.ma_n < t2.c.ma_m,  # 昨天：MA_n < MA_m
                    t1.c.ma_n > t1.c.ma_m,  # 今天：MA_n > MA_m
                )
            )
            .distinct()
        )

        return golden_cross

    @staticmethod
    def query_ma_golden_cross(n: int, m: int, start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选n日均线与m日均线金叉的股票

        quantitative_definition:
            昨日n日MA < m日MA，今日n日MA > m日MA（金叉信号）
        """
        return StockNewApi.query_ma_cross_above(n, m, start_date, end_date)

    @staticmethod
    def query_social_shareholding_quantity_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选社保持股家数在指定范围的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_social_security_hold_num(), interval, start_date, end_date
        )

    @staticmethod
    def query_fund_shareholding_quantity_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选基金持股家数在指定范围的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_fund_hold_num(), interval, start_date, end_date
        )

    @staticmethod
    def query_institutional_shareholding_quantity_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选机构持股家数在指定范围的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_organ_hold_total(), interval, start_date, end_date
        )

    @staticmethod
    def query_private_shareholding_quantity_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选持有本股私募数在指定范围的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_smyx_num(), interval, start_date, end_date)

    @staticmethod
    def query_increased_private_shareholding_quantity_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选增仓私募数在指定范围的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_smyx_zc_num(), interval, start_date, end_date
        )

    # 机构持股
    @staticmethod
    def query_institutional_shareholding(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选机构持股的股票

        quantitative_definition:
            机构持股家数> 0的股票
        """
        return StockNewApi.query_institutional_shareholding_quantity_within_specified_range(
            low=0, high=float("inf"), low_closed=False, high_closed=True, start_date=start_date, end_date=end_date
        )

    # 私募持股
    @staticmethod
    def query_private_shareholding(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选私募持股的股票

        quantitative_definition:
            持有本股私募数 > 0的股票
        """
        return StockNewApi.query_private_shareholding_quantity_within_specified_range(
            low=0, high=float("inf"), low_closed=False, high_closed=True, start_date=start_date, end_date=end_date
        )

    # 基金持股
    @staticmethod
    def query_fund_shareholding(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选基金持股的股票

        quantitative_definition:
            基金持股家数 > 0的股票
        """
        return StockNewApi.query_fund_shareholding_quantity_within_specified_range(
            low=0, high=float("inf"), low_closed=False, high_closed=True, start_date=start_date, end_date=end_date
        )

    # 社保持股
    @staticmethod
    def query_social_security_shareholding(start_date: str = "0d", end_date: str = "0d"):
        """
        term_explanation: 筛选社保持股的股票

        quantitative_definition:
            社保持股家数 > 0的股票
        """
        return StockNewApi.query_social_shareholding_quantity_within_specified_range(
            low=0, high=float("inf"), low_closed=False, high_closed=True, start_date=start_date, end_date=end_date
        )

    @staticmethod
    def query_pe_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选市盈率在指定范围的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 判断日期范围类型
        is_current_day_only = start_date == "0d" and end_date == "0d"  # 纯当天查询
        is_cross_to_current = end_date == "0d" and start_date != "0d"  # 跨越到当天（如 -10d0d）

        if is_current_day_only:
            # 纯当天查询：使用当前表字段
            return AStockGlobalQuery.query_field_in_range(
                AStockGlobalQuery.get_pe_ratio_static(mode=1), interval, start_date, end_date
            )
        elif is_cross_to_current:
            # 跨天查询（如 -10d0d）：union 当前表和历史表
            current_query = AStockGlobalQuery.query_field_in_range(
                AStockGlobalQuery.get_pe_ratio_static(mode=1), interval, start_date, end_date
            )
            history_query = AStockGlobalQuery.query_field_in_range(
                AStockGlobalQuery.get_pe_ratio_static(mode=0), interval, start_date, end_date
            )
            return current_query.union(history_query)
        else:
            # 纯历史查询：使用历史表字段
            return AStockGlobalQuery.query_field_in_range(
                AStockGlobalQuery.get_pe_ratio_static(mode=0), interval, start_date, end_date
            )

    @staticmethod
    def query_share_net_asset_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选每股净资产在指定范围的股票

        quantitative_definition:
            每股净资产在指定范围内的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 直接调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(
            AStockGlobalQuery.get_share_net_asset(), interval, start_date, end_date
        )

    @staticmethod
    def query_90_chip_concentration_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选90%筹码集中度在指定范围的股票

        quantitative_definition:
            90%筹码集中度在指定范围内的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 直接调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_cd_90(), interval, start_date, end_date)

    @staticmethod
    def query_70_chip_concentration_within_specified_range(
        low: float = 0.0,
        high: float = 0.0,
        low_closed: bool = True,
        high_closed: bool = True,
        start_date: str = "0d",
        end_date: str = "0d",
    ):
        """
        term_explanation: 筛选70%筹码集中度在指定范围的股票

        quantitative_definition:
            70%筹码集中度在指定范围内的股票
        """
        # 构建区间字符串
        low_bracket = "[" if low_closed else "("
        high_bracket = "]" if high_closed else ")"
        interval = f"{low_bracket}{low},{high}{high_bracket}"

        # 直接调用 AStockGlobalQuery.query_field_in_range
        return AStockGlobalQuery.query_field_in_range(AStockGlobalQuery.get_cd_70(), interval, start_date, end_date)

    @staticmethod
    def get_company_name():
        """
        term_explanation: 获取公司名称


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_detail_company_name()

    @staticmethod
    def get_company_phone():
        """
        term_explanation: 获取公司电话


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_detail_telephone()

    def get_main_business():
        """
        term_explanation: 获取主营业务


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_detail_main_business()

    @staticmethod
    def get_stock_code():
        """
        term_explanation: 获取股票代码


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_secucode()

    @staticmethod
    def get_net_profit_growth_rate():
        """
        term_explanation: 获取归母净利润同比增长率指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_yoy(AStockGlobalQuery.get_parent_net_profit())

    @staticmethod
    def get_chip_distribution_indicator():
        """
        term_explanation: 获取筹码分布指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_chip()

    @staticmethod
    def get_household_count_indicator():
        """
        term_explanation: 获取户数指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_total_people_num()

    @staticmethod
    def get_main_capital_inflow_indicator():
        """
        term_explanation: 获取主力资金流入指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_zhu_days(1)

    @staticmethod
    def get_stock_price_indicators():
        """
        term_explanation: 获取股价指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_close()

    @staticmethod
    def get_n_day_ma_indicator(n: int):
        """
        term_explanation: 获取n日均线指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_ma_days(n)

    @staticmethod
    def get_increase_rate_indicator():
        """
        term_explanation: 获取涨幅指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_zdf_days(1)

    @staticmethod
    def get_industry_indicators():
        """
        term_explanation: 获取行业指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_trade()

    def get_volume_ratio():
        """
        term_explanation: 获取量比指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_qrr()

    @staticmethod
    def get_pressure_level_indicator():
        """
        term_explanation: 获取压力位指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_r_price()

    @staticmethod
    def get_heat_value_indicator():
        """
        term_explanation: 获取热度值指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_hot_value()

    @staticmethod
    def get_roe_indicator():
        """
        term_explanation: 获取ROE指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_roe()

    @staticmethod
    def get_support_level_indicator():
        """
        term_explanation: 获取支撑位指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_s_price()

    @staticmethod
    def get_net_profit_attributable_to_parent_company():
        """
        term_explanation: 获取归母净利润指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_parent_net_profit()

    @staticmethod
    def get_belonging_sector():
        """
        term_explanation: 获取所属板块


        Returns:
            Field: 返回值说明
        """
        return [AStockGlobalQuery.get_trade(), AStockGlobalQuery.get_concept(), AStockGlobalQuery.get_detail_region()]

    @staticmethod
    def get_institutional_shareholding_number_indicator():
        """
        term_explanation: 获取机构持股数指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_organ_hold_total()

    @staticmethod
    def get_institutional_shareholding_ratio_indicator():
        """
        term_explanation: 获取机构持股比例指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_organ_hold_ratio_total()

    @staticmethod
    def get_capital_flow_indicator():
        """
        term_explanation: 获取资金流向指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_zhu_flag()

    @staticmethod
    def get_comprehensive_score_indicator():
        """
        term_explanation: 获取综合评分指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_synthetic()

    @staticmethod
    def get_listing_time_indicator():
        """
        term_explanation: 获取上市时间指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_issue_list_date()

    @staticmethod
    def get_order_status_indicator():
        """
        term_explanation: 获取挂单情况指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_annual_report_forecast_increase_indicator():
        """
        term_explanation: 获取年报预增指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_main_control_line_indicator():
        """
        term_explanation: 获取主力控盘线指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_attack_line_indicator():
        """
        term_explanation: 获取攻击线指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_optimal_score_indicator():
        """
        term_explanation: 获取最优评分指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_historical_high_point_indicator():
        """
        term_explanation: 获取历史最高点指标


        Returns:
            Field: 返回值说明
        """
        return "_get_historical_high_point_indicator", "historical_max_high", {}

    @staticmethod
    def _get_historical_high_point_indicator(start_date: str = "0d", end_date: str = "0d", secucode_query=None):
        """
        计算历史最高点的实际实现方法

        :参数:
            start_date: 起始日期
            end_date: 结束日期
            secucode_query: 股票代码查询
        """
        import uuid
        from sqlalchemy import cast, Float

        if secucode_query is None:
            raise ValueError("secucode_query must be provided")

        subquery = secucode_query.subquery()

        key_label = "historical_max_high"

        # 计算历史最高价：使用窗口函数
        # MAX(high) OVER (PARTITION BY secucode) 会计算每个股票的所有历史最高价
        historical_max_high_value = func.max(AStockMarket.high).over(
            partition_by=AStockMarket.secucode,
        )

        # 使用窗口函数计算每个股票的历史最高价（不限制日期范围）
        high_with_rn = (
            select(
                AStockMarket.secucode,
                AStockMarket.date,
                cast(historical_max_high_value, Float).label(key_label),
                func.row_number()
                .over(partition_by=AStockMarket.secucode, order_by=AStockMarket.date.desc())
                .label("rn"),
            )
            .where(AStockMarket.secucode.in_(select(subquery.c.secucode)))
            .subquery()
        )

        # 只选择每个股票的最新一天（rn=1）
        historical_max_high_cte = (
            select(high_with_rn.c.secucode, high_with_rn.c.date, high_with_rn.c[key_label])
            .where(high_with_rn.c.rn == 1)
            .cte(f"historical_max_high_cte_{uuid.uuid4().hex[:6]}")
        )

        return historical_max_high_cte

    @staticmethod
    def get_rsi_indicator():
        """
        term_explanation: 获取Rsi指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_main_control_degree_indicator():
        """
        term_explanation: 获取主力控盘度指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_margin_trading_ratio():
        """
        term_explanation: 获取融资融券占比指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_multi_dimensional_evaluation_score_indicators():
        """
        term_explanation: 获取多维度评估分指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_n_day_capital_inflow_indicator(n: int):
        """
        term_explanation: 获取n日的资金流入指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_zhu_in_days(n)

    def get_commitment_ratio():
        """
        term_explanation: 获取委比指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_loss_indicator():
        """
        term_explanation: 获取亏损指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_hangdian_share_hot_topic_industry_analysis():
        """
        term_explanation: 获取杭电股份热点题材行业分析


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_financial_data_indicators():
        """
        term_explanation: 获取财务资料指标


        Returns:
            Field: 返回值说明
        """
        # 营业收入,归母净利润,基本每股收益,销售毛利率,净资产收益率,营业收入同比增长率,归母净利润同比增长率
        return [
            AStockGlobalQuery.get_income_total(),
            AStockGlobalQuery.get_parent_net_profit(),
            AStockGlobalQuery.get_basic_eps(),
            AStockGlobalQuery.get_gross_margin(),
            AStockGlobalQuery.get_roe(),
            AStockGlobalQuery.get_yoy(AStockGlobalQuery.get_income_total()),
            AStockGlobalQuery.get_yoy(AStockGlobalQuery.get_parent_net_profit()),
        ]

    @staticmethod
    def get_private_placement_listing_time_indicator():
        """
        term_explanation: 获取增发上市时间指标


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_addition_date()

    @staticmethod
    def get_what_company_does():
        """
        term_explanation: 获取公司是做什么的


        Returns:
            Field: 返回值说明
        """
        return AStockGlobalQuery.get_detail_main_business()

    @staticmethod
    def get_growth_rate_indicator():
        """
        term_explanation: 获取增长率指标


        Returns:
            Field: 返回值说明
        """
        ...

    @staticmethod
    def get_investment_indicators():
        """
        term_explanation: 获取投资指标


        Returns:
            Field: 返回值说明
        """
        ...
