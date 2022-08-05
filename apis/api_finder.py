from flask_restful import Resource
from flask_restful import reqparse
from sqlalchemy import func, case, and_, literal_column
from sqlalchemy.orm import aliased
from db.db_model import UserFilterPreset, StockViewCount, UserStarredStock, StockInfo, StockDailyTechnical, \
    StockDerivedVar, StockValuationIndicator, DartFinancialRatio, DartSimpleFinancialStatementsTtm, session_scope
from db.db_connect import get_df_from_db
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from functools import reduce
from apis.api_user_info import check_user_validity
import json
from util.util_get_country import get_country
from util.util_get_status_msg import get_status_msg


class GetFilteredData(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        # 종목코드
        parser.add_argument('stock_cd', type=str)
        # 기업개요 필터
        parser.add_argument('market', type=str, action='append')
        parser.add_argument('market_cap_from', type=int)
        parser.add_argument('market_cap_to', type=int)
        parser.add_argument('sector', type=str, action='append')
        parser.add_argument('price_from', type=int)
        parser.add_argument('price_to', type=int)
        parser.add_argument('trading_vol_prev_1d_from', type=int)
        parser.add_argument('trading_vol_prev_1d_to', type=int)
        parser.add_argument('price_momt_1d_from', type=float)
        parser.add_argument('price_momt_1d_to', type=float)
        parser.add_argument('price_momt_1w_from', type=float)
        parser.add_argument('price_momt_1w_to', type=float)
        parser.add_argument('price_momt_1m_from', type=float)
        parser.add_argument('price_momt_1m_to', type=float)
        parser.add_argument('price_momt_1q_from', type=float)
        parser.add_argument('price_momt_1q_to', type=float)
        parser.add_argument('price_momt_1h_from', type=float)
        parser.add_argument('price_momt_1h_to', type=float)
        # 벨류에이션 필터
        parser.add_argument('per_from', type=float)
        parser.add_argument('per_to', type=float)
        parser.add_argument('pbr_from', type=float)
        parser.add_argument('pbr_to', type=float)
        parser.add_argument('eps_from', type=float)
        parser.add_argument('eps_to', type=float)
        parser.add_argument('roe_from', type=float)
        parser.add_argument('roe_to', type=float)
        parser.add_argument('bps_from', type=float)
        parser.add_argument('bps_to', type=float)
        parser.add_argument('eps_growth_3y_from', type=float)
        parser.add_argument('eps_growth_3y_to', type=float)
        parser.add_argument('eps_growth_1q_from', type=float)
        parser.add_argument('eps_growth_1q_to', type=float)
        # 재무 필터
        parser.add_argument('revenue_from', type=int)
        parser.add_argument('revenue_to', type=int)
        parser.add_argument('revenue_growth_1q_from', type=float)
        parser.add_argument('revenue_growth_1q_to', type=float)
        parser.add_argument('current_ratio_from', type=float)
        parser.add_argument('current_ratio_to', type=float)
        parser.add_argument('debt_ratio_from', type=float)
        parser.add_argument('debt_ratio_to', type=float)
        parser.add_argument('operating_margin_from', type=float)
        parser.add_argument('operating_margin_to', type=float)
        parser.add_argument('net_profit_margin_from', type=float)
        parser.add_argument('net_profit_margin_to', type=float)
        parser.add_argument('operating_income_loss_from', type=int)
        parser.add_argument('operating_income_loss_to', type=int)
        parser.add_argument('profit_loss_from', type=int)
        parser.add_argument('profit_loss_to', type=int)

        args = parser.parse_args()
        # print(args)

        # 종목코드 필터
        stock_cd = args['stock_cd']

        # 종목코드가 입력되었을 경우, get all filter data 함수를 사용해 데이터 return
        if stock_cd is not None:
            return_data = get_all_filter_data([stock_cd])

            return {'status': '000', 'msg': get_status_msg('000'),
                    'result_list': [{r.index[i]: r[i] for i in range(len(r))} for i, r in return_data.iterrows()]}

        # 기업개요 필터
        market = args['market']
        market_cap_from = args['market_cap_from']
        market_cap_from = None if market_cap_from == 50 else market_cap_from
        market_cap_to = args['market_cap_to']
        market_cap_to = None if market_cap_to == 10000 else market_cap_to
        sector = args['sector']
        price_from = args['price_from']
        price_from = None if price_from == 1000 else price_from
        price_to = args['price_to']
        price_to = None if price_to == 500000 else price_to

        # stock_derived_var
        trading_vol_prev_1d_from = args['trading_vol_prev_1d_from']
        trading_vol_prev_1d_from = None if trading_vol_prev_1d_from == 0 else trading_vol_prev_1d_from
        trading_vol_prev_1d_to = args['trading_vol_prev_1d_to']
        trading_vol_prev_1d_to = None if trading_vol_prev_1d_to == 10000000 else trading_vol_prev_1d_to
        price_momt_1d_from = args['price_momt_1d_from']
        price_momt_1d_from = None if price_momt_1d_from == -30 else price_momt_1d_from
        price_momt_1d_to = args['price_momt_1d_to']
        price_momt_1d_to = None if price_momt_1d_to == 30 else price_momt_1d_to
        price_momt_1w_from = args['price_momt_1w_from']
        price_momt_1w_from = None if price_momt_1w_from == -50 else price_momt_1w_from
        price_momt_1w_to = args['price_momt_1w_to']
        price_momt_1w_to = None if price_momt_1w_to == 50 else price_momt_1w_to
        price_momt_1m_from = args['price_momt_1m_from']
        price_momt_1m_from = None if price_momt_1m_from == -100 else price_momt_1m_from
        price_momt_1m_to = args['price_momt_1m_to']
        price_momt_1m_to = None if price_momt_1m_to == 100 else price_momt_1m_to
        price_momt_1q_from = args['price_momt_1q_from']
        price_momt_1q_from = None if price_momt_1q_from == -100 else price_momt_1q_from
        price_momt_1q_to = args['price_momt_1q_to']
        price_momt_1q_to = None if price_momt_1q_to == 200 else price_momt_1q_to
        price_momt_1h_from = args['price_momt_1h_from']
        price_momt_1h_from = None if price_momt_1h_from == -500 else price_momt_1h_from
        price_momt_1h_to = args['price_momt_1h_to']
        price_momt_1h_to = None if price_momt_1h_to == 500 else price_momt_1h_to

        # 벨류에이션 필터
        per_from = args['per_from']
        per_from = None if per_from == -500 else per_from
        per_to = args['per_to']
        per_to = None if per_to == 600 else per_to
        pbr_from = args['pbr_from']
        pbr_from = None if pbr_from == 0 else pbr_from
        pbr_to = args['pbr_to']
        pbr_to = None if pbr_to == 30 else pbr_to
        eps_from = args['eps_from']
        eps_from = None if eps_from == -5000 else eps_from
        eps_to = args['eps_to']
        eps_to = None if eps_to == 50000 else eps_to
        roe_from = args['roe_from']
        roe_from = None if roe_from == -150 else roe_from
        roe_to = args['roe_to']
        roe_to = None if roe_to == 100 else roe_to
        bps_from = args['bps_from']
        bps_from = None if bps_from == 50 else bps_from
        bps_to = args['bps_to']
        bps_to = None if bps_to == 500000 else bps_to
        eps_growth_3y_from = args['eps_growth_3y_from']
        eps_growth_3y_from = None if eps_growth_3y_from == -5000 else eps_growth_3y_from
        eps_growth_3y_to = args['eps_growth_3y_to']
        eps_growth_3y_to = None if eps_growth_3y_to == 5000 else eps_growth_3y_to
        eps_growth_1q_from = args['eps_growth_1q_from']
        eps_growth_1q_from = None if eps_growth_1q_from == -500 else eps_growth_1q_from
        eps_growth_1q_to = args['eps_growth_1q_to']
        eps_growth_1q_to = None if eps_growth_1q_to == 500 else eps_growth_1q_to

        # 재무 필터
        revenue_from = args['revenue_from']
        revenue_from = None if revenue_from == 10 else revenue_from
        revenue_to = args['revenue_to']
        revenue_to = None if revenue_to == 250000 else revenue_to
        revenue_growth_1q_from = args['revenue_growth_1q_from']
        revenue_growth_1q_from = None if revenue_growth_1q_from == 0 else revenue_growth_1q_from
        revenue_growth_1q_to = args['revenue_growth_1q_to']
        revenue_growth_1q_to = None if revenue_growth_1q_to == 100 else revenue_growth_1q_to
        current_ratio_from = args['current_ratio_from']
        current_ratio_from = None if current_ratio_from == 30 else current_ratio_from
        current_ratio_to = args['current_ratio_to']
        current_ratio_to = None if current_ratio_to == 2000 else current_ratio_to
        debt_ratio_from = args['debt_ratio_from']
        debt_ratio_from = None if debt_ratio_from == 0 else debt_ratio_from
        debt_ratio_to = args['debt_ratio_to']
        debt_ratio_to = None if debt_ratio_to == 500 else debt_ratio_to
        operating_margin_from = args['operating_margin_from']
        operating_margin_from = None if operating_margin_from == -200 else operating_margin_from
        operating_margin_to = args['operating_margin_to']
        operating_margin_to = None if operating_margin_to == 50 else operating_margin_to
        net_profit_margin_from = args['net_profit_margin_from']
        net_profit_margin_from = None if net_profit_margin_from == -200 else net_profit_margin_from
        net_profit_margin_to = args['net_profit_margin_to']
        net_profit_margin_to = None if net_profit_margin_to == 50 else net_profit_margin_to
        operating_income_loss_from = args['operating_income_loss_from']
        operating_income_loss_from = None if operating_income_loss_from == -500 else operating_income_loss_from
        operating_income_loss_to = args['operating_income_loss_to']
        operating_income_loss_to = None if operating_income_loss_to == 10000 else operating_income_loss_to
        profit_loss_from = args['profit_loss_from']
        profit_loss_from = None if profit_loss_from == -1000 else profit_loss_from
        profit_loss_to = args['profit_loss_to']
        profit_loss_to = None if profit_loss_to == 10000 else profit_loss_to

        # max date 가져오기
        max_date = get_df_from_db(f'select max(working_day) as date from date_working_day').iloc[0, 0]
        max_date = int(max_date)
        # max quarter 가져오기
        max_quarter = get_df_from_db(f'select max(quarter) as quarter '
                                     f'from dart_simple_financial_statements_ttm '
                                     f'where quarter like "%4"').iloc[0, 0]

        # stock_info 가져오기
        with session_scope() as session:
            sql_query = session.query(func.substr(StockInfo.stock_cd, 2, 6).label('stock_cd'),
                                      StockInfo.stock_nm, StockInfo.market, StockInfo.sector).\
                filter(StockInfo.market.in_(("KOSPI", "KOSDAQ")),
                       StockInfo.end_date == 99991231)

            if market is not None:
                sql_query = sql_query.filter(StockInfo.market.in_(market))
            if sector is not None:
                sql_query = sql_query.filter(StockInfo.sector.in_(sector))
            stock_info = pd.read_sql(sql_query.statement, session.bind)
            session.commit()
            # print(stock_info)

        # sql = f'select substr(stock_cd, 2, 6) as stock_cd, stock_nm, market, sector ' \
        #       f'from stock_info ' \
        #       f'where 1=1 ' \
        #       f'and market in ("KOSPI", "KOSDAQ") ' \
        #       f'and end_date = 99991231 '
        #
        # if stock_cd is not None:
        #     sql = sql + f'and stock_cd = "A{stock_cd}" '
        # else:
        #     if market is not None:
        #         sql = sql + f'and market in {str(market).replace("[", "(").replace("]", ")")} '
        #     if sector is not None:
        #         sql = sql + f'and sector in {str(sector).replace("[", "(").replace("]", ")")} '
        # stock_info = get_df_from_db(sql)

        # stock_daily_technical 가져오기
        with session_scope() as session:
            sql_query = session.query(StockDailyTechnical.stock_cd, StockDailyTechnical.close_price,
                                      StockDailyTechnical.market_capitalization, StockDailyTechnical.trading_volume). \
                filter(StockDailyTechnical.date == max_date)

            if market_cap_from is not None:
                sql_query = sql_query.filter(StockDailyTechnical.market_capitalization >= market_cap_from * 100000000)
            if market_cap_to is not None:
                sql_query = sql_query.filter(StockDailyTechnical.market_capitalization <= market_cap_to * 100000000)
            if price_from is not None:
                sql_query = sql_query.filter(StockDailyTechnical.close_price >= price_from)
            if price_to is not None:
                sql_query = sql_query.filter(StockDailyTechnical.close_price <= price_to)
            if trading_vol_prev_1d_from is not None:
                sql_query = sql_query.filter(StockDailyTechnical.trading_volume >= trading_vol_prev_1d_from)
            if trading_vol_prev_1d_to is not None:
                sql_query = sql_query.filter(StockDailyTechnical.trading_volume <= trading_vol_prev_1d_to)
            stock_daily_technical = pd.read_sql(sql_query.statement, session.bind)
            session.commit()
            # print(stock_daily_technical)

        # sql = f'select stock_cd, close_price, market_capitalization, trading_volume ' \
        #       f'from stock_daily_technical ' \
        #       f'where 1=1 ' \
        #       f'and date = {max_date} '
        #
        # if stock_cd is not None:
        #     sql = sql + f'and stock_cd = "{stock_cd}" '
        # else:
        #     if market_cap_from is not None:
        #         sql = sql + f'and market_capitalization >= {market_cap_from*100000000} '
        #     if market_cap_to is not None:
        #         sql = sql + f'and market_capitalization <= {market_cap_to*100000000} '
        #     if price_from is not None:
        #         sql = sql + f'and close_price >= {price_from} '
        #     if price_to is not None:
        #         sql = sql + f'and close_price <= {price_to} '
        #     if trading_vol_prev_1d_from is not None:
        #         sql = sql + f'and trading_volume >= {trading_vol_prev_1d_from} '
        #     if trading_vol_prev_1d_to is not None:
        #         sql = sql + f'and trading_volume <= {trading_vol_prev_1d_to} '
        # stock_daily_technical = get_df_from_db(sql)

        # derived var 가져오기
        with session_scope() as session:
            tbl = aliased(StockDerivedVar)
            sql_query = session.query(
                tbl.stock_cd,
                (((tbl.price_prev_1d + tbl.price_diff_1d) / tbl.price_prev_1d - 1) * 100).label('price_momt_1d'),
                (((tbl.price_prev_1w + tbl.price_diff_1w) / tbl.price_prev_1w - 1) * 100).label('price_momt_1w'),
                ((tbl.price_momt_1m - 1) * 100).label('price_momt_1m'),
                ((tbl.price_momt_3m - 1) * 100).label('price_momt_1q'),
                ((tbl.price_momt_6m - 1) * 100).label('price_momt_1h')
            ).\
                filter(tbl.date == max_date)

            if price_momt_1d_from is not None:
                sql_query = sql_query.filter(((tbl.price_prev_1d + tbl.price_diff_1d) / tbl.price_prev_1d) >= (price_momt_1d_from / 100) + 1)
            if price_momt_1d_to is not None:
                sql_query = sql_query.filter(((tbl.price_prev_1d + tbl.price_diff_1d) / tbl.price_prev_1d) <= (price_momt_1d_to / 100) + 1)
            if price_momt_1w_from is not None:
                sql_query = sql_query.filter(((tbl.price_prev_1w + tbl.price_diff_1w) / tbl.price_prev_1w) >= (price_momt_1w_from / 100) + 1)
            if price_momt_1w_to is not None:
                sql_query = sql_query.filter(((tbl.price_prev_1w + tbl.price_diff_1w) / tbl.price_prev_1w) <= (price_momt_1w_to / 100) + 1)
            if price_momt_1m_from is not None:
                sql_query = sql_query.filter(tbl.price_momt_1m >= (price_momt_1m_from / 100) + 1)
            if price_momt_1m_to is not None:
                sql_query = sql_query.filter(tbl.price_momt_1m <= (price_momt_1m_to / 100) + 1)
            if price_momt_1q_from is not None:
                sql_query = sql_query.filter(tbl.price_momt_3m >= (price_momt_1q_from / 100) + 1)
            if price_momt_1q_to is not None:
                sql_query = sql_query.filter(tbl.price_momt_3m <= (price_momt_1q_to / 100) + 1)
            if price_momt_1h_from is not None:
                sql_query = sql_query.filter(tbl.price_momt_6m >= (price_momt_1h_from / 100) + 1)
            if price_momt_1h_to is not None:
                sql_query = sql_query.filter(tbl.price_momt_6m <= (price_momt_1h_to / 100) + 1)
            derived_var = pd.read_sql(sql_query.statement, session.bind)
            session.commit()
            # print(derived_var)

        # sql = f'select stock_cd, ((price_prev_1d+price_diff_1d)/price_prev_1d - 1)*100 as price_momt_1d, ' \
        #       f'		((price_prev_1w+price_diff_1w)/price_prev_1w - 1)*100 as price_momt_1w, ' \
        #       f'        (price_momt_1m-1)*100 as price_momt_1m, ' \
        #       f'        (price_momt_3m-1)*100 as price_momt_1q, ' \
        #       f'        (price_momt_6m-1)*100 as price_momt_1h ' \
        #       f'from stock_derived_var ' \
        #       f'where 1=1 ' \
        #       f'and date = {max_date} '
        #
        # if stock_cd is not None:
        #     sql = sql + f'and stock_cd = "{stock_cd}" '
        # else:
        #     if price_momt_1d_from is not None:
        #         sql = sql + f'and (price_prev_1d+price_diff_1d)/price_prev_1d >= {(price_momt_1d_from/100)+1} '
        #     if price_momt_1d_to is not None:
        #         sql = sql + f'and (price_prev_1d+price_diff_1d)/price_prev_1d <= {(price_momt_1d_to/100)+1} '
        #     if price_momt_1w_from is not None:
        #         sql = sql + f'and (price_prev_1w+price_diff_1w)/price_prev_1w >= {(price_momt_1w_from/100)+1} '
        #     if price_momt_1w_to is not None:
        #         sql = sql + f'and (price_prev_1w+price_diff_1w)/price_prev_1w <= {(price_momt_1w_to/100)+1} '
        #     if price_momt_1m_from is not None:
        #         sql = sql + f'and price_momt_1m >= {(price_momt_1m_from/100)+1} '
        #     if price_momt_1m_to is not None:
        #         sql = sql + f'and price_momt_1m <= {(price_momt_1m_to/100)+1} '
        #     if price_momt_1q_from is not None:
        #         sql = sql + f'and price_momt_3m >= {(price_momt_1q_from/100)+1} '
        #     if price_momt_1q_to is not None:
        #         sql = sql + f'and price_momt_3m <= {(price_momt_1q_to/100)+1} '
        #     if price_momt_1h_from is not None:
        #         sql = sql + f'and price_momt_6m >= {(price_momt_1h_from/100)+1} '
        #     if price_momt_1h_to is not None:
        #         sql = sql + f'and price_momt_6m <= {(price_momt_1h_to/100)+1} '
        # derived_var = get_df_from_db(sql)

        # 밸류 정보 가져오기
        with session_scope() as session:
            tbl = aliased(StockValuationIndicator)
            sub_query_1 = session.query(
                tbl.stock_cd, tbl.quarter,
                func.concat(func.substr(tbl.quarter, 1, 4) - 3, func.substr(tbl.quarter, 5, 2)).label('quarter_prev_3y'),
                case([(func.substr(tbl.quarter, 6, 1) == "1", func.concat(func.substr(tbl.quarter, 1, 4) - 1, "q4"))],
                     else_=func.concat(func.substr(tbl.quarter, 1, 5), func.substr(tbl.quarter, 6, 1) - 1)).label('quarter_prev_1q')
            ).subquery()

            tbl1 = aliased(StockValuationIndicator)
            tbl2 = aliased(DartFinancialRatio)
            sub_query_2 = session.query(
                tbl1, tbl2.roe,
                tbl2.current_ratio_ttm.label('current_ratio'),
                tbl2.debt_ratio_ttm.label('debt_ratio'),
                tbl2.operating_margin_ttm.label('operating_margin'),
                tbl2.net_profit_margin_ttm.label('net_profit_margin')
            ).\
                join(tbl2, and_(tbl1.stock_cd == tbl2.stock_cd, tbl1.quarter == tbl2.quarter)).\
                subquery()

            tbl3 = aliased(StockValuationIndicator)
            tbl4 = aliased(StockValuationIndicator)
            sql_query = session.query(
                sub_query_1.c.stock_cd,
                sub_query_2.c.per_ttm.label('per'), sub_query_2.c.pbr_ttm.label('pbr'),
                sub_query_2.c.eps_ttm.label('eps'), sub_query_2.c.roe,
                sub_query_2.c.bps_ttm.label('bps'), sub_query_2.c.current_ratio,
                sub_query_2.c.debt_ratio, sub_query_2.c.operating_margin,
                sub_query_2.c.net_profit_margin,
                (((sub_query_2.c.eps_ttm / tbl3.eps_ttm) - 1) * 100).label('eps_growth_3y'),
                (((sub_query_2.c.eps_ttm / tbl4.eps_ttm) - 1) * 100).label('eps_growth_1q')
            ).join(sub_query_2,
                   and_(sub_query_1.c.stock_cd == sub_query_2.c.stock_cd,
                        sub_query_1.c.quarter == sub_query_2.c.quarter),
                   isouter=True).\
                join(tbl3,
                     and_(sub_query_1.c.stock_cd == tbl3.stock_cd,
                          sub_query_1.c.quarter_prev_3y == tbl3.quarter),
                     isouter=True).\
                join(tbl4,
                     and_(sub_query_1.c.stock_cd == tbl4.stock_cd,
                          sub_query_1.c.quarter_prev_1q == tbl4.quarter),
                     isouter=True).\
                filter(sub_query_1.c.quarter == max_quarter)

            if per_from is not None:
                sql_query = sql_query.filter(sub_query_2.c.per_ttm >= per_from)
            if per_to is not None:
                sql_query = sql_query.filter(sub_query_2.c.per_ttm <= per_to)
            if pbr_from is not None:
                sql_query = sql_query.filter(sub_query_2.c.pbr_ttm >= pbr_from)
            if pbr_to is not None:
                sql_query = sql_query.filter(sub_query_2.c.pbr_ttm <= pbr_to)
            if eps_from is not None:
                sql_query = sql_query.filter(sub_query_2.c.eps_ttm >= eps_from)
            if eps_to is not None:
                sql_query = sql_query.filter(sub_query_2.c.eps_ttm <= eps_to)
            if roe_from is not None:
                sql_query = sql_query.filter(sub_query_2.c.roe >= roe_from)
            if roe_to is not None:
                sql_query = sql_query.filter(sub_query_2.c.roe <= roe_to)
            if bps_from is not None:
                sql_query = sql_query.filter(sub_query_2.c.bps >= bps_from)
            if bps_to is not None:
                sql_query = sql_query.filter(sub_query_2.c.bps <= bps_to)
            if current_ratio_from is not None:
                sql_query = sql_query.filter(sub_query_2.c.current_ratio >= current_ratio_from)
            if current_ratio_to is not None:
                sql_query = sql_query.filter(sub_query_2.c.current_ratio <= current_ratio_to)
            if debt_ratio_from is not None:
                sql_query = sql_query.filter(sub_query_2.c.debt_ratio >= debt_ratio_from)
            if debt_ratio_to is not None:
                sql_query = sql_query.filter(sub_query_2.c.debt_ratio <= debt_ratio_to)
            if operating_margin_from is not None:
                sql_query = sql_query.filter(sub_query_2.c.operating_margin >= operating_margin_from)
            if operating_margin_to is not None:
                sql_query = sql_query.filter(sub_query_2.c.operating_margin <= operating_margin_to)
            if net_profit_margin_from is not None:
                sql_query = sql_query.filter(sub_query_2.c.net_profit_margin >= net_profit_margin_from)
            if net_profit_margin_to is not None:
                sql_query = sql_query.filter(sub_query_2.c.net_profit_margin <= net_profit_margin_to)
            if eps_growth_3y_from is not None:
                sql_query = sql_query.filter(sub_query_2.c.eps_ttm >= ((eps_growth_3y_from / 100) + 1) * tbl3.eps_ttm)
            if eps_growth_3y_to is not None:
                sql_query = sql_query.filter(sub_query_2.c.eps_ttm <= ((eps_growth_3y_to / 100) + 1) * tbl3.eps_ttm)
            if eps_growth_1q_from is not None:
                sql_query = sql_query.filter(sub_query_2.c.eps_ttm >= ((eps_growth_1q_from / 100) + 1) * tbl4.eps_ttm)
            if eps_growth_1q_to is not None:
                sql_query = sql_query.filter(sub_query_2.c.eps_ttm <= ((eps_growth_1q_to / 100) + 1) * tbl4.eps_ttm)

            value_info = pd.read_sql(sql_query.statement, session.bind)
            session.commit()
            # print(value_info)

        # # 밸류 정보 가져오기
        # sql = f'select a.stock_cd, b.per_ttm as per, b.pbr_ttm as pbr, b.eps_ttm as eps, b.roe, b.bps_ttm as bps, ' \
        #       f'        b.current_ratio, b.debt_ratio, b.operating_margin, b.net_profit_margin, ' \
        #       f'        (b.eps_ttm/c.eps_ttm - 1) * 100 as eps_growth_3y, ' \
        #       f'        (b.eps_ttm/d.eps_ttm - 1) * 100 as eps_growth_1q ' \
        #       f'from (  select stock_cd, quarter, ' \
        #       f'		concat(substr(quarter, 1, 4)-1, substr(quarter, 5, 2)) as quarter_prev_3y, ' \
        #       f'        case when substr(quarter, 6, 1) = "1" then concat(substr(quarter, 1, 4)-1, "q4") ' \
        #       f'             else concat(substr(quarter, 1, 5),substr(quarter, 6, 1) -1) end as quarter_prev_1q ' \
        #       f'		from stock_valuation_indicator) as a ' \
        #       f'left join (select a.*, b.roe, b.current_ratio_ttm as current_ratio, b.debt_ratio_ttm as debt_ratio,' \
        #       f'                  b.operating_margin_ttm as operating_margin, ' \
        #       f'                  b.net_profit_margin_ttm as net_profit_margin ' \
        #       f'		   from stock_valuation_indicator as a ' \
        #       f'		   join dart_financial_ratio as b ' \
        #       f'				on a.stock_cd = b.stock_cd ' \
        #       f'				and a.quarter = b.quarter) as b ' \
        #       f'	on a.stock_cd = b.stock_cd ' \
        #       f'	and a.quarter = b.quarter ' \
        #       f'left join stock_valuation_indicator as c ' \
        #       f'	on a.stock_cd = c.stock_cd ' \
        #       f'    and a.quarter_prev_3y = c.quarter ' \
        #       f'left join stock_valuation_indicator as d ' \
        #       f'	on a.stock_cd = d.stock_cd ' \
        #       f'    and a.quarter_prev_1q = d.quarter ' \
        #       f'where 1=1 ' \
        #       f'and a.quarter = "{max_quarter}" '
        #
        # if stock_cd is not None:
        #     sql = sql + f'and a.stock_cd = "{stock_cd}" '
        # else:
        #     if per_from is not None:
        #         sql = sql + f'and b.per >= {per_from} '
        #     if per_to is not None:
        #         sql = sql + f'and b.per <= {per_to} '
        #     if pbr_from is not None:
        #         sql = sql + f'and b.pbr >= {pbr_from} '
        #     if pbr_to is not None:
        #         sql = sql + f'and b.pbr <= {pbr_to} '
        #     if eps_from is not None:
        #         sql = sql + f'and b.eps >= {eps_from} '
        #     if eps_to is not None:
        #         sql = sql + f'and b.eps <= {eps_to} '
        #     if roe_from is not None:
        #         sql = sql + f'and b.roe >= {roe_from} '
        #     if roe_to is not None:
        #         sql = sql + f'and b.roe <= {roe_to} '
        #     if bps_from is not None:
        #         sql = sql + f'and b.bps >= {bps_from} '
        #     if bps_to is not None:
        #         sql = sql + f'and b.bps <= {bps_to} '
        #     if current_ratio_from is not None:
        #         sql = sql + f'and b.current_ratio >= {current_ratio_from} '
        #     if current_ratio_to is not None:
        #         sql = sql + f'and b.current_ratio <= {current_ratio_to} '
        #     if debt_ratio_from is not None:
        #         sql = sql + f'and b.debt_ratio >= {debt_ratio_from} '
        #     if debt_ratio_to is not None:
        #         sql = sql + f'and b.debt_ratio <= {debt_ratio_to} '
        #     if operating_margin_from is not None:
        #         sql = sql + f'and b.operating_margin >= {operating_margin_from} '
        #     if operating_margin_to is not None:
        #         sql = sql + f'and b.operating_margin <= {operating_margin_to} '
        #     if net_profit_margin_from is not None:
        #         sql = sql + f'and b.net_profit_margin >= {net_profit_margin_from} '
        #     if net_profit_margin_to is not None:
        #         sql = sql + f'and b.net_profit_margin <= {net_profit_margin_to} '
        #     if eps_growth_3y_from is not None:
        #         sql = sql + f'and b.eps >= {(eps_growth_3y_from/100) + 1} * c.eps '
        #     if eps_growth_3y_to is not None:
        #         sql = sql + f'and b.eps <= {(eps_growth_3y_to/100) + 1} * c.eps '
        #     if eps_growth_1q_from is not None:
        #         sql = sql + f'and b.eps >= {eps_growth_1q_from/100 + 1} * d.eps '
        #     if eps_growth_1q_to is not None:
        #         sql = sql + f'and b.eps <= {eps_growth_1q_to/100 + 1} * d.eps '
        #
        # value_info = get_df_from_db(sql)
        # sqlalchemy로 옮기며 수정한 것.
        # eps -> eps_ttm으로 수정.
        # prev_3y 잘못되어 있는 것 수정

        # 재무정보 가져오기
        with session_scope() as session:
            tbl = aliased(DartSimpleFinancialStatementsTtm)
            sub_query = session.query(
                tbl.stock_cd, tbl.quarter,
                case([(func.substr(tbl.quarter, 6, 1) == "1", func.concat(func.substr(tbl.quarter, 1, 4) - 1, "q4"))],
                     else_=func.concat(func.substr(tbl.quarter, 1, 5), func.substr(tbl.quarter, 6, 1) - 1)).label('quarter_prev_1q')
            ).\
                subquery()
            tbl1 = aliased(DartSimpleFinancialStatementsTtm)
            tbl2 = aliased(DartSimpleFinancialStatementsTtm)

            sql_query = session.query(
                sub_query.c.stock_cd, tbl1.revenue_ttm.label('revenue'),
                (((tbl1.revenue_ttm / tbl2.revenue_ttm) - 1) * 100).label('revenue_growth_1q'),
                tbl1.operating_income_loss_ttm.label('operating_income_loss'),
                tbl1.profit_loss_ttm.label('profit_loss')
            ).join(tbl1,
                   and_(sub_query.c.stock_cd == tbl1.stock_cd,
                        sub_query.c.quarter == tbl1.quarter),
                   isouter=True). \
                join(tbl2,
                     and_(sub_query.c.stock_cd == tbl2.stock_cd,
                          sub_query.c.quarter_prev_1q == tbl2.quarter),
                     isouter=True). \
                filter(sub_query.c.quarter == max_quarter)

            if revenue_from is not None:
                sql_query = sql_query.filter(tbl1.revenue_ttm >= revenue_from*100000000)
            if revenue_to is not None:
                sql_query = sql_query.filter(tbl1.revenue_ttm <= revenue_to*100000000)
            if revenue_growth_1q_from is not None:
                sql_query = sql_query.filter((tbl1.revenue_ttm/tbl2.revenue_ttm) >= (revenue_growth_1q_from / 100) + 1)
            if revenue_growth_1q_to is not None:
                sql_query = sql_query.filter((tbl1.revenue_ttm/tbl2.revenue_ttm) <= (revenue_growth_1q_to / 100) + 1)
            if operating_income_loss_from is not None:
                sql_query = sql_query.filter(tbl1.operating_income_loss_ttm >= operating_income_loss_from*100000000)
            if operating_income_loss_to is not None:
                sql_query = sql_query.filter(tbl1.operating_income_loss_ttm <= operating_income_loss_to*100000000)
            if profit_loss_from is not None:
                sql_query = sql_query.filter(tbl1.profit_loss_ttm >= profit_loss_from*100000000)
            if profit_loss_to is not None:
                sql_query = sql_query.filter(tbl1.profit_loss_ttm <= profit_loss_to*100000000)
            financial_info = pd.read_sql(sql_query.statement, session.bind)
            # print(financial_info)

        # # 재무정보 가져오기
        # sql = f'select a.stock_cd, b.revenue_ttm as revenue, ' \
        #       f'        (b.revenue_ttm/c.revenue_ttm - 1) * 100 as revenue_growth_1q, ' \
        #       f'        b.operating_income_loss_ttm as operating_income_loss, b.profit_loss_ttm as profit_loss ' \
        #       f'from (  select stock_cd, quarter, ' \
        #       f'		case when substr(quarter, 6, 1) = "1" then concat(substr(quarter, 1, 4)-1, "q4") ' \
        #       f'             else concat(substr(quarter, 1, 5),substr(quarter, 6, 1) -1) end as quarter_prev_1q ' \
        #       f'		from dart_simple_financial_statements_ttm) as a ' \
        #       f'left join dart_simple_financial_statements_ttm as b ' \
        #       f'    on a.stock_cd = b.stock_cd ' \
        #       f'    and a.quarter = b.quarter ' \
        #       f'left join dart_simple_financial_statements_ttm as c ' \
        #       f'    on a.stock_cd = c.stock_cd ' \
        #       f'    and a.quarter_prev_1q = c.quarter ' \
        #       f'where 1=1 ' \
        #       f'and a.quarter = "{max_quarter}" '
        #
        # if stock_cd is not None:
        #     sql = sql + f'and a.stock_cd = "{stock_cd}" '
        # else:
        #     if revenue_from is not None:
        #         sql = sql + f'and b.revenue_ttm >= {revenue_from*100000000} '
        #     if revenue_to is not None:
        #         sql = sql + f'and b.revenue_ttm <= {revenue_to*100000000} '
        #     if revenue_growth_1q_from is not None:
        #         sql = sql + f'and b.revenue_ttm/c.revenue_ttm >= {revenue_growth_1q_from / 100 + 1} '
        #     if revenue_growth_1q_to is not None:
        #         sql = sql + f'and b.revenue_ttm/c.revenue_ttm <= {revenue_growth_1q_to / 100 + 1} '
        #     if operating_income_loss_from is not None:
        #         sql = sql + f'and b.operating_income_loss_ttm >= {operating_income_loss_from*100000000} '
        #     if operating_income_loss_to is not None:
        #         sql = sql + f'and b.operating_income_loss_ttm <= {operating_income_loss_to*100000000} '
        #     if profit_loss_from is not None:
        #         sql = sql + f'and b.profit_loss_ttm >= {profit_loss_from*100000000} '
        #     if profit_loss_to is not None:
        #         sql = sql + f'and b.profit_loss_ttm <= {profit_loss_to*100000000} '
        # financial_info = get_df_from_db(sql)
        # # print(financial_info)

        # 데이터 join
        dfs = [stock_info, stock_daily_technical, derived_var, value_info, financial_info]
        # print(dfs)
        return_data = reduce(lambda left, right: pd.merge(left, right, how='inner', on='stock_cd'), dfs)
        return_data = return_data.iloc[:100, :]
        return_data = return_data.where(pd.notna(return_data), None)  # na가 아닌 부분을 보여주고, na인 부분은 None으로 보여줌.
        # print([{r.index[i]:r[i] for i in range(len(r))} for i, r in return_data.iterrows()])

        return {'status': '000', 'msg': get_status_msg('000'),
                'result_list': [{r.index[i]:r[i] for i in range(len(r))} for i, r in return_data.iterrows()]}


class SetUserFilter(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        # 필터 저장 정보
        parser.add_argument('user_code', type=int)
        parser.add_argument('preset_name', type=str)
        parser.add_argument('api_key', type=str)
        # 기업개요 필터
        parser.add_argument('market', type=str, action='append')
        parser.add_argument('market_cap_from', type=int)
        parser.add_argument('market_cap_to', type=int)
        parser.add_argument('sector', type=str, action='append')
        parser.add_argument('price_from', type=int)
        parser.add_argument('price_to', type=int)
        parser.add_argument('trading_vol_prev_1d_from', type=int)
        parser.add_argument('trading_vol_prev_1d_to', type=int)
        parser.add_argument('price_momt_1d_from', type=float)
        parser.add_argument('price_momt_1d_to', type=float)
        parser.add_argument('price_momt_1w_from', type=float)
        parser.add_argument('price_momt_1w_to', type=float)
        parser.add_argument('price_momt_1m_from', type=float)
        parser.add_argument('price_momt_1m_to', type=float)
        parser.add_argument('price_momt_1q_from', type=float)
        parser.add_argument('price_momt_1q_to', type=float)
        parser.add_argument('price_momt_1h_from', type=float)
        parser.add_argument('price_momt_1h_to', type=float)
        # 벨류에이션 필터
        parser.add_argument('per_from', type=float)
        parser.add_argument('per_to', type=float)
        parser.add_argument('pbr_from', type=float)
        parser.add_argument('pbr_to', type=float)
        parser.add_argument('eps_from', type=float)
        parser.add_argument('eps_to', type=float)
        parser.add_argument('roe_from', type=float)
        parser.add_argument('roe_to', type=float)
        parser.add_argument('bps_from', type=float)
        parser.add_argument('bps_to', type=float)
        parser.add_argument('eps_growth_3y_from', type=float)
        parser.add_argument('eps_growth_3y_to', type=float)
        parser.add_argument('eps_growth_1q_from', type=float)
        parser.add_argument('eps_growth_1q_to', type=float)
        # 재무 필터
        parser.add_argument('revenue_from', type=int)
        parser.add_argument('revenue_to', type=int)
        parser.add_argument('revenue_growth_1q_from', type=float)
        parser.add_argument('revenue_growth_1q_to', type=float)
        parser.add_argument('current_ratio_from', type=float)
        parser.add_argument('current_ratio_to', type=float)
        parser.add_argument('debt_ratio_from', type=float)
        parser.add_argument('debt_ratio_to', type=float)
        parser.add_argument('operating_margin_from', type=float)
        parser.add_argument('operating_margin_to', type=float)
        parser.add_argument('net_profit_margin_from', type=float)
        parser.add_argument('net_profit_margin_to', type=float)
        parser.add_argument('operating_income_loss_from', type=int)
        parser.add_argument('operating_income_loss_to', type=int)
        parser.add_argument('profit_loss_from', type=int)
        parser.add_argument('profit_loss_to', type=int)

        args = parser.parse_args()

        user_code = args['user_code']
        preset_name = args['preset_name']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        args.pop('user_code', None)
        args.pop('preset_name', None)
        args.pop('api_key', None)

        with session_scope() as session:
            filter_preset = session.query(UserFilterPreset) \
                .filter(UserFilterPreset.user_code == user_code,
                        UserFilterPreset.preset_name == preset_name) \
                .first()
            session.commit()

            tmp_args = args.copy()
            for arg in args:
                if args[arg] is None:
                    tmp_args.pop(arg, None)
            args = tmp_args

            if filter_preset is None:
                new_preset = UserFilterPreset(user_code=user_code, preset_name=preset_name, preset=str(args).replace("'", '"'))
                # lst_update_dtim은 default 처리

                session.add(new_preset)
                session.commit()
                return {'status': '000', 'msg': get_status_msg('000')}
            else:
                return {'status': '501', 'msg': get_status_msg('501')}


class GetUserFilter(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        # 필터 저장 정보
        parser.add_argument('user_code', type=int)
        parser.add_argument('api_key', type=str)

        args = parser.parse_args()

        user_code = args['user_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'filter_list': []}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'filter_list': []}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        args.pop('user_code', None)
        args.pop('api_key', None)

        with session_scope() as session:
            filter_preset = session.query(UserFilterPreset) \
                .filter(UserFilterPreset.user_code == user_code) \
                .all()
            session.commit()

            filter_list = [json.loads(e.preset) for e in filter_preset]
            for i in range(len(filter_list)):
                filter_list[i]['user_code'] = filter_preset[i].user_code
                filter_list[i]['preset_name'] = filter_preset[i].preset_name

            return {'status': '000', 'msg': get_status_msg('000'),
                    'filter_list': filter_list}


class DeleteUserFilter(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        # 필터 저장 정보
        parser.add_argument('user_code', type=int)
        parser.add_argument('preset_name', type=str)
        parser.add_argument('api_key', type=str)

        args = parser.parse_args()

        user_code = args['user_code']
        preset_name = args['preset_name']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        with session_scope() as session:
            filter_preset_query = session.query(UserFilterPreset) \
                .filter(UserFilterPreset.user_code == user_code,
                        UserFilterPreset.preset_name == preset_name)
            filter_preset = filter_preset_query.first()
            session.commit()

            if filter_preset is None:
                return {'status': '502', 'msg': get_status_msg('502')}  # 해당 preset 존재하지 않음
            else:
                filter_preset_query.delete()
                session.commit()
                return {'status': '000', 'msg': get_status_msg('000')}


def us_get_all_filter_data(stock_cd_list):
    # max date 가져오기
    max_date = get_df_from_db(f'select max(working_day) as date from us_date_working_day').iloc[0, 0]
    # stock_cd string 만들기
    stock_cd_list_string = reduce(lambda prev, curr: prev + f', "{curr}"', stock_cd_list, '')[2:]

    # stock_market_sector 가져오기
    sql = f'select stock_cd, stock_nm, exchange as market, industry as sector ' \
          f'from us_stock_info ' \
          f'where 1=1 ' \
          f'and stock_cd in ({stock_cd_list_string}) ' \
          f'and exchange in ("NYSE", "NASDAQ", "AMEX") '
    stock_market_sector = get_df_from_db(sql)

    # stock_daily_technical 가져오기
    sql = f'select stock_cd, close_price, market_cap as market_capitalization, trading_volume ' \
          f'from us_stock_daily_price ' \
          f'where 1=1 ' \
          f'and date = {max_date} ' \
          f'and stock_cd in ({stock_cd_list_string}) '
    stock_daily_technical = get_df_from_db(sql)

    # derived var 가져오기
    sql = f'select stock_cd, ((price_prev_1d+price_diff_1d)/price_prev_1d - 1)*100 as price_momt_1d, ' \
          f'		((price_prev_1w+price_diff_1w)/price_prev_1w - 1)*100 as price_momt_1w, ' \
          f'        (price_momt_1m-1)*100 as price_momt_1m, ' \
          f'        (price_momt_3m-1)*100 as price_momt_1q, ' \
          f'        (price_momt_6m-1)*100 as price_momt_1h ' \
          f'from us_stock_derived_var ' \
          f'where 1=1 ' \
          f'and date = {max_date} ' \
          f'and stock_cd in ({stock_cd_list_string}) '
    derived_var = get_df_from_db(sql)

    # 밸류 정보 가져오기
    sql = f'select a.stock_cd, b.per_ttm as per, b.pbr_ttm as pbr, b.eps_ttm as eps, b.roe, b.bps_ttm as bps, ' \
          f'        b.current_ratio, b.debt_ratio, b.operating_margin, b.net_profit_margin, ' \
          f'        (b.eps_ttm/c.eps_ttm - 1) * 100 as eps_growth_3y, ' \
          f'        (b.eps_ttm/d.eps_ttm - 1) * 100 as eps_growth_1q ' \
          f'from (  select a.*, c.date as quarter_prev_1q_date, d.date as quarter_prev_3y_date ' \
          f'        from us_date_quarter as a ' \
          f'        join (select stock_cd, max(seq) as seq from us_date_quarter group by stock_cd) as b ' \
          f'	        on a.stock_cd = b.stock_cd ' \
          f'	        and a.seq = b.seq ' \
          f'        left join us_date_quarter as c ' \
          f'	        on a.stock_cd = c.stock_cd ' \
          f'	        and a.seq = c.seq + 1 ' \
          f'        left join us_date_quarter as d ' \
          f'	        on a.stock_cd = d.stock_cd 	' \
          f'            and a.seq = d.seq + 12 ' \
          f'        where a.stock_cd in ({stock_cd_list_string})) as a ' \
          f'left join (select a.*, b.roe, b.current_ratio_ttm as current_ratio, b.debt_ratio_ttm as debt_ratio,' \
          f'                  b.operating_margin_ttm as operating_margin, ' \
          f'                  b.net_profit_margin_ttm as net_profit_margin ' \
          f'		   from us_stock_valuation_indicator as a ' \
          f'		   join us_financial_ratio as b ' \
          f'				on a.stock_cd = b.stock_cd ' \
          f'				and a.date = b.date) as b ' \
          f'	on a.stock_cd = b.stock_cd ' \
          f'	and a.date = b.date ' \
          f'left join us_stock_valuation_indicator as c ' \
          f'	on a.stock_cd = c.stock_cd ' \
          f'    and a.quarter_prev_3y_date = c.date ' \
          f'left join us_stock_valuation_indicator as d ' \
          f'	on a.stock_cd = d.stock_cd ' \
          f'    and a.quarter_prev_1q_date = d.date '
    value_info = get_df_from_db(sql)

    # 재무정보 가져오기
    sql = f'select a.stock_cd, b.revenue_ttm as revenue, ' \
          f'        (b.revenue_ttm/c.revenue_ttm - 1) * 100 as revenue_growth_1q, ' \
          f'        b.operatingIncome_ttm as operating_income_loss, b.netIncome_ttm as profit_loss ' \
          f'from (  select a.*, c.date as quarter_prev_1q_date ' \
          f'        from us_date_quarter as a ' \
          f'        join (select stock_cd, max(seq) as seq from us_date_quarter group by stock_cd) as b ' \
          f'	        on a.stock_cd = b.stock_cd ' \
          f'	        and a.seq = b.seq ' \
          f'        left join us_date_quarter as c ' \
          f'	        on a.stock_cd = c.stock_cd ' \
          f'	        and a.seq = c.seq + 1 ' \
          f'        where a.stock_cd in ({stock_cd_list_string})) as a ' \
          f'left join us_financial_statements_ttm as b ' \
          f'    on a.stock_cd = b.stock_cd ' \
          f'    and a.date = b.date ' \
          f'left join us_financial_statements_ttm as c ' \
          f'    on a.stock_cd = c.stock_cd ' \
          f'    and a.quarter_prev_1q_date = c.date '
    financial_info = get_df_from_db(sql)

    # 데이터 join
    dfs = [stock_market_sector, stock_daily_technical, derived_var, value_info, financial_info]
    # for t in dfs:
    #     print(t)
    result_dat = reduce(lambda left, right: pd.merge(left, right, how='left', on='stock_cd'), dfs)
    result_dat['close_price'] = result_dat['close_price'].apply(lambda x: int(x))

    return result_dat


def kr_get_all_filter_data(stock_cd_list):
    # max date 가져오기
    max_date = get_df_from_db(f'select max(working_day) as date from date_working_day').iloc[0, 0]
    # max quarter 가져오기
    max_quarter = \
        get_df_from_db(f'select max(quarter) as quarter from dart_simple_financial_statements_ttm where quarter like "%4"').iloc[0, 0]
    # stock_cd_list string
    stock_cd_list_string = reduce(lambda prev, curr: prev + f', "{curr}"', stock_cd_list, '')[2:]

    # stock_market_sector 가져오기
    sql = f'select stock_cd, stock_nm, market, sector ' \
          f'from stock_market_sector ' \
          f'where 1=1 ' \
          f'and date = {max_date} ' \
          f'and stock_cd in ({stock_cd_list_string}) ' \
          f'and market in ("KOSPI", "KOSDAQ") '
    stock_market_sector = get_df_from_db(sql)

    # stock_daily_technical 가져오기
    sql = f'select stock_cd, close_price, market_capitalization, trading_volume ' \
          f'from stock_daily_technical ' \
          f'where 1=1 ' \
          f'and date = {max_date} ' \
          f'and stock_cd in ({stock_cd_list_string}) '
    stock_daily_technical = get_df_from_db(sql)

    # derived var 가져오기
    sql = f'select stock_cd, ((price_prev_1d+price_diff_1d)/price_prev_1d - 1)*100 as price_momt_1d, ' \
          f'		((price_prev_1w+price_diff_1w)/price_prev_1w - 1)*100 as price_momt_1w, ' \
          f'        (price_momt_1m-1)*100 as price_momt_1m, ' \
          f'        (price_momt_3m-1)*100 as price_momt_1q, ' \
          f'        (price_momt_6m-1)*100 as price_momt_1h ' \
          f'from stock_derived_var ' \
          f'where 1=1 ' \
          f'and date = {max_date} ' \
          f'and stock_cd in ({stock_cd_list_string}) '
    derived_var = get_df_from_db(sql)

    # 밸류 정보 가져오기
    sql = f'select a.stock_cd, b.per_ttm as per, b.pbr_ttm as pbr, b.eps_ttm as eps, b.roe, b.bps_ttm as bps, ' \
          f'        b.current_ratio, b.debt_ratio, b.operating_margin, b.net_profit_margin, ' \
          f'        (b.eps_ttm/c.eps_ttm - 1) * 100 as eps_growth_3y, ' \
          f'        (b.eps_ttm/d.eps_ttm - 1) * 100 as eps_growth_1q ' \
          f'from (  select stock_cd, quarter, ' \
          f'		concat(substr(quarter, 1, 4)-1, substr(quarter, 5, 2)) as quarter_prev_3y, ' \
          f'        case when substr(quarter, 6, 1) = "1" then concat(substr(quarter, 1, 4)-1, "q4") ' \
          f'             else concat(substr(quarter, 1, 5),substr(quarter, 6, 1) -1) end as quarter_prev_1q ' \
          f'		from stock_valuation_indicator' \
          f'        where stock_cd in ({stock_cd_list_string})' \
          f'        and quarter = "{max_quarter}") as a ' \
          f'left join (select a.*, b.roe, b.current_ratio_ttm as current_ratio, b.debt_ratio_ttm as debt_ratio,' \
          f'                  b.operating_margin_ttm as operating_margin, ' \
          f'                  b.net_profit_margin_ttm as net_profit_margin ' \
          f'		   from stock_valuation_indicator as a ' \
          f'		   join dart_financial_ratio as b ' \
          f'				on a.stock_cd = b.stock_cd ' \
          f'				and a.quarter = b.quarter) as b ' \
          f'	on a.stock_cd = b.stock_cd ' \
          f'	and a.quarter = b.quarter ' \
          f'left join stock_valuation_indicator as c ' \
          f'	on a.stock_cd = c.stock_cd ' \
          f'    and a.quarter_prev_3y = c.quarter ' \
          f'left join stock_valuation_indicator as d ' \
          f'	on a.stock_cd = d.stock_cd ' \
          f'    and a.quarter_prev_1q = d.quarter '
    value_info = get_df_from_db(sql)

    # 재무정보 가져오기
    sql = f'select a.stock_cd, b.revenue_ttm as revenue, ' \
          f'        (b.revenue_ttm/c.revenue_ttm - 1) * 100 as revenue_growth_1q, ' \
          f'        b.operating_income_loss_ttm as operating_income_loss, b.profit_loss_ttm as profit_loss ' \
          f'from (  select stock_cd, quarter, ' \
          f'		case when substr(quarter, 6, 1) = "1" then concat(substr(quarter, 1, 4)-1, "q4") ' \
          f'             else concat(substr(quarter, 1, 5),substr(quarter, 6, 1) -1) end as quarter_prev_1q ' \
          f'		from dart_simple_financial_statements_ttm ' \
          f'        where stock_cd in ({stock_cd_list_string}) ' \
          f'        and quarter = "{max_quarter}") as a ' \
          f'left join dart_simple_financial_statements_ttm as b ' \
          f'    on a.stock_cd = b.stock_cd ' \
          f'    and a.quarter = b.quarter ' \
          f'left join dart_simple_financial_statements_ttm as c ' \
          f'    on a.stock_cd = c.stock_cd ' \
          f'    and a.quarter_prev_1q = c.quarter '
    financial_info = get_df_from_db(sql)

    # 데이터 join
    dfs = [stock_market_sector, stock_daily_technical, derived_var, value_info, financial_info]
    # print(dfs)
    result_dat = reduce(lambda left, right: pd.merge(left, right, how='inner', on='stock_cd'), dfs)

    return result_dat


def get_all_filter_data(stock_cd_list):
    kr_list = []
    us_list = []

    for s in stock_cd_list:
        c = get_country(s)
        if c == 'KR':
            kr_list.append(s)
        elif c == 'US':
            us_list.append(s)
        else:
            continue

    kr_dat = pd.DataFrame()
    us_dat = pd.DataFrame()
    if len(kr_list) != 0:
        kr_dat = kr_get_all_filter_data(kr_list)
    if len(us_list) != 0:
        us_dat = us_get_all_filter_data(us_list)

    # unit_currency 추가
    kr_dat['unit_currency'] = 'KRW'
    us_dat['unit_currency'] = 'USD'

    return_data = pd.concat([kr_dat, us_dat], axis=0)
    return_data = return_data.replace([np.nan, np.inf, -np.inf], np.nan)
    # print(return_data)
    return_data = return_data.where(pd.notna(return_data), None)  # na가 아닌 부분을 보여주고, na인 부분은 None으로 보여줌.
    # print(return_data)

    return return_data


class GetPopularStock(Resource):
    def post(self):
        with session_scope() as session:
            # 최근 30 인기 top 30 뽑아줌. 나중에 트래픽 늘어나면 1일로 줄여보자.
            popular_stock_list = session.query(StockViewCount.stock_cd,
                                               func.sum(StockViewCount.view_count).label('view_count')). \
                filter(StockViewCount.date_hour >= (datetime.today()-timedelta(days=30)).strftime('%Y%m%d%H')). \
                group_by(StockViewCount.stock_cd). \
                order_by(func.sum(StockViewCount.view_count).desc()). \
                limit(30).\
                all()
            session.commit()

            stock_list = [e.stock_cd for e in popular_stock_list]
            # print(stock_list)

            if len(stock_list) == 0:
                return_list = []
            else:
                return_data = get_all_filter_data(stock_list)
                return_data = pd.merge(return_data, pd.DataFrame(popular_stock_list), on='stock_cd')
                return_data = return_data.sort_values(by='view_count', ascending=False)
                return_data = return_data.iloc[:, :-1]

                # nan 처리해줌
                return_data = return_data.where(pd.notna(return_data), None)

                return_list = [{r.index[i]:r[i] for i in range(len(r))} for i, r in return_data.iterrows()]

            # print(return_list)
            return {'status': '000', 'msg': get_status_msg('000'),
                    'result_list': return_list}


class GetUserStaredStock(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status != '000':
            return {'status': check_status, 'msg': get_status_msg(check_status), 'stock_price_list': []}

        with session_scope() as session:
            user_starred_stock_list = session.query(UserStarredStock). \
                filter(UserStarredStock.user_code == user_code). \
                all()
            session.commit()

            stock_list = [e.stock_cd for e in user_starred_stock_list]
            # print(stock_list)

        if len(stock_list) == 0:
            return_list = []
        else:
            return_data = get_all_filter_data(stock_list)
            # print(return_data)
            # nan 처리해줌
            return_data = return_data.where(pd.notna(return_data), None)

            return_list = [{r.index[i]:r[i] for i in range(len(r))} for i, r in return_data.iterrows()]

        return {'status': '000', 'msg': get_status_msg('000'),
                'result_list': return_list}
