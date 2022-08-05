from flask_restful import Resource
from flask_restful import reqparse
from db.db_model import StockAutocompleteNameList, StockDerivedVar, StockDailyTechnical, StockViewCount, StockAdjPrice, \
    session_scope, UsStockDailyPrice, UsStockDerivedVar, UsStockDailyPriceRaw
from datetime import datetime
from dateutil.relativedelta import relativedelta
from util.util_get_country import get_country
from util.util_get_status_msg import get_status_msg
from sqlalchemy import func, Float


class GetAutoCompletedStockNameList(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('keyword', type=str)
        parser.add_argument('stock_only_flag', type=int)
        args = parser.parse_args()

        keyword = args['keyword']
        stock_only_flag = args['stock_only_flag']

        if keyword == '':
            return {'status': '000', 'msg': get_status_msg('000'),
                    'stock_name_list': []}

        with session_scope() as session:
            if stock_only_flag == 1:
                stock_name_list = session.query(StockAutocompleteNameList). \
                    filter(StockAutocompleteNameList.stock_nm.like(f'%{keyword}%'),
                           StockAutocompleteNameList.market.in_(['KOSPI', 'KOSDAQ', 'NASDAQ', 'NYSE', 'AMEX'])).\
                    order_by(func.length(StockAutocompleteNameList.stock_nm)).\
                    limit(10)
                session.commit()
            else:
                stock_name_list = session.query(StockAutocompleteNameList).\
                    filter(StockAutocompleteNameList.stock_nm.like(f'%{keyword}%')).\
                    order_by(func.length(StockAutocompleteNameList.stock_nm)).\
                    limit(10)
                session.commit()

            return {'status': '000', 'msg': get_status_msg('000'),
                    'stock_name_list': [{'stock_nm': e.stock_nm,
                                         'stock_cd': e.stock_cd,
                                         'market': e.market}
                                        if e.market in ('NASDAQ', 'NYSE', 'AMEX', 'US_ETF') else
                                        {'stock_nm': e.stock_nm,
                                         'stock_cd': e.stock_cd[1:],
                                         'market': e.market}
                                        for e in stock_name_list]}


class GetStockPriceInfo(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('stock_cd', type=str)
        args = parser.parse_args()

        stock_cd = args['stock_cd']

        # country 설정
        country = get_country(stock_cd)

        with session_scope() as session:
            if country == 'KR':
                stock_derived_var = session.query(StockDerivedVar).\
                    filter(StockDerivedVar.stock_cd == stock_cd).\
                    order_by(StockDerivedVar.date.desc()).\
                    first()
                session.commit()

                stock_daily_technical = session.query(StockDailyTechnical). \
                    filter(StockDailyTechnical.stock_cd == stock_cd). \
                    order_by(StockDailyTechnical.date.desc()). \
                    limit(2)
                session.commit()

                # 종가, 전일비, 거래량, 52주 최고가, 52주 최저가, 거래대금, 수익률, 외국인 보유비중, 시가총액, 발행주식수
                return_dict = {'close_price': stock_daily_technical[0].close_price,
                               'price_diff_1d': stock_derived_var.price_diff_1d,
                               'trading_volume': stock_daily_technical[0].trading_volume,
                               'price_high_52w': stock_derived_var.price_high_52w,
                               'price_low_52w': stock_derived_var.price_low_52w,
                               'transaction_amount': stock_daily_technical[0].transaction_amount,
                               'price_momt_1m': None if stock_derived_var.price_momt_1m is None else (stock_derived_var.price_momt_1m - 1) * 100,
                               'price_momt_3m': None if stock_derived_var.price_momt_3m is None else (stock_derived_var.price_momt_3m - 1) * 100,
                               'price_momt_6m': None if stock_derived_var.price_momt_6m is None else (stock_derived_var.price_momt_6m - 1) * 100,
                               'price_momt_12m': None if stock_derived_var.price_momt_12m is None else (stock_derived_var.price_momt_12m - 1) * 100,
                               'share_of_foreign_own': stock_daily_technical[1].share_of_foreign_own,
                               'market_capitalization': stock_daily_technical[0].market_capitalization,
                               'num_of_listed_stocks': stock_daily_technical[0].num_of_listed_stocks,
                               'unit_currency': 'KRW'}
            elif country == 'US':
                us_stock_derived_var = session.query(UsStockDerivedVar). \
                    filter(UsStockDerivedVar.stock_cd == stock_cd). \
                    order_by(UsStockDerivedVar.date.desc()). \
                    first()
                session.commit()

                us_stock_daily_price = session.query(UsStockDailyPrice). \
                    filter(UsStockDailyPrice.stock_cd == stock_cd). \
                    order_by(UsStockDailyPrice.date.desc()). \
                    limit(2)
                session.commit()

                # 종가, 전일비, 거래량, 52주 최고가, 52주 최저가, 거래대금, 수익률, 외국인 보유비중, 시가총액, 발행주식수
                # APP 수정 이후 int() 삭제, market_cap, share_of_foreign_own 등 null값도 그대로 리턴
                market_cap = 0 if us_stock_daily_price[0].market_cap is None else us_stock_daily_price[0].market_cap
                return_dict = {'close_price': us_stock_daily_price[0].close_price,
                               'price_diff_1d': us_stock_derived_var.price_diff_1d,
                               'trading_volume': us_stock_daily_price[0].trading_volume,
                               'price_high_52w': us_stock_derived_var.price_high_52w,
                               'price_low_52w': us_stock_derived_var.price_low_52w,
                               'transaction_amount': us_stock_daily_price[0].close_price * us_stock_daily_price[0].trading_volume,
                               'price_momt_1m': None if us_stock_derived_var.price_momt_1m is None else (us_stock_derived_var.price_momt_1m - 1) * 100,
                               'price_momt_3m': None if us_stock_derived_var.price_momt_3m is None else (us_stock_derived_var.price_momt_3m - 1) * 100,
                               'price_momt_6m': None if us_stock_derived_var.price_momt_6m is None else (us_stock_derived_var.price_momt_6m - 1) * 100,
                               'price_momt_12m': None if us_stock_derived_var.price_momt_12m is None else (us_stock_derived_var.price_momt_12m - 1) * 100,
                               'share_of_foreign_own': None,
                               'market_capitalization': market_cap,
                               'num_of_listed_stocks': int(market_cap / us_stock_daily_price[0].close_price),
                               'unit_currency': 'USD'}

            # count 1 추가
            stock_view_count = session.query(StockViewCount). \
                filter(StockViewCount.date_hour == datetime.today().strftime('%Y%m%d%H'),
                       StockViewCount.stock_cd == stock_cd). \
                first()
            session.commit()

            if stock_view_count is None:
                new_count = StockViewCount(date_hour=datetime.today().strftime('%Y%m%d%H'),
                                           stock_cd=stock_cd, view_count=1)
                session.add(new_count)
            else:
                stock_view_count.view_count = stock_view_count.view_count + 1
            session.commit()

            return {'status': '000', 'msg': get_status_msg('000'), 'stock_price_info': return_dict}


class GetStockPriceList(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('stock_cd', type=str)
        args = parser.parse_args()

        stock_cd = args['stock_cd']

        # country 설정
        country = get_country(stock_cd)

        begin_date = (datetime.today() - relativedelta(years=1)).strftime('%Y%m%d')

        with session_scope() as session:
            if country == 'KR':
                stock_price_list = session.query(StockAdjPrice.date,
                                                    StockAdjPrice.adj_close_price).\
                    filter(StockAdjPrice.stock_cd == stock_cd,
                           StockAdjPrice.date > begin_date).\
                    all()
                session.commit()
            elif country == 'US':
                stock_price_list = session.query(UsStockDailyPriceRaw.date,
                                                 UsStockDailyPriceRaw.adj_close_price).\
                    filter(UsStockDailyPriceRaw.stock_cd == stock_cd,
                           UsStockDailyPriceRaw.date > begin_date).\
                    all()
                session.commit()

            return {'status': '000', 'msg': get_status_msg('000'),
                    'stock_price_list': [{'date': str(e.date), 'price': e.adj_close_price} for e in stock_price_list]}
