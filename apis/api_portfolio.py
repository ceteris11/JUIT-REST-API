from flask_restful import Resource
from flask_restful import reqparse
from sqlalchemy import func, and_, Integer
from sqlalchemy.orm import aliased
from db.db_model import UserPortfolioInfo, UserPortfolioMap, UserPortfolio, UserTradingLog, session_scope, ExchangeRate, \
    UserDefaultPortfolio, SecuritiesCode, DateWorkingDay
from db.db_connect import exec_query, get_df_from_db
from datetime import datetime, timedelta
import pytz
from apis.api_user_info import check_user_validity
from util.util_update_portfolio import update_single_stock_portfolio
from util.util_get_portfolio_yield import calc_portfolio_yield, calc_all_portfolio_yield
from util.util_get_status_msg import get_status_msg
import pandas as pd
from flask import current_app as app
from flask import request
from module.merlot_logging import *


class GetPortfolioList(Resource):  # 포트폴리오 리스트 조회
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'portfolio_list': []}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'portfolio_list': []}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        with session_scope() as session:
            # CheckIsNewUser에 아래 코드 넣음.
            # # 최근 포트폴리오 일자 확인
            # max_date = session.query(func.max(UserPortfolio.date)).\
            #     filter(UserPortfolio.user_code == user_code).\
            #     first()
            # max_date = max_date[0]
            #
            # # 최근 영업일 확인set
            # max_working_day = session.query(func.max(DateWorkingDay.working_day)).first()
            # max_working_day = max_working_day[0]
            #
            # # 최근 포트폴리오 일자와 최근 영업일이 다를 경우, 포트폴리오 업데이트 해줌
            # # 새로운 거래내역이 들어올 경우, 자동으로 포트폴리오 업데이트를 진행하므로, 여기에서는 날짜만 체크해서 업데이트 해 준다.
            # if max_date is not None and max_date != max_working_day:
            #     # 포트폴리오 업데이트 리스트 확인
            #     update_list = session.query(UserPortfolio).\
            #         filter(UserPortfolio.user_code == user_code,
            #                UserPortfolio.date == max_date,
            #                UserPortfolio.holding_quantity != 0).\
            #         all()
            #
            #     # 업데이트할 포트폴리오가 존재한다면, 포트폴리오 업데이트 해줌.
            #     if len(update_list) != 0:
            #         # 포트폴리오 업데이트
            #         for p in update_list:
            #             update_single_stock_portfolio(user_code=user_code, begin_date=max_date,
            #                                           account_number=p.account_number,
            #                                           securities_code=p.securities_code,
            #                                           stock_cd=p.stock_cd)

            user_portfolio_info = session.query(UserPortfolioInfo). \
                filter(UserPortfolioInfo.user_code == user_code). \
                order_by(UserPortfolioInfo.portfolio_order).\
                all()

            return {'status': '000', 'msg': get_status_msg('000'),
                    'portfolio_list': [{'portfolio_code': e.portfolio_code,
                                        'portfolio_nm': e.portfolio_nm} for e in user_portfolio_info]}


class SetPortfolioOrder(Resource):  # 포트폴리오 순서 변경 api
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('portfolio_code', type=int, action='append')
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        portfolio_code = args['portfolio_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        # 전체 포트폴리오 개수 추출
        with session_scope() as session:
            user_portfolio_all_info = session.query(UserPortfolioInfo). \
                filter(UserPortfolioInfo.user_code == user_code). \
                all()
            session.commit()

            # 전체 포트폴리오가 포함된 경우, 순서대로 부여해줌
            if len(portfolio_code) != len(user_portfolio_all_info):
                return {'status': '208', 'msg': get_status_msg('208')}
            else:
                for i in range(len(portfolio_code)):
                    code = portfolio_code[i]
                    user_portfolio_info = session.query(UserPortfolioInfo). \
                        filter(UserPortfolioInfo.user_code == user_code,
                               UserPortfolioInfo.portfolio_code == code). \
                        first()
                    user_portfolio_info.portfolio_order = i + 1
                    session.commit()

                return {'status': '000', 'msg': get_status_msg('000')}


class CreateNewPortfolio(Resource):  # 신규 포트폴리오 생성
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('portfolio_nm', type=str)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        portfolio_nm = args['portfolio_nm']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'portfolio_code': -99}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'portfolio_code': -99}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        # portfolio_nm이 60byte이상인지 확인
        if len(portfolio_nm.encode()) > 60:
            return {'status': '204', 'msg': get_status_msg('204'), 'portfolio_code': -99}

        # max(포트폴리오 코드) 값 가져오기
        with session_scope() as session:
            user_portfolio_info = session.query(func.max(UserPortfolioInfo.portfolio_code).label('max_code'),
                                                func.max(UserPortfolioInfo.portfolio_order).label('max_order')). \
                filter(UserPortfolioInfo.user_code == user_code). \
                first()
            session.commit()

            # 신규 포트폴리오 생성
            new_user_portfolio_info = UserPortfolioInfo(user_code=user_code,
                                                        portfolio_code=user_portfolio_info.max_code + 1,
                                                        portfolio_nm=portfolio_nm,
                                                        portfolio_order=user_portfolio_info.max_order + 1,
                                                        lst_update_dtim=datetime.today().strftime('%Y%m%d%H%M%S'))
            session.add(new_user_portfolio_info)
            session.commit()

            return {'status': '000', 'msg': get_status_msg('000'), 'portfolio_code': new_user_portfolio_info.portfolio_code}


class RenamePortfolio(Resource):  # 포트폴리오 이름 변경
    def post(self):
        # print(request.headers)
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('portfolio_code', type=int)
        parser.add_argument('new_portfolio_nm', type=str)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        portfolio_code = args['portfolio_code']
        new_portfolio_nm = args['new_portfolio_nm']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        # portfolio_nm이 60byte이상인지 확인
        if len(new_portfolio_nm.encode()) > 60:
            return {'status': '204', 'msg': get_status_msg('204'), 'portfolio_code': -99}

        # 기존 포트폴리오 가져오기
        with session_scope() as session:
            old_portfolio_info = session.query(UserPortfolioInfo). \
                filter(UserPortfolioInfo.user_code == user_code,
                       UserPortfolioInfo.portfolio_code == portfolio_code). \
                first()

            if old_portfolio_info is None:
                return {'status': '201', 'msg': get_status_msg('201')}  # 해당 포트폴리오 존재하지 않음
            else:
                # 이름바꾸기
                old_portfolio_info.portfolio_nm = new_portfolio_nm

                return {'status': '000', 'msg': get_status_msg('000')}


class SetStockPortfolio(Resource):  # 종목별 포트폴리오 지정
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('securities_code', type=str, action='append')
        parser.add_argument('stock_cd', type=str, action='append')
        parser.add_argument('old_portfolio_code', type=int)
        parser.add_argument('new_portfolio_code', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        securities_code = args['securities_code']
        stock_cd = args['stock_cd']
        old_portfolio_code = args['old_portfolio_code']
        new_portfolio_code = args['new_portfolio_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        # new portfolio code가 존재하는 코드인지 확인
        with session_scope() as session:
            user_portfolio_code = session.query(UserPortfolioInfo).\
                filter(UserPortfolioInfo.user_code == user_code,
                       UserPortfolioInfo.portfolio_code == new_portfolio_code).\
                first()
            session.commit()
            if user_portfolio_code is None:
                return {'status': '201', 'msg': get_status_msg('201')}  # 해당 포트폴리오 존재하지 않음

        key_list = [securities_code[i]+stock_cd[i] for i in range(len(securities_code))]

        # 조건에 만족하는 포트폴리오 검색
        with session_scope() as session:
            user_portfolio_map = session.query(UserPortfolioMap). \
                filter(UserPortfolioMap.user_code == user_code,
                       func.concat(UserPortfolioMap.securities_code, UserPortfolioMap.stock_cd).in_(key_list),
                       UserPortfolioMap.portfolio_code == old_portfolio_code). \
                all()
            session.commit()

            if user_portfolio_map is None:
                return {'status': '202', 'msg': get_status_msg('202')}  # 해당 종목 존재하지 않음
            else:
                for s in user_portfolio_map:
                    if s.securities_code == 'SELF':
                        # trading_log의 account number 수정
                        tr_log = session.query(UserTradingLog). \
                            filter(UserTradingLog.user_code == user_code,
                                   UserTradingLog.account_number == s.account_number,
                                   UserTradingLog.stock_cd == s.stock_cd).\
                            all()
                        date_list = []
                        for t in tr_log:
                            t.account_number = f'SELF_{new_portfolio_code}'
                            date_list.append(t.date)
                        min_date = min(date_list)

                        # 옮기려고 하는 포트폴리오에 직접입력된 해당 종목의 거래내역이 있는지 확인
                        new_map = session.query(UserPortfolioMap).\
                            filter(UserPortfolioMap.user_code == user_code,
                                   UserPortfolioMap.portfolio_code == new_portfolio_code,
                                   UserPortfolioMap.stock_cd == s.stock_cd,
                                   UserPortfolioMap.securities_code == s.securities_code).\
                            all()
                        session.commit()
                        # 거래내역 있는 경우 기존 map 삭제, 없는 경우 map 이동
                        if len(new_map) > 0:
                            session.query(UserPortfolioMap).\
                                filter(UserPortfolioMap.user_code == user_code,
                                       UserPortfolioMap.account_number == s.account_number,
                                       UserPortfolioMap.stock_cd == s.stock_cd,
                                       UserPortfolioMap.portfolio_code == s.portfolio_code).\
                                delete(synchronize_session='fetch')
                            session.commit()
                        else:
                            s.account_number = f'SELF_{new_portfolio_code}'
                            s.portfolio_code = new_portfolio_code
                            session.commit()

                        # 기존 portfolio 삭제
                        session.query(UserPortfolio). \
                            filter(UserPortfolio.user_code == user_code,
                                   UserPortfolio.account_number == f'SELF_{old_portfolio_code}',
                                   UserPortfolio.stock_cd == s.stock_cd). \
                            delete(synchronize_session='fetch')
                        session.commit()

                        # portfolio update
                        update_single_stock_portfolio(user_code=user_code,
                                                      begin_date=min_date,
                                                      account_number=f'SELF_{new_portfolio_code}',
                                                      securities_code='SELF',
                                                      stock_cd=s.stock_cd)
                    else:
                        s.portfolio_code = new_portfolio_code
                session.commit()

        return {'status': '000', 'msg': get_status_msg('000')}


class GetUserPortfolio(Resource):  # 포트폴리오 조회
    def post(self):
        if len(request.data) != 0:
            log_msg = make_log_msg("/api/portfolio/getportfoliolist", request.data)
            app.logger.info(log_msg)  # 안드로이드 로깅
        else:
            log_msg = make_log_msg("/api/portfolio/getportfoliolist", request.data)
            app.logger.info(log_msg)  # ios 로깅
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('portfolio_code', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        portfolio_code = args['portfolio_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'user_portfolio': []}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'user_portfolio': []}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        kr_latest_date = exec_query(f'select max(date) '
                                    f'from user_portfolio '
                                    f'where country = "KR" '
                                    f'and user_code = {user_code}')[0][0]
        us_latest_date = exec_query(f'select max(date) '
                                    f'from user_portfolio '
                                    f'where country = "US" '
                                    f'and user_code = {user_code}')[0][0]
        if kr_latest_date is None and us_latest_date is None:  # 포트폴리오 존재하지 않음(보유 종목 없음)
            return {'status': '201', 'msg': get_status_msg('201'),
                    'user_portfolio': []}

        # user_portfolio 변수 초기화
        kr_user_portfolio = pd.DataFrame()
        us_user_portfolio = pd.DataFrame()

        # 한국주식
        if kr_latest_date is not None:
            sql = f'select user_code, portfolio_code, date, stock_cd, ' \
                  f'            max(stock_nm) as stock_nm, sum(holding_quantity) as holding_quantity, ' \
                  f'			round(sum(holding_quantity*avg_purchase_price)/sum(holding_quantity), 1) as avg_purchase_price, ' \
                  f'			max(close_price) as close_price, ' \
                  f'			max(prev_1w_close_price) as prev_1w_close_price, ' \
                  f'			cast(sum(total_value) as signed) as total_value, ' \
                  f'			min(first_purchase_date) as first_purchase_date, ' \
                  f'			max(retention_period) as retention_period, ' \
                  f'			cast(sum(new_purchase_amount) as signed) as new_purchase_amount, ' \
                  f'			cast(sum(realized_profit_loss) as signed) as realized_profit_loss, ' \
                  f'			cast(sum(purchase_amount_of_stocks_to_sell) as signed) as purchase_amount_of_stocks_to_sell, ' \
                  f'			max(unit_currency) as unit_currency, ' \
                  f'			max(update_dtim) as update_dtim, ' \
                  f'			max(portfolio_nm) as portfolio_nm, ' \
                  f'			max(market) as market, ' \
                  f'			max(sector) as sector ' \
                  f'	from (  SELECT a.*, c.portfolio_code, c.portfolio_nm, e.market, e.sector ' \
                  f'        	FROM sauvignon.user_portfolio AS a, ' \
                  f'			 sauvignon.user_portfolio_map AS b, ' \
                  f'			 sauvignon.user_portfolio_info as c, ' \
                  f'			 sauvignon.date_working_day_mapping as d, ' \
                  f'			 sauvignon.stock_market_sector as e ' \
                  f'		    WHERE a.user_code = b.user_code ' \
                  f'		    AND a.account_number = b.account_number ' \
                  f'	    	AND a.stock_cd = b.stock_cd ' \
                  f'	    	AND a.user_code = c.user_code ' \
                  f'	    	AND b.portfolio_code = c.portfolio_code ' \
                  f'	    	and a.date = d.date ' \
                  f'	    	and a.stock_cd = e.stock_cd ' \
                  f'	    	and d.working_day = e.date' \
                  f'            and a.user_code = {user_code} ' \
                  f'            and c.portfolio_code = {portfolio_code} ' \
                  f'            and a.date = {kr_latest_date} ' \
                  f'            and a.country = "KR") as a ' \
                  f'	group by user_code, portfolio_code, date, stock_cd ' \
                  f'    having cast(sum(total_value) as signed) > 0 ' \
                  f'    order by (max(close_price) / round(sum(holding_quantity*avg_purchase_price)/sum(holding_quantity), 1)) desc '
                  # f'    order by cast(sum(total_value) as signed) desc '
            kr_user_portfolio = get_df_from_db(sql)

        # 미국주식
        if us_latest_date is not None:
            sql = f'select user_code, portfolio_code, date, stock_cd, ' \
                  f'            max(stock_nm) as stock_nm, sum(holding_quantity) as holding_quantity, ' \
                  f'			round(sum(holding_quantity*avg_purchase_price)/sum(holding_quantity), 1) as avg_purchase_price, ' \
                  f'			max(close_price) as close_price, ' \
                  f'			max(prev_1w_close_price) as prev_1w_close_price, ' \
                  f'			cast(sum(total_value) as signed) as total_value, ' \
                  f'			min(first_purchase_date) as first_purchase_date, ' \
                  f'			max(retention_period) as retention_period, ' \
                  f'			cast(sum(new_purchase_amount) as signed) as new_purchase_amount, ' \
                  f'			cast(sum(realized_profit_loss) as signed) as realized_profit_loss, ' \
                  f'			cast(sum(purchase_amount_of_stocks_to_sell) as signed) as purchase_amount_of_stocks_to_sell, ' \
                  f'			max(unit_currency) as unit_currency, ' \
                  f'			max(update_dtim) as update_dtim, ' \
                  f'			max(portfolio_nm) as portfolio_nm, ' \
                  f'			max(market) as market, ' \
                  f'			case when max(sector) is null then "" else max(sector) end as sector ' \
                  f'	from (  SELECT a.*, c.portfolio_code, c.portfolio_nm, e.exchange as market, e.industry as sector ' \
                  f'        	FROM sauvignon.user_portfolio AS a, ' \
                  f'			 sauvignon.user_portfolio_map AS b, ' \
                  f'			 sauvignon.user_portfolio_info as c, ' \
                  f'			 sauvignon.us_date_working_day_mapping as d, ' \
                  f'			 sauvignon.us_stock_info as e ' \
                  f'		    WHERE a.user_code = b.user_code ' \
                  f'		    AND a.account_number = b.account_number ' \
                  f'	    	AND a.stock_cd = b.stock_cd ' \
                  f'	    	AND a.user_code = c.user_code ' \
                  f'	    	AND b.portfolio_code = c.portfolio_code ' \
                  f'	    	and a.date = d.date ' \
                  f'	    	and a.stock_cd = e.stock_cd ' \
                  f'            and a.user_code = {user_code} ' \
                  f'            and c.portfolio_code = {portfolio_code} ' \
                  f'            and a.date = {us_latest_date} ' \
                  f'            and a.country = "US") as a ' \
                  f'	group by user_code, portfolio_code, date, stock_cd ' \
                  f'    having cast(sum(total_value) as signed) > 0 ' \
                  f'    order by (max(close_price) / round(sum(holding_quantity*avg_purchase_price)/sum(holding_quantity), 1)) desc '
                  # f'    order by cast(sum(total_value) as signed) desc '
            us_user_portfolio = get_df_from_db(sql)
            # kr_latest_date가 us_latest_date보다 클 경우(한국 장 종료시간 ~ 미국 장 종료시간), us_user_portfolio의 date와 retention_period를 +1일 해준다.
            if kr_latest_date is not None and kr_latest_date > us_latest_date:
                for i in range(us_user_portfolio.shape[0]):
                    us_user_portfolio.iloc[i, 2] = int((datetime.strptime(str(us_user_portfolio.iloc[i, 2]), '%Y%m%d') +
                                                        timedelta(days=1)).strftime('%Y%m%d'))
                    us_user_portfolio.iloc[i, 11] = us_user_portfolio.iloc[i, 11] + 1

        # user_portfolio 생성하기(한국주식 + 미국주식 해줌)
        user_portfolio = pd.concat([kr_user_portfolio, us_user_portfolio], axis=0)
        # 상장한지 1주가 안지난 애들(prev_1w_close_price, prev_week_return이 NA인 경우)
        # 전주 가격이 없을 경우 0으로 표시 <- 추후 앱 개선 시 None값으로 내보낼 것
        user_portfolio['prev_1w_close_price'] = user_portfolio['prev_1w_close_price'].apply(lambda x: 0 if pd.isna(x) else x)

        if user_portfolio.shape[0] == 0:  # 포트폴리오 존재하지 않음
            return {'status': '201', 'msg': get_status_msg('201'),
                    'user_portfolio': []}

        else:  # 정상
            # return dict list
            return_list = []
            for i in range(user_portfolio.shape[0]):
                p = user_portfolio.iloc[i, :]
                tmp_dict = {'user_code': int(p[0]),
                            'portfolio_code': int(p[1]),
                            'date': int(p[2]),
                            'stock_cd': p[3],
                            'stock_nm': p[4],
                            'holding_quantity': float(p[5]),
                            'avg_purchase_price': float(p[6]),
                            'close_price': float(p[7]),
                            'prev_1w_close_price': float(p[8]),
                            'total_value': int(p[9]),
                            # APP 개선 후 float로 바꿔줄 것
                            # 'total_value': float(p[9]),
                            'first_purchase_date': p[10],
                            'retention_period': int(p[11]),
                            'new_purchase_amount': int(p[12]),
                            # 'new_purchase_amount': float(p[12]),
                            'realized_profit_loss': int(p[13]),
                            # 'realized_profit_loss': float(p[13]),
                            'purchase_amount_of_stocks_to_sell': int(p[14]),
                            # 'purchase_amount_of_stocks_to_sell': float(p[14]),
                            'unit_currency': p[15],
                            'update_dtim': p[16],
                            'portfolio_nm': p[17],
                            'market': p[18],
                            'sector': p[19]
                            }
                tmp_dict['date'] = str(tmp_dict['date'])
                # 전주 가격이 0일 경우, 0으로 표시한다.
                tmp_dict.update({'prev_week_return': round(((p[7]/p[8])-1)*100 if p[8] != 0 else 0, 2)})
                return_list.append(tmp_dict)

            # print(return_list)
            return {'status': '000', 'msg': get_status_msg('000'),
                    'user_portfolio': return_list}


class DeleteUserPortfolio(Resource):  # 포트폴리오 삭제
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('portfolio_code', type=int, action='append')
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        portfolio_code = args['portfolio_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        # 포트폴리오 값 가져오기
        with session_scope() as session:
            user_portfolio_info_query = session.query(UserPortfolioInfo). \
                filter(UserPortfolioInfo.user_code == user_code)
            user_portfolio_info = user_portfolio_info_query.all()
            session.commit()

            # portfolio_code에 해당하는 애들만 filter
            n_portfolio = len(user_portfolio_info)
            user_portfolio_info = [p for p in user_portfolio_info if p.portfolio_code in portfolio_code]

            # 포트폴리오 내 종목 가져오기
            user_portfolio_map = session.query(UserPortfolioMap). \
                filter(UserPortfolioMap.user_code == user_code,
                       UserPortfolioMap.portfolio_code.in_(portfolio_code)). \
                all()
            session.commit()

            if len(user_portfolio_info) == 0:  # 포트폴리오 없음
                return {'status': '201', 'msg': get_status_msg('201')}
            elif len(user_portfolio_info) == n_portfolio:  # 모든 포트폴리오 삭제
                return {'status': '207', 'msg': get_status_msg('207')}
            else:
                if len(user_portfolio_map) != 0:  # 삭제 대상 포트폴리오 내 종목 존재
                    return {'status': '203', 'msg': get_status_msg('203')}
                else:
                    session.query(UserPortfolioInfo). \
                        filter(UserPortfolioInfo.user_code == user_code,
                               UserPortfolioInfo.portfolio_code.in_(portfolio_code)).\
                        delete(synchronize_session='fetch')  # 포트폴리오 삭제
                    session.commit()
                    return {'status': '000', 'msg': get_status_msg('000')}


class GetPortfolioYield(Resource):  # 포트폴리오 수익률 조회
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=str)
        parser.add_argument('portfolio_code', type=str)
        parser.add_argument('period_code', type=str)
        parser.add_argument('begin_date', type=int)
        parser.add_argument('end_date', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        portfolio_code = args['portfolio_code']
        period_code = args['period_code']
        begin_date = args['begin_date']
        end_date = args['end_date']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status),
                    'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                    'current_asset': 0, 'daily_return': []}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status),
                    'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                    'current_asset': 0, 'daily_return': []}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        # period code가 CUSTOM인 경우, None으로 바꿔준다
        if period_code == 'CUSTOM':
            period_code = None

        # 포트폴리오 정보 가져오기
        with session_scope() as session:
            portfolio_info = session.query(UserPortfolioInfo). \
                filter(UserPortfolioInfo.user_code == user_code,
                       UserPortfolioInfo.portfolio_code == portfolio_code). \
                first()

        if portfolio_info is None:
            return {'status': '201', 'msg': get_status_msg('201'),  # 해당 포트폴리오 존재하지 않음
                    'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                    'current_asset': 0, 'daily_return': []}

        # begin_date, end_date가 존재하지 않을 경우, begin_date, end_date를 정의해준다.
        if period_code is not None:
            # 1 day 처리
            if period_code == '1D':
                return get_oneday_portfolio_yield(user_code=user_code, portfolio_code=portfolio_code)

            # period code 변환
            if period_code.endswith('D'):
                period = int(period_code[:-1])
            elif period_code.endswith('M'):
                period = int(period_code[:-1])*30.5
            elif period_code.endswith('Y'):
                period = int(period_code[:-1])*365
            else:
                return {'status': '206', 'msg': get_status_msg('206'),  # 올바르지 않은 기간코드
                        'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                        'current_asset': 0, 'daily_return': []}

            # end_date 정의 <- 가장 최근 포트폴리오가 존재하는 일자
            # 해당 날짜에 아직 미국주식 포트폴리오가 생성되지 않은 상황이더라도, calc_portfolio_yield에서 해당 케이스를 처리해주므로 신경쓰지 않아도 된다.
            end_date = exec_query(f'select max(date) from user_portfolio where user_code = {user_code}')[0][0]

            # 포트폴리오가 존재하지 않는 경우, 기간 내 보유 종목 없음 응답
            if end_date is None:
                return {'status': '205', 'msg': get_status_msg('205'),  # 기간 내 보유 종목 없음
                        'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                        'current_asset': 0, 'daily_return': []}
            else:
                end_date = str(end_date)

            # begin_date 정의
            # 수익률을 보고싶은 기간의 첫 영업일보다 1영업일 이전을 begin_date로 한다.
            # 왜냐하면 첫 일자의 수익률은 무조건 0으로 표시되기 때문. 5영업일 전부터 수익률을 보고싶다면, 6영업일 전부터 조회를 해야한다.
            # 일별 수익률의 경우 영업일 처리를 해 준다
            if period_code.endswith('D'):
                if period < 5:
                    period = 5
                begin_date = exec_query(f'select working_day '
                                        f'from date_working_day '
                                        f'where seq = (select b.seq - {period} '
                                        f'             from date_working_day_mapping as a, date_working_day as b '
                                        f'             where a.working_day = b.working_day'
                                        f'             and a.date = {end_date})')[0][0]
            else:
                begin_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=period)).strftime('%Y%m%d')
                begin_date = exec_query(f'select b.working_day '
                                        f'from date_working_day_mapping as a, date_working_day as b '
                                        f'where a.working_day = b.working_day '
                                        f'and a.date = {begin_date} ')[0][0]

        else:
            # period 변수 초기화
            period = None

            # end_date 를 string으로 바꿔준다.
            end_date = str(end_date)

            # end_date 유효성 확인
            tmp_end_date = str(exec_query(f'select max(date) from user_portfolio where user_code = {user_code}')[0][0])
            if end_date > tmp_end_date:
                end_date = tmp_end_date

        return calc_portfolio_yield(user_code=user_code, begin_date=begin_date, end_date=end_date,
                                    portfolio_code=portfolio_code, period_code=period_code, period=period)


def get_oneday_portfolio_yield(user_code, portfolio_code):
    # 포트폴리오 정보 가져오기
    with session_scope() as session:
        portfolio_info = session.query(UserPortfolioInfo). \
            filter(UserPortfolioInfo.user_code == user_code,
                   UserPortfolioInfo.portfolio_code == portfolio_code). \
            first()

        if portfolio_info is None:
            return {'status': '201', 'msg': get_status_msg('201'),  # 해당 포트폴리오 존재하지 않음
                    'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                    'current_asset': 0, 'kr_daily_return': [], 'us_daily_return': []}

    # kr_target_date 정의 <- 가장 최근 한국주식 실시간데이터가 쌓인 날짜
    kr_target_dtim = exec_query('select max(dtim) from stock_realtime_price')[0][0]
    kr_target_date = str(kr_target_dtim)[:8]
    # print(f'kr_target_date: {kr_target_date}')

    # us_target_date 정의 <- 가장 최근 미국주식 실시간데이터가 쌓인 날짜(미국날짜 기준)
    us_target_dtim = exec_query('select max(dtim) from us_stock_realtime_price')[0][0]
    us_target_date = str(us_target_dtim)[:8]
    # print(f'us_target_date: {us_target_date}')

    # kr_portfolio_date 정의 <- 가장 최근 영업일 데이터가 쌓인 날짜
    kr_portfolio_date = exec_query('select max(working_day) from date_working_day')[0][0]
    kr_portfolio_date = str(kr_portfolio_date)
    # print(f'kr_portfolio_date: {kr_portfolio_date}')

    # kr_portfolio_date와 kr_target_date가 동일할 경우, kr_portfolio_date를 전 영업일로 바꿔준다.
    if kr_portfolio_date == kr_target_date:
        sql = f'select a.working_day ' \
              f'from date_working_day as a ' \
              f'join date_working_day as b ' \
              f'	on a.seq = b.seq - 1 ' \
              f'where b.working_day = {kr_target_date}'
        kr_portfolio_date = str(exec_query(sql)[0][0])
        # print(f'kr_portfolio_date: {kr_portfolio_date}')

    # us_portfolio_date 정의 <- 가장 최근 미국 주식 영업일 데이터가 쌓인 날짜
    us_portfolio_date = exec_query('select max(working_day) from us_date_working_day')[0][0]
    us_portfolio_date = str(us_portfolio_date)
    # print(f'us_portfolio_date: {us_portfolio_date}')

    # us_portfolio_date와 us_target_date가 동일할 경우, us_portfolio_date를 전 영업일로 바꿔준다.
    if us_portfolio_date == us_target_date:
        sql = f'select a.working_day ' \
              f'from us_date_working_day as a ' \
              f'join us_date_working_day as b ' \
              f'	on a.seq = b.seq - 1 ' \
              f'where b.working_day = {us_target_date}'
        us_portfolio_date = str(exec_query(sql)[0][0])
        # print(f'us_portfolio_date: {us_portfolio_date}')

    # 한국주식 포트폴리오 불러오기
    sql = f'select a.*, b.portfolio_code ' \
          f'from user_portfolio as a ' \
          f'left join user_portfolio_map as b ' \
          f'	on a.user_code = b.user_code ' \
          f'    and a.account_number = b.account_number ' \
          f'    and a.stock_cd = b.stock_cd ' \
          f'where a.user_code = {user_code} ' \
          f'and a.date = {kr_portfolio_date} ' \
          f'and b.portfolio_code = {portfolio_code} ' \
          f'and a.holding_quantity != 0 ' \
          f'and a.country = "KR"'
    kr_user_portfolio = get_df_from_db(sql)
    # print('---------- kr_user_portfolio ----------')
    # print(kr_user_portfolio)

    # 미국주식 포트폴리오 불러오기
    sql = f'select a.*, b.portfolio_code ' \
          f'from user_portfolio as a ' \
          f'left join user_portfolio_map as b ' \
          f'	on a.user_code = b.user_code ' \
          f'    and a.account_number = b.account_number ' \
          f'    and a.stock_cd = b.stock_cd ' \
          f'where a.user_code = {user_code} ' \
          f'and a.date = {us_portfolio_date} ' \
          f'and b.portfolio_code = {portfolio_code} ' \
          f'and a.holding_quantity != 0 ' \
          f'and a.country = "US"'
    us_user_portfolio = get_df_from_db(sql)
    # print('---------- us_user_portfolio ----------')
    # print(us_user_portfolio)

    if kr_user_portfolio.shape[0] == 0 and us_user_portfolio.shape[0] == 0:
        return {'status': '205', 'msg': get_status_msg('205'),  # 기간 내 보유 종목 없음
                'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                'current_asset': 0, 'kr_daily_return': [], 'us_daily_return': []}

    # 한국주식 포트폴리오 존재하는 경우 처리
    if kr_user_portfolio.shape[0] != 0:
        # 가격 정보 불러오기
        sql = f'select dtim, stock_cd, stock_nm, ' \
              f'        case when substr(dtim, 9,4) = "0920" and open_price != 0 then open_price else close_price end as price ' \
              f'from stock_realtime_price ' \
              f'where dtim > {kr_target_date}000000 ' \
              f'and dtim < {kr_target_date}999999 ' \
              f'and stock_cd in {str(list(kr_user_portfolio["stock_cd"])).replace("[", "(").replace("]", ")")} ' \
              f'union all ' \
              f'select dtim, stock_cd, stock_nm, ' \
              f'        case when substr(dtim, 9,4) = "0920" and open_price != 0 then open_price else close_price end as price ' \
              f'from stock_realtime_etf ' \
              f'where dtim > {kr_target_date}000000 ' \
              f'and dtim < {kr_target_date}999999 ' \
              f'and stock_cd in {str(list(kr_user_portfolio["stock_cd"])).replace("[", "(").replace("]", ")")} ' \
              f'union all ' \
              f'select dtim, stock_cd, stock_nm, ' \
              f'        case when substr(dtim, 9,4) = "0920" and open_price != 0 then open_price else close_price end as price ' \
              f'from stock_realtime_etn ' \
              f'where dtim > {kr_target_date}000000 ' \
              f'and dtim < {kr_target_date}999999 ' \
              f'and stock_cd in {str(list(kr_user_portfolio["stock_cd"])).replace("[", "(").replace("]", ")")} '
        kr_price_info = get_df_from_db(sql)
        # print('---------- kr_price_info ----------')
        # print(kr_price_info)

        # 보유 주식수 곱해주기
        kr_price_info = kr_price_info.merge(kr_user_portfolio, how='inner', on='stock_cd')
        kr_price_info['price_value'] = kr_price_info['price'] * kr_price_info['holding_quantity']
        # print('---------- kr_price_info ----------')
        # print(kr_price_info)

        # 시간대별 group by
        kr_price_info = kr_price_info.groupby('dtim').sum()
        kr_price_info = kr_price_info.reset_index()
        # print('---------- kr_price_info ----------')
        # print(kr_price_info)

        # 전일 포트폴리오 가치(최초시점가치) 확인
        kr_base_value = sum(kr_user_portfolio['total_value'])
        # print(f'kr_base_value: {kr_base_value}')

        # 수익률 계산
        kr_price_info['return'] = kr_price_info['price_value'] / kr_base_value - 1
        # print('---------- kr_price_info ----------')
        # print(kr_price_info)

        # 실시간 코스피, 코스닥 정보 불러오기
        sql = f'select a.dtim, ' \
              f'       sum(case when index_nm = "KOSPI" then a.index else 0 end) as kospi, ' \
              f'       sum(case when index_nm = "KOSDAQ" then a.index else 0 end) as kosdaq  ' \
              f'from stock_realtime_index as a ' \
              f'where dtim > {kr_target_date}000000 ' \
              f'and dtim < {kr_target_date}999999 ' \
              f'group by dtim '
        kospi_kosdaq = get_df_from_db(sql)
        # print('---------- kospi_kosdaq ----------')
        # print(kospi_kosdaq)

        # 전일 코스피, 코스닥 정보 불러오기
        sql = f'select date, ' \
              f'        sum(case when index_nm = "KOSPI" then a.index else 0 end) as kospi, ' \
              f'        sum(case when index_nm = "KOSDAQ" then a.index else 0 end) as kosdaq ' \
              f'from stock_kospi_kosdaq as a ' \
              f'where date = {kr_portfolio_date} ' \
              f'group by date '
        prev_kospi_kosdaq = get_df_from_db(sql)

        # 전일(최초시점) 코스피, 코스닥
        base_kospi = prev_kospi_kosdaq['kospi'].values[0]
        base_kosdaq = prev_kospi_kosdaq['kosdaq'].values[0]
        # print(f'base_kospi: {base_kospi}')
        # print(f'base_kosdaq: {base_kosdaq}')

        # 코스피, 코스닥 수익률
        kospi_kosdaq['kospi_daily_return'] = kospi_kosdaq['kospi'] / base_kospi - 1
        kospi_kosdaq['kosdaq_daily_return'] = kospi_kosdaq['kosdaq'] / base_kosdaq - 1
        # print('---------- kospi_kosdaq ----------')
        # print(kospi_kosdaq)

        # kr_price_info, kospi_kosdaq merging
        kr_price_info = kr_price_info.merge(kospi_kosdaq, how='inner', on='dtim')
        # print('---------- kr_price_info ----------')
        # print(kr_price_info)

    # 미국주식 포트폴리오 존재하는 경우 처리
    if us_user_portfolio.shape[0] != 0:
        # 환율 정보 불러오기 -> target date 날짜 환율이 없을경우 최근 날짜의 환율을 가져오기 위해 지난 1년치 환율 가지고 온다
        sql = f'select * from exchange_rate where date <= {us_target_date} and date >= {int(us_target_date)-10000}'
        usd_value = list(get_df_from_db(sql)['usd_krw'])[-1]

        # 가격 정보 불러오기
        sql = f'select dtim, stock_cd, close_price as price ' \
              f'from us_stock_realtime_price ' \
              f'where dtim > {us_target_date}000000 ' \
              f'and dtim < {us_target_date}999999 ' \
              f'and stock_cd in {str(list(us_user_portfolio["stock_cd"])).replace("[", "(").replace("]", ")")} '
        us_price_info = get_df_from_db(sql)
        # print('---------- us_price_info ----------')
        # print(us_price_info)

        # 가격에 환율 적용하기
        us_price_info['price'] = us_price_info['price'] * usd_value

        # 보유 주식수 곱해주기
        us_price_info = us_price_info.merge(us_user_portfolio, how='inner', on='stock_cd')
        us_price_info['price_value'] = us_price_info['price'] * us_price_info['holding_quantity']
        # print('---------- us_price_info ----------')
        # print(us_price_info)

        # 시간대별 group by
        us_price_info = us_price_info.groupby('dtim').sum()
        us_price_info = us_price_info.reset_index()
        # print('---------- us_price_info ----------')
        # print(us_price_info)

        # 전일 포트폴리오 가치(최초시점가치) 확인(환율 곱해줌)
        us_base_value = sum(us_user_portfolio['total_value']) * usd_value
        # print(f'us_base_value: {us_base_value}')

        # 수익률 계산
        us_price_info['return'] = us_price_info['price_value'] / us_base_value - 1
        # print('---------- us_price_info ----------')
        # print(us_price_info)

        # 실시간 S&P500, NASDAQ 정보 불러오기
        sql = f'select a.dtim, ' \
              f'       sum(case when index_nm = "S&P500" then a.close_price else 0 end) as `S&P500`, ' \
              f'       sum(case when index_nm = "NASDAQ" then a.close_price else 0 end) as NASDAQ  ' \
              f'from us_stock_realtime_index as a ' \
              f'where dtim > {us_target_date}000000 ' \
              f'and dtim < {us_target_date}999999 ' \
              f'group by dtim '
        snp_nasdaq = get_df_from_db(sql)
        # print('---------- snp_nasdaq ----------')
        # print(snp_nasdaq)

        # 전일 S&P500, NASDAQ 정보 불러오기
        sql = f'select date, ' \
              f'        sum(case when index_nm = "S&P500" then a.close_index else 0 end) as `S&P500`, ' \
              f'        sum(case when index_nm = "NASDAQ" then a.close_index else 0 end) as NASDAQ ' \
              f'from us_stock_index as a ' \
              f'where date = {us_portfolio_date} ' \
              f'group by date '
        prev_snp_nasdaq = get_df_from_db(sql)
        # print('---------- prev_snp_nasdaq ----------')
        # print(prev_snp_nasdaq)

        # 전일(최초시점) S&P500, NASDAQ
        base_snp = prev_snp_nasdaq['S&P500'].values[0]
        base_nasdaq = prev_snp_nasdaq['NASDAQ'].values[0]
        # print(f'base_snp: {base_snp}')
        # print(f'base_nasdaq: {base_nasdaq}')

        # S&P500, NASDAQ 수익률
        snp_nasdaq['snp_daily_return'] = snp_nasdaq['S&P500'] / base_snp - 1
        snp_nasdaq['nasdaq_daily_return'] = snp_nasdaq['NASDAQ'] / base_nasdaq - 1
        # print('---------- snp_nasdaq ----------')
        # print(snp_nasdaq)

        # us_price_info, kospi_kosdaq merging
        us_price_info = us_price_info.merge(snp_nasdaq, how='inner', on='dtim')
        # print('---------- us_price_info ----------')
        # print(us_price_info)

        # 시간 바꿔주기(한국시간 기준으로)
        isindst = datetime.strptime(str(us_price_info['dtim'][0]), '%Y%m%d%H%M%S').replace(tzinfo=pytz.timezone('US/Eastern')).dst() == timedelta(0)
        if isindst:
            # 서머타임 적용 중일 때, 13시간 더해줌
            us_price_info['dtim'] = us_price_info['dtim'].apply(
                lambda x: int(
                    (datetime.strptime(str(x), '%Y%m%d%H%M%S') + timedelta(hours=13)).strftime('%Y%m%d%H%M%S')))
        else:
            # 서머타임 미적용 중일 때, 14시간 더해줌
            us_price_info['dtim'] = us_price_info['dtim'].apply(
                lambda x: int(
                    (datetime.strptime(str(x), '%Y%m%d%H%M%S') + timedelta(hours=14)).strftime('%Y%m%d%H%M%S')))
        # print('---------- us_price_info ----------')
        # print(us_price_info)

    # 한국주식이 있는 경우
    if kr_user_portfolio.shape[0] != 0:
        # kr_return_dict
        kr_return_dict_list = [{'date': f'{kr_portfolio_date} 08:30:00',
                                'daily_return': 0,
                                'kospi_daily_return': 0,
                                'kosdaq_daily_return': 0,
                                'daily_profit': 0,
                                'kospi_daily_profit': 0,
                                'kosdaq_daily_profit': 0}]

        for i, r in kr_price_info.iterrows():
            dtim = str(int(r['dtim']))
            dtim = (datetime.strptime(dtim, '%Y%m%d%H%M%S') - timedelta(minutes=20)).strftime('%Y%m%d%H%M%S')
            ith_dict = {'date': f'{dtim[:8]} {dtim[8:10]}:{dtim[10:12]}:{dtim[12:14]}',
                        'daily_return': r['return'],
                        'kospi_daily_return': r['kospi_daily_return'],
                        'kosdaq_daily_return': r['kosdaq_daily_return'],
                        'daily_profit': r['price_value'] - kr_base_value,
                        'kospi_daily_profit': (r['kospi_daily_return']) * kr_base_value,
                        'kosdaq_daily_profit': (r['kosdaq_daily_return']) * kr_base_value}

            kr_return_dict_list.append(ith_dict)
    else:
        kr_return_dict_list = []

    # 미국주식이 있는 경우
    if us_user_portfolio.shape[0] != 0:
        # us_return_dict
        us_return_dict_list = [{'date': f'{str(min(us_price_info["dtim"]))[:8]} {str(min(us_price_info["dtim"]))[8:10]}:00:00',
                                'daily_return': 0,
                                'snp_daily_return': 0,
                                'nasdaq_daily_return': 0,
                                'daily_profit': 0,
                                'snp_daily_profit': 0,
                                'nasdaq_daily_profit': 0}]

        for i, r in us_price_info.iterrows():
            dtim = str(int(r['dtim']))
            dtim = (datetime.strptime(dtim, '%Y%m%d%H%M%S') - timedelta(minutes=20)).strftime('%Y%m%d%H%M%S')
            ith_dict = {'date': f'{dtim[:8]} {dtim[8:10]}:{dtim[10:12]}:{dtim[12:14]}',
                        'daily_return': r['return'],
                        'snp_daily_return': r['snp_daily_return'],
                        'nasdaq_daily_return': r['nasdaq_daily_return'],
                        'daily_profit': r['price_value'] - us_base_value,
                        'snp_daily_profit': (r['snp_daily_return']) * us_base_value,
                        'nasdaq_daily_profit': (r['nasdaq_daily_return']) * us_base_value}

            us_return_dict_list.append(ith_dict)
    else:
        us_return_dict_list = []

    # price_info, base_value 설정
    # 한국주식만 있는 경우
    if kr_user_portfolio.shape[0] != 0 and us_user_portfolio.shape[0] == 0:
        price_info = kr_price_info
        base_value = kr_base_value
    # 미국주식만 있는 경우
    elif kr_user_portfolio.shape[0] == 0 and us_user_portfolio.shape[0] != 0:
        price_info = us_price_info
        base_value = us_base_value
    # 둘다 있는 경우
    else:
        # 어떤게 먼저 오느냐에 따라서 두번째에 오는 수익률은 base_value가 아니라 전 수익률 종가를 더해줘야 한다.
        kr_dtim = max(kr_price_info['dtim'])
        us_dtim = max(us_price_info['dtim'])
        if kr_dtim < us_dtim:
            # us_price_info에 kr_price_info의 마지막 price_value를 더하고, kr_price_info에 us_base_value를 더해야 함.
            us_price_info['price_value'] = us_price_info['price_value'] + \
                                           kr_price_info.loc[kr_price_info['dtim'] == kr_dtim, 'price_value'].values[0]
            kr_price_info['price_value'] = kr_price_info['price_value'] + us_base_value
            us_price_info['kospi_daily_return'] = kr_price_info.loc[kr_price_info['dtim'] == kr_dtim, 'kospi_daily_return'].values[0]
            us_price_info['kosdaq_daily_return'] = kr_price_info.loc[kr_price_info['dtim'] == kr_dtim, 'kosdaq_daily_return'].values[0]
        else:
            # kr_price_info에 us_price_info의 마지막 price_value를 더하고, us_price_info에 kr_base_value를 더해야 함.
            kr_price_info['price_value'] = kr_price_info['price_value'] + \
                                           us_price_info.loc[us_price_info['dtim'] == us_dtim, 'price_value'].values[0]
            us_price_info['price_value'] = us_price_info['price_value'] + kr_base_value
        base_value = kr_base_value + us_base_value

        price_info = pd.concat([kr_price_info, us_price_info], axis=0)
        price_info.sort_values(by='dtim', inplace=True)
        price_info['return'] = price_info['price_value'] / base_value - 1

    # return
    return {'status': '000', 'msg': get_status_msg('000'),
            'total_return': price_info.loc[price_info['dtim'] == max(price_info['dtim']), 'return'].values[0],
            'realized_profit_loss': 0,
            'total_purchase': base_value,
            'total_income': price_info.loc[price_info['dtim'] == max(price_info['dtim']), 'price_value'].values[
                                0] - base_value,
            'current_asset': price_info.loc[price_info['dtim'] == max(price_info['dtim']), 'price_value'].values[0],
            'kr_daily_return': kr_return_dict_list,
            'us_daily_return': us_return_dict_list}


class GetAllPortfolioYield2021(Resource):  # 2021 포트폴리오 수익률 조회
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=str)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status),
                    'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                    'current_asset': 0, 'daily_return': []}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status),
                    'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                    'current_asset': 0, 'daily_return': []}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        # begin_date 정의
        with session_scope() as session:
            # portfolio 시작일 확인
            portfolio_start_date = session.query(func.min(UserPortfolio.date)). \
                filter(UserPortfolio.date >= 20210101,
                       UserPortfolio.user_code == user_code). \
                first()
            session.commit()
            if portfolio_start_date is None:
                return {'status': '205', 'msg': get_status_msg('205'),  # 기간 내 보유 종목 없음
                        'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                        'current_asset': 0, 'kr_daily_return': [], 'us_daily_return': []}
            else:
                portfolio_start_date = portfolio_start_date[0]

            tbl1 = aliased(DateWorkingDay)
            tbl2 = aliased(DateWorkingDay)
            begin_date = session.query(tbl1.working_day,
                                       tbl2.working_day.label("working_day")).\
                join(tbl2, tbl1.seq == tbl2.seq + 1).\
                filter(tbl1.working_day >= portfolio_start_date).\
                first()
            session.commit()

            begin_date = begin_date[1]

        # end_date 정의
        end_date = "20211230"

        return calc_all_portfolio_yield(user_code=user_code, begin_date=begin_date, end_date=end_date)


class GetPortfolioCompositionRatio(Resource):  # 포트폴리오 구성비율 조회
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('portfolio_code', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        portfolio_code = args['portfolio_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'composition_ratio_list': []}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'composition_ratio_list': []}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        kr_latest_date = exec_query('select max(working_day) from date_working_day')[0][0]
        us_latest_date = exec_query('select max(working_day) from us_date_working_day')[0][0]
        exchange_rate = exec_query('select usd_krw from exchange_rate order by date DESC limit 1')[0][0]

        with session_scope() as session:
            kr_user_portfolio = session.query(UserPortfolio.stock_cd,
                                              func.max(UserPortfolio.stock_nm).label('stock_nm'),
                                              func.sum(UserPortfolio.total_value).cast(Integer).label('total_value'))\
                .join(UserPortfolioMap,
                      and_(UserPortfolio.user_code == UserPortfolioMap.user_code,
                           UserPortfolio.account_number == UserPortfolioMap.account_number,
                           UserPortfolio.stock_cd == UserPortfolioMap.stock_cd))\
                .filter(UserPortfolio.user_code == user_code,
                        UserPortfolioMap.portfolio_code == portfolio_code,
                        UserPortfolio.date == kr_latest_date,
                        UserPortfolio.country == 'KR')\
                .group_by(UserPortfolio.user_code,
                          UserPortfolioMap.portfolio_code,
                          UserPortfolio.date,
                          UserPortfolio.stock_cd)\
                .having(func.sum(UserPortfolio.total_value) > 0)\
                .order_by(func.sum(UserPortfolio.total_value).desc())
            kr_user_portfolio = pd.read_sql(kr_user_portfolio.statement, session.bind)
            session.commit()

            us_user_portfolio = session.query(UserPortfolio.stock_cd,
                                              func.max(UserPortfolio.stock_nm).label('stock_nm'),
                                              func.sum(UserPortfolio.total_value * exchange_rate).label('total_value'))\
                .join(UserPortfolioMap,
                      and_(UserPortfolio.user_code == UserPortfolioMap.user_code,
                           UserPortfolio.account_number == UserPortfolioMap.account_number,
                           UserPortfolio.stock_cd == UserPortfolioMap.stock_cd))\
                .filter(UserPortfolio.user_code == user_code,
                        UserPortfolioMap.portfolio_code == portfolio_code,
                        UserPortfolio.date == us_latest_date,
                        UserPortfolio.country == 'US')\
                .group_by(UserPortfolio.user_code,
                          UserPortfolioMap.portfolio_code,
                          UserPortfolio.date,
                          UserPortfolio.stock_cd)\
                .having(func.sum(UserPortfolio.total_value) > 0)\
                .order_by(func.sum(UserPortfolio.total_value).desc())
            us_user_portfolio = pd.read_sql(us_user_portfolio.statement, session.bind)
            session.commit()

            user_portfolio = pd.concat([kr_user_portfolio, us_user_portfolio])
            user_portfolio = user_portfolio.sort_values('total_value', ascending=False).reset_index(drop=True)

            if user_portfolio.shape[0] == 0:  # 포트폴리오 존재하지 않음
                return {'status': '205', 'msg': get_status_msg('205'),  # 기간 내 보유 종목 없음
                        'composition_ratio_list': []}
            else:  # 정상
                total_value = 0
                composition_sum = 0
                for i, r in user_portfolio.iterrows():
                    total_value = total_value + r['total_value']

                composition_ratio = {}
                for i, r in user_portfolio.iterrows():
                    composition_ratio[r['stock_cd']] = round(r['total_value'] / total_value, 4)
                    composition_sum = composition_sum + composition_ratio[r['stock_cd']]

                if composition_sum < 1:
                    composition_ratio[user_portfolio.iloc[0, 0]] = composition_ratio[user_portfolio.iloc[0, 0]] + \
                                                                    (1 - composition_sum)

                composition_ratio_list = []
                # composition_ratio_list 설정
                for i, r in user_portfolio.iterrows():
                    if i < 15:  # 15개 이하일 경우
                        composition_ratio_list.append({'stock_cd': r['stock_cd'],
                                                       'stock_nm': r['stock_nm'],
                                                       'composition_ratio': composition_ratio[r['stock_cd']]})
                    else:  # 15개 이상일 경우
                        composition_ratio_list[14] = {'stock_cd': '999999',
                                                      'stock_nm': 'ETC',
                                                      'composition_ratio': composition_ratio_list[14]['composition_ratio'] +
                                                                           composition_ratio[r['stock_cd']]}

                return {'status': '000', 'msg': get_status_msg('000'),
                        'composition_ratio_list': composition_ratio_list}


class GetPortfolioSectorCompositionRatio(Resource):  # 포트폴리오 섹터 구성비율 조회
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('portfolio_code', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        portfolio_code = args['portfolio_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'composition_ratio_list': []}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'composition_ratio_list': []}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        kr_latest_date = exec_query('select max(working_day) from date_working_day')[0][0]
        us_latest_date = exec_query('select max(working_day) from us_date_working_day')[0][0]
        exchange_rate = exec_query('select usd_krw from exchange_rate order by date DESC limit 1')[0][0]

        sql = f'select sector, ' \
              f'        cast(sum(total_value) as signed) as total_value ' \
              f'	from (  SELECT a.*, c.portfolio_code, c.portfolio_nm, e.market, e.sector ' \
              f'        	FROM sauvignon.user_portfolio AS a, ' \
              f'			 sauvignon.user_portfolio_map AS b, ' \
              f'			 sauvignon.user_portfolio_info as c, ' \
              f'			 sauvignon.date_working_day_mapping as d, ' \
              f'			 sauvignon.stock_market_sector as e ' \
              f'		    WHERE a.user_code = b.user_code ' \
              f'		    AND a.account_number = b.account_number ' \
              f'	    	AND a.stock_cd = b.stock_cd ' \
              f'	    	AND a.user_code = c.user_code ' \
              f'	    	AND b.portfolio_code = c.portfolio_code ' \
              f'	    	and a.date = d.date ' \
              f'	    	and a.stock_cd = e.stock_cd ' \
              f'	    	and d.working_day = e.date' \
              f'            and a.user_code = {user_code} ' \
              f'            and c.portfolio_code = {portfolio_code} ' \
              f'            and a.date = {kr_latest_date}) as a ' \
              f'	group by user_code, portfolio_code, date, sector ' \
              f'    having cast(sum(total_value) as signed) > 0 ' \
              f'    order by cast(sum(total_value) as signed) desc '
        kr_user_portfolio = get_df_from_db(sql)

        sql = f'select industry as sector, ' \
              f'        cast((sum(total_value) * {int(exchange_rate)}) as signed) as total_value ' \
              f'	from (  SELECT a.*, c.portfolio_code, c.portfolio_nm, e.exchange, e.industry ' \
              f'        	FROM sauvignon.user_portfolio AS a, ' \
              f'			 sauvignon.user_portfolio_map AS b, ' \
              f'			 sauvignon.user_portfolio_info as c, ' \
              f'			 sauvignon.date_working_day_mapping as d, ' \
              f'			 sauvignon.us_stock_info as e ' \
              f'		    WHERE a.user_code = b.user_code ' \
              f'		    AND a.account_number = b.account_number ' \
              f'	    	AND a.stock_cd = b.stock_cd ' \
              f'	    	AND a.user_code = c.user_code ' \
              f'	    	AND b.portfolio_code = c.portfolio_code ' \
              f'	    	and a.date = d.date ' \
              f'	    	and a.stock_cd = e.stock_cd ' \
              f'            and a.user_code = {user_code} ' \
              f'            and c.portfolio_code = {portfolio_code} ' \
              f'            and a.date = {us_latest_date}) as a ' \
              f'	group by user_code, portfolio_code, date, industry ' \
              f'    having cast(sum(total_value) as signed) > 0 ' \
              f'    order by cast(sum(total_value) as signed) desc '
        us_user_portfolio = get_df_from_db(sql)

        user_portfolio = pd.concat([kr_user_portfolio, us_user_portfolio])
        user_portfolio = user_portfolio.sort_values('total_value', ascending=False)

        if user_portfolio.shape[0] == 0:  # 포트폴리오 존재하지 않음
            return {'status': '205', 'msg': get_status_msg('205'),  # 기간 내 보유 종목 없음
                    'composition_ratio_list': []}
        else:  # 정상
            total_value = 0
            composition_sum = 0
            comp_ratio_dict = {}
            for i, r in user_portfolio.iterrows():
                total_value = total_value + int(r['total_value'])

            for i, r in user_portfolio.iterrows():
                comp_ratio_dict[r['sector']] = round(int(r['total_value'])/total_value, 4)
                composition_sum = composition_sum + comp_ratio_dict[r['sector']]

            if composition_sum < 1:
                comp_ratio_dict[list(comp_ratio_dict.keys())[0]] = \
                    round(comp_ratio_dict[list(comp_ratio_dict.keys())[0]] + (1 - composition_sum), 4)

            # unknown_sector_ratio 정의
            unknown_sector_ratio = 0
            unknown_sector_portfolio = user_portfolio.loc[user_portfolio['sector'].apply(lambda x: x is None), :]
            if unknown_sector_portfolio.shape[0] != 0:
                for i, r in unknown_sector_portfolio.iterrows():
                    unknown_sector_ratio = unknown_sector_ratio + comp_ratio_dict[r['sector']]
            # user_portfolio에 섹터 있는 애들만 남김
            user_portfolio = user_portfolio.loc[user_portfolio['sector'].apply(lambda x: x is not None), :]

            # composition_ratio_list 설정
            composition_ratio_list = []
            user_portfolio.reset_index(drop=True, inplace=True)
            for i, r in user_portfolio.iterrows():
                if i < 15:  # 15개 이하일 경우
                    composition_ratio_list.append({'sector': r['sector'],
                                                   'composition_ratio': comp_ratio_dict[r['sector']]})
                else:  # 15개 이상일 경우
                    composition_ratio_list[14] = {'sector': 'ETC',
                                                  'composition_ratio': composition_ratio_list[14]['composition_ratio'] +
                                                                       comp_ratio_dict[r['sector']]}

            if unknown_sector_ratio > 0:
                if len(composition_ratio_list) == 15:
                    composition_ratio_list[14] = {'sector': 'ETC',
                                                  'composition_ratio': composition_ratio_list[14]['composition_ratio'] +
                                                                       unknown_sector_ratio}
                else:
                    composition_ratio_list.append({'sector': 'ETC',
                                                   'composition_ratio': unknown_sector_ratio})

            return {'status': '000', 'msg': get_status_msg('000'),
                    'composition_ratio_list': composition_ratio_list}


class SetUserDefaultPortfolio(Resource):  # 디폴트 포트폴리오 등록
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('securities_code', type=str)
        parser.add_argument('infotech_code', type=str)
        parser.add_argument('portfolio_code', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        securities_code = args['securities_code']
        infotech_code = args['infotech_code']
        portfolio_code = args['portfolio_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        # print(check_status)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        with session_scope() as session:
            # infotech_code 체크하기
            _infotech_securities_code = session.query(SecuritiesCode). \
                filter(SecuritiesCode.infotech_code == infotech_code). \
                first()
            session.commit()

            # securities_code 체크하기
            _securities_code = session.query(SecuritiesCode). \
                filter(SecuritiesCode.securities_code == securities_code). \
                first()
            session.commit()

            # infotech_code, securities_code 유효성 검사
            if _infotech_securities_code is None and _securities_code is None:
                return {'status': '309', 'msg': get_status_msg('309')}  # 에러코드: 올바르지 않은 증권사 코드
            elif _infotech_securities_code is None and _securities_code is not None:
                securities_code = _securities_code.securities_code
            elif _infotech_securities_code is not None and _securities_code is None:
                securities_code = _infotech_securities_code.securities_code
            elif _infotech_securities_code is not None and _securities_code is not None:
                if _infotech_securities_code != _securities_code:
                    return {'status': '309', 'msg': get_status_msg('309')}  # 에러코드: 올바르지 않은 증권사 코드
                else:
                    securities_code = _securities_code.securities_code

            # 기존 포트폴리오 존재여부 확인
            dp = session.query(UserDefaultPortfolio).\
                filter(UserDefaultPortfolio.user_code == user_code,
                       UserDefaultPortfolio.securities_code == securities_code).\
                first()
            if dp is not None:
                dp.portfolio_code = portfolio_code
                dp.lst_update_dtim = datetime.now().strftime('%Y%m%d%H%M%S')
                session.commit()
            else:
                default_portfolio = UserDefaultPortfolio(user_code=user_code,
                                                         securities_code=securities_code,
                                                         portfolio_code=portfolio_code,
                                                         lst_update_dtim=datetime.now().strftime('%Y%m%d%H%M%S'))
                session.add(default_portfolio)
                session.commit()

        return {'status': '000', 'msg': get_status_msg('000')}
