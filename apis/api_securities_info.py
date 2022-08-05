from flask_restful import Resource
from flask_restful import reqparse
from securities.securities_scrap import securities_update_log
from db.db_model import UserSecuritiesInfo, UserTradingLog, UserPortfolioInfo, SecuritiesCode, UserInfo, UserSimpleTradingLog, \
    StockInfo, UserPortfolioMap, UserPortfolio, UserTradingLogRaw, session_scope, DateWorkingDay, UsStockInfo, UsDateWorkingDay, \
    UsDateWorkingDayMapping, TmpUserTradingLog, TmpUserPortfolio, TmpUserPortfolioMap, AppScrapLog, \
    UserDefaultPortfolio, PushNotiAcctStatus
from util.kafka_update_portfolio import update_user_portfolio_by_account
from util.util_update_portfolio import update_single_stock_portfolio, \
    util_get_avg_purchase_price, util_get_recent_split_release_quantity, update_user_portfolio, util_get_pulled_transaction_date
from util.util_get_portfolio_yield import calc_portfolio_yield
from util.util_stock_cd_converter import get_stock_cd_from_trfr_transaction
from util.util_get_country import get_country
from module.tradinglog import UserTradingLogModule
from util.util_get_status_msg import get_status_msg
from db.db_connect import exec_query, insert_data, get_df_from_db, get_api_url
from apis.api_portfolio import get_oneday_portfolio_yield
from datetime import datetime, timedelta
from apis.api_user_info import check_user_validity
import json
import pandas as pd
from flask import request
from sqlalchemy import and_, not_, or_
from flask import current_app as app
from module.merlot_logging import *

# 증권사 목록 조회 API
class GetAvailableSecuritiesList(Resource):
    def post(self):
        with session_scope() as session:
            securities_code = session.query(SecuritiesCode).\
                filter(SecuritiesCode.end_date == '99991231',
                       SecuritiesCode.available_flag == 1).\
                order_by(SecuritiesCode.securities_nm).\
                all()
            session.commit()

            return {'status': '000', 'msg': get_status_msg('000'),
                    'securities_list': [
                        {'securities_code': e.securities_code,
                         'securities_nm': e.securities_nm,
                         'available_flag': e.available_flag,
                         'login_type': e.login_type,
                         'infotech_code': e.infotech_code,
                         'ico_url': f'https://{get_api_url()}/static/image/ico_{e.securities_code}.png',
                         'banner_url': f'https://{get_api_url()}/static/image/{e.securities_code}.png'}
                        for e in securities_code]}


# 증권사 ID/PW 세팅 API
class SetUserSecuritiesInfo(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('securities_code', type=str)
        parser.add_argument('securities_id', type=str)
        parser.add_argument('securities_pw', type=str)
        parser.add_argument('secret_key', type=str)
        parser.add_argument('api_key', type=str)

        args = parser.parse_args()

        user_code = args['user_code']
        securities_code = args['securities_code']
        securities_id = args['securities_id']
        securities_pw = args['securities_pw']
        secret_key = args['secret_key']
        api_key = args['api_key']

        # print(secret_key)

        check_status = check_user_validity(user_code, api_key)
        if check_status != '000':
            return {'status': check_status, 'msg': get_status_msg(check_status)}

        if len(secret_key.encode('utf-8')) != 32:
            return {'status': '301', 'msg': get_status_msg('301')}
        # random 문자열 생성 코드
        # "".join([random.choice(string.ascii_letters+'1234567890') for _ in range(32)])

        # id, pw 암호화 <- 클라이언트에서 암호화 되어 전달받음.
        # encryption_module = AESCipher(secret_key)
        # securities_id = encryption_module.encrypt_str(securities_id)
        # securities_pw = encryption_module.encrypt_str(securities_pw)

        # 로그인 정보 생성
        user_securities_info = UserSecuritiesInfo(user_code=user_code, securities_code=securities_code,
                                                  securities_id=securities_id, securities_pw=securities_pw,
                                                  valid_flag=-99)

        with session_scope() as session:
            securities_info_query = session.query(UserSecuritiesInfo).\
                filter(UserSecuritiesInfo.user_code == user_code,
                       UserSecuritiesInfo.securities_code == securities_code)
            securities_info = securities_info_query.all()
            session.commit()

            if len(securities_info) == 0:
                session.add(user_securities_info)  # 미존재시 insert
                session.commit()
            else:
                securities_info_query.delete()
                session.add(user_securities_info)  # 존재 시 삭제 후 삽입
                session.commit()

            # min(포트폴리오 코드) 값 가져오기 <- 거래내역이 업데이트 될 포트폴리오
            user_portfolio_info = session.query(UserPortfolioInfo). \
                filter(UserPortfolioInfo.user_code == user_code).\
                order_by(UserPortfolioInfo.portfolio_order).\
                first()
            session.commit()

            # 스크래핑 모듈 호출
            end_date = datetime.today().strftime("%Y%m%d")
            begin_date = (datetime.today() - timedelta(days=365*10)).strftime("%Y%m%d")
            securities_update_log.apply_async((user_code, securities_code, secret_key, begin_date, end_date,
                                               user_portfolio_info.portfolio_code),
                                              queue='securities')

            return {'status': '000', 'msg': get_status_msg('000')}


# 증권사 ID/PW 삭제 API
class DeleteUserSecuritiesInfo(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('securities_code', type=str)
        parser.add_argument('data_delete_flag', type=int)
        parser.add_argument('api_key', type=str)

        args = parser.parse_args()

        user_code = args['user_code']
        securities_code = args['securities_code']
        data_delete_flag = args['data_delete_flag']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status != '000':
            return {'status': check_status, 'msg': get_status_msg(check_status)}

        with session_scope() as session:
            securities_info_query = session.query(UserSecuritiesInfo).\
                filter(UserSecuritiesInfo.user_code == user_code,
                       UserSecuritiesInfo.securities_code == securities_code)
            securities_info = securities_info_query.all()
            session.commit()

            if len(securities_info) == 0:
                return {'status': '303', 'msg': get_status_msg('303')}  # 등록된 ID/PW 없음
            else:
                securities_info_query.delete()  # 존재 시 삭제
                session.commit()

            if data_delete_flag == 1:
                # 데이터 삭제
                # user_portfolio
                session.query(UserPortfolio).\
                    filter(UserPortfolio.user_code == user_code,
                           UserPortfolio.securities_code == securities_code).\
                    delete()
                session.commit()
                # user_portfolio_map
                session.query(UserPortfolioMap).\
                    filter(UserPortfolioMap.user_code == user_code,
                           UserPortfolioMap.securities_code == securities_code).\
                    delete()
                session.commit()
                # usr_trading_log
                session.query(UserTradingLog).\
                    filter(UserTradingLog.user_code == user_code,
                           UserTradingLog.securities_code == securities_code).\
                    delete()
                session.commit()

            return {'status': '000', 'msg': get_status_msg('000')}


# 스크래핑 상태 조회 API
class GetUserSecuritiesInfoValidFlag(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('api_key', type=str)

        args = parser.parse_args()

        user_code = args['user_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'valid_flag': '', 'message': '일치하는 회원이 없습니다.', 'lst_update_dtim': ''}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'valid_flag': '', 'message': '', 'lst_update_dtim': ''}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        with session_scope() as session:
            # user 확인
            checked_user = session.query(UserInfo).filter(UserInfo.user_code == user_code,
                                                             UserInfo.end_dtim == '99991231235959').all()
            session.commit()

            if len(checked_user) == 0:  # 일치하는 회원이 없는 경우
                return {'status': '103', 'msg': get_status_msg('103'), 'valid_flag': '', 'message': '일치하는 회원이 없습니다.', 'lst_update_dtim': ''}

            # 계정 정보 쿼리
            securities_info = session.query(UserSecuritiesInfo). \
                filter(UserSecuritiesInfo.user_code == user_code).all()
            session.commit()

            if len(securities_info) == 0:
                return {'status': '303', 'msg': get_status_msg('303'), 'valid_flag': '', 'message': '등록된 증권사 계정이 존재하지 않습니다.', 'lst_update_dtim': ''}
            else:
                return {'status': '000', 'msg': get_status_msg('000'),
                        'valid_flag_list': [{'securities_code': e.securities_code,
                                             'valid_flag': e.valid_flag,
                                             'message': e.message,
                                             'lst_update_dtim': e.lst_update_dtim} for e in securities_info]}


# 거래내역 스크래핑 API
class UpdateTradingLog(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('secret_key', type=str)
        parser.add_argument('api_key', type=str)

        args = parser.parse_args()

        user_code = args['user_code']
        secret_key = args['secret_key']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        with session_scope() as session:
            # min(포트폴리오 코드) 값 가져오기
            user_portfolio_info = session.query(UserPortfolioInfo). \
                filter(UserPortfolioInfo.user_code == user_code).\
                order_by(UserPortfolioInfo.portfolio_order).\
                first()
            session.commit()

            # 업데이트 할 증권사 코드 가져오기
            user_securities_info = session.query(UserSecuritiesInfo).\
                filter(UserSecuritiesInfo.user_code == user_code).\
                all()
            session.commit()

            if len(user_securities_info) == 0:  # 업데이트할 증권사 id 없는 경우
                return {'status': '303', 'msg': get_status_msg('303')}  # 등록된 증권사 계정 없음

            for s in user_securities_info:
                # 시작일자 확인
                user_trading_log = session.query(UserTradingLog).\
                    filter(UserTradingLog.user_code == user_code,
                           UserTradingLog.securities_code == s.securities_code).\
                    order_by(UserTradingLog.date.desc()).\
                    first()
                session.commit()

                # 시작일자 세팅
                if user_trading_log is None:
                    begin_date = (datetime.today() - timedelta(days=3650)).strftime('%Y%m%d')
                else:
                    begin_date = user_trading_log.update_dtim[:8]
                # trading log update 모듈 호출
                securities_update_log.apply_async((user_code, s.securities_code, secret_key,
                                                   begin_date,
                                                   datetime.today().strftime('%Y%m%d'),
                                                   user_portfolio_info.portfolio_code),
                                                  queue='securities')

            return {'status': '000', 'msg': get_status_msg('000')}


# 스크래핑 상태 초기화 API
class ResetUserSecuritiesInfoValidFlag(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('securities_code', type=str)
        parser.add_argument('api_key', type=str)

        args = parser.parse_args()

        user_code = args['user_code']
        securities_code = args['securities_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        with session_scope() as session:
            securities_info = session.query(UserSecuritiesInfo).\
                filter(UserSecuritiesInfo.user_code == user_code,
                       UserSecuritiesInfo.securities_code == securities_code).all()

            if len(securities_info) == 0:  # 일치하는 계정 없을 경우
                return {'status': '303', 'msg': get_status_msg('303')}
            else:  # 일치하는 계정 있을 경우
                securities_info[0].valid_flag = -99
                session.commit()
                return {'status': '000', 'msg': get_status_msg('000')}


# 거래내역 조회 API
class GetTradingLog(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('portfolio_code', type=int)
        parser.add_argument('period_code', type=str)
        parser.add_argument('begin_date', type=str)
        parser.add_argument('end_date', type=str)
        parser.add_argument('transaction_type', type=str)
        parser.add_argument('sort_order', type=str)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()
        # print(args)

        user_code = args['user_code']
        portfolio_code = args['portfolio_code']
        period_code = args['period_code']
        begin_date = args['begin_date']
        end_date = args['end_date']
        transaction_type = args['transaction_type']
        sort_order = args['sort_order']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        # period code가 CUSTOM인 경우, None으로 바꿔준다
        if period_code == 'CUSTOM':
            period_code = None

        # begin_date, end_date가 존재하지 않을 경우, begin_date, end_date를 정의해준다.
        if period_code is not None:
            # period code 변환
            if period_code.endswith('D'):
                period = int(period_code[:-1])
            elif period_code.endswith('M'):
                period = int(period_code[:-1]) * 30.5
            elif period_code.endswith('Y'):
                period = int(period_code[:-1]) * 365
            else:
                return {'status': '206', 'msg': get_status_msg('206'),
                        'trading_log': [],
                        'return_ratio': [{'stock_cd': '', 'account_number': '', 'return_ratio': 0}],
                        'total_return_ratio': 0,
                        'realized_profit_loss': 0,
                        'total_buy': 0,
                        'total_sell': 0}

            # end_date 정의 <- 가장 최근 포트폴리오가 존재하는 일자
            end_date = exec_query(f'select max(date) from user_portfolio where user_code = {user_code}')[0][0]

            # 포트폴리오가 존재하지 않는 경우, 기간 내 보유 종목 없음 응답
            if end_date is None:
                return {'status': '205', 'msg': get_status_msg('205'),  # 기간 내 보유 종목 없음
                        'trading_log': [],
                        'return_ratio': [{'stock_cd': '', 'account_number': '', 'return_ratio': 0}],
                        'total_return_ratio': 0,
                        'realized_profit_loss': 0,
                        'total_buy': 0,
                        'total_sell': 0}
            end_date = str(end_date)

            # begin_date 정의
            # 수익률을 보고싶은 기간의 첫 영업일보다 1영업일 이전을 begin_date로 한다.
            # 왜냐하면 첫 일자의 수익률은 무조건 0으로 표시되기 때문. 5영업일 전부터 수익률을 보고싶다면, 6영업일 전부터 조회를 해야한다.
            # 일별 수익률의 경우 영업일 처리를 해 준다
            if period_code.endswith('D'):
                begin_date = exec_query(f'select working_day '
                                        f'from date_working_day '
                                        f'where seq = (select seq - {period} '
                                        f'             from date_working_day '
                                        f'             where working_day = "{end_date}")')[0][0]
            else:
                begin_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=period)).strftime('%Y%m%d')
                begin_date = exec_query(f'select b.working_day '
                                        f'from date_working_day_mapping as a, date_working_day as b '
                                        f'where a.working_day = b.working_day '
                                        f'and a.date = {begin_date} ')[0][0]

            begin_date = str(begin_date)
        else:
            # period 변수 초기화
            period = None

        # 총 수익률 계산하기
        if period_code == '1D':
            portfolio_yield = get_oneday_portfolio_yield(user_code=user_code, portfolio_code=portfolio_code)
        else:
            portfolio_yield = calc_portfolio_yield(user_code=user_code, begin_date=begin_date, end_date=end_date,
                                                   portfolio_code=portfolio_code, period_code=period_code, period=period)
        total_return_ratio = portfolio_yield['total_return']
        realized_profit_loss = int(portfolio_yield['realized_profit_loss'])

        # print(f'user_code: {user_code}')
        # print(f'portfolio_code: {portfolio_code}')
        # print(f'begin_date: {begin_date}')
        # print(f'end_date: {end_date}')
        # 거래내역 불러오기
        with session_scope() as session:
            # user_simple_trading_log subquery
            user_simple_trading_log_subquery = session.query(UserSimpleTradingLog).\
                filter(UserSimpleTradingLog.user_code == user_code,
                       UserSimpleTradingLog.portfolio_code == portfolio_code,
                       UserSimpleTradingLog.date >= begin_date,
                       UserSimpleTradingLog.date <= end_date).\
                subquery()

            # query join
            trading_log_query = session.query(user_simple_trading_log_subquery,
                                              UserPortfolio.avg_purchase_price,
                                              UserPortfolio.purchase_amount_of_stocks_to_sell,
                                              UserPortfolio.country).\
                join(UserPortfolio,
                     and_(user_simple_trading_log_subquery.c.user_code == UserPortfolio.user_code,
                          user_simple_trading_log_subquery.c.account_number == UserPortfolio.account_number,
                          user_simple_trading_log_subquery.c.stock_cd == UserPortfolio.stock_cd,
                          user_simple_trading_log_subquery.c.date == UserPortfolio.date))

            # transaction_type 처리
            if transaction_type == 'all':
                pass
            elif transaction_type == 'buy':
                trading_log_query = trading_log_query.filter(user_simple_trading_log_subquery.c.transaction_type == '매수')
            elif transaction_type == 'sell':
                trading_log_query = trading_log_query.filter(user_simple_trading_log_subquery.c.transaction_type == '매도')
            else:
                return {'status': '108', 'msg': get_status_msg('108')}

            # sort_order 처리
            if sort_order == 'asc':
                trading_log_query = trading_log_query.\
                    order_by(user_simple_trading_log_subquery.c.date.asc(), user_simple_trading_log_subquery.c.seq.asc())
            elif sort_order == 'desc':
                trading_log_query = trading_log_query. \
                    order_by(user_simple_trading_log_subquery.c.date.desc(), user_simple_trading_log_subquery.c.seq.desc())
            else:
                return {'status': '109', 'msg': get_status_msg('109')}

            # trading_log = trading_log_query.all()
            trading_log = pd.read_sql(trading_log_query.statement, session.bind)
            session.commit()
            # print(trading_log)
            # print(trading_log.loc[trading_log['securities_nm'] != '미래에셋대우', :])

            # avg_purchase_price 업데이트하기
            tmp_avg_purchase_price = trading_log.loc[trading_log['transaction_type'].isin(['매도', '타사대체출고', '대체출고', '액면분할병합출고', '감자출고', '해외주식매도']), :].\
                                         groupby(['user_code', 'date', 'securities_code', 'account_number', 'stock_cd']).\
                                         apply(lambda x: x['purchase_amount_of_stocks_to_sell'] / sum(x['transaction_quantity'])).reset_index()

            if tmp_avg_purchase_price.shape[0] != 0:  # tmp_avg_purchase_price가 생성된 경우에만 avg_purchase_price udpate
                if len(tmp_avg_purchase_price.columns) == 7:
                    tmp_avg_purchase_price = tmp_avg_purchase_price.iloc[:, [0, 1, 2, 3, 4, 6]]

                tmp_avg_purchase_price.columns = ['user_code', 'date', 'securities_code', 'account_number', 'stock_cd', 'purchase_amount_of_stocks_to_sell']
                tmp_avg_purchase_price = tmp_avg_purchase_price.drop_duplicates()

                # print(tmp_avg_purchase_price)
                trading_log = trading_log.merge(tmp_avg_purchase_price, on=['user_code', 'date', 'securities_code', 'account_number', 'stock_cd'], how='left')
                # print(trading_log)
                trading_log['avg_purchase_price'] = trading_log.apply(lambda x: x['avg_purchase_price'] if pd.isna(x['purchase_amount_of_stocks_to_sell_y']) else x['purchase_amount_of_stocks_to_sell_y'], axis=1)
                trading_log = trading_log.drop(['purchase_amount_of_stocks_to_sell_x', 'purchase_amount_of_stocks_to_sell_y'], axis=1)

            # transaction_type:transaction_ui_type mapping dict 생성
            ui_type_dict = {'매수': '매수', '유상주입고': '매수', '무상주입고': '매수', '공모주입고': '매수', '타사대체입고': '매수',
                            '대체입고': '매수', '주식합병입고': '매수', '액면분할병합입고': '매수', '해외주식매수': '매수',
                            '매도': '매도', '타사대체출고': '매도', '대체출고': '매도', '주식합병출고': '매도', '액면분할병합출고': '매도',
                            '해외주식매도': '매도',
                            '배당': '배당', '배당세출금': '배당', '해외주식배당': '배당'}

            # ui_type_dict에 정의된 trading_log만 표시
            trading_log = trading_log.loc[trading_log['transaction_type'].isin(ui_type_dict.keys()), :]

            # transaction_ui_type 추가
            trading_log['transaction_ui_type'] = trading_log['transaction_type'].apply(lambda x: ui_type_dict[x])

            # unit_currency 추가
            trading_log['unit_currency'] = trading_log['country'].apply(lambda x: 'KRW' if x == 'KR' else 'USD')
            # print(trading_log)

            # 종목별 수익률 계산 쿼리
            sql = f'select a.stock_cd, a.account_number, ' \
                  f'            (realized_profit_loss + ' \
                  f'             (case when total_value is null then 0 else total_value end) - ' \
                  f'             new_purchase_amount) / ' \
                  f'            (new_purchase_amount) as return_ratio, ' \
                  f'            (realized_profit_loss + ' \
                  f'             case when total_value is null then 0 else total_value end) as total_value, ' \
                  f'            new_purchase_amount ' \
                  f'from ( ' \
                  f' 		select stock_cd, account_number, ' \
                  f' 				sum(realized_profit_loss) as realized_profit_loss,' \
                  f' 				sum(new_purchase_amount) as new_purchase_amount' \
                  f' 		from user_portfolio' \
                  f' 		where user_code = {user_code}' \
                  f' 		group by stock_cd, account_number) as a ' \
                  f'left join( ' \
                  f' 		select stock_cd, account_number, total_value' \
                  f' 		from user_portfolio' \
                  f' 		where user_code = {user_code}' \
                  f' 		and ((date = (select max(date) ' \
                  f'                      from user_portfolio ' \
                  f'                      where user_code = {user_code} ' \
                  f'                      and country = "KR") ' \
                  f'              and country = "KR") or ' \
                  f'             (date = (select max(date) ' \
                  f'                      from user_portfolio ' \
                  f'                      where user_code = {user_code} ' \
                  f'                      and country = "US") ' \
                  f'              and country = "US"))) as b' \
                  f' 	on a.stock_cd = b.stock_cd' \
                  f'    and a.account_number = b.account_number'
            return_ratio = get_df_from_db(sql)
            # print(return_ratio)
            return_ratio = return_ratio.where(pd.notna(return_ratio), None)  # na가 아닌 부분을 보여주고, na인 부분은 None으로 보여줌.
            # <- 이 부분 임시로 NaN을 처리해 놓았지만, 수익률 계산하는 로직을 바꿔서 다시 계산해야한다. 감자액면병합입고일 경우!

            # return_ratio에서 trading_log가 존재하는 애들만 남긴다.
            return_ratio = return_ratio.merge(trading_log.loc[:, ['account_number', 'stock_cd']].drop_duplicates())

            # 총 매수, 총 매도 금액 계산
            # 환율 불러오기
            sql = f'select * from exchange_rate where date = (select max(date) from exchange_rate)'  # 환율은 한국날짜 기준
            exchange_rate = get_df_from_db(sql)['usd_krw'].values[0]
            # 미국주식의 경우 환율 곱해준다
            if trading_log.shape[0] != 0:
                tmp_trading_log = trading_log.copy()
                tmp_trading_log['total_transaction_value'] = tmp_trading_log.apply(lambda x: x['transaction_unit_price'] * x['transaction_quantity'] if x['unit_currency'] == 'KRW' else x['transaction_unit_price'] * x['transaction_quantity'] * exchange_rate, axis=1)

                total_buy = tmp_trading_log.loc[tmp_trading_log['transaction_ui_type'] == '매수', 'total_transaction_value'].sum()
                total_sell = tmp_trading_log.loc[tmp_trading_log['transaction_ui_type'] == '매도', 'total_transaction_value'].sum()
            else:
                total_buy = 0
                total_sell = 0

            return_result = {'status': '000', 'msg': get_status_msg('000'),
                             'trading_log': json.loads(trading_log.to_json(orient="records")),
                             'return_ratio': [{'stock_cd': r[0],
                                               'account_number': r[1],
                                               'return_ratio': r[2]} for i, r in return_ratio.iterrows()],
                             'total_return_ratio': total_return_ratio,
                             'realized_profit_loss': realized_profit_loss,
                             'total_buy': total_buy,
                             'total_sell': total_sell}
            # print(return_result)

            return return_result


# 거래내역 INSERT 함수
def insert_self_trading_log(user_code, portfolio_code, stock_cd, transaction_type, transaction_date, transaction_unit_price,
                            transaction_quantity, transaction_fee_tax, country):
    with session_scope() as session:
        # user 확인
        checked_user = session.query(UserInfo).filter(UserInfo.user_code == user_code,
                                                         UserInfo.end_dtim == '99991231235959').all()
        session.commit()

        if len(checked_user) == 0:  # 일치하는 회원이 없는 경우
            return {'status': '103', 'msg': get_status_msg('103')}  # 올바르지 않은 ID or PW

        # portfolio 확인
        portfolio_info = session.query(UserPortfolioInfo). \
            filter(UserPortfolioInfo.user_code == user_code,
                   UserPortfolioInfo.portfolio_code == portfolio_code).all()
        session.commit()
        if len(portfolio_info) == 0:
            return {'status': '201', 'msg': get_status_msg('201')}  # 해당 포트폴리오 없음

        # seq 불러오기 <- 직접입력된 건에 대해서 같은 날짜에 거래된 내역에 seq를 붙인다.
        trading_log_order_by_seq = session.query(UserTradingLog). \
            filter(UserTradingLog.user_code == user_code,
                   UserTradingLog.securities_code == 'SELF',
                   UserTradingLog.date == transaction_date). \
            order_by(UserTradingLog.seq.desc()).all()
        session.commit()

        if len(trading_log_order_by_seq) == 0:
            seq = 1
        else:
            seq = trading_log_order_by_seq[0].seq + 1

    with session_scope() as session:
        # stock_type 체크 / unit_currency 세팅
        if country == 'KR':
            # 한국주식
            stock_info = session.query(StockInfo). \
                filter(or_(StockInfo.stock_cd == f'A{stock_cd}',
                           StockInfo.stock_cd == f'Q{stock_cd}'),
                       StockInfo.end_date >= transaction_date).all()
            if len(stock_info) == 0:
                return {'status': '305', 'msg': get_status_msg('305')}  # 올바르지 않은 종목코드
            else:
                tmp_market = stock_info[0].market
                stock_nm = stock_info[0].stock_nm
                if tmp_market in ('KOSPI', 'KOSDAQ'):
                    stock_type = 'domestic_stock'
                    unit_currency = 'KRW'
                elif tmp_market == 'ETF':
                    stock_type = 'domestic_etf'
                    unit_currency = 'KRW'
                elif tmp_market == 'ETN':
                    stock_type = 'domestic_etn'
                    unit_currency = 'KRW'
        else:
            # 미국주식

            # transaction_date는 미국 시간 기준 일자이다. us_stock_info에 적재된 일자도 미국 시간 기준 일자이다. 따라서 비교가 가능하다.
            stock_info = session.query(UsStockInfo). \
                filter(UsStockInfo.stock_cd == stock_cd,
                       UsStockInfo.latest_date >= transaction_date). \
                all()
            if len(stock_info) == 0:
                return {'status': '305', 'msg': get_status_msg('305')}  # 올바르지 않은 종목코드
            else:
                stock_nm = stock_info[0].stock_nm
                stock_type = 'us_stock'
                unit_currency = 'USD'
                country = 'US'

        # 기존 포트폴리오 불러오기
        last_portfolio = session.query(UserPortfolio).\
            filter(UserPortfolio.user_code == user_code,
                   UserPortfolio.stock_cd == stock_cd,
                   UserPortfolio.account_number == f'SELF_{portfolio_code}').\
            order_by(UserPortfolio.date.desc()).\
            first()
        session.commit()

        # 매도 case
        if transaction_type in ['매도', '해외주식매도']:
            if last_portfolio is None or last_portfolio.holding_quantity < transaction_quantity:
                session.rollback()
                return {'status': '304', 'msg': get_status_msg('304')}  # 매도 가능수량 없음
            else:
                pass
        elif transaction_type in ['매수', '해외주식매수']:
            pass
        else:
            session.rollback()
            return {'status': '306', 'msg': get_status_msg('306')}  # 올바르지 않은 거래 종류

        # 기존 거래내역 없는 경우
        if last_portfolio is None:
            # portfolio map 설정
            # 기존 portfolio map 확인
            existing_map = session.query(UserPortfolioMap).\
                filter(UserPortfolioMap.user_code == user_code,
                       UserPortfolioMap.stock_cd == stock_cd,
                       UserPortfolioMap.account_number == f'SELF_{portfolio_code}'). \
                first()
            session.commit()

            # 기존 거래내역 없음에도 기존 portfolio map 있는 경우 -> portfolio map 삭제
            if existing_map is not None:
                session.query(UserPortfolioMap). \
                    filter(UserPortfolioMap.user_code == user_code,
                           UserPortfolioMap.stock_cd == stock_cd,
                           UserPortfolioMap.account_number == f'SELF_{portfolio_code}'). \
                    delete(synchronize_session='fetch')
                session.commit()

            portfolio_map = UserPortfolioMap(user_code=user_code,
                                             account_number=f'SELF_{portfolio_code}',
                                             stock_cd=stock_cd,
                                             portfolio_code=portfolio_code,
                                             securities_code='SELF',
                                             lst_update_dtim=datetime.today().strftime("%Y%m%d%H%M%S"))
            session.add(portfolio_map)  # portfolio map insert(commit은 아래에서 한번에)

        new_user_trading_log = UserTradingLog(user_code=user_code,
                                              account_number=f'SELF_{portfolio_code}',
                                              date=transaction_date,
                                              seq=seq,
                                              stock_cd=stock_cd,
                                              stock_type=stock_type,
                                              securities_code='SELF',
                                              stock_nm=stock_nm,
                                              transaction_type=transaction_type,
                                              transaction_detail_type=None,
                                              transaction_quantity=transaction_quantity,
                                              transaction_unit_price=transaction_unit_price,
                                              transaction_fee=0,
                                              transaction_tax=transaction_fee_tax,
                                              unit_currency=unit_currency,
                                              update_dtim=datetime.today().strftime("%Y%m%d%H%M%S"),
                                              country=country)
        session.add(new_user_trading_log)  # 신규 거래내역 insert
        session.commit()

        return {'status': '000', 'msg': get_status_msg('000')}


# 거래내역 직접 입력 API
class InsertTradingLog(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('portfolio_code', type=int)
        parser.add_argument('stock_cd', type=str)
        parser.add_argument('transaction_date', type=str)
        parser.add_argument('transaction_type', type=str)
        parser.add_argument('transaction_unit_price', type=float)
        parser.add_argument('transaction_quantity', type=int)
        parser.add_argument('transaction_fee_tax', type=float)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        portfolio_code = args['portfolio_code']
        stock_cd = args['stock_cd']
        parser.add_argument('transaction_date', type=str)
        transaction_type = args['transaction_type']
        transaction_date = args['transaction_date']
        transaction_unit_price = args['transaction_unit_price']
        transaction_quantity = args['transaction_quantity']
        transaction_fee_tax = args['transaction_fee_tax']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        # country 체크
        country = get_country(stock_cd)

        if country == 'foreign':
            return {'status': '305', 'msg': get_status_msg('305')}  # 올바르지 않은 종목코드

        # 영업일 확인
        if country == 'KR':
            working_day = exec_query(f'select * from date_working_day where working_day = {transaction_date} ')
            if len(working_day) == 0:
                today_check = exec_query(f'select * '
                                         f'from stock_realtime_price '
                                         f'where dtim > {transaction_date}000000 '
                                         f'and  dtim < {transaction_date}999999 '
                                         f'limit 1 ')
                if len(today_check) == 0:
                    return {'status': '308', 'msg': get_status_msg('308')}  # 개장일이 아닌 일자
                else:
                    return {'status': '312', 'msg': get_status_msg('312')}  # 장 마감 이전 당일 거래내역 입력
        else:
            working_day = exec_query(f'select * from us_date_working_day where working_day = {transaction_date} ')
            if len(working_day) == 0:
                today_check = exec_query(f'select * '
                                         f'from us_stock_realtime_price '
                                         f'where dtim > {transaction_date}000000 '
                                         f'and  dtim < {transaction_date}999999 '
                                         f'limit 1 ')
                if len(today_check) == 0:
                    return {'status': '308', 'msg': get_status_msg('308')}
                else:
                    return {'status': '312', 'msg': get_status_msg('312')}  # 장 마감 이전 당일 거래내역 입력

        # 해외주식(미국주식)일 경우 매수/매도 대신 해외주식매수/해외주식매도 로 입력함
        if country != 'KR':
            transaction_type = '해외주식' + transaction_type

        # 거래내역 입력
        return_result = insert_self_trading_log(user_code, portfolio_code, stock_cd, transaction_type, transaction_date,
                                                transaction_unit_price, transaction_quantity, transaction_fee_tax, country)

        # portfolio update
        update_user_portfolio(user_code)  # 기존 포트폴리오 최신화
        update_single_stock_portfolio(user_code=user_code,
                                      begin_date=transaction_date,
                                      account_number=f'SELF_{portfolio_code}',
                                      securities_code='SELF',
                                      stock_cd=stock_cd)

        return return_result


# 거래내역 INSERT 함수
def insert_trading_log(user_code, portfolio_code, securities_code, account_number,
                       stock_cd, stock_nm, transaction_type, transaction_detail_type, transaction_date,
                       transaction_unit_price, transaction_quantity, transaction_amount, balance, total_unit,
                       total_sum, transaction_fee, transaction_tax, unit_currency, country):

    with session_scope() as session:
        # 영업일 확인
        if country == 'KR':
            working_day = exec_query(f'select * from date_working_day where working_day = {transaction_date} ')
        else:
            working_day = exec_query(f'select * from us_date_working_day where working_day = {transaction_date} ')
        if len(working_day) == 0:
            # print('영업일 아님')
            return {'status': '000', 'msg': get_status_msg('000')}  # 개장일이 아닌 거래일자 -> raw data만 저장한다.

        # user 확인
        checked_user = session.query(UserInfo).filter(UserInfo.user_code == user_code,
                                                         UserInfo.end_dtim == '99991231235959').all()
        session.commit()

        if len(checked_user) == 0:  # 일치하는 회원이 없는 경우
            return {'status': '103', 'msg': get_status_msg('103')}  # 올바르지 않은 ID or PW

        # securities_code 확인
        securities_code_result = session.query(SecuritiesCode).\
            filter(SecuritiesCode.end_date == '99991231',
                   SecuritiesCode.available_flag == 1,
                   SecuritiesCode.securities_code == securities_code).\
            all()
        session.commit()

        if len(securities_code_result) == 0:  # 일치하는 증권사 코드가 없을 경우
            return {'status': '309', 'msg': get_status_msg('309')}  # 올바르지 않은 증권사 코드

        # 거래날짜 2영업일 당기기
        if country == 'KR':
            real_trading_date = session.query(DateWorkingDay).\
                filter(DateWorkingDay.working_day == transaction_date).\
                first()
            trading_date = session.query(DateWorkingDay).\
                filter(DateWorkingDay.seq == real_trading_date.seq-2).\
                first()
            transaction_date_old = transaction_date
            transaction_date = trading_date.working_day
        else:
            # 한국 영업일 기준 결제일이므로, 가장 가까운 미국 영업일로 거래날짜를 바꿔준다
            us_settlement_date = session.query(UsDateWorkingDayMapping).\
                filter(UsDateWorkingDayMapping.date == transaction_date).\
                first()
            us_settlement_date = us_settlement_date.working_day

            # 거래날짜를 2영업일 당겨준다(미국날짜 기준)
            real_trading_date = session.query(UsDateWorkingDay). \
                filter(UsDateWorkingDay.working_day == us_settlement_date). \
                first()
            trading_date = session.query(UsDateWorkingDay). \
                filter(UsDateWorkingDay.seq == real_trading_date.seq - 2). \
                first()
            transaction_date_old = transaction_date
            transaction_date = trading_date.working_day

        # portfolio 확인
        portfolio_info = session.query(UserPortfolioInfo). \
            filter(UserPortfolioInfo.user_code == user_code,
                   UserPortfolioInfo.portfolio_code == portfolio_code).\
            all()
        session.commit()
        if len(portfolio_info) == 0:
            return {'status': '201', 'msg': get_status_msg('201')}  # 해당 포트폴리오 없음

    # 증권사별 설정
    # 거래내역 종류: 매수, 매도, 유상주입고, 무상주입고, 공모주입고, 타사대체입고, 타사대체출고, 대체입고, 대체출고, 주식합병입고, 주식합병출고, 배당, 배당세출금, 액면분할병합입고, 액면분할병합출고
    # 해외 거래내역 종류: 해외주식매수, 해외주식매도, 해외주식배당
    # 처리예정: 주식합병입금, 신용융자이자출금
    if securities_code == 'MIRAE':
        if transaction_type in ['주식매수입고', '주식매도출고', '주식매수', '주식매도',
                                '자기융자매수입고', '자기융자매도상환', '일반담보융자매도상환출고', '융자매도']:
            if '매수' in transaction_type:
                transaction_type = '매수'
            elif '매도' in transaction_type:
                transaction_type = '매도'
        elif transaction_type in ['유상주입고', '무상주입고']:
            pass
        elif transaction_type == '이체입고':
            transaction_date = transaction_date_old
            transaction_type = '타사대체입고'
        elif transaction_type == '이체출고':
            transaction_date = transaction_date_old
            transaction_type = '타사대체출고'
        elif transaction_type == '계좌대체입고':
            transaction_date = transaction_date_old
            transaction_type = '대체입고'
        elif transaction_type == '계좌대체출고':
            transaction_date = transaction_date_old
            transaction_type = '대체출고'
        elif transaction_type == '공모주입고':
            transaction_date = transaction_date_old
        elif transaction_type == '배당금입금':
            transaction_type = '배당'
            transaction_quantity = '1'
            transaction_unit_price = transaction_amount
        elif transaction_type == '해외주식매수입고':
            transaction_type = '해외주식매수'
        elif transaction_type == '해외주식매도출고':
            transaction_type = '해외주식매도'
        elif transaction_detail_type in ['액면분할입고(액면분할)', '액면분할입고', '액면병합입고(액면병합)', '액면병합입고']:
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합입고'
        elif transaction_detail_type in ['액면분할출고', '액면병합출고']:
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합출고'
        else:
            # 신주인수권 코스닥매수, 이체입고(자동)
            # print('파싱 에러')
            return {'status': '000', 'msg': get_status_msg('000')}  # raw data만 저장한다.

    elif securities_code == "NAMUH" or securities_code == "NHQV":
        if transaction_detail_type in ['코스피매수', '코스피매도', 'KOSDAQ매수', 'KOSDAQ매도', 'ETN매수', 'ETN매도',
                                       '신용대출매수(KOSDAQ)(유통융자)', '신용대출매도(KOSDAQ)(유통융자)',
                                       '신용대출매수(코스피)(유통융자)', '신용대출매도(코스피)(유통융자)']:
            if '매수' in transaction_detail_type:
                transaction_type = '매수'
            elif '매도' in transaction_detail_type:
                transaction_type = '매도'
        elif transaction_detail_type in ['주식합병입고', '주식합병출고']:
            transaction_type = transaction_detail_type
        elif transaction_detail_type in ['타사대체입고', '타사대체출고', '대체입고', '대체출고']:
            transaction_date = transaction_date_old
            transaction_type = transaction_detail_type
        elif transaction_detail_type == '배당금':
            transaction_type = '배당'
            transaction_quantity = '1'
            transaction_unit_price = transaction_amount
        elif transaction_detail_type == '무상주':
            transaction_type = '무상주입고'
        elif transaction_detail_type == '유상주':
            transaction_type = '유상주입고'
        elif transaction_detail_type == '액면분할입고':
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합입고'
        elif transaction_detail_type == '액면분할출고':
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합출고'
        elif transaction_detail_type == '공모주입고':
            transaction_date = transaction_date_old
            transaction_type = '공모주입고'
        # elif transaction_detail_type == '감자입고':
        #     transaction_type = '감자입고'
        # elif transaction_detail_type == '감자출고':
        #     transaction_type = '감자출고'
        # elif transaction_detail_type == '회사분할입고':
        #     transaction_type = '회사분할입고'
        elif transaction_detail_type == '외화증권매수':
            transaction_type = '해외주식매수'
        elif transaction_detail_type == '외화증권매도':
            transaction_type = '해외주식매도'
        elif transaction_detail_type == '외화배당금입금':
            transaction_type = '해외주식배당'
            transaction_quantity = '1'
            transaction_unit_price = transaction_amount
        else:
            # 합병단수주대금, 액면병합단수주대금, 대여출고, 대여상환입고, 무상단수주대금, 배당단수주대금, 감자출고, 감자입고, 감자단수주대금, 회사분할입고
            # 외화배당주, 외화단수주매각대금, 외화주식액면분할출고, 외화주식액면분할입고, 외화세금출금, 외화제세금환급
            # print('파싱 에러')
            return {'status': '000', 'msg': get_status_msg('000')}  # raw data만 저장한다.

        # 숫자형 값 내 콤마(',') 제거
        transaction_unit_price = transaction_unit_price.replace(',', '')
        transaction_quantity = transaction_quantity.replace(',', '')
        transaction_fee = transaction_fee.replace(',', '')
        transaction_tax = transaction_tax.replace(',', '')

    elif securities_code == "SHINHAN":
        if transaction_detail_type in ['장내매수', '장내매도', '코스닥매수', '코스닥매도', '매수', '매도']:
            if '매수' in transaction_detail_type:
                transaction_type = '매수'
            elif '매도' in transaction_detail_type:
                transaction_type = '매도'
        elif transaction_detail_type in ['타사대체입고', '타사대체출고']:
            transaction_date = transaction_date_old
            transaction_type = transaction_detail_type
        elif transaction_detail_type == '무상주':
            transaction_type = '무상주입고'
        elif transaction_detail_type == '유상주':
            transaction_type = '유상주입고'
        elif transaction_detail_type == '합병입고':
            transaction_type = '주식합병입고'
        elif transaction_detail_type == '합병출고':
            transaction_type = '주식합병출고'
        elif transaction_detail_type == '계좌대체출고':
            transaction_date = transaction_date_old
            transaction_type = '대체출고'
        elif transaction_detail_type == '계좌대체입고':
            transaction_date = transaction_date_old
            transaction_type = '대체입고'
        elif transaction_detail_type == '공모주입고':
            transaction_date = transaction_date_old
            transaction_type = '공모주입고'
        elif transaction_detail_type == '배당금':
            transaction_type = '배당'
            transaction_quantity = '1'
            transaction_unit_price = transaction_amount
        elif transaction_detail_type in ['해외증권해외주식매수', '해외매수입고']:
            transaction_type = '해외주식매수'
        elif transaction_detail_type in ['해외증권해외주식매도', '해외매도출고']:
            transaction_type = '해외주식매도'
        elif transaction_detail_type == '해외배당금':
            transaction_type = '해외주식배당'
            transaction_quantity = '1'
            transaction_unit_price = transaction_amount
        elif transaction_detail_type == '액면분할.병합입고':
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합입고'
        elif transaction_detail_type == '액면분할.병합출고':
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합출고'
        else:
            # 액면분할.병합출고, 액면분할.병합입고
            # print('파싱 에러')
            return {'status': '000', 'msg': get_status_msg('000')}  # raw data만 저장한다.

    elif securities_code == "KIWOOM":
        if transaction_detail_type in ['장내매수', '장내매도', 'KOSDAQ매수', 'KOSDAQ매도',
                                       '장내매수(융자)', '장내매도(융자상환)', 'KOSDAQ매수(융자)', 'KOSDAQ매도(융자상환)']:
            if '매수' in transaction_detail_type:
                transaction_type = '매수'
            elif '매도' in transaction_detail_type:
                transaction_type = '매도'
        # 어라.. 감자액면병합입고/출고가 합병에만 쓰이는게 아니라 여러가지 케이스로 쓰이는구나
        elif transaction_detail_type in ['감자액면병합입고', '감자액면병합출고']:
            if '입고' in transaction_detail_type:
                transaction_type = '주식합병입고'
            elif '출고' in transaction_detail_type:
                transaction_type = '주식합병출고'
        elif transaction_detail_type in ['타사대체입고', '타사대체출고', '대체입고', '대체출고']:
            transaction_date = transaction_date_old
            transaction_type = transaction_detail_type
        elif transaction_detail_type == '무상주입고':
            transaction_type = transaction_detail_type
        elif transaction_detail_type == '공모주입고':
            transaction_date = transaction_date_old
            transaction_type = '공모주입고'
        elif transaction_detail_type == '배당금입금':
            transaction_type = '배당'
            transaction_quantity = '1'
            transaction_unit_price = transaction_tax
            transaction_tax = '0'
        elif transaction_detail_type == '액면분할병합출고':
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합출고'
        elif transaction_detail_type == '액면분할병합입고':
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합입고'
        else:
            # 액면분할병합출고, 액면분할병합입고, 감자액면병합입금
            # print('파싱 에러')
            return {'status': '000', 'msg': get_status_msg('000')}  # raw data만 저장한다.

        # 숫자형 값 내 콤마(',') 제거
        transaction_unit_price = transaction_unit_price.replace(',', '')
        transaction_quantity = transaction_quantity.replace(',', '')
        transaction_fee = transaction_fee.replace(',', '')
        transaction_tax = transaction_tax.replace(',', '')

    elif securities_code == "KBSEC":
        if transaction_type in ['주식장내매수', '주식장내매도', 'KOSDAQ매수', 'KOSDAQ매도', '매수', '매도'
                                '자기융자매수', '자기매도상환 매도', '자기대용매수', '자기대용매도상환 매도']:
            if country == 'KR':
                if '매수' in transaction_type:
                    transaction_type = '매수'
                elif '매도' in transaction_type:
                    transaction_type = '매도'
            else:
                if '매수' in transaction_type:
                    transaction_type = '해외주식매수'
                elif '매도' in transaction_type:
                    transaction_type = '해외주식매도'
        elif transaction_type in ['타사대체 입고', '타사대체 출고']:
            transaction_date = transaction_date_old
            transaction_type = transaction_type.replace(' ', '')
        elif transaction_type in ['대체 입고', '대체 출고']:
            transaction_date = transaction_date_old
            transaction_type = transaction_type.replace(' ', '')
        elif transaction_type == '공모주 입고':
            transaction_date = transaction_date_old
            transaction_type = '공모주입고'
        elif transaction_detail_type == '무상주 입고':
            transaction_type = '무상주입고'
        elif transaction_detail_type == '유상주 입고':
            transaction_type = '유상주입고'
        elif transaction_detail_type in ['액면분할 입고', '액면병합 입고']:
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합입고'
        elif transaction_detail_type in ['액면분할 출고', '액면병합 출고']:
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합출고'
        else:
            # print('파싱 에러')
            return {'status': '000', 'msg': get_status_msg('000')}  # raw data만 저장한다.

    elif securities_code == "SAMSUNG":
        if transaction_type in ['매수', '매도', '융자매수', '융자매도']:
            if '매수' in transaction_type:
                transaction_type = '매수'
            elif '매도' in transaction_type:
                transaction_type = '매도'
        elif transaction_type == '배당금입금':
            transaction_type = '배당'
            transaction_quantity = '1'
            transaction_unit_price = transaction_tax
            transaction_tax = '0'
        elif transaction_type in ['대체입고', '대체출고']:
            transaction_date = transaction_date_old
        elif transaction_type == '합병입고':
            transaction_type = '주식합병입고'
        elif transaction_type == '합병출고':
            transaction_type = '주식합병출고'
        elif transaction_type == '타사입고':
            transaction_date = transaction_date_old
            transaction_type = '타사대체입고'
        elif transaction_type == '타사출고':
            transaction_date = transaction_date_old
            transaction_type = '타사대체출고'
        elif transaction_type == '무상입고':
            transaction_type = '무상주입고'
        elif transaction_type == '유상입고':
            transaction_type = '유상주입고'
        elif transaction_type == '청약입고':
            transaction_date = transaction_date_old
            transaction_type = '공모주입고'
        elif transaction_type == '미국(NYSE)주식매수':
            transaction_type = '해외주식매수'
        elif transaction_type == '미국(NYSE)주식매도':
            transaction_type = '해외주식매도'
        elif transaction_type == '미국(NASDAQ)주식매수':
            transaction_type = '해외주식매수'
        elif transaction_type == '미국(NASDAQ)주식매도':
            transaction_type = '해외주식매도'
        elif transaction_detail_type in ['분할입고', '액면분할입고', '액면병합입고', '분할상장입고']:
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합입고'
        elif transaction_detail_type in ['분할출고', '액면분할출고', '액면병합출고']:
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합출고'
        else:
            # 단수주입금, 배당입고, 신주인수권입고, 신주인수권출고, 감자출고, 감자입고 , 대차출고, 상환입고, 상환매도
            # print('파싱 에러')
            return {'status': '000', 'msg': get_status_msg('000')}  # raw data만 저장한다.

    elif securities_code == "TRUEFR":
        # 한국투자증권
        if country == 'KR' and transaction_type in ['매수', '매도']:
            pass
        elif country != 'KR' and transaction_type == '매수':
            transaction_type = '해외주식매수'
        elif country != 'KR' and transaction_type == '매도':
            transaction_type = '해외주식매도'
        elif transaction_detail_type == '유상주입고':
            transaction_type = '유상주입고'
        elif (transaction_detail_type == '공모주입고') or (transaction_type == '입고' and transaction_detail_type == '자문사일괄청약대행'):
            transaction_date = transaction_date_old
            transaction_type = '공모주입고'
        elif transaction_detail_type in ['Smart+당사이체입고', '주식지급']:
            transaction_date = transaction_date_old
            transaction_type = '대체입고'
        elif transaction_detail_type == 'Smart+당사이체출고':
            transaction_date = transaction_date_old
            transaction_type = '대체출고'
        elif transaction_detail_type == '타사이체입고':
            transaction_date = transaction_date_old
            transaction_type = '타사대체입고'
        elif transaction_detail_type == '타사이체출고':
            transaction_date = transaction_date_old
            transaction_type = '타사대체출고'
        elif transaction_detail_type in ['액면분할입고', '미니스탁액면분할입고']:
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합입고'
            transaction_unit_price = util_get_avg_purchase_price(user_code, account_number, stock_cd)
            split_release_quantity = util_get_recent_split_release_quantity(user_code, account_number, stock_cd)
            if transaction_unit_price is None or split_release_quantity is None:
                return {'status': '000'}  # raw data만 저장한다.
            transaction_unit_price = transaction_unit_price / (transaction_quantity / split_release_quantity)
        elif transaction_detail_type in ['액면분할출고', '미니스탁액면분할출고']:
            transaction_date = transaction_date_old
            transaction_type = '액면분할병합출고'
            transaction_unit_price = util_get_avg_purchase_price(user_code, account_number, stock_cd)
            if transaction_unit_price is None:
                return {'status': '000'}  # raw data만 저장한다.
        else:
            # print('파싱 에러')
            # 해외증권배당금입금(원화도 있고 외국환도 있네?), 현지세금재징수(?), 배당금입금, 종목변경입고, 종목변경출고
            return {'status': '000', 'msg': get_status_msg('000')}  # raw data만 저장한다.

    with session_scope() as session:
        # seq 불러오기 <- 동일한 계좌에 대해서 같은 날짜에 거래된 내역에 seq를 붙인다.
        trading_log_order_by_seq = session.query(TmpUserTradingLog). \
            filter(TmpUserTradingLog.user_code == user_code,
                   TmpUserTradingLog.account_number == account_number,
                   TmpUserTradingLog.date == transaction_date). \
            order_by(TmpUserTradingLog.seq.desc()).all()
        session.commit()
        # print(f'user_code: {user_code} / account_nubmer: {account_number} / transaction_date: {transaction_date}')
        # print(trading_log_order_by_seq)

        if len(trading_log_order_by_seq) == 0:
            seq = 1
        else:
            seq = trading_log_order_by_seq[0].seq + 1

        # stock_type 체크 / unit_currency 세팅
        if country == 'KR':
            # 한국주식
            stock_info = session.query(StockInfo). \
                filter(or_(StockInfo.stock_cd == f'A{stock_cd}',
                           StockInfo.stock_cd == f'Q{stock_cd}'),
                       StockInfo.end_date >= transaction_date).all()
            if len(stock_info) != 0:
                tmp_market = stock_info[0].market
                stock_nm = stock_info[0].stock_nm
                if tmp_market in ('KOSPI', 'KOSDAQ'):
                    stock_type = 'domestic_stock'
                    unit_currency = 'KRW'
                elif tmp_market == 'ETF':
                    stock_type = 'domestic_etf'
                    unit_currency = 'KRW'
                elif tmp_market == 'ETN':
                    stock_type = 'domestic_etn'
                    unit_currency = 'KRW'
                else:
                    stock_type = 'etc'
                    unit_currency = unit_currency
            else:
                stock_nm = stock_nm
                stock_type = 'etc'
                unit_currency = unit_currency

                if len(stock_cd) > 6:
                    stock_cd = stock_cd[:6]
        else:
            # 미국주식
            # transaction_date는 미국 시간 기준 일자이다. us_stock_info에 적재된 일자도 미국 시간 기준 일자이다. 따라서 비교가 가능하다.
            stock_info = session.query(UsStockInfo). \
                filter(UsStockInfo.stock_cd == stock_cd,
                       UsStockInfo.latest_date >= transaction_date).\
                all()
            if len(stock_info) != 0:
                stock_nm = stock_info[0].stock_nm
                stock_type = 'us_stock'
                unit_currency = 'USD'
                country = 'US'
            else:
                stock_nm = stock_nm
                stock_type = 'foreign_etc'
                unit_currency = unit_currency

        # portfolio map 입력
        # 기존 portfolio map 확인
        tmp_map = session.query(TmpUserPortfolioMap).\
            filter(TmpUserPortfolioMap.user_code == user_code,
                   TmpUserPortfolioMap.account_number == account_number,
                   TmpUserPortfolioMap.stock_cd == stock_cd).\
            first()
        session.commit()

        if tmp_map is None:
            # 기존 map이 없을 경우 insert
            portfolio_map = TmpUserPortfolioMap(user_code=user_code,
                                                account_number=account_number,
                                                stock_cd=stock_cd,
                                                portfolio_code=portfolio_code,
                                                securities_code=securities_code,
                                                lst_update_dtim=datetime.today().strftime("%Y%m%d%H%M%S"))
            session.add(portfolio_map)  # portfolio map insert(commit은 아래에서 한번에)

        new_user_trading_log = TmpUserTradingLog(user_code=user_code,
                                                 account_number=account_number,
                                                 date=transaction_date,
                                                 seq=seq,
                                                 stock_cd=stock_cd,
                                                 stock_type=stock_type,
                                                 securities_code=securities_code,
                                                 stock_nm=stock_nm,
                                                 transaction_type=transaction_type,
                                                 transaction_detail_type=transaction_detail_type,
                                                 transaction_quantity=transaction_quantity,
                                                 transaction_unit_price=transaction_unit_price,
                                                 transaction_fee=transaction_fee,
                                                 transaction_tax=transaction_tax,
                                                 unit_currency=unit_currency,
                                                 update_dtim=datetime.today().strftime("%Y%m%d%H%M%S"),
                                                 country=country)
        session.add(new_user_trading_log)  # 신규 거래내역 insert
        session.commit()

        # print('정상 처리')
        return {'status': '000', 'msg': get_status_msg('000')}


# 다수 거래내역 업데이트 API
class MultiInsertTradingLog(Resource):
    def post(self):
        log_msg = make_log_msg("/api/securities/multiinserttradinglog", request.data)
        app.logger.info(log_msg)
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('securities_code', type=str)
        parser.add_argument('infotech_code', type=str)
        parser.add_argument('account_number', type=str)
        parser.add_argument('trading_log_list', type=str, action='append')
        parser.add_argument('api_key', type=str)
        parser.add_argument('fcm_token', type=str)
        parser.add_argument('use_raw_flag', type=int)
        args = parser.parse_args()

        user_code = args['user_code']
        securities_code = args['securities_code']
        infotech_code = args['infotech_code']
        account_number = args['account_number']
        trading_log_list = args['trading_log_list']
        api_key = args['api_key']
        fcm_token = args['fcm_token']  # fcm push 발송을 위한 token 값
        use_raw_flag = args['use_raw_flag']

        # print('multi insert trading log start')
        # print(args)
        # print(user_code)
        # print(securities_code)
        # print(infotech_code)
        # print(account_number)
        # print(trading_log_list[:100])
        # print(api_key)
        # print(fcm_token)
        # print(use_raw)

        check_status = check_user_validity(user_code, api_key)
        # print(check_status)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            # user_tr_insert_status update
            self.update_acct_sync_status(user_code, account_number)
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            # user_tr_insert_status update
            self.update_acct_sync_status(user_code, account_number)
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        with session_scope() as session:
            # tmp_user_portfolio 체크하기
            tmp_user_portfolio = session.query(TmpUserPortfolio). \
                filter(TmpUserPortfolio.user_code == user_code,
                       TmpUserPortfolio.account_number == account_number). \
                first()
            session.commit()

            if tmp_user_portfolio is not None:
                # user_tr_insert_status update
                self.update_acct_sync_status(user_code, account_number)
                return {'status': '209', 'msg': get_status_msg('209')}  # 에러코드: 포트폴리오 업데이트 진행 중

        use_raw = True if use_raw_flag == 1 else False

        if not use_raw:
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

                # infotech_code, securities_code 유효성 검사, securities_code 생성
                if _infotech_securities_code is None and _securities_code is None:
                    # user_tr_insert_status update
                    self.update_acct_sync_status(user_code, account_number)
                    return {'status': '309', 'msg': get_status_msg('309')}  # 에러코드: 올바르지 않은 증권사 코드
                elif _infotech_securities_code is None and _securities_code is not None:
                    securities_code = _securities_code.securities_code
                elif _infotech_securities_code is not None and _securities_code is None:
                    securities_code = _infotech_securities_code.securities_code
                elif _infotech_securities_code is not None and _securities_code is not None:
                    if _infotech_securities_code != _securities_code:
                        # user_tr_insert_status update
                        self.update_acct_sync_status(user_code, account_number)
                        return {'status': '309', 'msg': get_status_msg('309')}  # 에러코드: 올바르지 않은 증권사 코드
                    else:
                        securities_code = _securities_code.securities_code

                # 기존 등록 증권사 여부 변수 생성
                with session_scope() as session:
                    existing_securities = session.query(UserPortfolioMap). \
                        filter(UserPortfolioMap.user_code == user_code,
                               UserPortfolioMap.securities_code == securities_code). \
                        first()
                    if existing_securities is None:
                        is_new_securities = True
                    else:
                        is_new_securities = False

                app.logger.info(make_log_msg("/api/securities/multiinserttradinglog", f"is_new_securities - {is_new_securities}"))
                # 기존 등록된 증권사가 아닐 경우, push_noti_acct_status insert
                if is_new_securities:
                    new_status = PushNotiAcctStatus(user_code=user_code,
                                                    account_number=account_number,
                                                    sync_complete_flag=0,
                                                    fcm_token=fcm_token,
                                                    lst_update_dtim=datetime.now().strftime('%Y%m%d%H%M%S'))
                    try:
                        session.add(new_status)
                        session.commit()
                        app.logger.debug(make_log_msg("/api/securities/multiinserttradinglog", "is_new_securiteis info okay ! insert commit completed!"))
                    except Exception as e:
                        app.logger.error(make_log_msg("/api/securities/multiinserttradinglog", e))
                        session.rollback()

            # trading_log_list parsing
            try:
                # print(trading_log_list)
                tr = [json.loads(e.replace("'", '"')) for e in trading_log_list]
                # print(tr)
            except Exception as e:
                # print(e)
                # user_tr_insert_status update
                self.update_acct_sync_status(user_code, account_number, is_new_securities=is_new_securities)
                return {'status': '310', 'msg': get_status_msg('310')}

            # print(tr)
            tr_df = pd.DataFrame(tr)
            # print(tr_df)

            tr_df = tr_df.loc[:, ['itemCd', 'itemNm', 'trDiv', 'trTp', 'trDt', 'trTm',
                                  'unitPrice', 'trQt', 'totalSum', 'totalUnit', 'trAmt', 'balance',
                                  'fees', 'tax', 'curCd', 'exYn']]
            tr_df.columns = ['stock_cd', 'stock_nm', 'transaction_type', 'transaction_detail_type', 'transaction_date',
                             'transaction_time', 'transaction_unit_price', 'transaction_quantity', 'total_sum',
                             'total_unit', 'transaction_amount', 'balance',
                             'transaction_fee', 'transaction_tax', 'unit_currency', 'ext_yn']

            # 정렬
            sort_order = {'MIRAE': 'DESC',
                          'NHQV': 'DESC',
                          'KBSEC': 'ASC',
                          'KIWOOM': 'ASC',
                          'SAMSUNG': 'ASC',
                          'SHINHAN': 'DESC',
                          'TRUEFR': 'ASC',
                          'CREON': 'ASC',
                          'EUGENE': 'ASC',
                          'IBK': 'ASC',
                          'NAMUH': 'ASC',
                          'YUANTA': 'ASC'}

            if sort_order[securities_code] == 'DESC':
                tr_df = tr_df.sort_index(ascending=False).reset_index(drop=True)

            # 한투의 경우, 종목코드 부여해 줌
            # 여기서는 국내주식만 종목코드 부여해줌: 뒤에서 종목코드 없을 경우 해외주식으로 분류하도록 로직 설계됨
            if securities_code == 'TRUEFR':
                # stock_cd 가져오기
                tr_df['stock_cd'] = tr_df.apply(lambda x: get_stock_cd_from_trfr_transaction(x), axis=1)
                # print(tr_df)

            # 날짜, 종목, 거래순서 재정렬 -> 동일 일자일 경우, 매수가 매도보다 앞에 오도록 정렬됨.
            order_dict = {'장내매수': 100, 'KOSDAQ매수': 101, '외화증권매수': 102, '해외매수입고': 103, '해외증권매수': 104,
                          '장내매도': 200, 'KOSDAQ매도': 201, '외화증권매도': 202, '해외매도출고': 203, '해외증권매도': 204}
            tr_df = tr_df.sort_values(by=['transaction_detail_type'], key=lambda x: x.map(order_dict)).reset_index(drop=True)
            order_dict = {'매수': 100, '주식매수입고': 101, '주식매수': 102, '유상주입고': 103, '무상주입고': 104, '장내매수': 105, 'KOSDAQ매수': 106,
                          '해외주식매수입고': 107, '미국(NYSE)주식매수': 108, '미국(NASDAQ)주식매수': 109,
                          '자기융자매수입고': 110, '이체입고': 111,
                          '매도': 200, '주식매도출고': 201, '주식매도': 202, '장내매도': 203, 'KOSDAQ매도': 204,
                          '해외주식매도출고': 205, '미국(NYSE)주식매도': 206, '미국(NASDAQ)주식매도': 207,
                          '자기융자매도상환': 208, '일반담보융자매도상환출고': 209, '융자매도': 210, '이체출고': 211}
            tr_df = tr_df.sort_values(by=['transaction_type'], key=lambda x: x.map(order_dict)).reset_index(drop=True)
            tr_df = tr_df.sort_values(by=['transaction_date', 'stock_cd']).reset_index(drop=True)
            # print(tr_df)

            # 시작일자
            begin_date = min(tr_df['transaction_date'])

            # print(tr_df)
            # print(begin_date)

            # 어차피 거래기록은 겹치는 애들이 있으면 동일한 건이므로, 겹치는 애가 있으면 overwrite 한다.
            # 같은 날 같은 종목을 두번 샀으면 어떡하지? -> 그냥 애들이 insert될 기간동안의 거래내역을 지우고 다시 넣는다.
            # 포트폴리오도 지우고 다시 넣어야 되는가? -> 그러자.
            # 종목별로 넣는게 이득일까? row별로 넣는게 이득일까? -> 그냥 row별로 넣자.

            # raw 거래내역 정제
            raw_tr = tr_df.copy()
            raw_tr['user_code'] = user_code
            raw_tr['securities_code'] = securities_code
            raw_tr['account_number'] = account_number
            raw_tr['seq'] = list(raw_tr.index)
            raw_tr = raw_tr.loc[:, ['user_code', 'securities_code', 'account_number', 'transaction_date', 'seq',
                                    'stock_cd', 'transaction_time', 'stock_nm', 'transaction_type',
                                    'transaction_detail_type', 'transaction_unit_price', 'transaction_quantity',
                                    'transaction_amount', 'balance', 'total_unit', 'total_sum', 'transaction_fee',
                                    'transaction_tax', 'unit_currency', 'ext_yn']]

            # raw 거래내역에 lst_update_date 추가
            raw_tr['lst_update_dtim'] = datetime.today().strftime('%Y%m%d%H%M%S')
            # print(raw_tr)

            # country 변수 추가
            # mirae(transaction_type): 해외주식매수입고, 해외주식매도출고
            # namuh(transaction_detail_type): 외화증권매수, 외화배당주, 외화배당금입금, 외화단수주매각대금, 외화주식액면분할출고, 외화주식액면분할입고, 외화증권매도, 외화세금출금, 외화제세금환급
            # shinhan(transaction_detail_type): 해외매수입고, 해외매도출고
            # kiwoom(transaction_detail_type): 해외배당세출금, 배당금(외화)입금 -> 아 맞다 키움증권은 외국주식 안나오지...
            # kbsec(transaction_type): 매수, 매도 -> 국내주식과 동일하게 나옴. unit_currency로 구분하면 된다.
            # samsung(transaction_type): 미국(NYSE)주식매수, 미국(NYSE)주식매도, 미국(NASDAQ)주식매수, 미국(NASDAQ)주식매도
            # truefr(transaction_detail_type): 해외증권매수, 해외증권매도, mini/ -> mini/는 도대체 뭔지 모르겠다. -> mini/는 미니스탁이다. .... -> 종목코드 없으면 해외주식으로 하자.

            if securities_code == 'MIRAE':
                raw_tr['country'] = raw_tr['ext_yn'].apply(lambda x: 'foreign' if x == 'Y' else 'KR')
            elif securities_code == 'NAMUH' or securities_code == 'NHQV':
                raw_tr['country'] = raw_tr['transaction_detail_type'].apply(lambda x: 'foreign' if x in ['외화증권매수', '외화배당주', '외화배당금입금', '외화단수주매각대금', '외화주식액면분할출고', '외화주식액면분할입고', '외화증권매도', '외화세금출금', '외화제세금환급'] else 'KR')
            elif securities_code == 'SHINHAN':
                raw_tr['country'] = raw_tr['transaction_detail_type'].apply(lambda x: 'foreign' if x in ['해외증권해외주식매수', '해외매수입고', '해외증권해외주식매도', '해외매도출고'] else 'KR')
            elif securities_code == 'KIWOOM':
                raw_tr['country'] = raw_tr['transaction_detail_type'].apply(lambda x: 'foreign' if x in ['해외배당세출금', '배당금(외화)입금'] else 'KR')
            elif securities_code == 'KBSEC':
                raw_tr['country'] = raw_tr['unit_currency'].apply(lambda x: 'foreign' if x not in ['', 'KRW'] else 'KR')
            elif securities_code == 'SAMSUNG':
                raw_tr['country'] = raw_tr['unit_currency'].apply(lambda x: 'foreign' if x not in ['', 'KRW'] else 'KR')
            elif securities_code == 'TRUEFR':
                raw_tr['country'] = raw_tr['stock_cd'].apply(lambda x: 'foreign' if x == '' else 'KR')  # 한투의 경우 국내주식은 종목명 기준으로 종목코드를 부여해줌

            with session_scope() as session:
                # 오늘 날짜가 working_day에 아직 추가되어 있지 않으면, 2영업일 전으로 transaction_date를 변경하지 않는 거래내역 중 당일에 거래된 내역은 빼 준다.

                # 최근 working_day 불러오기
                rct_kor_working_day = session.query(DateWorkingDay).order_by(DateWorkingDay.seq.desc()).first().working_day
                rct_us_working_day = session.query(UsDateWorkingDay).order_by(UsDateWorkingDay.seq.desc()).first().working_day

                # 장 마감 전 오늘 날짜의 거래내역은 raw_tr에서 날려줌
                # 이렇게 하면 실제 거래일자가 -2영업일인 거래내역도 장이 끝나기 전에는 반영되지 않는다. 괜찮을까?
                # 관리의 편의성 vs 8시간 빠른 반영 -> 우선은 관리의 편의성을 선택한다.
                raw_tr = raw_tr.loc[((raw_tr['transaction_date'] <= str(rct_kor_working_day)) &
                                     (raw_tr['country'] == 'KR')) |
                                    ((raw_tr['transaction_date'] <= str(rct_us_working_day)) &
                                     (raw_tr['country'] == 'foreign')), :]

                # 기존 raw 거래내역과 신규 raw 거래내역 비교
                # Raw 거래내역 불러오기
                raw_tr_query = session.query(UserTradingLogRaw). \
                    filter(UserTradingLogRaw.user_code == user_code,
                           UserTradingLogRaw.account_number == account_number,
                           UserTradingLogRaw.date >= begin_date). \
                    statement
                origin_raw_tr = pd.read_sql(raw_tr_query, session.bind)
                origin_raw_tr.columns = raw_tr.columns

            # 기존 raw 거래내역과 불러온 거래내역이 같으면 여기서 종료
            # 비교할 컬럼 정의
            compare_cols = ['user_code', 'securities_code', 'account_number', 'transaction_date',
                            'stock_cd', 'transaction_time', 'stock_nm', 'transaction_type',
                            'transaction_detail_type', 'transaction_unit_price', 'transaction_quantity',
                            'transaction_amount', 'balance', 'total_unit', 'total_sum', 'transaction_fee',
                            'transaction_tax', 'unit_currency', 'ext_yn', 'country']

            tmp_raw_tr = raw_tr.loc[:, compare_cols]
            tmp_origin_raw_tr = origin_raw_tr.loc[:, compare_cols]

            if tmp_raw_tr.equals(tmp_origin_raw_tr):
                # user_tr_insert_status update
                self.update_acct_sync_status(user_code, account_number, is_new_securities=is_new_securities)
                return {'status': '000', 'msg': get_status_msg('000')}

            # raw 거래내역 삭제
            with session_scope() as session:
                # Raw 거래내역 삭제
                session.query(UserTradingLogRaw). \
                    filter(UserTradingLogRaw.user_code == user_code,
                           UserTradingLogRaw.account_number == account_number,
                           UserTradingLogRaw.date >= begin_date). \
                    delete(synchronize_session='fetch')
                # db commit
                session.commit()

            # raw 거래내역 insert
            insert_data(raw_tr, 'user_trading_log_raw')
        else:
            # use_raw == True일 경우

            # is_new_securities 설정
            is_new_securities = False

            with session_scope() as session:
                # Raw 거래내역 불러오기
                raw_tr_query = session.query(UserTradingLogRaw). \
                    filter(UserTradingLogRaw.user_code == user_code,
                           UserTradingLogRaw.account_number == account_number). \
                    statement
                raw_tr = pd.read_sql(raw_tr_query, session.bind)
                raw_tr.columns = ['user_code', 'securities_code', 'account_number', 'transaction_date', 'seq',
                                  'stock_cd', 'transaction_time', 'stock_nm', 'transaction_type',
                                  'transaction_detail_type', 'transaction_unit_price', 'transaction_quantity',
                                  'transaction_amount', 'balance', 'total_unit', 'total_sum', 'transaction_fee',
                                  'transaction_tax', 'unit_currency', 'ext_yn', 'lst_update_dtim', 'country']

                # securities_code 검사
                if securities_code is None:
                    return {'status': '999', 'msg': 'custom err msg: securities_code is required when using use_raw option.'}

                # 거래내역, 포트폴리오 삭제
                # user_portfolio
                session.query(UserPortfolio). \
                    filter(UserPortfolio.user_code == user_code,
                           UserPortfolio.account_number == account_number). \
                    delete()
                session.commit()
                # user_trading_log
                session.query(UserTradingLog). \
                    filter(UserTradingLog.user_code == user_code,
                           UserTradingLog.account_number == account_number). \
                    delete()
                session.commit()

        # portfolio_code 확인 <- 거래내역이 업데이트 될 포트폴리오
        with session_scope() as session:
            # default portfolio 불러오기
            user_portfolio_info = session.query(UserDefaultPortfolio).\
                filter(UserDefaultPortfolio.user_code == user_code,
                       UserDefaultPortfolio.securities_code == securities_code).\
                first()
            session.commit()

            if user_portfolio_info is not None:
                portfolio_code = user_portfolio_info.portfolio_code
            else:
                # default portfolio 없을 경우 가장 첫번째 포트폴리오 코드 가져오기
                user_portfolio_info = session.query(UserPortfolioInfo). \
                    filter(UserPortfolioInfo.user_code == user_code). \
                    order_by(UserPortfolioInfo.portfolio_order). \
                    first()
                session.commit()
                portfolio_code = user_portfolio_info.portfolio_code

        # 거래내역 insert
        user_trading_log_module = UserTradingLogModule(user_code=user_code, portfolio_code=portfolio_code,
                                                       securities_code=securities_code, account_number=account_number,
                                                       raw_tr=raw_tr, app=app)
        return_result = user_trading_log_module.insert_tr_log()
        if return_result['status'] != '000':
            # user_tr_insert_status update
            self.update_acct_sync_status(user_code, account_number, use_raw=use_raw, is_new_securities=is_new_securities)
            return return_result

        # for i, tr in raw_tr.iterrows():
        #     # print(f"----------------------- {i}th tr -----------------------")
        #     # print(tr)
        #     stock_cd = tr['stock_cd']
        #     stock_nm = tr['stock_nm']
        #     transaction_type = tr['transaction_type']
        #     transaction_detail_type = tr['transaction_detail_type']
        #     transaction_date = tr['transaction_date']
        #     transaction_unit_price = tr['transaction_unit_price']
        #     transaction_quantity = tr['transaction_quantity']
        #     transaction_amount = tr['transaction_amount']
        #     balance = tr['balance']
        #     total_unit = tr['total_unit']
        #     total_sum = tr['total_sum']
        #     transaction_fee = tr['transaction_fee']
        #     transaction_tax = tr['transaction_tax']
        #     unit_currency = tr['unit_currency']
        #     country = tr['country']
        #
        #     with session_scope() as session:
        #         # portfolio_code 확인
        #         # min(포트폴리오 코드) 값 가져오기 <- 거래내역이 업데이트 될 포트폴리오
        #         user_portfolio_info = session.query(UserPortfolioInfo). \
        #             filter(UserPortfolioInfo.user_code == user_code). \
        #             order_by(UserPortfolioInfo.portfolio_order). \
        #             first()
        #         session.commit()
        #
        #         # 거래내역 입력
        #         return_result = insert_trading_log(user_code, user_portfolio_info.portfolio_code, securities_code,
        #                                            account_number, stock_cd, stock_nm, transaction_type,
        #                                            transaction_detail_type, transaction_date, transaction_unit_price,
        #                                            transaction_quantity, transaction_amount, balance, total_unit,
        #                                            total_sum, transaction_fee, transaction_tax, unit_currency,
        #                                            country)
        #         # print(f'insert_trading_log - return_result: {return_result}')
        #         if return_result['status'] != '000':
        #             return return_result
        #
        # # 거래내역 덮어쓰기
        # # tmp_user_trading_log의 min_date 구하기
        # min_date = exec_query(f'select min(date) '
        #                       f'from tmp_user_trading_log '
        #                       f'where user_code = {user_code} '
        #                       f'and account_number = "{account_number}" ')[0][0]
        #
        # # 거래내역 삭제
        # with session_scope() as session:
        #     session.query(UserTradingLog). \
        #         filter(UserTradingLog.user_code == user_code,
        #                UserTradingLog.account_number == account_number,
        #                UserTradingLog.date >= min_date). \
        #         delete(synchronize_session='fetch')
        #     # db commit
        #     session.commit()
        #
        # # 거래내역 insert
        # exec_query(f'insert into user_trading_log '
        #            f'select * '
        #            f'from tmp_user_trading_log '
        #            f'where user_code = {user_code} '
        #            f'and account_number = "{account_number}" ')
        #
        # # delete tmp_user_trading_log
        # exec_query(f'delete from tmp_user_trading_log '
        #            f'where user_code = {user_code} '
        #            f'and account_number = "{account_number}"')
        #
        # # user_portfolio_map 덮어쓰기
        # exec_query(f'replace into user_portfolio_map '
        #            f'select * '
        #            f'from tmp_user_portfolio_map '
        #            f'where user_code = {user_code} '
        #            f'and account_number = "{account_number}"')
        #
        # # delete user_portfolio_map
        # exec_query(f'delete from tmp_user_portfolio_map '
        #            f'where user_code = {user_code} '
        #            f'and account_number = "{account_number}"')

        # tmp_user_portfolio 세팅
        # 포트폴리오 복제
        exec_query(f'insert into tmp_user_portfolio '
                   f'select * '
                   f'from user_portfolio '
                   f'where user_code = {user_code} '
                   f'and account_number = "{account_number}" ')

        # tmp_user_trading_log의 min_date 구하기
        min_date = user_trading_log_module.min_date
        if min_date is None:
            min_date = 0

        # TmpUserPortfolio 최근 거래내역 삭제
        with session_scope() as session:
            session.query(TmpUserPortfolio). \
                filter(TmpUserPortfolio.user_code == user_code,
                       TmpUserPortfolio.account_number == account_number,
                       TmpUserPortfolio.date >= min_date). \
                delete(synchronize_session='fetch')
            # db commit
            session.commit()

        # tmp portfolio update
        update_user_portfolio_by_account(user_code=user_code, account_number=account_number, tmp_portfolio=True)

        # user_portfolio 삭제
        with session_scope() as session:
            session.query(UserPortfolio). \
                filter(UserPortfolio.user_code == user_code,
                       UserPortfolio.account_number == account_number,
                       UserPortfolio.date >= min_date). \
                delete(synchronize_session='fetch')
            # db commit
            session.commit()

        # insert user_portfolio
        exec_query(f'insert into user_portfolio '
                   f'select * '
                   f'from tmp_user_portfolio '
                   f'where user_code = {user_code} '
                   f'and account_number = "{account_number}" '
                   f'and date >= {min_date}')

        # delete tmp_user_portfolio
        exec_query(f'delete from tmp_user_portfolio '
                   f'where user_code = {user_code} '
                   f'and account_number = "{account_number}"')

        # user_tr_insert_status update
        self.update_acct_sync_status(user_code, account_number, use_raw=use_raw, is_new_securities=is_new_securities)

        # print(f'multi insert trading log - status 000: user_code: {user_code}, securities_code: {securities_code}, account_number: {account_number}')
        return {'status': '000', 'msg': get_status_msg('000')}

    def update_acct_sync_status(self, user_code, account_number, use_raw=False, is_new_securities=False):
        if (not use_raw) & is_new_securities:
            with session_scope() as session:
                sync_status = session.query(PushNotiAcctStatus). \
                    filter(PushNotiAcctStatus.user_code == user_code,
                           PushNotiAcctStatus.account_number == account_number). \
                    first()

                sync_status.sync_complete_flag = 1
                sync_status.lst_update_dtim = datetime.now().strftime('%Y%m%d%H%M%S')
                session.commit()


# 최근 업데이트 일자 api
class GetRecentUpdateDate(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('securities_code', type=str)
        parser.add_argument('infotech_code', type=str)
        parser.add_argument('account_number', type=str)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()
        # print(args)

        user_code = args['user_code']
        securities_code = args['securities_code']
        # 서버에서는 어떤 증권사 계정이 등록되어 있는지 알 수 없기 때문에 securities code를 input으로 받아야 함.
        infotech_code = args['infotech_code']
        account_number = args['account_number']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'last_update_date': ''}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'last_update_date': ''}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        with session_scope() as session:
            # infotech_code 체크하기
            _infotech_securities_code = session.query(SecuritiesCode).\
                filter(SecuritiesCode.infotech_code == infotech_code).\
                first()
            session.commit()

            # securities_code 체크하기
            _securities_code = session.query(SecuritiesCode).\
                filter(SecuritiesCode.securities_code == securities_code).\
                first()
            session.commit()

            # infotech_code, securities_code 유효성 검사
            if _infotech_securities_code is None and _securities_code is None:
                return {'status': '309', 'msg': get_status_msg('309'), 'last_update_date': ''}  # 에러코드: 올바르지 않은 증권사 코드
            elif _infotech_securities_code is None and _securities_code is not None:
                securities_code = _securities_code.securities_code
            elif _infotech_securities_code is not None and _securities_code is None:
                securities_code = _infotech_securities_code.securities_code
            elif _infotech_securities_code is not None and _securities_code is not None:
                if _infotech_securities_code != _securities_code:
                    return {'status': '309', 'msg': get_status_msg('309'), 'last_update_date': ''}  # 에러코드: 올바르지 않은 증권사 코드
                else:
                    securities_code = _securities_code.securities_code

            # 마지막 업데이트 일자 가져오기
            max_trading_log = session.query(UserSimpleTradingLog). \
                filter(UserSimpleTradingLog.user_code == user_code,
                       UserSimpleTradingLog.securities_code == securities_code,
                       UserSimpleTradingLog.account_number == account_number). \
                order_by(UserSimpleTradingLog.date.desc()). \
                first()
            session.commit()

            if max_trading_log is None:
                last_update_date = '20110101'
            else:
                last_update_date = max_trading_log.date

        # print({'status': '000', 'msg': get_status_msg('000'), 'last_update_date': last_update_date,
        #        'securities_code': securities_code, 'account_number': account_number})
        return {'status': '000', 'msg': get_status_msg('000'), 'last_update_date': last_update_date}


# 거래내역 수정 API
class ReviseTradingLog(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('portfolio_code', type=int)
        parser.add_argument('stock_cd', type=str)
        parser.add_argument('transaction_date', type=str)
        parser.add_argument('seq', type=int)
        parser.add_argument('new_transaction_date', type=str)
        parser.add_argument('transaction_unit_price', type=float)
        parser.add_argument('transaction_quantity', type=int)
        parser.add_argument('transaction_fee_tax', type=float)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        portfolio_code = args['portfolio_code']
        stock_cd = args['stock_cd']
        transaction_date = args['transaction_date']
        seq = args['seq']
        new_transaction_date = args['new_transaction_date']
        transaction_unit_price = args['transaction_unit_price']
        transaction_quantity = args['transaction_quantity']
        transaction_fee_tax = args['transaction_fee_tax']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        # 영업일 확인
        working_day = exec_query(f'select * from date_working_day where working_day = {new_transaction_date} ')
        if len(working_day) == 0:
            return {'status': '308', 'msg': get_status_msg('308')}  # 영업일이 아님

        with session_scope() as session:
            # 기존 거래내역 불러오기
            trading_log = session.query(UserTradingLog). \
                filter(UserTradingLog.user_code == user_code,
                       UserTradingLog.account_number == f'SELF_{portfolio_code}',
                       UserTradingLog.date == transaction_date,
                       UserTradingLog.seq == seq,
                       UserTradingLog.stock_cd == stock_cd).\
                first()
            session.commit()

            if trading_log is None:
                return {'status': '307', 'msg': get_status_msg('307')}  # 일치하는 거래내역 없음

            # 바꾸려고 하는 날짜가 다른 날짜인 경우, seq를 바꿔준다
            if transaction_date != new_transaction_date:
                # seq 불러오기 <- 직접입력된 건에 대해서 같은 날짜에 거래된 내역에 seq를 붙인다.
                trading_log_order_by_seq = session.query(UserTradingLog). \
                    filter(UserTradingLog.user_code == user_code,
                           UserTradingLog.stock_cd == stock_cd,
                           UserTradingLog.securities_code == 'SELF',
                           UserTradingLog.date == new_transaction_date). \
                    order_by(UserTradingLog.seq.desc()).all()
                session.commit()

                if len(trading_log_order_by_seq) == 0:
                    seq = 1
                else:
                    if transaction_date == new_transaction_date:
                        seq = seq
                    else:
                        seq = trading_log_order_by_seq[0].seq + 1

            # 기존 거래내역과 충돌하는지 확인
            # 기존 거래내역 리스트 불러오기(현재 수정내역 제외)
            trading_log_list_query = session.query(UserTradingLog). \
                filter(UserTradingLog.user_code == user_code,
                       UserTradingLog.account_number == f'SELF_{portfolio_code}',
                       not_(and_(UserTradingLog.date == transaction_date,
                                 UserTradingLog.seq == seq)),
                       UserTradingLog.stock_cd == stock_cd).\
                statement
            trading_log_df = pd.read_sql(trading_log_list_query, session.bind)
            # print(trading_log_df)

            # 매도가능수량 확인용 임시 변수 추가
            trading_log_df['tmp_quantity'] = \
                [r['transaction_quantity'] if r['transaction_type'] == '매수' else -r['transaction_quantity']
                 for i, r in trading_log_df.iterrows()]
            # print(trading_log_df)

            # 수정 시점 이전/이후 거래내역 확인
            prev_log_df = trading_log_df.loc[
                          (trading_log_df['date'] < new_transaction_date) |
                          ((trading_log_df['date'] == new_transaction_date) & (trading_log_df['seq'] < seq)), :]
            next_log_df = trading_log_df.loc[
                          (trading_log_df['date'] > new_transaction_date) |
                          ((trading_log_df['date'] == new_transaction_date) & (trading_log_df['seq'] > seq)), :]
            # print(prev_log_df)
            # print(next_log_df)

            # 매도 가능수량 확인
            available_quantity = sum(prev_log_df['tmp_quantity'])
            # print(f'available_quantity: {available_quantity}')
            # 이후 필요수량 확인
            required_quantity = 0
            tmp_quantity = 0
            for i, r in next_log_df.iterrows():
                tmp_quantity = tmp_quantity + r['tmp_quantity']
                # print(f'tmp_quantity: {tmp_quantity}')
                if tmp_quantity < required_quantity:
                    required_quantity = tmp_quantity
                    # print(f'required_quantity: {required_quantity}')
            # 필요 수량 정의(이후 필요수량 - 매도가능수량) <- 필요수량이 음수라 아래 코드에서 양수로 바꿔줌.
            required_quantity = -required_quantity - available_quantity
            # print(f'required_quantity: {required_quantity}')

            # 매도 가능수량 체크
            if trading_log.transaction_type == '매도':
                if transaction_quantity > available_quantity:
                    return {'status': '304', 'msg': get_status_msg('304')}  # 매도 가능수량 없음

            # 이후 거래내역 충돌 여부 확인
            if transaction_quantity < required_quantity:
                return {'status': '311', 'msg': get_status_msg('311')}  # 이후 거래를 위한 최소 보유 수량 미충족

            # 거래내역 수정
            trading_log.date = new_transaction_date
            trading_log.seq = seq
            trading_log.transaction_unit_price = transaction_unit_price
            trading_log.transaction_quantity = transaction_quantity
            trading_log.transaction_fee = transaction_fee_tax
            session.commit()

            # portfolio update
            update_single_stock_portfolio(user_code=user_code,
                                          begin_date=transaction_date,
                                          account_number=f'SELF_{portfolio_code}',
                                          securities_code='SELF',
                                          stock_cd=stock_cd)

            return {'status': '000', 'msg': get_status_msg('000')}


# 거래내역 삭제 API
class DeleteTradingLog(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('portfolio_code', type=int)
        parser.add_argument('stock_cd', type=str)
        parser.add_argument('transaction_date', type=str)
        parser.add_argument('seq', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        portfolio_code = args['portfolio_code']
        stock_cd = args['stock_cd']
        transaction_date = args['transaction_date']
        seq = args['seq']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        with session_scope() as session:
            # 기존 거래내역 불러오기
            trading_log = session.query(UserTradingLog). \
                filter(UserTradingLog.user_code == user_code,
                       UserTradingLog.account_number == f'SELF_{portfolio_code}',
                       UserTradingLog.date == transaction_date,
                       UserTradingLog.seq == seq,
                       UserTradingLog.stock_cd == stock_cd).\
                first()
            session.commit()

            if trading_log is None:
                return {'status': '307', 'msg': get_status_msg('307')}  # 일치하는 거래내역 없음

            # 기존 거래내역과 충돌하는지 확인
            # 기존 거래내역 리스트 불러오기(현재 수정내역 제외)
            trading_log_list_query = session.query(UserTradingLog). \
                filter(UserTradingLog.user_code == user_code,
                       UserTradingLog.account_number == f'SELF_{portfolio_code}',
                       not_(and_(UserTradingLog.date == transaction_date,
                                 UserTradingLog.seq == seq)),
                       UserTradingLog.stock_cd == stock_cd). \
                statement
            trading_log_df = pd.read_sql(trading_log_list_query, session.bind)
            # print(trading_log_df)

            # 매도가능수량 확인용 임시 변수 추가
            trading_log_df['tmp_quantity'] = \
                [r['transaction_quantity'] if r['transaction_type'] == '매수' else -r['transaction_quantity']
                 for i, r in trading_log_df.iterrows()]
            # print(trading_log_df)

            # 수정 시점 이전/이후 거래내역 확인
            prev_log_df = trading_log_df.loc[
                          (trading_log_df['date'] < transaction_date) |
                          ((trading_log_df['date'] == transaction_date) & (trading_log_df['seq'] < seq)), :]
            next_log_df = trading_log_df.loc[
                          (trading_log_df['date'] > transaction_date) |
                          ((trading_log_df['date'] == transaction_date) & (trading_log_df['seq'] > seq)), :]
            # print(prev_log_df)
            # print(next_log_df)

            # 매도 가능수량 확인
            available_quantity = sum(prev_log_df['tmp_quantity'])
            # print(f'available_quantity: {available_quantity}')
            # 이후 필요수량 확인
            required_quantity = 0
            tmp_quantity = 0
            for i, r in next_log_df.iterrows():
                tmp_quantity = tmp_quantity + r['tmp_quantity']
                # print(f'tmp_quantity: {tmp_quantity}')
                if tmp_quantity < required_quantity:
                    required_quantity = tmp_quantity
                    # print(f'required_quantity: {required_quantity}')
            # 필요 수량 정의(이후 필요수량 - 매도가능수량) <- 필요수량이 음수라 아래 코드에서 양수로 바꿔줌.
            required_quantity = -required_quantity - available_quantity
            # print(f'required_quantity: {required_quantity}')

            # 이후 거래내역 충돌 여부 확인
            if trading_log.transaction_type == '매수':
                if required_quantity > 0:
                    return {'status': '311', 'msg': get_status_msg('311')}  # 이후 거래를 위한 최소 보유 수량 미충족

            # 거래내역 삭제
            session.query(UserTradingLog). \
                filter(UserTradingLog.user_code == user_code,
                       UserTradingLog.account_number == f'SELF_{portfolio_code}',
                       UserTradingLog.date == transaction_date,
                       UserTradingLog.seq == seq,
                       UserTradingLog.stock_cd == stock_cd). \
                delete()

            # portfolio map 삭제(남아있는 거래내역 없는 경우)
            trading_log_all = session.query(UserTradingLog). \
                filter(UserTradingLog.user_code == user_code,
                       UserTradingLog.account_number == f'SELF_{portfolio_code}',
                       UserTradingLog.stock_cd == stock_cd). \
                all()

            if len(trading_log_all) == 0:
                session.query(UserPortfolioMap). \
                    filter(UserPortfolioMap.user_code == user_code,
                           UserPortfolioMap.account_number == f'SELF_{portfolio_code}',
                           UserPortfolioMap.stock_cd == stock_cd). \
                    delete()

            # db commit
            session.commit()

            # portfolio update
            update_single_stock_portfolio(user_code=user_code,
                                          begin_date=transaction_date,
                                          account_number=f'SELF_{portfolio_code}',
                                          securities_code='SELF',
                                          stock_cd=stock_cd)

            return {'status': '000', 'msg': get_status_msg('000')}


# 포트폴리오별 종목 조회 API
class GetPortfolioStockList(Resource):
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
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        sql = f'select a.user_code, a.portfolio_code, a.securities_code, a.stock_cd, b.stock_nm, ' \
              f'        case when a.securities_code like "SELF%" then "직접입력" ' \
              f'             else c.securities_nm end as securities_nm ' \
              f'from user_portfolio_map as a ' \
              f'join (select stock_nm, ' \
              f'             case when market in ("KOSPI", "KOSDAQ", "ETF", "ETN") then substr(stock_cd, 2, 6) ' \
              f'             else stock_cd end as stock_cd ' \
              f'      from stock_autocomplete_name_list) as b ' \
              f'	on a.stock_cd = b.stock_cd ' \
              f'left join securities_code as c ' \
              f'	on a.securities_code = c.securities_code ' \
              f'where a.user_code = {user_code} ' \
              f'and a.portfolio_code = {portfolio_code} ' \
              f'order by c.securities_nm, b.stock_nm'
        user_portfolio_map = exec_query(sql)

        return {'status': '000', 'msg': get_status_msg('000'),
                'stock_list': [{'user_code': e[0],
                                'portfolio_code': e[1],
                                'securities_code': e[2],
                                'securities_nm': e[5],
                                'stock_cd': e[3],
                                'stock_nm': e[4]} for e in user_portfolio_map]}


# 증권사별 거래내역 삭제 API
class DeleteTradingLogBySecurities(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('securities_code', type=str)
        parser.add_argument('infotech_code', type=str)
        parser.add_argument('api_key', type=str)

        args = parser.parse_args()

        user_code = args['user_code']
        securities_code = args['securities_code']
        infotech_code = args['infotech_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status != '000':
            return {'status': check_status, 'msg': get_status_msg(check_status)}

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

            # tmp_user_portfolio 체크하기 <- 이거 user_tr_insert_status 참고하도록 수정해야함.
            tmp_user_portfolio = session.query(TmpUserPortfolio). \
                filter(TmpUserPortfolio.user_code == user_code). \
                first()
            session.commit()

            if tmp_user_portfolio is not None:
                return {'status': '209', 'msg': get_status_msg('209')}  # 에러코드: 포트폴리오 업데이트 진행 중

        with session_scope() as session:
            # 데이터 삭제
            # user_portfolio
            session.query(UserPortfolio).\
                filter(UserPortfolio.user_code == user_code,
                       UserPortfolio.securities_code == securities_code).\
                delete()
            session.commit()
            # tmp_user_portfolio
            session.query(TmpUserPortfolio).\
                filter(TmpUserPortfolio.user_code == user_code,
                       TmpUserPortfolio.securities_code == securities_code).\
                delete()
            session.commit()
            # user_portfolio_map
            session.query(UserPortfolioMap).\
                filter(UserPortfolioMap.user_code == user_code,
                       UserPortfolioMap.securities_code == securities_code).\
                delete()
            session.commit()
            # tmp_user_portfolio_map
            session.query(TmpUserPortfolioMap).\
                filter(TmpUserPortfolioMap.user_code == user_code,
                       TmpUserPortfolioMap.securities_code == securities_code).\
                delete()
            session.commit()
            # user_trading_log
            session.query(UserTradingLog).\
                filter(UserTradingLog.user_code == user_code,
                       UserTradingLog.securities_code == securities_code).\
                delete()
            session.commit()
            # tmp_user_trading_log
            session.query(TmpUserTradingLog).\
                filter(TmpUserTradingLog.user_code == user_code,
                       TmpUserTradingLog.securities_code == securities_code).\
                delete()
            session.commit()
            # user_trading_log_raw
            session.query(UserTradingLogRaw).\
                filter(UserTradingLogRaw.user_code == user_code,
                       UserTradingLogRaw.securities_code == securities_code).\
                delete()
            session.commit()

            return {'status': '000', 'msg': get_status_msg('000')}


class SetScrapLog(Resource):  # 스크래핑 로그 수집
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('infotech_code', type=str)
        parser.add_argument('account_number', type=str)
        parser.add_argument('os', type=str)
        parser.add_argument('input', type=str)
        parser.add_argument('output', type=str)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        infotech_code = args['infotech_code']
        account_number = args['account_number']
        os = args['os']
        input = args['input']
        output = args['output']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '106':  # 일치하는 회원이 없는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        elif check_status == '105':  # API KEY가 일치하지 않는 경우
            return {'status': check_status, 'msg': get_status_msg(check_status)}
        else:  # 유효한 사용자 및 API Key인 경우
            pass

        # securities_code 가져오기
        with session_scope() as session:
            # infotech_code 체크하기
            _infotech_securities_code = session.query(SecuritiesCode). \
                filter(SecuritiesCode.infotech_code == infotech_code). \
                first()
            session.commit()

            if _infotech_securities_code is None:
                return {'status': '309', 'msg': get_status_msg('309')}  # 에러코드: 올바르지 않은 증권사 코드
            else:
                securities_code = _infotech_securities_code.securities_code

        # status_code 생성
        if output.find('"errYn":"N"') > 0 or output.find('"errYn": "N"') > 0:
            status_code = '000'  # success
        elif output.find('"errYn":"Y"') > 0 or output.find('"errYn": "Y') > 0:
            status_code = '001'  # error
        else:
            print(output)
            status_code = '002'  # unknown error

        # 스크래핑 로그 수집 / 전처리 / app_scrap_log 저장
        with session_scope() as session:
            new_log = AppScrapLog(user_code=user_code,
                                  dtim=datetime.today().strftime('%Y%m%d%H%M%S%f'),
                                  securities_code=securities_code,
                                  account_number=account_number,
                                  status_code=status_code,
                                  os=os,
                                  input=input,
                                  output=output[:10000])  # 길이 제한
            session.add(new_log)
            session.commit()

        return {'status': '000', 'msg': get_status_msg('000')}
