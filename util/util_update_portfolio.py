import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from db.db_model import session_scope, UserPortfolio, DateWorkingDay, UsDateWorkingDay, UserTradingLog, TmpUserPortfolio
from db.db_connect import get_df_from_db, exec_query, insert_data
from util.util_get_country import get_country
from sqlalchemy import func
from sqlalchemy.orm import aliased


# def get_close_price(stock_type, date, stock_cd):
#     # db session 설정
#     db_session = get_sqlalchemy_session()
#
#     # date 보정
#     date = str(exec_query(f'select max(date) from stock_daily_technical where date <= {date}')[0][0])
#
#     if stock_type == 'domestic_stock':
#         close_price = db_session.query(StockDailyTechnical). \
#             filter(StockDailyTechnical.date == date,
#                    StockDailyTechnical.stock_cd == stock_cd).first()
#         db_session.commit()
#         if close_price is not None:
#             return_value = close_price.close_price
#             db_session.close()
#             return return_value
#     elif stock_type == 'domestic_etf':
#         close_price = db_session.query(StockDailyEtf). \
#             filter(StockDailyEtf.date == date,
#                    StockDailyEtf.stock_cd == stock_cd).first()
#         db_session.commit()
#         if close_price is not None:
#             return_value = close_price.close_price
#             db_session.close()
#             return return_value
#     elif stock_type == 'domestic_etn':
#         close_price = db_session.query(StockDailyEtn). \
#             filter(StockDailyEtn.date == date,
#                    StockDailyEtn.stock_cd == stock_cd).first()
#         db_session.commit()
#         if close_price is not None:
#             return_value = close_price.close_price
#             db_session.close()
#             return return_value
#     else:
#         return None
#
#
# def update_portfolio(user_code, begin_date, account_number, securities_code, stock_cd):
#     # db session 설정
#     db_session = get_sqlalchemy_session()
#     # db connection get
#     conn = get_sqlalchemy_connection()
#
#     # today
#     today = datetime.today().strftime('%Y%m%d')
#
#     # 최근 종가 수집일자 확인
#     max_date = str(exec_query(f'select max(date) from stock_daily_technical where date >= {begin_date}')[0][0])
#
#     if max_date == 'None':  # <- str()을 씌워서 문자가 되어버렸네 ㅋㅋㅋㅋ
#         max_date = today
#     else:
#         # max_date를 마지막으로 종가가 수집된 일의 익일로 정해줌
#         max_date = (datetime.strptime(max_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
#
#     # end_date 설정
#     end_date = max(today, max_date)
#
#     # 겹치는 구간 portfolio 삭제
#     exec_query(f'delete '
#                f'from user_portfolio '
#                f'where user_code = {user_code} '
#                f'and account_number = "{account_number}" '
#                f'and stock_cd = "{stock_cd}" '
#                f'and date >= "{begin_date}" '
#                f'and date <= "{end_date}" ')
#
#     # date_list 생성
#     date_span = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(begin_date, '%Y%m%d')).days
#     date_list = [(datetime.strptime(begin_date, '%Y%m%d')+timedelta(days=i)).strftime('%Y%m%d') for i in range(date_span+1)]
#
#     # prev_date 생성
#     sql = f'select max(date) as date from stock_daily_technical where date < "{begin_date}"'
#     prev_date = str(pd.read_sql_query(sql, conn)['date'][0])
#
#     # date list for loop
#     for date in date_list:
#         # 해당 date의 trading log 불러오기
#         sql = f'select * ' \
#               f'from user_trading_log ' \
#               f'where 1=1 ' \
#               f'and user_code = {user_code} ' \
#               f'and account_number = "{account_number}" ' \
#               f'and stock_cd = "{stock_cd}" ' \
#               f'and ((begin_date <= "{date}" and end_date > "{date}") or (begin_date = "{date}" and end_date = "{date}"))'
#         trading_log = pd.read_sql_query(sql, conn)
#
#         # 전일자의 portfolio 가져오기
#         sql = f'select * ' \
#               f'from user_portfolio ' \
#               f'where 1=1 ' \
#               f'and user_code = {user_code} ' \
#               f'and account_number = "{account_number}" ' \
#               f'and stock_cd = "{stock_cd}" ' \
#               f'and date = "{prev_date}" '
#         prev_portfolio = pd.read_sql_query(sql, conn)
#
#         # trading log for loop: portfolio 생성
#         for i, r in trading_log.iterrows():
#             # 해당 일자 portfolio에 해당 종목이 있는지 확인. <- 한 날짜에 두번 이상 거래할 경우 있음.
#             existing_pf = db_session.query(UserPortfolio).filter(UserPortfolio.user_code == user_code,
#                                                                  UserPortfolio.date == date,
#                                                                  UserPortfolio.account_number == account_number,
#                                                                  UserPortfolio.stock_cd == r['stock_cd']).first()
#             db_session.commit()
#
#             # 전일 종가 초기화
#             prev_close_price = get_close_price(r['stock_type'], prev_date, r['stock_cd'])
#
#             # 1주 전 영업일 확인
#             tmp_prev_1w_date = (datetime.strptime(str(date), '%Y%m%d') - timedelta(days=7)).strftime('%Y%m%d')
#             sql = f'select max(date) as date from stock_daily_technical where date <= {tmp_prev_1w_date}'
#             prev_1w_date = str(pd.read_sql_query(sql, conn)['date'][0])
#
#             # 1주전 종가 초기화
#             prev_1w_close_price = get_close_price(r['stock_type'], prev_1w_date, r['stock_cd'])
#
#             # 평가 손익 초기화
#             total_value = 0
#             if prev_close_price is not None:
#                 total_value = r['holding_quantity'] * prev_close_price
#
#             # transaction_type 초기화
#             transaction_type = r['transaction_type']
#
#             # 실현 손익, 판매주식 매입금액, 신규 매입금액 초기화
#             realized_profit_loss = 0
#             new_purchase_amount = 0
#             purchase_amount_of_stocks_to_sell = 0
#             if r['begin_date'] == str(date):
#                 realized_profit_loss = realized_profit_loss - r['transaction_fee'] - r['transaction_tax']
#                 if transaction_type == '매도':
#                     realized_profit_loss = realized_profit_loss + r['transaction_quantity']*r['transaction_unit_price']
#                     purchase_amount_of_stocks_to_sell = purchase_amount_of_stocks_to_sell + \
#                                                         r['transaction_quantity']*r['avg_purchase_price']
#                 elif transaction_type == '매수':
#                     new_purchase_amount = new_purchase_amount + r['transaction_quantity']*r['transaction_unit_price']
#                 elif transaction_type == '배당금입금':
#                     realized_profit_loss = realized_profit_loss + r['transaction_unit_price']
#
#             # etc 항목의 경우 portfolio에 포함하지 않음 - 현재 서비스 하지 않는 해외주식 등
#             if r['stock_type'] == 'etc':
#                 continue
#
#             # 최초 매입일자 세팅
#             if prev_portfolio.shape[0] == 0:
#                 first_purchase_date = str(date)
#             elif r['stock_cd'] not in list(prev_portfolio['stock_cd']):
#                 first_purchase_date = str(date)
#             else:
#                 first_purchase_date = prev_portfolio.loc[
#                     prev_portfolio['stock_cd'] == r['stock_cd'], 'first_purchase_date'].values[0]
#
#             # 보유기간 세팅
#             retention_period = (datetime.strptime(str(date), '%Y%m%d') -
#                                 datetime.strptime(first_purchase_date, '%Y%m%d')).days + 1
#
#             # 해당 일자 최초 거래일 경우: portfolio 생성
#             if existing_pf is None:
#                 # portfolio 생성
#                 new_portfolio = UserPortfolio(user_code=user_code, date=date, account_number=account_number,
#                                               stock_cd=r['stock_cd'], securities_code=securities_code, stock_nm=r['stock_nm'],
#                                               holding_quantity=r['holding_quantity'],
#                                               avg_purchase_price=r['avg_purchase_price'],
#                                               prev_close_price=prev_close_price,
#                                               prev_1w_close_price=prev_1w_close_price,
#                                               total_value=total_value,
#                                               first_purchase_date=first_purchase_date,
#                                               retention_period=retention_period,
#                                               new_purchase_amount=new_purchase_amount,
#                                               realized_profit_loss=realized_profit_loss,
#                                               purchase_amount_of_stocks_to_sell=purchase_amount_of_stocks_to_sell,
#                                               unit_currency=r['unit_currency'],
#                                               update_dtim=datetime.today().strftime('%Y%m%d%H%M%S'))
#
#                 # insert
#                 db_session.add(new_portfolio)
#                 db_session.commit()
#
#             # 해당 일자 최초 거래가 아닐 경우: portfolio 업데이트
#             else:
#                 if transaction_type == '매수':
#                     existing_pf.holding_quantity = r['holding_quantity']
#                     existing_pf.avg_purchase_price = r['avg_purchase_price']
#                     existing_pf.total_value = total_value
#                     existing_pf.new_purchase_amount = existing_pf.new_purchase_amount + new_purchase_amount
#                 elif transaction_type == '매도':
#                     existing_pf.holding_quantity = r['holding_quantity']
#                     existing_pf.total_value = total_value
#                     existing_pf.realized_profit_loss = existing_pf.realized_profit_loss + realized_profit_loss
#                     existing_pf.purchase_amount_of_stocks_to_sell = \
#                         existing_pf.purchase_amount_of_stocks_to_sell + purchase_amount_of_stocks_to_sell
#                 elif transaction_type == '배당금입금':
#                     existing_pf.realized_profit_loss = existing_pf.realized_profit_loss + realized_profit_loss
#                 elif transaction_type == '배당세출금':
#                     existing_pf.realized_profit_loss = existing_pf.realized_profit_loss + realized_profit_loss
#
#                 existing_pf.update_dtim = datetime.today().strftime('%Y%m%d%H%M%S')
#                 db_session.commit()
#
#         prev_date = date
#
#     db_session.close()
#     conn.close()


def get_receiving_transaction_list():
    return ['매수', '유상주입고', '무상주입고', '공모주입고', '타사대체입고', '대체입고', '액면분할병합입고',
            '감자입고', '회사분할입고', '배당입고', '해외주식매수']


def get_releasing_transaction_list():
    return ['매도', '타사대체출고', '대체출고', '액면분할병합출고', '감자출고', '해외주식매도']


def get_dividend_transaction_list():
    return ['배당', '배상세출금', '해외주식배당']  # 배당세 출금은 2021.06.23 현재 거래내역이 사실상 존재하지 않아 어떻게 처리될지 모른다.


def util_get_avg_purchase_price(user_code, account_number, stock_cd, extra_tr=None):
    # 거래내역 불러오기
    sql = f'select stock_cd, transaction_date, transaction_type, transaction_unit_price, transaction_quantity ' \
          f'from user_trading_log ' \
          f'where user_code = {user_code} ' \
          f'and account_number = "{account_number}" ' \
          f'and stock_cd = "{stock_cd}" '
    tr_log = get_df_from_db(sql)

    if extra_tr is not None:
        tr_log = pd.concat([tr_log, extra_tr])

    avg_purchase_price = 0
    holding_quantity = 0
    for i, r in tr_log.iterrows():
        if r['transaction_type'] in get_releasing_transaction_list():
            avg_purchase_price = (avg_purchase_price * holding_quantity +
                                  r['transaction_unit_price'] * r['transaction_quantity']) / \
                                 (holding_quantity + r['transaction_quantity'])
            holding_quantity = holding_quantity + r['transaction_quantity']

    if holding_quantity == 0:
        return None
    else:
        return avg_purchase_price


def util_get_recent_split_release_quantity(user_code, account_number, stock_cd, extra_tr=None):
    # 거래내역 불러오기
    sql = f'select stock_cd, transaction_date, transaction_type, transaction_unit_price, transaction_quantity ' \
          f'from user_trading_log ' \
          f'where user_code = {user_code} ' \
          f'and account_number = "{account_number}" ' \
          f'and stock_cd = "{stock_cd}" '
    tr_log = get_df_from_db(sql)

    if extra_tr is not None:
        tr_log = pd.concat([tr_log, extra_tr])

    tr_log = tr_log.loc[tr_log['transaction_type'].values == '액면분할병합출고', :]
    tr_log = tr_log.sort_values(by='transaction_date', ascending=False)

    if tr_log.shape[0] == 0:
        return None
    else:
        return tr_log['transaction_quantity'][0]


def util_get_pulled_transaction_date(date):
    with session_scope() as session:
        # kr working day
        kr_working_day_1 = aliased(DateWorkingDay)
        kr_working_day_2 = aliased(DateWorkingDay)
        kr_working_day = session.query(kr_working_day_1.working_day.label('date'),
                                       kr_working_day_2.working_day.label('pulled_transaction_date')). \
            join(kr_working_day_2, kr_working_day_1.seq == kr_working_day_2.seq + 2). \
            filter(kr_working_day_1.working_day == date).first()
        session.commit()
        return kr_working_day[1]


def update_single_stock_portfolio(user_code, begin_date, account_number, securities_code, stock_cd,
                                  country=None, tmp_portfolio=False):
    # print(f'update_single_stock_portfolio - user_code: {user_code}, begin_date: {begin_date}, '
    #       f'account_number: {account_number}, securities_code: {securities_code}, stock_cd: {stock_cd}, '
    #       f'country: {country}, tmp_portfolio: {tmp_portfolio}')

    # user_portfolio, user_trading_log table 설정
    if tmp_portfolio:
        user_portfolio_table = 'tmp_user_portfolio'
    else:
        user_portfolio_table = 'user_portfolio'

    # begin_date
    begin_date = str(begin_date)

    # country 설정
    if country is None:
        country = get_country(stock_cd)
    # print(f'country: {country}')

    # end_date 설정
    if country == 'KR':
        end_date = exec_query(f'select max(working_day) from date_working_day')[0][0]
    else:
        end_date = exec_query(f'select max(working_day) from us_date_working_day')[0][0]
    end_date = str(end_date)

    # begin_date가 end_date보다 크다면 함수 실행 종료
    if end_date < begin_date:
        # print('end_date < begin_date')
        return None

    # prev_date 설정
    prev_date = (datetime.strptime(begin_date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')

    # 겹치는 구간 portfolio 삭제
    exec_query(f'delete '
               f'from {user_portfolio_table} '
               f'where user_code = {user_code} '
               f'and account_number = "{account_number}" '
               f'and securities_code = "{securities_code}" '
               f'and stock_cd = "{stock_cd}" '
               f'and date >= "{begin_date}" '
               f'and date <= "{end_date}" ')

    # 전일 포트폴리오 가져오기
    sql = f'select * ' \
          f'from {user_portfolio_table} ' \
          f'where 1=1 ' \
          f'and user_code = {user_code} ' \
          f'and date = {prev_date} ' \
          f'and account_number = "{account_number}" ' \
          f'and stock_cd = "{stock_cd}" '
    prev_portfolio = get_df_from_db(sql)

    # 거래내역 가져오기
    sql = f'select * ' \
          f'from user_trading_log ' \
          f'where 1=1 ' \
          f'and user_code = {user_code} ' \
          f'and account_number = "{account_number}" ' \
          f'and date >= "{begin_date}" ' \
          f'and stock_cd = "{stock_cd}" ' \
          f'and stock_type in ("domestic_stock", "domestic_etf", "domestic_etn", "us_stock")'
    trading_data = get_df_from_db(sql)
    # print(trading_data)

    # prev_portfolio가 없을 경우(최초 포트폴리오 등록일 경우 => begin_date가 20110101일 경우) begin_date, prev_date 재설정
    # 직접입력한 거래내역을 delete할 경우, prev_portfolio도 없고 user_trading_log도 없는 케이스 존재. 해당 케이스 제외 위해 trading_data 조건 추가
    if len(prev_portfolio) == 0 and len(trading_data) != 0:
        begin_date = str(min(trading_data['date']))
        # prev_date 설정
        prev_date = (datetime.strptime(begin_date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')

    # 가격 가져오기
    price_begin_date = (datetime.strptime(prev_date, '%Y%m%d') - timedelta(days=7 + 1)).strftime('%Y%m%d')
    if country == 'KR':
        sql = f'select map.date as date, a.date as working_day, ' \
              f'        a.stock_cd, a.stock_nm, a.close_price ' \
              f'from date_working_day_mapping as map ' \
              f'join (select date, stock_cd, stock_nm, close_price ' \
              f'      from stock_daily_technical ' \
              f'      where 1=1 ' \
              f'      and date >= {price_begin_date} ' \
              f'      and stock_cd = "{stock_cd}" ' \
              f'      ' \
              f'      union all ' \
              f'      ' \
              f'      select date, stock_cd, stock_nm, close_price ' \
              f'      from stock_daily_etf ' \
              f'      where 1=1 ' \
              f'      and date >= {price_begin_date} ' \
              f'      and stock_cd = "{stock_cd}" ' \
              f'      ' \
              f'      union all ' \
              f'      ' \
              f'      select date, stock_cd, stock_nm, close_price ' \
              f'      from stock_daily_etn ' \
              f'      where 1=1 ' \
              f'      and date >= {price_begin_date} ' \
              f'      and stock_cd = "{stock_cd}" ) as a ' \
              f'    on map.working_day = a.date '
        price_data = get_df_from_db(sql)
    else:
        sql = f'select map.date as date, a.date as working_day, ' \
              f'        a.stock_cd, b.stock_nm, a.close_price ' \
              f'from us_date_working_day_mapping as map ' \
              f'join (select date, stock_cd, close_price ' \
              f'      from us_stock_daily_price ' \
              f'      where 1=1 ' \
              f'      and date >= {price_begin_date} ' \
              f'      and stock_cd = "{stock_cd}") as a ' \
              f'    on map.working_day = a.date ' \
              f'join (select stock_cd, stock_nm' \
              f'      from us_stock_info ' \
              f'      where stock_cd = "{stock_cd}") as b' \
              f'    on a.stock_cd = b.stock_cd '
        price_data = get_df_from_db(sql)

    # 해당 stock_cd에 대해 데이터가 존재하지 않을 경우 None 리턴하고 끝냄
    if price_data.shape[0] == 0:
        # print('price_data.shape[0] == 0')
        return None

    # date 컬럼 타입 변경
    price_data['date'] = price_data['date'].apply(lambda x: str(x))

    # 신규 컬럼 생성
    price_data['price_prev_1w'] = price_data.groupby('stock_cd')['close_price'].shift(7)

    # date_list 생성
    date_span = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(begin_date, '%Y%m%d')).days
    date_list = [(datetime.strptime(begin_date, '%Y%m%d') + timedelta(days=i)).strftime('%Y%m%d') for i in
                 range(date_span + 1)]

    # new_portfolio, prev_portfolio 생성
    new_portfolio = prev_portfolio.copy()
    prev_row = prev_portfolio.copy()

    if prev_row.shape[0] == 0:
        tr_log = trading_data.loc[trading_data['date'] == begin_date, :]
        if tr_log.shape[0] != 0:
            tmp_stock_nm = tr_log['stock_nm'].values[0]
            tmp_unit_currency = tr_log['unit_currency'].values[0]
            tmp_country = tr_log['country'].values[0]
        else:
            tmp_stock_nm = ''
            if country == 'KR':
                tmp_unit_currency = 'KRW'
                tmp_country = 'KR'
            else:
                tmp_unit_currency = 'USD'
                tmp_country = 'US'
        tmp_df = pd.DataFrame([[user_code, prev_date, account_number, stock_cd, securities_code, tmp_stock_nm, 0, 0, 0,
                                0, 0, None, 0, 0, 0, 0, tmp_unit_currency, 0, tmp_country]])
        tmp_df.columns = prev_row.columns
        prev_row = tmp_df

    # 거래내역 목록 정의
    receiving_transaction_list = get_receiving_transaction_list()
    releasing_transaction_list = get_releasing_transaction_list()
    dividend_transaction_list = get_dividend_transaction_list()  # 배당세 출금은 2021.06.23 현재 거래내역이 사실상 존재하지 않아 어떻게 처리될지 모른다.

    # portfolio 생성
    for date in date_list:
        new_row = prev_row.copy()
        tr_log = trading_data.loc[trading_data['date'] == date, :]
        price = price_data.loc[price_data['date'] == date, :]

        # new_purchase_amount, realized_profit_loss, purchase_amount_of_stocks_to_sell 초기화
        new_row['new_purchase_amount'] = 0
        new_row['realized_profit_loss'] = 0
        new_row['purchase_amount_of_stocks_to_sell'] = 0

        # date update
        new_row['date'] = date

        # close_price, prev_1w_close_price, stock_nm update
        if price.shape[0] != 0:
            new_row['close_price'] = price['close_price'].values[0]
            new_row['prev_1w_close_price'] = price['price_prev_1w'].values[0]
            new_row['stock_nm'] = price['stock_nm'].values[0]

        # retention_period update
        if new_row['first_purchase_date'].values[0] is not None:
            new_row['retention_period'] = new_row['retention_period'].values[0] + 1

        # update_dtim update
        new_row['update_dtim'] = datetime.today().strftime('%Y%m%d%H%M%S')

        # 매매기록이 있을 경우 데이터 업데이트
        for i, r in tr_log.iterrows():
            tmp_row = new_row.copy()

            # stock_nm update
            new_row['stock_nm'] = r['stock_nm']

            # 매도거래내역의 경우, 기존 holding quantity가 충분한지 확인
            if r['transaction_type'] in releasing_transaction_list:
                tmp_holding_quantity = tmp_row['holding_quantity'].values[0]
                if tmp_holding_quantity == 0:  # 매도할 내역 없으면 패스
                    continue
                elif tmp_holding_quantity - r['transaction_quantity'] < 0:  # 매도할 내역 부족하면 거래내역을 줄여준다.
                    r['transaction_fee'] = r['transaction_fee'] * (
                                tmp_holding_quantity / r['transaction_quantity'])  # 비율 맞춰 처리해줌
                    r['transaction_tax'] = r['transaction_tax'] * (tmp_holding_quantity / r['transaction_quantity'])
                    r['transaction_quantity'] = tmp_holding_quantity

            # holding_quantity update
            if r['transaction_type'] in receiving_transaction_list:
                new_row['holding_quantity'] = tmp_row['holding_quantity'] + r['transaction_quantity']
            elif r['transaction_type'] in releasing_transaction_list:
                new_row['holding_quantity'] = tmp_row['holding_quantity'] - r['transaction_quantity']
            elif r['transaction_type'] in dividend_transaction_list:
                pass
            elif r['transaction_type'] == '주식합병출고':
                # 주식합병출고 거래내역이 있으면, 해당 거래내역이 존재하는 날부터 포트폴리오에 포함되지 않도록 보유수량을 0으로 바꿔줌
                new_row['holding_quantity'] = 0
            elif r['transaction_type'] == '주식합병입고':
                new_row['holding_quantity'] = tmp_row['holding_quantity'] + r['transaction_quantity']
            else:
                raise Exception

            # avg_purchase_price update
            if r['transaction_type'] in receiving_transaction_list:
                # (기존 매입금액 + 신규 매입금액 + 수수료/세금) / (기존 매입주수 + 신규 매입주수)
                new_row['avg_purchase_price'] = \
                    ((tmp_row['avg_purchase_price'] * tmp_row['holding_quantity']) +
                     (r['transaction_quantity'] * r['transaction_unit_price']) +
                     (r['transaction_fee'] + r['transaction_tax'])) / \
                    (tmp_row['holding_quantity'] + r['transaction_quantity'])
            elif r['transaction_type'] == '주식합병입고':
                # 피합병 종목의 거래내역을 가지고 와서 평균매입단가, 최초매입일자, 보유기간을 계산한다.
                # 같은 날 주식합병출고된 종목 = 피합병종목으로 본다.
                # 2영업일 이전 날짜 구하기(주식합병출고는 2영업일 이전 처리되고, 입고는 당일처리되므로, 2영업일 전 날짜로 검색해야함)
                pulled_transaction_date = util_get_pulled_transaction_date(date)

                # 주식합병출고 확인
                sql = f'select * ' \
                      f'from user_trading_log ' \
                      f'where 1=1 ' \
                      f'and user_code = {user_code} ' \
                      f'and account_number = "{account_number}" ' \
                      f'and date = {pulled_transaction_date} ' \
                      f'and transaction_type = "주식합병출고" ' \
                      f'and stock_type in ("domestic_stock", "domestic_etf", "domestic_etn", "us_stock")'
                merged_company = get_df_from_db(sql)
                # print(merged_company)
                # 같은 날 주식합병출고된 종목이 1개가 아닐 경우, 에러를 뱉는다.
                if merged_company.shape[0] != 1:
                    raise Exception

                # 종목코드 추출
                merged_stock_cd = merged_company['stock_cd'].values[0]

                sql = f'select * ' \
                      f'from user_trading_log ' \
                      f'where 1=1 ' \
                      f'and user_code = {user_code} ' \
                      f'and account_number = "{account_number}" ' \
                      f'and stock_cd = "{merged_stock_cd}" ' \
                      f'and stock_type in ("domestic_stock", "domestic_etf", "domestic_etn", "us_stock")'
                merged_corp_tr_log = get_df_from_db(sql)
                # print(merged_corp_tr_log)

                mc_holding_quantity = 0
                mc_avg_purchase_price = 0
                mc_first_purchase_date = None
                for j, mc_tr in merged_corp_tr_log.iterrows():
                    # print(mc_tr)

                    if mc_holding_quantity == 0 and \
                            mc_tr['transaction_type'] in receiving_transaction_list:
                        mc_first_purchase_date = mc_tr['date']

                    if mc_tr['transaction_type'] in receiving_transaction_list:
                        mc_avg_purchase_price = ((mc_avg_purchase_price * mc_holding_quantity) +
                                                 (mc_tr['transaction_unit_price'] * mc_tr['transaction_quantity']) +
                                                 mc_tr['transaction_fee'] + mc_tr['transaction_tax']) / \
                                                (mc_holding_quantity + mc_tr['transaction_quantity'])
                        mc_holding_quantity = mc_holding_quantity + mc_tr['transaction_quantity']
                    elif mc_tr['transaction_type'] in releasing_transaction_list:
                        mc_holding_quantity = mc_holding_quantity - mc_tr['transaction_quantity']

                    if mc_holding_quantity <= 0:
                        mc_holding_quantity = 0
                        mc_avg_purchase_price = 0
                        mc_first_purchase_date = None

                # 최초매입일자가 None이면 합병일자를 넣어준다.
                if mc_first_purchase_date is None:
                    mc_first_purchase_date = date
                # 피합병 holding_quantity와 mc_holding_quantity가 동일하면 계산한 값을 사용한다.
                if mc_holding_quantity == merged_company['transaction_quantity'].values[0]:
                    new_row['first_purchase_date'] = mc_first_purchase_date
                    new_row['retention_period'] = \
                        (datetime.strptime(date, '%Y%m%d') - datetime.strptime(mc_first_purchase_date, '%Y%m%d')).days + 1
                    new_row['avg_purchase_price'] = (mc_avg_purchase_price * mc_holding_quantity) / r['transaction_quantity']
                else:
                    new_row['first_purchase_date'] = mc_first_purchase_date
                    new_row['retention_period'] = 1
                    new_row['avg_purchase_price'] = r['transaction_unit_price']

            # first_purchase_date / retention_period update
            if new_row['first_purchase_date'].values[0] is None and r['transaction_type'] in receiving_transaction_list:
                new_row['first_purchase_date'] = date
                new_row['retention_period'] = 1

            # new_purchase_amount update
            if r['transaction_type'] in receiving_transaction_list:
                new_row['new_purchase_amount'] = tmp_row['new_purchase_amount'] + \
                                                 (r['transaction_quantity'] * r['transaction_unit_price']) + \
                                                 (r['transaction_fee'] + r['transaction_tax'])

            # realized_profit_loss update
            if r['transaction_type'] in (releasing_transaction_list + dividend_transaction_list):
                new_row['realized_profit_loss'] = \
                    tmp_row['realized_profit_loss'] + (r['transaction_quantity'] * r['transaction_unit_price']) - \
                    (r['transaction_fee'] + r['transaction_tax'])

            # purchase_amount_of_stocks_to_sell
            if r['transaction_type'] in releasing_transaction_list:
                new_row['purchase_amount_of_stocks_to_sell'] = \
                    tmp_row['purchase_amount_of_stocks_to_sell'] + \
                    (r['transaction_quantity'] * tmp_row['avg_purchase_price'])

            # 청약 케이스: close_price 설정
            if new_row['close_price'].values[0] == 0:
                new_row['close_price'] = r['transaction_unit_price']

        # total_value update
        new_row['total_value'] = new_row['holding_quantity'].values[0] * new_row['close_price'].values[0]

        # portfolio 상에서 종목 보유 내역이 0 이하일 경우, 0으로 바꿔줌.
        if new_row['holding_quantity'].values[0] <= 0:
            new_row['holding_quantity'] = 0

        # 종목 보유량이 0이고, 실현 손익도 없다면 portfolio에 저장하지 않음.
        if new_row['holding_quantity'].values[0] == 0 and new_row['realized_profit_loss'].values[0] == 0:
            new_row['first_purchase_date'] = None
            pass
        else:
            new_portfolio = pd.concat([new_portfolio, new_row])

        # prev_row update
        prev_row = new_row

    # portfolio 데이터 업데이트
    # print(f'update_single_stock_portfolio - new_portfolio: {new_portfolio}')
    insert_data(new_portfolio, user_portfolio_table)
    # print('Done')


def update_user_portfolio_by_country(user_code, country, account_number=None, tmp_portfolio=False):
    with session_scope() as session:
        # 최근 포트폴리오 일자 확인
        if tmp_portfolio:
            if account_number is None:
                max_date = session.query(func.max(TmpUserPortfolio.date)). \
                    filter(TmpUserPortfolio.user_code == user_code,
                           TmpUserPortfolio.country == country). \
                    first()
                session.commit()
            else:
                max_date = session.query(func.max(TmpUserPortfolio.date)). \
                    filter(TmpUserPortfolio.user_code == user_code,
                           TmpUserPortfolio.country == country,
                           TmpUserPortfolio.account_number == account_number). \
                    first()
                session.commit()
        else:
            if account_number is None:
                max_date = session.query(func.max(UserPortfolio.date)). \
                    filter(UserPortfolio.user_code == user_code,
                           UserPortfolio.country == country). \
                    first()
                session.commit()
            else:
                max_date = session.query(func.max(UserPortfolio.date)). \
                    filter(UserPortfolio.user_code == user_code,
                           UserPortfolio.country == country,
                           UserPortfolio.account_number == account_number). \
                    first()
                session.commit()
        max_date = max_date[0]

        # 최근 영업일 확인
        if country == 'KR':
            max_working_day = session.query(func.max(DateWorkingDay.working_day)).first()
            max_working_day = max_working_day[0]
            session.commit()
        elif country == 'US':
            max_working_day = session.query(func.max(UsDateWorkingDay.working_day)).first()
            max_working_day = max_working_day[0]
            session.commit()
        else:
            return None

    # 최근 포트폴리오 일자와 최근 영업일이 다를 경우, 포트폴리오 업데이트 해줌
    # 새로운 거래내역이 들어올 경우, 아래쪽에서 포트폴리오 업데이트를 진행하므로, 여기에서는 날짜만 체크해서 업데이트 해 준다.
    update_list_df = None
    if max_date is not None and max_date != max_working_day:
        with session_scope() as session:
            # 포트폴리오 업데이트 리스트 확인
            if tmp_portfolio:
                if account_number is None:
                    update_list_query = session.query(TmpUserPortfolio). \
                        filter(TmpUserPortfolio.user_code == user_code,
                               TmpUserPortfolio.date == max_date,
                               TmpUserPortfolio.holding_quantity != 0,
                               TmpUserPortfolio.country == country)
                    update_list_df = pd.read_sql(update_list_query.statement, session.bind)
                    session.commit()
                else:
                    update_list_query = session.query(TmpUserPortfolio). \
                        filter(TmpUserPortfolio.user_code == user_code,
                               TmpUserPortfolio.date == max_date,
                               TmpUserPortfolio.holding_quantity != 0,
                               TmpUserPortfolio.country == country,
                               TmpUserPortfolio.account_number == account_number)
                    update_list_df = pd.read_sql(update_list_query.statement, session.bind)
                    session.commit()
            else:
                if account_number is None:
                    update_list_query = session.query(UserPortfolio). \
                        filter(UserPortfolio.user_code == user_code,
                               UserPortfolio.date == max_date,
                               UserPortfolio.holding_quantity != 0,
                               UserPortfolio.country == country)
                    update_list_df = pd.read_sql(update_list_query.statement, session.bind)
                    session.commit()
                else:
                    update_list_query = session.query(UserPortfolio). \
                        filter(UserPortfolio.user_code == user_code,
                               UserPortfolio.date == max_date,
                               UserPortfolio.holding_quantity != 0,
                               UserPortfolio.country == country,
                               UserPortfolio.account_number == account_number)
                    update_list_df = pd.read_sql(update_list_query.statement, session.bind)
                    session.commit()

        # 업데이트할 포트폴리오가 존재한다면, 포트폴리오 업데이트 해줌.
        if update_list_df.shape[0] != 0:
            # 포트폴리오 업데이트
            for i, r in update_list_df.iterrows():
                update_single_stock_portfolio(user_code=user_code, begin_date=max_date,
                                              account_number=r["account_number"],
                                              securities_code=r["securities_code"],
                                              stock_cd=r["stock_cd"],
                                              tmp_portfolio=tmp_portfolio)

    # max_date 이후 거래내역 업데이트
    if max_date is None:
        max_date = 20110101

    # max_date 이후 거래내역 불러오기
    with session_scope() as session:
        if account_number is None:
            update_list_query = session.query(UserTradingLog). \
                filter(UserTradingLog.user_code == user_code,
                       UserTradingLog.date >= max_date,
                       UserTradingLog.country == country)
            tr_update_list_df = pd.read_sql(update_list_query.statement, session.bind)
            session.commit()
        else:
            update_list_query = session.query(UserTradingLog). \
                filter(UserTradingLog.user_code == user_code,
                       UserTradingLog.date >= max_date,
                       UserTradingLog.country == country,
                       UserTradingLog.account_number == account_number)
            tr_update_list_df = pd.read_sql(update_list_query.statement, session.bind)
            session.commit()

    tr_update_list_df = tr_update_list_df.drop_duplicates(['user_code', 'account_number', 'securities_code', 'stock_cd'])

    # update_list_df에 존재하지 않는 애들만 추림
    if update_list_df is not None:
        tr_update_list_df = tr_update_list_df.merge(update_list_df,
                                                    on=['user_code', 'account_number', 'securities_code', 'stock_cd'],
                                                    how='left')
        tr_update_list_df = tr_update_list_df.loc[tr_update_list_df['country_y'].isna(), :]

    # etc 제외
    tr_update_list_df = tr_update_list_df.loc[~np.isin(tr_update_list_df['stock_type'], ['etc', 'foreign_etc']), :]

    # 업데이트할 포트폴리오가 존재한다면, 포트폴리오 업데이트 해줌.
    if tr_update_list_df.shape[0] != 0:
        # 포트폴리오 업데이트
        for i, r in tr_update_list_df.iterrows():
            update_single_stock_portfolio(user_code=user_code, begin_date=max_date,
                                          account_number=r["account_number"],
                                          securities_code=r["securities_code"],
                                          stock_cd=r["stock_cd"],
                                          tmp_portfolio=tmp_portfolio)


def update_user_portfolio(user_code):
    update_user_portfolio_by_country(user_code=user_code, country='KR')
    update_user_portfolio_by_country(user_code=user_code, country='US')


def update_user_portfolio_by_account(user_code, account_number, tmp_portfolio=False):
    update_user_portfolio_by_country(user_code=user_code, country='KR', account_number=account_number, tmp_portfolio=tmp_portfolio)
    update_user_portfolio_by_country(user_code=user_code, country='US', account_number=account_number, tmp_portfolio=tmp_portfolio)


if __name__ == '__main__':
    update_single_stock_portfolio(user_code=888,
                                  begin_date=20100101,
                                  account_number='27024581913',
                                  securities_code='SHINHAN',
                                  stock_cd='096770',
                                  country='KR')
