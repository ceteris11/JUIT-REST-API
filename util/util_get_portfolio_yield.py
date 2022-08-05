import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from db.db_connect import get_df_from_db, exec_query
from datetime import datetime, timedelta
import pandas as pd


def calc_portfolio_yield(user_code, begin_date, end_date, portfolio_code, period_code, period):
    # print input params
    # print(f'user_code: {user_code}({type(user_code)}), begin_date: {begin_date}({type(begin_date)}), '
    #       f'end_date: {end_date}({type(end_date)}), portfolio_code: {portfolio_code}({type(portfolio_code)}), '
    #       f'period_code: {period_code}({type(period_code)}), period: {period}({type(period)})')

    # 국내 주식
    # 포트폴리오 불러오기
    sql = f'select a.*, b.portfolio_code ' \
          f'from user_portfolio as a ' \
          f'left join user_portfolio_map as b ' \
          f'	on a.user_code = b.user_code ' \
          f'    and a.account_number = b.account_number ' \
          f'    and a.stock_cd = b.stock_cd ' \
          f'where a.user_code = {user_code} ' \
          f'and a.date >= {begin_date} ' \
          f'and a.date <= {end_date} ' \
          f'and b.portfolio_code = {portfolio_code} ' \
          f'and a.country = "KR"'
    user_portfolio = get_df_from_db(sql)

    # 거래내역 불러오기
    sql = f'select a.*, b.portfolio_code ' \
          f'              from user_trading_log as a ' \
          f'              left join user_portfolio_map as b ' \
          f'              	on a.user_code = b.user_code ' \
          f'                  and a.account_number = b.account_number ' \
          f'                  and a.stock_cd = b.stock_cd ' \
          f'                  and a.stock_type in ("domestic_stock", "domestic_etf", "domestic_etn") ' \
          f'              where a.user_code = {user_code} ' \
          f'              and a.date >= {begin_date} ' \
          f'              and a.date <= {end_date} ' \
          f'              and b.portfolio_code = {portfolio_code} ' \
          f'              and a.country = "KR"'
    user_trading_log = get_df_from_db(sql)

    # 미국 주식
    # 포트폴리오 불러오기
    sql = f'select a.*, b.portfolio_code ' \
          f'from user_portfolio as a ' \
          f'left join user_portfolio_map as b ' \
          f'	on a.user_code = b.user_code ' \
          f'    and a.account_number = b.account_number ' \
          f'    and a.stock_cd = b.stock_cd ' \
          f'where a.user_code = {user_code} ' \
          f'and a.date >= {begin_date} ' \
          f'and a.date <= {end_date} ' \
          f'and b.portfolio_code = {portfolio_code} ' \
          f'and a.country = "US"'
    us_user_portfolio = get_df_from_db(sql)
    # print(us_user_portfolio)

    # 미국주식 포트폴리오가 존재한다면
    if (us_user_portfolio is not None) and (us_user_portfolio.shape[0] != 0):
        # 환율 불러오기
        tmp_date = (datetime.strptime(str(begin_date), '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')
        sql = f'select * from exchange_rate where date >= {tmp_date}'  # 환율은 한국날짜 기준
        exchange_rate = get_df_from_db(sql)

        # 환율 적용하여 원화 단위로 변환 -> 미국 시간으로 7월 28일에 열리는 장에는 한국 시간으로 7월 28일에 열렸던 외환시장 환율 종가를 적용함.
        # 만약 해당 일자가 공휴일일 경우, 그 이전 가장 최근 환율을 적용함
        for i in range(us_user_portfolio.shape[0]):
            tmp_date = us_user_portfolio.iloc[i, 1]
            tmp_usd_value = exchange_rate.loc[exchange_rate['date'] <= tmp_date, 'usd_krw'].values[-1]
            # avg_purchase_price
            us_user_portfolio.iloc[i, 7] = us_user_portfolio.iloc[i, 7] * tmp_usd_value
            # close_price
            us_user_portfolio.iloc[i, 8] = us_user_portfolio.iloc[i, 8] * tmp_usd_value
            # prev_1w_close_price
            us_user_portfolio.iloc[i, 9] = us_user_portfolio.iloc[i, 9] * tmp_usd_value
            # total_value
            us_user_portfolio.iloc[i, 10] = us_user_portfolio.iloc[i, 10] * tmp_usd_value
            # new_purchase_amount
            us_user_portfolio.iloc[i, 13] = us_user_portfolio.iloc[i, 13] * tmp_usd_value
            # realized_profit_loss
            us_user_portfolio.iloc[i, 14] = us_user_portfolio.iloc[i, 14] * tmp_usd_value
            # purchase_amount_of_stocks_to_sell
            us_user_portfolio.iloc[i, 15] = us_user_portfolio.iloc[i, 15] * tmp_usd_value

        # 최근 날짜에 미국주식 포트폴리오가 존재하지만 한국주식 포트폴리오가 없는 경우, 마지막 한국주식 포트폴리오를 채우고, 반대의 경우 마지막 미국주식 포트폴리오를 채워준다.
        # 미국주식 포트폴리오가 존재하는 가장 마지막 날짜가 한국주식 포트폴리오가 존재하는 마지막 날짜보다 작을 경우(한국장 마감 후 ~ 미국장 마감 전)
        kr_max_date = exec_query(f'select max(working_day) from date_working_day')[0][0]
        us_max_date = exec_query(f'select max(working_day) from us_date_working_day')[0][0]
        if user_portfolio.shape[0] != 0 and kr_max_date > us_max_date:
            diff_days = (datetime.strptime(str(kr_max_date), '%Y%m%d') - datetime.strptime(str(us_max_date), '%Y%m%d')).days
            tmp_us_portfolio = us_user_portfolio.loc[us_user_portfolio['date'] == us_max_date, :].copy()
            tmp_max_date = us_max_date
            for i in range(diff_days):
                tmp_max_date = int((datetime.strptime(str(tmp_max_date), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d'))
                tmp_us_portfolio.loc[:, 'date'] = tmp_max_date
                us_user_portfolio = pd.concat([us_user_portfolio, tmp_us_portfolio], axis=0)

        # 한국주식 포트폴리오가 존재하는 가장 마지막 날짜가 미국주식 포트폴리오가 존재하는 마지막 날짜보다 작을 경우(미국은 개장했으나 한국은 공휴일인 경우 등)
        if user_portfolio.shape[0] != 0 and kr_max_date < us_max_date:
            diff_days = (datetime.strptime(str(us_max_date), '%Y%m%d') - datetime.strptime(str(kr_max_date), '%Y%m%d')).days
            tmp_portfolio = user_portfolio.loc[user_portfolio['date'] == kr_max_date, :].copy()
            tmp_max_date = kr_max_date
            for i in range(diff_days):
                tmp_max_date = int((datetime.strptime(str(tmp_max_date), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d'))
                tmp_portfolio.loc[:, 'date'] = tmp_max_date
                user_portfolio = pd.concat([user_portfolio, tmp_portfolio], axis=0)

        # 거래내역 불러오기
        sql = f'select a.*, b.portfolio_code ' \
              f'              from user_trading_log as a ' \
              f'              left join user_portfolio_map as b ' \
              f'              	on a.user_code = b.user_code ' \
              f'                  and a.account_number = b.account_number ' \
              f'                  and a.stock_cd = b.stock_cd ' \
              f'                  and a.stock_type in ("us_stock") ' \
              f'              where a.user_code = {user_code} ' \
              f'              and a.date >= {begin_date} ' \
              f'              and a.date <= {end_date} ' \
              f'              and b.portfolio_code = {portfolio_code} ' \
              f'              and a.country = "US"'
        us_user_trading_log = get_df_from_db(sql)

        # 환율 적용하여 원화 단위로 변환 -> 미국 시간으로 7월 28일에 열리는 장에는 한국 시간으로 7월 28일에 열렸던 외환시장 환율 종가를 적용함.
        for i in range(us_user_trading_log.shape[0]):
            tmp_date = int(us_user_trading_log.iloc[i, 2])
            tmp_usd_value = exchange_rate.loc[exchange_rate['date'] <= tmp_date, 'usd_krw'].values[-1]
            # transaction_unit_price
            us_user_trading_log.iloc[i, 11] = us_user_trading_log.iloc[i, 11] * tmp_usd_value
            # transaction_fee
            us_user_trading_log.iloc[i, 12] = us_user_trading_log.iloc[i, 12] * tmp_usd_value
            # transaction_tax
            us_user_trading_log.iloc[i, 13] = us_user_trading_log.iloc[i, 13] * tmp_usd_value

        # 한국주식 + 미국주식 df 합쳐주기
        user_portfolio = pd.concat([user_portfolio, us_user_portfolio], axis=0)
        user_trading_log = pd.concat([user_trading_log, us_user_trading_log], axis=0)
        # print(user_portfolio)
        # print(user_trading_log)

    if (user_portfolio is None) or (user_portfolio.shape[0] == 0):
        return {'status': '205',  # 기간 내 보유 종목 없음
                'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                'current_asset': 0, 'daily_return': []}

    # begin_date에 포트폴리오가 존재하지 않을 경우, 포트폴리오가 최초 시작하기 1영업일 전으로 begin_date 조정
    # (기간이 5영업일 이하일 경우 조정하지 않음)
    # first_purchase_date가 없는 경우(배당금 입금 등)를 제외하고 포트폴리오 최초 시작일을 정함
    first_portfolio_date = min(user_portfolio.loc[
                                   user_portfolio['first_purchase_date'].apply(lambda x: x is not None), "date"])
    if period_code is not None and period > 5 and int(begin_date) != first_portfolio_date:
        begin_date = exec_query(f'select b.working_day '
                                f'from date_working_day as a, date_working_day as b '
                                f'where a.seq = b.seq + 1 '
                                f'and a.working_day = {first_portfolio_date} ')[0][0]

    # 코스피, 코스닥 정보 불러오기
    sql = f'select a.date, ' \
          f'       sum(case when index_nm = "KOSPI" then a.index else 0 end) as kospi, ' \
          f'       sum(case when index_nm = "KOSDAQ" then a.index else 0 end) as kosdaq  ' \
          f'from stock_kospi_kosdaq as a ' \
          f'where a.date >= {begin_date} ' \
          f'and a.date <= {end_date} ' \
          f'group by date '
    kospi_kosdaq = get_df_from_db(sql)

    if kospi_kosdaq.shape[0] == 0:
        return {'status': '000',  # 휴일 기간 조회
                'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0, 'current_asset': 0,
                'daily_return': [{"date": str(begin_date),
                                  "daily_return": 0.0,
                                  "kospi_daily_return": 0.0,
                                  "kosdaq_daily_return": 0.0,
                                  "daily_profit": 0,
                                  "kospi_daily_profit": 0.0,
                                  "kosdaq_daily_profit": 0.0}]}

    # 일일 수익률로 만들어주기(수익률 + 1)
    kospi_kosdaq['kospi_daily_return'] = kospi_kosdaq['kospi'].pct_change().fillna(0) + 1
    kospi_kosdaq['kosdaq_daily_return'] = kospi_kosdaq['kosdaq'].pct_change().fillna(0) + 1

    # date list 정의
    date_list = list(kospi_kosdaq['date'].apply(lambda x: str(x)))

    # 코스피/코스닥 수익률은 지수 수익률 그대로 보여준다
    kospi_kosdaq['kospi_return'] = kospi_kosdaq['kospi'] / (kospi_kosdaq.loc[kospi_kosdaq['date'] == int(min(date_list)), 'kospi'].values[0]) - 1
    kospi_kosdaq['kosdaq_return'] = kospi_kosdaq['kosdaq'] / (kospi_kosdaq.loc[kospi_kosdaq['date'] == int(min(date_list)), 'kosdaq'].values[0]) - 1

    # 투자 손익 : 기말평가금액 - 기초평가금액 + Σ출금고액(매도총액) - Σ입금고액(매수총액)
    # 실현 손익 : 매도금액 - 최초매입금액(매도주식)
    # 투자 원금 : 기초평가금액 + Σ입금고액(매수총액)

    # 수익률: 투자 손익 / 투자 원금
    # 수익금액: 투자 손익
    # 수익금액(실현손익): (기준일 이후 주식 실현손익)

    # 초기 포트폴리오
    # 결과 제공용 list 생성
    return_dict_list = []

    # 최소필요현금 변수 생성
    required_cash_reserve = 0
    # 현금 변수 생성
    cash = 0
    # 실현손익 변수 생성
    realized_profit_loss = 0
    # 누적 입금 금액 변수 생성
    deposit_amount = 0
    # 수익률, 손익금액, 현재가치 변수 생성
    curr_daily_return = 0
    curr_daily_profit = 0
    curr_asset = 0

    # 기초시점 포트폴리오 변수 생성
    base_portfolio = user_portfolio.loc[user_portfolio['date'].apply(lambda x: str(x) == date_list[0]), :]
    # 기초 현금 업데이트
    base_cash = sum(base_portfolio['realized_profit_loss']) - sum(base_portfolio['new_purchase_amount'])
    # 기초 포트폴리오 가치 업데이트
    base_value = sum(base_portfolio['total_value']) + base_cash

    # 기초시점 코스피, 코스닥 가치 변수 생성
    kospi_value = base_value
    kosdaq_value = base_value

    for d in date_list:
        # current_portfolio, current_market_index 정의
        current_portfolio = user_portfolio.loc[user_portfolio['date'].apply(lambda x: str(x) == d), :]
        current_trading_log = user_trading_log.loc[(user_trading_log['date'].apply(lambda x: str(x) == d)) &
                                                   (user_trading_log['stock_cd'].isin(current_portfolio['stock_cd'])), :]
        current_market_index = kospi_kosdaq.loc[kospi_kosdaq['date'].apply(lambda x: str(x) == d), :]
        # print(current_portfolio)
        # print(current_trading_log)
        # print(current_market_index)

        # 신규 매입금액 변수 초기화
        new_purchase_amount = 0
        # 최소필요현금 변수 초기화
        required_cash_reserve = 0

        # portfolio가 없는 경우 - 이전 수익률 그대로 가져감. 이전 수익률 없을 경우 수익률 0처리
        if current_portfolio.shape[0] == 0:
            if len(return_dict_list) == 0:  # 이전 수익률 없는 경우
                curr_daily_profit = 0
                curr_daily_return = 0
            else:  # 이전에는 포트폴리오가 있었으나, 해당 날짜에는 없는 경우
                curr_daily_profit = return_dict_list[-1]['daily_profit']
                curr_daily_return = return_dict_list[-1]['daily_return']

        # portfolio가 있는 경우 - 수익률 계산
        else:
            # trading_log가 있는 경우 - 최소필요현금 계산
            if current_trading_log.shape[0] != 0:
                account_list = list(current_trading_log['account_number'].unique())
                for account in account_list:
                    account_trading_log = current_trading_log.loc[
                                          current_trading_log['account_number'].apply(lambda x: x == account), :]
                    account_trading_log = account_trading_log.sort_values(by=['seq'])
                    tmp_cash = 0
                    for i in range(account_trading_log.shape[0]):
                        tr = account_trading_log.iloc[i, :]
                        if tr['transaction_type'] in ['매수', '유상주입고', '무상주입고', '공모주입고', '타사대체입고', '대체입고',
                                                      '해외주식매수', '배당세출금']:
                            tmp_cash = tmp_cash - ((tr['transaction_quantity'] * tr['transaction_unit_price']) +
                                                   (tr['transaction_fee'] + tr['transaction_tax']))
                            if tmp_cash < 0:
                                required_cash_reserve = required_cash_reserve + (-tmp_cash)
                                tmp_cash = 0
                        elif tr['transaction_type'] in ['매도', '타사대체출고', '대체출고', '해외주식매도', '배당', '해외주식배당']:
                            tmp_cash = tmp_cash + ((tr['transaction_quantity'] * tr['transaction_unit_price']) -
                                                   (tr['transaction_fee'] + tr['transaction_tax']))
            # 실현손익 업데이트: 기존 실현손익 + 매도금액 - 매입비용
            realized_profit_loss = realized_profit_loss + \
                                   sum(current_portfolio['realized_profit_loss']) - \
                                   sum(current_portfolio['purchase_amount_of_stocks_to_sell'])

            # 누적 입금 금액, 신규 입금 금액 업데이트
            if cash < required_cash_reserve:
                deposit_amount = deposit_amount + (required_cash_reserve - cash)  # 모자란 금액만큼 입금한 것으로 본다.
                new_purchase_amount = new_purchase_amount + (required_cash_reserve - cash)  # 해당 일자의 신규 입금 금액 세팅
                cash = required_cash_reserve

            # 현금 업데이트: 기존 현금 + 매도금액 - 매입금액
            cash = cash + sum(current_portfolio['realized_profit_loss']) - \
                   sum(current_portfolio['new_purchase_amount'])

            # 수익률 계산: (현재 평가금액(포트폴리오 가치 + 현금) - (기초 평가금액 + 총 입금 금액)) / (기초 평가금액 + 총 입금 금액)
            curr_asset = sum(current_portfolio['total_value'])
            curr_daily_profit = (curr_asset + cash) - (base_value + deposit_amount)
            # 기초 평가금액 + 총 입금 금액이 0원일 경우, 수익률 999% 표시.
            if (base_value + deposit_amount) == 0:
                curr_daily_return = 9.99
            else:
                # print(f'----- date: {d} -----')
                # print(current_trading_log)
                # print(current_portfolio)
                # print(f'required_cash_reserve: {required_cash_reserve}')
                # print(f'curr_asset: {curr_asset}')
                # print(f'cash: {cash}')
                # print(f'base_value: {base_value}')
                # print(f'deposit_amount: {deposit_amount}')
                # print(f'curr_daily_profit: {curr_daily_profit}')
                curr_daily_return = curr_daily_profit / (base_value + deposit_amount)
                # print(f'curr_daily_return: {curr_daily_return}')

        # KOSPI, KOSDAQ 수익률 계산: 이전 가치 * 일일 수익률 + 신규 매입금액
        kospi_value = kospi_value * (current_market_index['kospi_daily_return'].values[0]) + new_purchase_amount
        kosdaq_value = kosdaq_value * (current_market_index['kosdaq_daily_return'].values[0]) + new_purchase_amount

        if (base_value + deposit_amount) != 0:
            # kospi_daily_return = kospi_value / (base_value + deposit_amount) - 1
            # kosdaq_daily_return = kosdaq_value / (base_value + deposit_amount) - 1
            kospi_daily_return = current_market_index['kospi_return'].values[0]
            kosdaq_daily_return = current_market_index['kosdaq_return'].values[0]
        else:
            # 기초 평가금액 + 총 입금금액이 0일 경우, 0으로 세팅
            kospi_daily_return = 0
            kosdaq_daily_return = 0
            # koapi, kosdaq 수익률 기준일을 현재일자로 변경
            kospi_kosdaq['kospi_return'] = kospi_kosdaq['kospi'] / (kospi_kosdaq.loc[kospi_kosdaq['date'] == int(d), 'kospi'].values[0]) - 1
            kospi_kosdaq['kosdaq_return'] = kospi_kosdaq['kosdaq'] / (kospi_kosdaq.loc[kospi_kosdaq['date'] == int(d), 'kosdaq'].values[0]) - 1

        # return dict append
        return_dict_list.append(
            {'date': d,
             'daily_return': curr_daily_return,
             'kospi_daily_return': kospi_daily_return,
             'kosdaq_daily_return': kosdaq_daily_return,
             'daily_profit': curr_daily_profit,
             'kospi_daily_profit': kospi_value - (base_value + deposit_amount),
             'kosdaq_daily_profit': kosdaq_value - (base_value + deposit_amount)})

    # 일자가 너무 많을 경우, 줄여준다
    while len(date_list) > 300:
        date_list = date_list[:-1][::2] + [date_list[-1]]
        return_dict_list = return_dict_list[:-1][::2] + [return_dict_list[-1]]

    # print(f'total_return: {curr_daily_return}, realized_profit_loss: {realized_profit_loss}')
    return {'status': '000',
            'total_return': curr_daily_return,
            'realized_profit_loss': realized_profit_loss,
            'total_purchase': (base_value + deposit_amount),
            'total_income': curr_daily_profit,
            'current_asset': curr_asset,
            'daily_return': return_dict_list}


def calc_all_portfolio_yield(user_code, begin_date, end_date):
    # print input params
    # print(f'user_code: {user_code}({type(user_code)}), begin_date: {begin_date}({type(begin_date)}), '
    #       f'end_date: {end_date}({type(end_date)}), portfolio_code: {portfolio_code}({type(portfolio_code)}), '
    #       f'period_code: {period_code}({type(period_code)}), period: {period}({type(period)})')

    # 국내 주식
    # 포트폴리오 불러오기
    sql = f'select a.*, b.portfolio_code ' \
          f'from user_portfolio as a ' \
          f'left join user_portfolio_map as b ' \
          f'	on a.user_code = b.user_code ' \
          f'    and a.account_number = b.account_number ' \
          f'    and a.stock_cd = b.stock_cd ' \
          f'where a.user_code = {user_code} ' \
          f'and a.date >= {begin_date} ' \
          f'and a.date <= {end_date} ' \
          f'and a.securities_code != "SELF" ' \
          f'and a.country = "KR"'
    user_portfolio = get_df_from_db(sql)

    # 거래내역 불러오기
    sql = f'select a.*, b.portfolio_code ' \
          f'              from user_trading_log as a ' \
          f'              left join user_portfolio_map as b ' \
          f'              	on a.user_code = b.user_code ' \
          f'                  and a.account_number = b.account_number ' \
          f'                  and a.stock_cd = b.stock_cd ' \
          f'                  and a.stock_type in ("domestic_stock", "domestic_etf", "domestic_etn") ' \
          f'              where a.user_code = {user_code} ' \
          f'              and a.date >= {begin_date} ' \
          f'              and a.date <= {end_date} ' \
          f'              and a.securities_code != "SELF" ' \
          f'              and a.country = "KR"'
    user_trading_log = get_df_from_db(sql)

    # 미국 주식
    # 포트폴리오 불러오기
    sql = f'select a.*, b.portfolio_code ' \
          f'from user_portfolio as a ' \
          f'left join user_portfolio_map as b ' \
          f'	on a.user_code = b.user_code ' \
          f'    and a.account_number = b.account_number ' \
          f'    and a.stock_cd = b.stock_cd ' \
          f'where a.user_code = {user_code} ' \
          f'and a.date >= {begin_date} ' \
          f'and a.date <= {end_date} ' \
          f'and a.securities_code != "SELF" ' \
          f'and a.country = "US"'
    us_user_portfolio = get_df_from_db(sql)
    # print(us_user_portfolio)

    # 미국주식 포트폴리오가 존재한다면
    if (us_user_portfolio is not None) and (us_user_portfolio.shape[0] != 0):
        # 환율 불러오기
        tmp_date = (datetime.strptime(str(begin_date), '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')
        sql = f'select * from exchange_rate where date >= {tmp_date}'  # 환율은 한국날짜 기준
        exchange_rate = get_df_from_db(sql)

        # 환율 적용하여 원화 단위로 변환 -> 미국 시간으로 7월 28일에 열리는 장에는 한국 시간으로 7월 28일에 열렸던 외환시장 환율 종가를 적용함.
        # 만약 해당 일자가 공휴일일 경우, 그 이전 가장 최근 환율을 적용함
        for i in range(us_user_portfolio.shape[0]):
            tmp_date = us_user_portfolio.iloc[i, 1]
            tmp_usd_value = exchange_rate.loc[exchange_rate['date'] <= tmp_date, 'usd_krw'].values[-1]
            # avg_purchase_price
            us_user_portfolio.iloc[i, 7] = us_user_portfolio.iloc[i, 7] * tmp_usd_value
            # close_price
            us_user_portfolio.iloc[i, 8] = us_user_portfolio.iloc[i, 8] * tmp_usd_value
            # prev_1w_close_price
            us_user_portfolio.iloc[i, 9] = us_user_portfolio.iloc[i, 9] * tmp_usd_value
            # total_value
            us_user_portfolio.iloc[i, 10] = us_user_portfolio.iloc[i, 10] * tmp_usd_value
            # new_purchase_amount
            us_user_portfolio.iloc[i, 13] = us_user_portfolio.iloc[i, 13] * tmp_usd_value
            # realized_profit_loss
            us_user_portfolio.iloc[i, 14] = us_user_portfolio.iloc[i, 14] * tmp_usd_value
            # purchase_amount_of_stocks_to_sell
            us_user_portfolio.iloc[i, 15] = us_user_portfolio.iloc[i, 15] * tmp_usd_value

        # 최근 날짜에 미국주식 포트폴리오가 존재하지만 한국주식 포트폴리오가 없는 경우, 마지막 한국주식 포트폴리오를 채우고, 반대의 경우 마지막 미국주식 포트폴리오를 채워준다.
        # 미국주식 포트폴리오가 존재하는 가장 마지막 날짜가 한국주식 포트폴리오가 존재하는 마지막 날짜보다 작을 경우(한국장 마감 후 ~ 미국장 마감 전)
        kr_max_date = exec_query(f'select max(working_day) from date_working_day')[0][0]
        us_max_date = exec_query(f'select max(working_day) from us_date_working_day')[0][0]
        if user_portfolio.shape[0] != 0 and kr_max_date > us_max_date:
            diff_days = (datetime.strptime(str(kr_max_date), '%Y%m%d') - datetime.strptime(str(us_max_date), '%Y%m%d')).days
            tmp_us_portfolio = us_user_portfolio.loc[us_user_portfolio['date'] == us_max_date, :].copy()
            tmp_max_date = us_max_date
            for i in range(diff_days):
                tmp_max_date = int((datetime.strptime(str(tmp_max_date), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d'))
                tmp_us_portfolio.loc[:, 'date'] = tmp_max_date
                us_user_portfolio = pd.concat([us_user_portfolio, tmp_us_portfolio], axis=0)

        # 한국주식 포트폴리오가 존재하는 가장 마지막 날짜가 미국주식 포트폴리오가 존재하는 마지막 날짜보다 작을 경우(미국은 개장했으나 한국은 공휴일인 경우 등)
        if user_portfolio.shape[0] != 0 and kr_max_date < us_max_date:
            diff_days = (datetime.strptime(str(us_max_date), '%Y%m%d') - datetime.strptime(str(kr_max_date), '%Y%m%d')).days
            tmp_portfolio = user_portfolio.loc[user_portfolio['date'] == kr_max_date, :].copy()
            tmp_max_date = kr_max_date
            for i in range(diff_days):
                tmp_max_date = int((datetime.strptime(str(tmp_max_date), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d'))
                tmp_portfolio.loc[:, 'date'] = tmp_max_date
                user_portfolio = pd.concat([user_portfolio, tmp_portfolio], axis=0)

        # 거래내역 불러오기
        sql = f'select a.*, b.portfolio_code ' \
              f'              from user_trading_log as a ' \
              f'              left join user_portfolio_map as b ' \
              f'              	on a.user_code = b.user_code ' \
              f'                  and a.account_number = b.account_number ' \
              f'                  and a.stock_cd = b.stock_cd ' \
              f'                  and a.stock_type in ("us_stock") ' \
              f'              where a.user_code = {user_code} ' \
              f'              and a.date >= {begin_date} ' \
              f'              and a.date <= {end_date} ' \
              f'              and a.securities_code != "SELF" ' \
              f'              and a.country = "US"'
        us_user_trading_log = get_df_from_db(sql)

        # 환율 적용하여 원화 단위로 변환 -> 미국 시간으로 7월 28일에 열리는 장에는 한국 시간으로 7월 28일에 열렸던 외환시장 환율 종가를 적용함.
        for i in range(us_user_trading_log.shape[0]):
            tmp_date = int(us_user_trading_log.iloc[i, 2])
            tmp_usd_value = exchange_rate.loc[exchange_rate['date'] <= tmp_date, 'usd_krw'].values[-1]
            # transaction_unit_price
            us_user_trading_log.iloc[i, 11] = us_user_trading_log.iloc[i, 11] * tmp_usd_value
            # transaction_fee
            us_user_trading_log.iloc[i, 12] = us_user_trading_log.iloc[i, 12] * tmp_usd_value
            # transaction_tax
            us_user_trading_log.iloc[i, 13] = us_user_trading_log.iloc[i, 13] * tmp_usd_value

        # 한국주식 + 미국주식 df 합쳐주기
        user_portfolio = pd.concat([user_portfolio, us_user_portfolio], axis=0)
        user_trading_log = pd.concat([user_trading_log, us_user_trading_log], axis=0)
        # print(user_portfolio)
        # print(user_trading_log)

    if user_portfolio.shape[0] == 0:
        return {'status': '205',  # 기간 내 보유 종목 없음
                'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0,
                'current_asset': 0, 'daily_return': []}

    # calc_all_portfolio_yield에서는 begin_date, end_date를 입력받아 그대로 사용함.
    # # begin_date에 포트폴리오가 존재하지 않을 경우, 포트폴리오가 최초 시작하기 1영업일 전으로 begin_date 조정
    # # (기간이 5영업일 이하일 경우 조정하지 않음)
    # # first_purchase_date가 없는 경우(배당금 입금 등)를 제외하고 포트폴리오 최초 시작일을 정함
    # first_portfolio_date = min(user_portfolio.loc[
    #                                user_portfolio['first_purchase_date'].apply(lambda x: x is not None), "date"])
    # if period_code is not None and period > 5 and int(begin_date) != first_portfolio_date:
    #     begin_date = exec_query(f'select b.working_day '
    #                             f'from date_working_day as a, date_working_day as b '
    #                             f'where a.seq = b.seq + 1 '
    #                             f'and a.working_day = {first_portfolio_date} ')[0][0]

    # 코스피, 코스닥 정보 불러오기
    sql = f'select a.date, ' \
          f'       sum(case when index_nm = "KOSPI" then a.index else 0 end) as kospi, ' \
          f'       sum(case when index_nm = "KOSDAQ" then a.index else 0 end) as kosdaq  ' \
          f'from stock_kospi_kosdaq as a ' \
          f'where a.date >= {begin_date} ' \
          f'and a.date <= {end_date} ' \
          f'group by date '
    kospi_kosdaq = get_df_from_db(sql)

    if kospi_kosdaq.shape[0] == 0:
        return {'status': '000',  # 휴일 기간 조회
                'total_return': 0, 'realized_profit_loss': 0, 'total_purchase': 0, 'total_income': 0, 'current_asset': 0,
                'daily_return': [{"date": str(begin_date),
                                  "daily_return": 0.0,
                                  "kospi_daily_return": 0.0,
                                  "kosdaq_daily_return": 0.0,
                                  "daily_profit": 0,
                                  "kospi_daily_profit": 0.0,
                                  "kosdaq_daily_profit": 0.0}]}

    # 일일 수익률로 만들어주기(수익률 + 1)
    kospi_kosdaq['kospi_daily_return'] = kospi_kosdaq['kospi'].pct_change().fillna(0) + 1
    kospi_kosdaq['kosdaq_daily_return'] = kospi_kosdaq['kosdaq'].pct_change().fillna(0) + 1

    # date list 정의
    date_list = list(kospi_kosdaq['date'].apply(lambda x: str(x)))

    # 코스피/코스닥 수익률은 지수 수익률 그대로 보여준다
    kospi_kosdaq['kospi_return'] = kospi_kosdaq['kospi'] / (kospi_kosdaq.loc[kospi_kosdaq['date'] == int(min(date_list)), 'kospi'].values[0]) - 1
    kospi_kosdaq['kosdaq_return'] = kospi_kosdaq['kosdaq'] / (kospi_kosdaq.loc[kospi_kosdaq['date'] == int(min(date_list)), 'kosdaq'].values[0]) - 1

    # 투자 손익 : 기말평가금액 - 기초평가금액 + Σ출금고액(매도총액) - Σ입금고액(매수총액)
    # 실현 손익 : 매도금액 - 최초매입금액(매도주식)
    # 투자 원금 : 기초평가금액 + Σ입금고액(매수총액)

    # 수익률: 투자 손익 / 투자 원금
    # 수익금액: 투자 손익
    # 수익금액(실현손익): (기준일 이후 주식 실현손익)

    # 초기 포트폴리오
    # 결과 제공용 list 생성
    return_dict_list = []

    # 최소필요현금 변수 생성
    required_cash_reserve = 0
    # 현금 변수 생성
    cash = 0
    # 실현손익 변수 생성
    realized_profit_loss = 0
    # 누적 입금 금액 변수 생성
    deposit_amount = 0
    # 수익률, 손익금액, 현재가치 변수 생성
    curr_daily_return = 0
    curr_daily_profit = 0
    curr_asset = 0

    # 기초시점 포트폴리오 변수 생성
    base_portfolio = user_portfolio.loc[user_portfolio['date'].apply(lambda x: str(x) == date_list[0]), :]
    # 기초 현금 업데이트
    base_cash = sum(base_portfolio['realized_profit_loss']) - sum(base_portfolio['new_purchase_amount'])
    # 기초 포트폴리오 가치 업데이트
    base_value = sum(base_portfolio['total_value']) + base_cash

    # 기초시점 코스피, 코스닥 가치 변수 생성
    kospi_value = base_value
    kosdaq_value = base_value

    for d in date_list:
        # current_portfolio, current_market_index 정의
        current_portfolio = user_portfolio.loc[user_portfolio['date'].apply(lambda x: str(x) == d), :]
        current_trading_log = user_trading_log.loc[(user_trading_log['date'].apply(lambda x: str(x) == d)) &
                                                   (user_trading_log['stock_cd'].isin(current_portfolio['stock_cd'])), :]
        current_market_index = kospi_kosdaq.loc[kospi_kosdaq['date'].apply(lambda x: str(x) == d), :]
        # print(current_portfolio)
        # print(current_trading_log)
        # print(current_market_index)

        # 신규 매입금액 변수 초기화
        new_purchase_amount = 0
        # 최소필요현금 변수 초기화
        required_cash_reserve = 0

        # portfolio가 없는 경우 - 이전 수익률 그대로 가져감. 이전 수익률 없을 경우 수익률 0처리
        if current_portfolio.shape[0] == 0:
            if len(return_dict_list) == 0:  # 이전 수익률 없는 경우
                curr_daily_profit = 0
                curr_daily_return = 0
            else:  # 이전에는 포트폴리오가 있었으나, 해당 날짜에는 없는 경우
                curr_daily_profit = return_dict_list[-1]['daily_profit']
                curr_daily_return = return_dict_list[-1]['daily_return']

        # portfolio가 있는 경우 - 수익률 계산
        else:
            # trading_log가 있는 경우 - 최소필요현금 계산
            if current_trading_log.shape[0] != 0:
                account_list = list(current_trading_log['account_number'].unique())
                for account in account_list:
                    account_trading_log = current_trading_log.loc[
                                          current_trading_log['account_number'].apply(lambda x: x == account), :]
                    account_trading_log = account_trading_log.sort_values(by=['seq'])
                    tmp_cash = 0
                    for i in range(account_trading_log.shape[0]):
                        tr = account_trading_log.iloc[i, :]
                        if tr['transaction_type'] in ['매수', '유상주입고', '무상주입고', '공모주입고', '타사대체입고', '대체입고',
                                                      '해외주식매수', '배당세출금']:
                            tmp_cash = tmp_cash - ((tr['transaction_quantity'] * tr['transaction_unit_price']) +
                                                   (tr['transaction_fee'] + tr['transaction_tax']))
                            if tmp_cash < 0:
                                required_cash_reserve = required_cash_reserve + (-tmp_cash)
                                tmp_cash = 0
                        elif tr['transaction_type'] in ['매도', '타사대체출고', '대체출고', '해외주식매도', '배당', '해외주식배당']:
                            tmp_cash = tmp_cash + ((tr['transaction_quantity'] * tr['transaction_unit_price']) -
                                                   (tr['transaction_fee'] + tr['transaction_tax']))
            # 실현손익 업데이트: 기존 실현손익 + 매도금액 - 매입비용
            realized_profit_loss = realized_profit_loss + \
                                   sum(current_portfolio['realized_profit_loss']) - \
                                   sum(current_portfolio['purchase_amount_of_stocks_to_sell'])

            # 누적 입금 금액, 신규 입금 금액 업데이트
            if cash < required_cash_reserve:
                deposit_amount = deposit_amount + (required_cash_reserve - cash)  # 모자란 금액만큼 입금한 것으로 본다.
                new_purchase_amount = new_purchase_amount + (required_cash_reserve - cash)  # 해당 일자의 신규 입금 금액 세팅
                cash = required_cash_reserve

            # 현금 업데이트: 기존 현금 + 매도금액 - 매입금액
            cash = cash + sum(current_portfolio['realized_profit_loss']) - \
                   sum(current_portfolio['new_purchase_amount'])

            # 수익률 계산: (현재 평가금액(포트폴리오 가치 + 현금) - (기초 평가금액 + 총 입금 금액)) / (기초 평가금액 + 총 입금 금액)
            curr_asset = sum(current_portfolio['total_value'])
            curr_daily_profit = (curr_asset + cash) - (base_value + deposit_amount)
            # 기초 평가금액 + 총 입금 금액이 0원일 경우, 수익률 999% 표시.
            if (base_value + deposit_amount) == 0:
                curr_daily_return = 9.99
            else:
                # print(f'----- date: {d} -----')
                # print(current_trading_log)
                # print(current_portfolio)
                # print(f'required_cash_reserve: {required_cash_reserve}')
                # print(f'curr_asset: {curr_asset}')
                # print(f'cash: {cash}')
                # print(f'base_value: {base_value}')
                # print(f'deposit_amount: {deposit_amount}')
                # print(f'curr_daily_profit: {curr_daily_profit}')
                curr_daily_return = curr_daily_profit / (base_value + deposit_amount)
                # print(f'curr_daily_return: {curr_daily_return}')

        # KOSPI, KOSDAQ 수익률 계산: 이전 가치 * 일일 수익률 + 신규 매입금액
        kospi_value = kospi_value * (current_market_index['kospi_daily_return'].values[0]) + new_purchase_amount
        kosdaq_value = kosdaq_value * (current_market_index['kosdaq_daily_return'].values[0]) + new_purchase_amount

        if (base_value + deposit_amount) != 0:
            # kospi_daily_return = kospi_value / (base_value + deposit_amount) - 1
            # kosdaq_daily_return = kosdaq_value / (base_value + deposit_amount) - 1
            kospi_daily_return = current_market_index['kospi_return'].values[0]
            kosdaq_daily_return = current_market_index['kosdaq_return'].values[0]
        else:
            # 기초 평가금액 + 총 입금금액이 0일 경우, 0으로 세팅
            kospi_daily_return = 0
            kosdaq_daily_return = 0
            # koapi, kosdaq 수익률 기준일을 현재일자로 변경
            kospi_kosdaq['kospi_return'] = kospi_kosdaq['kospi'] / (kospi_kosdaq.loc[kospi_kosdaq['date'] == int(d), 'kospi'].values[0]) - 1
            kospi_kosdaq['kosdaq_return'] = kospi_kosdaq['kosdaq'] / (kospi_kosdaq.loc[kospi_kosdaq['date'] == int(d), 'kosdaq'].values[0]) - 1

        # return dict append
        return_dict_list.append(
            {'date': d,
             'daily_return': curr_daily_return,
             'kospi_daily_return': kospi_daily_return,
             'kosdaq_daily_return': kosdaq_daily_return,
             'daily_profit': curr_daily_profit,
             'kospi_daily_profit': kospi_value - (base_value + deposit_amount),
             'kosdaq_daily_profit': kosdaq_value - (base_value + deposit_amount)})

    # 일자가 너무 많을 경우, 줄여준다
    while len(date_list) > 300:
        date_list = date_list[:-1][::2] + [date_list[-1]]
        return_dict_list = return_dict_list[:-1][::2] + [return_dict_list[-1]]

    # print(f'total_return: {curr_daily_return}, realized_profit_loss: {realized_profit_loss}')
    return {'status': '000',
            'total_return': curr_daily_return,
            'realized_profit_loss': realized_profit_loss,
            'total_purchase': (base_value + deposit_amount),
            'total_income': curr_daily_profit,
            'current_asset': curr_asset,
            'daily_return': return_dict_list}
