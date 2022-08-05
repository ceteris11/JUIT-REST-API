import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from db.db_model import UserTradingLog, UserPortfolioInfo, SecuritiesCode, UserInfo, \
    StockInfo, UserPortfolioMap, session_scope, DateWorkingDay, UsStockInfo, UsDateWorkingDay, \
    UsDateWorkingDayMapping, TmpUserPortfolioMap, SecuritiesTrCondition
from util.util_update_portfolio import util_get_avg_purchase_price, util_get_recent_split_release_quantity
from util.util_stock_cd_converter import get_us_symbol_from_isin, get_us_symbol_from_trfr_stock_nm, \
    get_kr_stock_cd_from_isin
from db.db_connect import exec_query, insert_data, get_df_from_db
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy.orm import aliased


class UserTradingLogModule:
    def __init__(self, user_code, portfolio_code, securities_code, account_number, raw_tr, app):
        self.user_code = user_code
        self.portfolio_code = portfolio_code
        self.securities_code = securities_code
        self.account_number = account_number
        self.raw_tr = raw_tr
        self.min_date = None
        self.app = app

    def insert_tr_log(self):
        if not self.is_valid_user():
            return {'status': '103'}  # 올바르지 않은 ID or PW
        if not self.is_valid_securities():
            return {'status': '309'}  # 올바르지 않은 증권사 코드
        if not self.is_valid_portfolio():
            return {'status': '201'}  # 해당 포트폴리오 없음

        self.standardize()
        self.insert_data()

        return {'status': '000'}

    def standardize(self):
        self.standardize_stock_cd()

        self.filter_holiday()

        self.generate_pulled_transaction_date()

        self.standardize_tr()

        self.update_seq()

        self.generate_stock_type()

        self.update_stock_nm()

        self.update_unit_currency()

        self.update_country()

    def insert_data(self):
        self.update_tmp_tr_map()

        self.insert_tmp_tr_log()

        self.overwrite_map()

        self.overwrite_tr()

    def is_valid_user(self):
        """유효 회원 여부 확인"""
        with session_scope() as session:
            checked_user = session.query(UserInfo).filter(UserInfo.user_code == self.user_code,
                                                          UserInfo.end_dtim == '99991231235959').all()
            session.commit()

            if len(checked_user) == 0:  # 일치하는 회원이 없는 경우
                return False
            else:
                return True

    def is_valid_securities(self):
        """유효 증권사 여부 확인"""
        with session_scope() as session:
            # securities_code 확인
            securities_code_result = session.query(SecuritiesCode). \
                filter(SecuritiesCode.end_date == '99991231',
                       SecuritiesCode.available_flag == 1,
                       SecuritiesCode.securities_code == self.securities_code). \
                all()
            session.commit()

            if len(securities_code_result) == 0:  # 일치하는 증권사 코드가 없을 경우
                return False
            else:
                return True

    def is_valid_portfolio(self):
        """유효 포트폴리오 여부 확인"""
        with session_scope() as session:
            # portfolio 확인
            portfolio_info = session.query(UserPortfolioInfo). \
                filter(UserPortfolioInfo.user_code == self.user_code,
                       UserPortfolioInfo.portfolio_code == self.portfolio_code).\
                all()
            session.commit()
            if len(portfolio_info) == 0:
                return False
            else:
                return True

    def update_stock_cd(self, securities_list, country, func, target_col='stock_cd'):
        cond = np.isin(self.raw_tr['securities_code'].values, securities_list) & \
               (self.raw_tr['country'].values == country)
        new_stock_cd = self.raw_tr.loc[cond, target_col].apply(func)
        # update data
        self.raw_tr.loc[cond, 'stock_cd'] = new_stock_cd

    def standardize_stock_cd(self):
        """증권사별 종목코드 표준화(이미 표준 코드인 경우 표준화하지 않음)"""
        # 한국주식 - 미래, 키움, 삼성
        self.update_stock_cd(["MIRAE", "KIWOOM", "SAMSUNG"], 'KR', lambda x: x[1:])
        # 한국주식 - KB
        self.update_stock_cd(["KBSEC"], 'KR', lambda x: get_kr_stock_cd_from_isin(x))

        # 미국주식 - 나무, NH, KB
        self.update_stock_cd(["NAMUH", "NHQV", "KBSEC"], 'foreign', lambda x: get_us_symbol_from_isin(x))
        # 미국주식 - 신한
        self.update_stock_cd(["SHINHAN"], 'foreign', lambda x: get_us_symbol_from_isin('US' + x))
        # 미국주식 - 삼성
        self.update_stock_cd(['SAMSUNG'], 'foreign', lambda x: x.split('.')[0])
        # 미국주식 - 한투
        self.update_stock_cd(['TRUEFR'], 'foreign', lambda x: get_us_symbol_from_trfr_stock_nm(x), target_col='stock_nm')

    def filter_holiday(self):
        """휴장일 필터링"""
        # 한국주식
        kr_days = self.raw_tr.loc[self.raw_tr['country'].values == 'KR', 'transaction_date']
        if len(kr_days) > 0:
            kr_min_date = min(kr_days)
            # 영업일 데이터 로드
            with session_scope() as session:
                kr_working_day = session.query(DateWorkingDay.working_day). \
                    filter(DateWorkingDay.working_day >= kr_min_date)
                kr_working_day = pd.read_sql(kr_working_day.statement, session.bind)
                kr_working_day['country'] = 'KR'
                session.commit()

        # 미국주식
        foreign_days = self.raw_tr.loc[self.raw_tr['country'].values == 'foreign', 'transaction_date']
        if len(foreign_days) > 0:
            foreign_min_date = min(foreign_days)
            # 영업일 데이터 로드
            with session_scope() as session:
                us_working_day = session.query(UsDateWorkingDay.working_day). \
                    filter(UsDateWorkingDay.working_day >= foreign_min_date)
                us_working_day = pd.read_sql(us_working_day.statement, session.bind)
                us_working_day['country'] = 'foreign'
                session.commit()

        # working_day 생성
        if len(kr_days) > 0 and len(foreign_days) > 0:
            working_day = pd.concat([kr_working_day, us_working_day]).reset_index(drop=True)
        elif len(kr_days) > 0:
            working_day = kr_working_day
        else:
            working_day = us_working_day

        working_day['working_day'] = working_day['working_day'].astype('string')
        working_day['country'] = working_day['country'].astype('string')

        # filtering 대상 아닌 거래내역 분리
        tmp_raw_tr = self.raw_tr.loc[(self.raw_tr['transaction_type'] == '종목변경출고') &
                                     (self.raw_tr['securities_code'] == 'MIRAE'), :]
        self.raw_tr = self.raw_tr.loc[~((self.raw_tr['transaction_type'] == '종목변경출고') &
                                        (self.raw_tr['securities_code'] == 'MIRAE')), :]

        # 휴장일 필터링
        self.raw_tr = pd.merge(self.raw_tr, working_day, how='inner', left_on=['transaction_date', 'country'],
                               right_on=['working_day', 'country'])

        # 거래내역 합치기
        self.raw_tr = pd.concat([self.raw_tr, tmp_raw_tr])

    def generate_pulled_transaction_date(self):
        """2영업일 전 거래일 변수 생성"""
        # 한국 주식
        kr_days = self.raw_tr.loc[self.raw_tr['country'].values == 'KR', 'transaction_date']
        if len(kr_days) > 0:
            kr_min_date = min(kr_days)
            # 영업일 데이터 로드
            with session_scope() as session:
                # kr working day
                kr_working_day_1 = aliased(DateWorkingDay)
                kr_working_day_2 = aliased(DateWorkingDay)
                kr_working_day = session.query(kr_working_day_1.working_day.label('date'),
                                               kr_working_day_2.working_day.label('pulled_transaction_date')). \
                    join(kr_working_day_2, kr_working_day_1.seq == kr_working_day_2.seq + 2). \
                    filter(kr_working_day_1.working_day >= kr_min_date)
                kr_working_day = pd.read_sql(kr_working_day.statement, session.bind)
                kr_working_day['country'] = 'KR'
                session.commit()

        # 미국 주식
        foreign_days = self.raw_tr.loc[self.raw_tr['country'].values == 'foreign', 'transaction_date']
        if len(foreign_days) > 0:
            foreign_min_date = min(foreign_days)
            # 영업일 데이터 로드
            with session_scope() as session:
                # us working day
                us_working_day_1 = aliased(UsDateWorkingDay)
                us_working_day_2 = aliased(UsDateWorkingDay)
                us_working_day = session.query(UsDateWorkingDayMapping.date.label('date'),
                                               us_working_day_2.working_day.label('pulled_transaction_date')). \
                    join(us_working_day_1, UsDateWorkingDayMapping.working_day == us_working_day_1.working_day). \
                    join(us_working_day_2, us_working_day_1.seq == us_working_day_2.seq + 2). \
                    filter(UsDateWorkingDayMapping.date >= foreign_min_date)
                us_working_day = pd.read_sql(us_working_day.statement, session.bind)
                us_working_day['country'] = 'foreign'
                session.commit()

        # working_day 생성
        if len(kr_days) > 0 and len(foreign_days) > 0:
            working_day = pd.concat([kr_working_day, us_working_day])
        elif len(kr_days) > 0:
            working_day = kr_working_day
        else:
            working_day = us_working_day
        working_day['date'] = working_day['date'].astype('string')

        # pulled_transaction_date 추가
        self.raw_tr = pd.merge(self.raw_tr, working_day, how='left', left_on=['transaction_date', 'country'], right_on=['date', 'country'])
        self.raw_tr['pulled_transaction_date'] = self.raw_tr['pulled_transaction_date'].astype('string')

    def standardize_tr(self):
        """증권사별 거래내역 표준화"""
        # 숫자형 값 내 ',' 제거
        col_list = ['transaction_unit_price', 'transaction_quantity', 'transaction_amount', 'balance', 'total_unit',
                    'total_sum', 'transaction_fee', 'transaction_tax']
        for col in col_list:
            self.raw_tr[col] = self.raw_tr[col].apply(lambda x: x.replace(',', ''))

        # 증권사별 조건 로드
        with session_scope() as session:
            securities_tr_condition = session.query(SecuritiesTrCondition).\
                filter(SecuritiesTrCondition.securities_code == self.securities_code,
                       SecuritiesTrCondition.std_tr != '')
            tr_cond = pd.read_sql(securities_tr_condition.statement, session.bind)
            session.commit()

        # namuh일 경우 tr 나눠줌
        if (self.securities_code.upper() == 'NAMUH') or (self.securities_code.upper() == 'NHQV'):
            self.raw_tr['transaction_unit_price'] = self.raw_tr['transaction_unit_price'].apply(lambda x: str(int(x)/100))
            self.raw_tr['transaction_quantity'] = self.raw_tr['transaction_quantity'].apply(lambda x: str(int(x)/1000000))
            self.raw_tr['transaction_amount'] = self.raw_tr['transaction_amount'].apply(lambda x: str(int(x)/100))
            self.raw_tr['balance'] = self.raw_tr['balance'].apply(lambda x: str(int(x)/100))
            self.raw_tr['total_unit'] = self.raw_tr['total_unit'].apply(lambda x: str(int(x)/1000000))

        # 거래내역 표준화
        for i, r in tr_cond.iterrows():
            cond = np.array([True] * self.raw_tr.shape[0])
            if r['transaction_type'] is not None:
                cond = cond & (self.raw_tr['transaction_type'].values == r['transaction_type'])
            if r['transaction_detail_type'] is not None:
                cond = cond & (self.raw_tr['transaction_detail_type'].values == r['transaction_detail_type'])
            if r['country'] is not None:
                cond = cond & (self.raw_tr['country'].values == r['country'])

            # transaction_type 표준 거래내역으로 변경
            if any(cond):
                # 거래내역 업데이트
                self.raw_tr.loc[cond, 'transaction_type'] = r['std_tr']

                # 2영업일 당기기
                if r['std_tr'] in ['매수', '매도', '해외주식매수', '해외주식매도', '주식합병출고']:
                    # 주식합병출고의 경우, 결제일처리 시 해당일에 피합병된 주식이 상폐되어 주가정보가 없어 2영업일 전 처리 함.
                    self.raw_tr.loc[cond, 'transaction_date'] = self.raw_tr.loc[cond, 'pulled_transaction_date'].values

                # 배당 처리
                if r['std_tr'] in ['배당', '해외주식배당']:
                    self.raw_tr.loc[cond, 'transaction_quantity'] = 1
                    self.raw_tr.loc[cond, 'transaction_unit_price'] = self.raw_tr.loc[cond, 'transaction_amount'].values

                # 한투 액면분할 처리
                self.trfr_split_helper()

        # 거래내역 필터링
        self.raw_tr = self.raw_tr.loc[np.isin(self.raw_tr['transaction_type'].values, tr_cond['std_tr']), :].copy()

    def trfr_split_helper(self):
        """한국투자증권 액면분할 거래내역 처리"""
        # 한투 액면분할 처리
        if self.securities_code == 'TRUEFR':
            # transaction_unit_price 업데이트
            if any((self.raw_tr['transaction_type'].values == '액면분할병합출고') |
                   (self.raw_tr['transaction_type'].values == '액면분할병합입고')):
                for i, r in self.raw_tr.loc[(self.raw_tr['transaction_type'].values == '액면분할병합출고') |
                                            (self.raw_tr['transaction_type'].values == '액면분할병합입고'), :].iterrows():
                    # extra_tr 생성
                    extra_tr = self.raw_tr.loc[(self.raw_tr['transaction_date'].values < r['transaction_date']) &
                                               (self.raw_tr['stock_cd'].values == r['stock_cd']),
                                               ['stock_cd', 'transaction_date', 'transaction_type', 'transaction_unit_price', 'transaction_quantity']]
                    # transaction_unit_price 생성
                    tmp_unit_price = util_get_avg_purchase_price(self.user_code, self.account_number, r['stock_cd'], extra_tr=extra_tr)

                    if r['transaction_type'] == '액면분할병합입고':
                        split_release_quantity = util_get_recent_split_release_quantity(self.user_code, self.account_number, r['stock_cd'], extra_tr=extra_tr)
                        if r['transaction_unit_price'] is None or split_release_quantity is None:
                            continue
                        tmp_unit_price = tmp_unit_price / (r['transaction_quantity'] / split_release_quantity)

                    # transaction_unit_price 업데이트
                    self.raw_tr.loc[(self.raw_tr['transaction_date'].values == r['transaction_date']) &
                                    (self.raw_tr['seq'].values == r['seq']), 'transaction_unit_price'] = tmp_unit_price

    def update_seq(self):
        self.raw_tr['seq'] = self.raw_tr.groupby(['transaction_date']).cumcount() + 1

    def generate_stock_type(self):
        """stock_type 변수 생성"""
        with session_scope() as session:
            # get kr_stock_info
            kr_stock_info = session.query(StockInfo.stock_cd, StockInfo.stock_nm, StockInfo.end_date, StockInfo.market)
            kr_stock_info = pd.read_sql(kr_stock_info.statement, session.bind)
            kr_stock_info['stock_cd'] = kr_stock_info['stock_cd'].apply(lambda x: x[1:])
            session.commit()

            # get us_stock_info
            us_stock_info = session.query(UsStockInfo.stock_cd, UsStockInfo.stock_nm,
                                          UsStockInfo.latest_date.label('end_date'), UsStockInfo.exchange.label('market'))
            us_stock_info = pd.read_sql(us_stock_info.statement, session.bind)
            session.commit()

        # stock_info 생성
        stock_info = pd.concat([kr_stock_info, us_stock_info])
        stock_info = stock_info.merge(stock_info.groupby('stock_cd').max('end_date').reset_index(), on='stock_cd')
        stock_info = stock_info.loc[stock_info['end_date_x'].values == stock_info['end_date_y'].values, :]
        stock_info = stock_info.drop(columns='end_date_y')
        stock_info.columns = ['stock_cd', 'new_stock_nm', 'end_date', 'market']

        # raw_tr, stock_info join
        self.raw_tr = pd.merge(self.raw_tr, stock_info, how='left', on=['stock_cd'])

        # stock_type 생성
        stock_type_map = {'KOSPI': 'domestic_stock',
                          'KOSDAQ': 'domestic_stock',
                          'ETF': 'domestic_etf',
                          'ETN': 'domestic_etn',
                          'NYSE': 'us_stock',
                          'NASDAQ': 'us_stock',
                          'AMEX': 'us_stock'}
        self.raw_tr['stock_type'] = self.raw_tr['market'].apply(
            lambda x: 'etc' if x not in stock_type_map.keys() else stock_type_map[x])

        # stock_type: etc filtering
        self.raw_tr = self.raw_tr.loc[self.raw_tr['stock_type'] != 'etc', :].copy()
        # self.raw_tr['stock_type'] = self.raw_tr.apply(
        #     lambda x: 'foreign_etc' if (x['stock_type'] == 'etc') & (x['country'] == 'foreign') else x['stock_type'],
        #     axis=1)

    def update_stock_nm(self):
        self.raw_tr['stock_nm'] = self.raw_tr.apply(
            lambda x: x['stock_nm'] if pd.isna(x['new_stock_nm']) else x['new_stock_nm'], axis=1)

    def update_unit_currency(self):
        self.raw_tr['unit_currency'] = self.raw_tr.apply(
            lambda x: 'KRW' if x['stock_type'] == 'domestic_stock' else 'USD' if x['stock_type'] == 'us_stock' else x['unit_currency'], axis=1)

    def update_country(self):
        self.raw_tr['country'] = self.raw_tr.apply(
            lambda x: 'US' if x['stock_type'] == 'us_stock' else x['country'], axis=1)

    def update_tmp_tr_map(self):
        """tmp_user_portfolio_map 테이블 업데이트"""
        # 기존 map 로드
        with session_scope() as session:
            # get user_portfolio_map
            portfolio_map = session.query(UserPortfolioMap).\
                filter(UserPortfolioMap.user_code == self.user_code,
                       UserPortfolioMap.account_number == self.account_number)
            portfolio_map = pd.read_sql(portfolio_map.statement, session.bind)
            session.commit()

        target_cd = self.raw_tr.loc[self.raw_tr['stock_type'].apply(lambda x: x not in ['etc', 'foreign_etc']), 'stock_cd'].unique()
        new_map = target_cd[~np.isin(target_cd, portfolio_map['stock_cd'])]
        if len(new_map) == 0:
            return None
        else:
            for cd in new_map:
                # 기존 map이 없을 경우 insert
                portfolio_map = TmpUserPortfolioMap(user_code=self.user_code,
                                                    account_number=self.account_number,
                                                    stock_cd=cd,
                                                    portfolio_code=self.portfolio_code,
                                                    securities_code=self.securities_code,
                                                    lst_update_dtim=datetime.today().strftime("%Y%m%d%H%M%S"))
                session.add(portfolio_map)
                session.commit()

    def insert_tmp_tr_log(self):
        """tmp_user_trading_log 테이블 업데이트"""
        col_list = ['user_code', 'account_number', 'transaction_date', 'seq', 'stock_cd', 'stock_type',
                    'securities_code', 'stock_nm', 'transaction_type', 'transaction_detail_type',
                    'transaction_quantity', 'transaction_unit_price', 'transaction_fee', 'transaction_tax',
                    'unit_currency', 'update_dtim', 'country']
        self.raw_tr['update_dtim'] = datetime.now().strftime('%Y%m%d%H%M%S')
        tmp_tr_log = self.raw_tr.loc[:, col_list]
        insert_data(tmp_tr_log, 'tmp_user_trading_log')

    def overwrite_map(self):
        """tmp_user_portfolio_map을 user_portfolio_map에 overwrite"""
        # user_portfolio_map 덮어쓰기
        exec_query(f'replace into user_portfolio_map '
                   f'select * '
                   f'from tmp_user_portfolio_map '
                   f'where user_code = {self.user_code} '
                   f'and account_number = "{self.account_number}"')

        # delete user_portfolio_map
        exec_query(f'delete from tmp_user_portfolio_map '
                   f'where user_code = {self.user_code} '
                   f'and account_number = "{self.account_number}"')

    def overwrite_tr(self):
        """tmp_user_trading_log를 user_trading_log에 overwrite"""
        # tmp_user_trading_log의 min_date 구하기
        tr_days = self.raw_tr['transaction_date']
        if len(tr_days) == 0:  # 거래내역 없을 경우 return none
            return None
        self.min_date = min(tr_days)
        # 거래내역 삭제
        with session_scope() as session:
            session.query(UserTradingLog). \
                filter(UserTradingLog.user_code == self.user_code,
                       UserTradingLog.account_number == self.account_number,
                       UserTradingLog.date >= self.min_date). \
                delete(synchronize_session='fetch')
            # db commit
            session.commit()

        # 거래내역 insert
        exec_query(f'insert into user_trading_log '
                   f'select * '
                   f'from tmp_user_trading_log '
                   f'where user_code = {self.user_code} '
                   f'and account_number = "{self.account_number}" ')

        # delete tmp_user_trading_log
        exec_query(f'delete from tmp_user_trading_log '
                   f'where user_code = {self.user_code} '
                   f'and account_number = "{self.account_number}"')


if __name__ == '__main__':
    # pandas show 옵션
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    user_code = 695
    portfolio_code = 1
    securities_code = 'TRUEFR'
    account_nubmer = '6880649101'

    raw_tr = get_df_from_db(f'select * '
                            f'from user_trading_log_raw '
                            f'where user_code = {user_code} '
                            f'and securities_code = "{securities_code}" '
                            f'and account_number ="{account_nubmer}" ')
    raw_tr.columns = ['user_code', 'securities_code', 'account_number', 'transaction_date', 'seq',
                      'stock_cd', 'transaction_time', 'stock_nm', 'transaction_type',
                      'transaction_detail_type', 'transaction_unit_price', 'transaction_quantity',
                      'transaction_amount', 'balance', 'total_unit', 'total_sum', 'transaction_fee',
                      'transaction_tax', 'unit_currency', 'ext_yn', 'lst_update_dtim', 'country']

    user_trading_log_module = UserTradingLogModule(user_code=user_code, portfolio_code=portfolio_code,
                                                   securities_code=securities_code, account_number=account_nubmer,
                                                   raw_tr=raw_tr)
    user_trading_log_module.insert_tr_log()
