import json
import redis
from kafka import KafkaProducer
import os
import pandas as pd
import numpy as np
from datetime import datetime
from db.db_model import session_scope, UserPortfolio, DateWorkingDay, UsDateWorkingDay, UserTradingLog, TmpUserPortfolio
from db.db_connect import get_df_from_db
from sqlalchemy import func
from flask import current_app as app
from module.merlot_logging import *

class TradeLog:
    def __init__(self, user_code, stock_code, begin_date, account_number, securities_code, country, tmp_portfolio):
        self.user_code = user_code
        self.stock_code = stock_code
        self.begin_date = begin_date
        self.account_number = account_number
        self.securities_code = securities_code
        self.country = country
        self.tmp_portfolio = tmp_portfolio
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def print_log(self):
        print(f"[{self.timestamp}] : stock_cd - {self.stock_code} begin_date - {self.begin_date} "
              f"account_no-{self.account_number} securities_cd : {self.securities_code} country:{self.country}")

    def to_json(self):
        dictform = {"user_code": self.user_code, "stock_code":self.stock_code,
                    "begin_date": self.begin_date, "account_number":self.account_number,
                    "securities_code": self.securities_code, "country": self.country, "tmp_portfolio": self.tmp_portfolio,
                    "timestamp": self.timestamp}
        return json.dumps(dictform).encode("utf-8")




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





class UpdaterByKafka:
    def __init__(self):
        self.partition_status={}
        self.kafka_host = os.environ.get("KAFKA_HOST")
        self.kafka_port = os.environ.get("KAFKA_PORT")
        self.redis_host = os.environ.get("REDIS_HOST")

    def get_receiving_transaction_list(self):
        return ['매수', '유상주입고', '무상주입고', '공모주입고', '타사대체입고', '대체입고', '액면분할병합입고',
                '감자입고', '회사분할입고', '배당입고', '해외주식매수']

    def get_releasing_transaction_list(self ):
        return ['매도', '타사대체출고', '대체출고', '액면분할병합출고', '감자출고', '해외주식매도']

    def get_dividend_transaction_list(self):
        return ['배당', '배상세출금', '해외주식배당']  # 배당세 출금은 2021.06.23 현재 거래내역이 사실상 존재하지 않아 어떻게 처리될지 모른다.

    def util_get_avg_purchase_price(self,user_code, account_number, stock_cd):
        # 거래내역 불러오기
        sql = f'select * ' \
              f'from user_trading_log ' \
              f'where user_code = {user_code} ' \
              f'and account_number = "{account_number}" ' \
              f'and stock_cd = "{stock_cd}" '
        tr_log = get_df_from_db(sql)

        avg_purchase_price = 0
        holding_quantity = 0
        for i, r in tr_log.iterrows():
            if r['transaction_type'] in self.get_releasing_transaction_list():
                avg_purchase_price = (avg_purchase_price * holding_quantity +
                                      r['transaction_unit_price'] * r['transaction_quantity']) / \
                                     (holding_quantity + r['transaction_quantity'])
                holding_quantity = holding_quantity + r['transaction_quantity']

        if holding_quantity == 0:
            return None
        else:
            return avg_purchase_price

    def update_single_stock_portfolio(self, topic_name, to_update_list, user_code, max_date, country, tmp_portfolio):
        final_kafka_url = self.kafka_host + ":" + self.kafka_port
        PRODUCER = KafkaProducer(bootstrap_servers=[final_kafka_url])
        redis_client = redis.StrictRedis(host=self.redis_host, port=6379, db=0)
        subscriber = redis_client.pubsub()
        subscriber.psubscribe(f"partition*")
        self.partition_status={}
        PAUSE = False
        publish_stock_cnt = 0
        if to_update_list.shape[0] != 0:
            # 포트폴리오 업데이트
            cnt = 0
            for item in subscriber.listen():
                item_channel = item['channel'].decode("utf-8")

                ## partition_status 는 produce 하는 당시에 갱신되는 값이니깐, 항상 redis로부터 받아오는 값보다 클 수 밖에 없다.
                ## 그래서 평소에는 이 로직을 안타다가, produce는 끝나서 더 이상 갱신은 안되고, redis는 점점 처리해서 offset을 따라오게 되면,
                # 그래서 그 처리한 offsest이 같아지면 이 로직을 타게 되는 것이다.(=여기 까지 처리했다고 나타내는 부분)
                if item_channel in self.partition_status.keys() and self.partition_status[item_channel] <= int(
                        item['data']):
                    print(
                        f"item {item['channel'].decode('utf-8')} - data {int(item['data'])} & {self.partition_status[item['channel'].decode('utf-8')]}")
                    cnt += 1
                if PAUSE == False:
                    for i, r in to_update_list.iterrows():
                        trade_log = TradeLog(user_code, r["stock_cd"], max_date, r["account_number"],
                                             r["securities_code"], country, tmp_portfolio)
                        publish_stock_cnt += 1
                        self.publish_to_kafka(topic_name, trade_log, PRODUCER)
                    PAUSE = True
                    print(self.partition_status)
                    PRODUCER.flush()

                if cnt == len(self.partition_status.keys()):
                    sent_stocks = len(self.partition_status.keys())
                    received_stocks = cnt
                    msg = f"sent stocks={sent_stocks}_received_stocks={received_stocks}"
                    app.logger.info(make_log_msg("/api/securities/multiinserttradinglog", msg))
                    app.logger.debug(make_log_msg("/api/securities/multiinserttradinglog", self.partition_status))
                    break



    def util_get_recent_split_release_quantity(self, user_code, account_number, stock_cd):
        # 거래내역 불러오기
        sql = f'select * ' \
              f'from user_trading_log ' \
              f'where user_code = {user_code} ' \
              f'and account_number = "{account_number}" ' \
              f'and stock_cd = "{stock_cd}" ' \
              f'and transaction_type = "액면분할병합출고" ' \
              f'order by date desc '
        tr_log = get_df_from_db(sql)

        if tr_log.shape[0] == 0:
            return None
        else:
            return tr_log['transaction_quantity'][0]

    def update_user_portfolio_by_country(self, user_code, country, topic_name, account_number=None, tmp_portfolio=False):

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
                log_msg = make_log_msg("/api/securities/multiinserttradinglog", f"to update stock: {update_list_df.shape[0]}")
                app.logger.info(log_msg)
                self.update_single_stock_portfolio(topic_name, update_list_df, user_code, max_date, country, tmp_portfolio)


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
        # 업데이트할 포트폴리오가 존재한다면, 포트폴리오 업데이트 해줌.

        # etc 제외
        tr_update_list_df = tr_update_list_df.loc[~np.isin(tr_update_list_df['stock_type'], ['etc', 'foreign_etc']), :]

        log_msg = make_log_msg("/api/securities/multiinserttradinglog", f"to update stock: {tr_update_list_df.shape[0]}")

        app.logger.info(log_msg)
        self.update_single_stock_portfolio(topic_name, tr_update_list_df, user_code, max_date, country, tmp_portfolio)


    def on_send_error(self, excp):
        app.logger.error(make_log_msg("/api/securities/multiinserttradinglog", str(excp)))

    def on_send_success(self, record_metadata):
        self.partition_status[f"partition{record_metadata.partition}"] = record_metadata.offset+1
        msg = f"sent data - topic:, {record_metadata.topic} partition: {record_metadata.partition}, offset: {record_metadata.offset+1}"
        app.logger.debug(make_log_msg("/api/securities/multiinserttradinglog", msg))

    def publish_to_kafka(self, topic_name, tradeLog: TradeLog, produ):
        produ.send(topic=topic_name, value=tradeLog.to_json()).add_callback(
            self.on_send_success).add_errback(self.on_send_error)

def update_user_portfolio(user_code, topic_name):
    kafkaUpdater = UpdaterByKafka()
    kafkaUpdater.update_user_portfolio_by_country(user_code=user_code, country='KR', topic_name=topic_name)
    kafkaUpdater.update_user_portfolio_by_country(user_code=user_code, country='US', topic_name=topic_name)


def update_user_portfolio_by_account(user_code, account_number, tmp_portfolio=False):
    topic_name = os.environ.get("TOPIC_NAME")
    kafkaUpdater = UpdaterByKafka()
    kafkaUpdater.update_user_portfolio_by_country(user_code=user_code, country='KR', topic_name=topic_name, account_number=account_number, tmp_portfolio=tmp_portfolio)
    kafkaUpdater.update_user_portfolio_by_country(user_code=user_code, country='US', topic_name=topic_name, account_number=account_number, tmp_portfolio=tmp_portfolio)


