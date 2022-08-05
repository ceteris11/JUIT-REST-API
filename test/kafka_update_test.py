import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from util.kafka_update_portfolio import *
import time

def kafka_update_by_country():
    print("===== config log ======")
    kafkaUpdater = UpdaterByKafka()
    start = time.time()
    kafkaUpdater.update_user_portfolio_by_country(user_code=215, country="KR", topic_name="stocktest40")
    mid = time.time()
    kafkaUpdater.partition_status={}
    print(f"elapsed : {mid - start}")
    kafkaUpdater.update_user_portfxolio_by_country(user_code=215, country="US", topic_name="stocktest40")
    end = time.time()
    print(f"elapsed : {end - mid}")

def kafka_update_by_user():
    start = time.time()
    update_user_portfolio(215, "stocktest40")

    end = time.time()
    print(f"elapsed : {end - start}")

def kafka_update_by_account():
    start = time.time()
    update_user_portfolio_by_account(215, account_number="5521993510", tmp_portfolio=True)
    end = time.time()
    print(f"elapsed : {end - start}")

if __name__=='__main__':
    #kafka_update_by_user()
    kafka_update_by_account()