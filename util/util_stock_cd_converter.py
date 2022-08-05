import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import requests
from requests import adapters
import ssl
from urllib3 import poolmanager
import json
from db.db_model import session_scope, UsIsinCdMap, UsTrfrStockNmSymbolMap, TrfrStockNmStockCdMap, StockInfo, \
    StockIsinCdMap
from bs4 import BeautifulSoup


def get_api_key():
    return '1be374d4-aa40-454b-8cf4-5fec143959f2'


def get_symbol_from_figi(isin):
    url = 'https://api.openfigi.com/v1/mapping'
    headers = {'Content-Type': 'text/json',
               'X-OPENFIGI-APIKEY': get_api_key()}
    payload = f'[{{"idType":"ID_ISIN", "idValue":"{isin}"}}]'

    r = requests.post(url, headers=headers, data=payload)
    rst = json.loads(r.text)

    if 'data' in rst[0].keys():
        symbol = rst[0]['data'][0]['ticker']
    else:
        symbol = ''
    return symbol


def get_us_symbol_from_isin(isin_cd):
    with session_scope() as session:
        # db에 isin_cd가 있는지 확인
        stock_cd_map = session.query(UsIsinCdMap). \
            filter(UsIsinCdMap.isin_cd == isin_cd). \
            first()
        session.commit()

        if stock_cd_map is None:
            # 없을 경우 insert
            stock_cd = get_symbol_from_figi(isin_cd)
            new_isin_map = UsIsinCdMap(isin_cd=isin_cd,
                                       stock_cd=stock_cd)
            session.add(new_isin_map)
            session.commit()
        else:
            # 있을 경우 가져온다
            stock_cd = stock_cd_map.stock_cd

    return stock_cd


class TLSAdapter(adapters.HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        """Create and initialize the urllib3 PoolManager."""
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        self.poolmanager = poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLS,
            ssl_context=ctx)


def get_symbol_from_truefr(stock_nm):
    url = 'https://globalmonitor.einfomax.co.kr/apis/getitemsearch'
    headers = {'Content-Type': 'application/json',
               'Content-Length': '98',
               'Host': 'globalmonitor.einfomax.co.kr'}
    payload = f'{{"country": "USA", "exchange": "", "code": "{stock_nm}", ' \
              f'"param": {{}}, "isic": "전체", "support_country": []}}'

    session = requests.session()
    session.mount('https://', TLSAdapter())
    r = session.post(url, headers=headers, data=payload.encode('utf-8'))

    rst = json.loads(r.text)
    # rst 형식이 올바르지 않을 경우
    if type(rst) != dict:
        return ''
    if 'items' not in rst.keys():
        return ''

    symbol = ''
    for i in rst['items']:
        if i['kornm'] == stock_nm:
            symbol = i['ticker']

    return symbol


def get_us_symbol_from_trfr_stock_nm(stock_nm):
    with session_scope() as session:
        # db에 isin_cd가 있는지 확인
        stock_cd_map = session.query(UsTrfrStockNmSymbolMap). \
            filter(UsTrfrStockNmSymbolMap.stock_nm == stock_nm). \
            first()
        session.commit()

        if stock_cd_map is None:
            # 없을 경우 insert
            stock_cd = get_symbol_from_truefr(stock_nm)
            new_stock_nm_map = UsTrfrStockNmSymbolMap(stock_nm=stock_nm,
                                                      stock_cd=stock_cd)
            session.add(new_stock_nm_map)
            session.commit()
        else:
            # 있을 경우 가져온다
            stock_cd = stock_cd_map.stock_cd

    return stock_cd


def get_stock_cd_from_trfr_transaction(r):
    # stock_nm 생성 / '보통주'로 끝나는 경우 '보통주' 제외
    stock_nm = r['stock_nm'][:-3] if r['stock_nm'].endswith('보통주') else r['stock_nm']

    # stock_cd mapping
    with session_scope() as session:
        trfr_stock_cd = session.query(StockInfo.stock_nm, StockInfo.stock_cd). \
            filter(StockInfo.stock_nm == stock_nm,
                   StockInfo.market != 'KONEX',
                   StockInfo.begin_date <= r['transaction_date']).\
            order_by(StockInfo.begin_date.desc()).\
            first()

        if trfr_stock_cd is None:
            stock_cd = ''
        else:
            stock_cd = trfr_stock_cd.stock_cd[-6:]

    if stock_cd == '':
        # 종목코드 매핑이 안된 경우
        with session_scope() as session:
            # db에 stock_nm이 있는지 확인
            stock_cd_map = session.query(TrfrStockNmStockCdMap). \
                filter(TrfrStockNmStockCdMap.stock_nm == stock_nm). \
                first()
            session.commit()

            if stock_cd_map is None:
                # 없을 경우 insert
                new_stock_nm_map = TrfrStockNmStockCdMap(stock_nm=stock_nm,
                                                         stock_cd='')
                session.add(new_stock_nm_map)
                session.commit()
                stock_cd = ''
            else:
                # 있을 경우 가져온다
                stock_cd = stock_cd_map.stock_cd

    return stock_cd


def get_kr_stock_cd_from_isin(isin_cd):
    # db 확인
    with session_scope() as session:
        db_isin_cd = session.query(StockIsinCdMap).filter(StockIsinCdMap.isin_cd == isin_cd).first()
        if db_isin_cd is not None:
            return db_isin_cd.stock_cd

    # isin_cd 크롤링
    # isin_cd = 'KR7148070006'
    isin_url = 'https://isin.krx.co.kr/srch/srch.do?method=srchPopup2'
    isin_data = {
        'stdcd_type': '2',
        'std_cd': isin_cd,
    }

    r = requests.post(isin_url, data=isin_data)
    soup = BeautifulSoup(r.content, 'lxml')
    stock_cd = soup.select('#wrapper-pop > div > table > tbody > tr:nth-child(2) > td.last')[0].text
    stock_cd = stock_cd[1:]

    if len(stock_cd) > 6:
        stock_cd = ''

    # DB 추가
    with session_scope() as session:
        session.add(StockIsinCdMap(isin_cd=isin_cd, stock_cd=stock_cd))
        session.commit()

    return stock_cd


if __name__ == '__main__':
    from datetime import datetime

    print(datetime.now())
    get_us_symbol_from_isin('US4592001014')
    print(datetime.now())
