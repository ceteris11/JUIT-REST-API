import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from db.db_model import session_scope, StockAutocompleteNameList


def get_country(stock_cd):
    with session_scope() as session:
        stock_autocomplete_name_list = session.query(StockAutocompleteNameList). \
            filter(StockAutocompleteNameList.stock_cd.like('%' + stock_cd)). \
            first()
        session.commit()

        if stock_autocomplete_name_list is not None:
            if stock_autocomplete_name_list.market in ['KOSPI', 'KOSDAQ', 'ETF', 'ETN', ]:
                country = 'KR'
            else:
                country = 'US'
        else:
            country = 'foreign'

    return country
