from db.db_connect import *


user_trading_log_row_count = 250
user_portfolio_row_count = 5957
stock_daily_technical_row_count = 21049

def test_query():

    query_user_trading_log = 'select * from  user_trading_log'
    query_user_portfolio = 'select * from  user_portfolio'
    query_stock_daily_technical = 'select * from  stock_daily_technical'

    assert get_df_from_db(query_user_trading_log).shape[0] \
           == user_trading_log_row_count

    assert get_df_from_db(query_user_portfolio).shape[0] \
           == user_portfolio_row_count

    assert get_df_from_db(query_stock_daily_technical).shape[0] \
           == stock_daily_technical_row_count