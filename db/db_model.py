from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import time
from contextlib import contextmanager
from config.config_manager import get_config
from module.merlot_logging import *

app = Flask(__name__)

ctx = app.app_context()
ctx.push()

app.config['SQLALCHEMY_DATABASE_URI'] = \
    f"mysql+pymysql://{get_config()['user']}:{get_config()['pwd']}@{get_config()['db_url']}:{get_config()['port']}/sauvignon" \
    f'?autocommit=true&charset=utf8mb4'

app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        "pool_pre_ping" : True,
        "pool_recycle": 280,
        "pool_size" : 100
}

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = db.create_scoped_session()
    try:
        yield session
        session.commit()
    except Exception as e:
        app.logger.error(make_log_msg("flask: session_scope", e))
        session.rollback()
    finally:
        session.close()


class UserInfo(db.Model):
    __table_name__ = 'user_info'

    user_code = db.Column(db.Integer, primary_key=True)
    login_type = db.Column(db.String, nullable=False)
    login_key = db.Column(db.String, nullable=False)
    beg_dtim = db.Column(db.String, nullable=False, default=time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))
    end_dtim = db.Column(db.String, nullable=False, default='99991231235959')
    passwd = db.Column(db.String, nullable=False)
    salt = db.Column(db.String, nullable=False)
    user_name = db.Column(db.String, nullable=True)
    user_birth = db.Column(db.String, nullable=True)
    user_sex = db.Column(db.String, nullable=True)
    user_foreign = db.Column(db.String, nullable=True)
    user_phone_number = db.Column(db.String, nullable=True)


class UserMobileVerifInfo(db.Model):
    __table_name__ = 'user_mobile_verif_info'

    req_num = db.Column(db.Integer, primary_key=True)
    user_name = db.Column(db.String, nullable=True)
    user_birth = db.Column(db.String, nullable=True)
    user_sex = db.Column(db.String, nullable=True)
    user_foreign = db.Column(db.String, nullable=True)
    user_di = db.Column(db.String, nullable=True)
    user_phone_number = db.Column(db.String, nullable=True)
    lst_update_dtim = db.Column(db.String, nullable=True, default=time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))


class UserEmailVerificationCode(db.Model):
    __table_name__ = 'user_email_verification_code'

    email = db.Column(db.String, primary_key=True)
    beg_dtim = db.Column(db.String, nullable=False, primary_key=True)
    end_dtim = db.Column(db.String, nullable=False)
    verification_code = db.Column(db.Integer, nullable=False)


class UserTradingLog(db.Model):
    __table_name__ = 'user_trading_log'

    user_code = db.Column(db.Integer, primary_key=True)
    account_number = db.Column(db.String, primary_key=True)
    date = db.Column(db.String, primary_key=True)
    seq = db.Column(db.Integer, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    stock_type = db.Column(db.String, nullable=True)
    securities_code = db.Column(db.String, nullable=False)
    stock_nm = db.Column(db.String, nullable=True)
    transaction_type = db.Column(db.String, nullable=True)
    transaction_detail_type = db.Column(db.String, nullable=True)
    transaction_quantity = db.Column(db.Float, nullable=True)
    transaction_unit_price = db.Column(db.Float, nullable=True)
    transaction_fee = db.Column(db.Float, nullable=True)
    transaction_tax = db.Column(db.Float, nullable=True)
    unit_currency = db.Column(db.String, nullable=True)
    update_dtim = db.Column(db.String, nullable=True)
    country = db.Column(db.String, nullable=True)


class TmpUserTradingLog(db.Model):
    __table_name__ = 'tmp_user_trading_log'

    user_code = db.Column(db.Integer, primary_key=True)
    account_number = db.Column(db.String, primary_key=True)
    date = db.Column(db.String, primary_key=True)
    seq = db.Column(db.Integer, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    stock_type = db.Column(db.String, nullable=True)
    securities_code = db.Column(db.String, nullable=False)
    stock_nm = db.Column(db.String, nullable=True)
    transaction_type = db.Column(db.String, nullable=True)
    transaction_detail_type = db.Column(db.String, nullable=True)
    transaction_quantity = db.Column(db.Float, nullable=True)
    transaction_unit_price = db.Column(db.Float, nullable=True)
    transaction_fee = db.Column(db.Float, nullable=True)
    transaction_tax = db.Column(db.Float, nullable=True)
    unit_currency = db.Column(db.String, nullable=True)
    update_dtim = db.Column(db.String, nullable=True)
    country = db.Column(db.String, nullable=True)


class UserSimpleTradingLog(db.Model):
    __table_name__ = 'user_simple_trading_log'

    user_code = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.String, primary_key=True)
    seq = db.Column(db.Integer, primary_key=True)
    securities_code = db.Column(db.String, primary_key=True)
    securities_nm = db.Column(db.String, primary_key=True)
    account_number = db.Column(db.String, primary_key=True)
    portfolio_code = db.Column(db.Integer, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    stock_nm = db.Column(db.String, nullable=True)
    transaction_type = db.Column(db.String, nullable=False)
    transaction_quantity = db.Column(db.Integer, nullable=True)
    transaction_unit_price = db.Column(db.Float, nullable=True)
    transaction_unit_price_without_fee_tax = db.Column(db.Float, nullable=True)
    transaction_fee_tax = db.Column(db.Float, nullable=True)
    update_dtim = db.Column(db.String, nullable=True)


class UserTradingLogRaw(db.Model):
    __table_name__ = 'user_trading_log_raw'

    user_code = db.Column(db.Integer, primary_key=True)
    securities_code = db.Column(db.String, nullable=False)
    account_number = db.Column(db.String, primary_key=True)
    date = db.Column(db.String, primary_key=True)
    seq = db.Column(db.Integer, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    time = db.Column(db.String, nullable=True)
    stock_nm = db.Column(db.String, nullable=True)
    transaction_type = db.Column(db.String, nullable=True)
    transaction_detail_type = db.Column(db.String, nullable=True)
    transaction_unit_price = db.Column(db.String, nullable=True)
    transaction_quantity = db.Column(db.String, nullable=True)
    transaction_amount = db.Column(db.String, nullable=True)
    balance = db.Column(db.String, nullable=True)
    total_unit = db.Column(db.String, nullable=True)
    total_sum = db.Column(db.String, nullable=True)
    transaction_fee = db.Column(db.String, nullable=True)
    transaction_tax = db.Column(db.String, nullable=True)
    unit_currency = db.Column(db.String, nullable=True)
    ext_yn = db.Column(db.String, nullable=True)
    lst_update_dtim = db.Column(db.Integer, nullable=True)
    country = db.Column(db.String, nullable=True)


class UserSecuritiesInfo(db.Model):
    __table_name__ = 'user_securities_info'

    user_code = db.Column(db.Integer, primary_key=True)
    securities_code = db.Column(db.String, primary_key=True)
    securities_id = db.Column(db.String, nullable=True)
    securities_pw = db.Column(db.String, nullable=True)
    valid_flag = db.Column(db.Integer, nullable=True, default=-99)
    message = db.Column(db.String, nullable=True)
    lst_update_dtim = db.Column(db.String, nullable=False,
                                default=time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))


class UserPortfolio(db.Model):
    __table_name__ = 'user_portfolio'

    user_code = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Integer, primary_key=True)
    account_number = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    securities_code = db.Column(db.String, nullable=False)
    stock_nm = db.Column(db.String, nullable=True)
    holding_quantity = db.Column(db.Float, nullable=True)
    avg_purchase_price = db.Column(db.Float, nullable=True)
    close_price = db.Column(db.Float, nullable=True)
    prev_1w_close_price = db.Column(db.Float, nullable=True)
    total_value = db.Column(db.Float, nullable=True)
    first_purchase_date = db.Column(db.String, nullable=True)
    retention_period = db.Column(db.Integer, nullable=True)
    new_purchase_amount = db.Column(db.Float, nullable=True)
    realized_profit_loss = db.Column(db.Float, nullable=True)
    purchase_amount_of_stocks_to_sell = db.Column(db.Float, nullable=True)
    unit_currency = db.Column(db.String, nullable=True)
    update_dtim = db.Column(db.String, nullable=False)
    country = db.Column(db.String, nullable=True)


class TmpUserPortfolio(db.Model):
    __table_name__ = 'tmp_user_portfolio'

    user_code = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Integer, primary_key=True)
    account_number = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    securities_code = db.Column(db.String, nullable=False)
    stock_nm = db.Column(db.String, nullable=True)
    holding_quantity = db.Column(db.Float, nullable=True)
    avg_purchase_price = db.Column(db.Float, nullable=True)
    close_price = db.Column(db.Float, nullable=True)
    prev_1w_close_price = db.Column(db.Float, nullable=True)
    total_value = db.Column(db.Integer, nullable=True)
    first_purchase_date = db.Column(db.String, nullable=True)
    retention_period = db.Column(db.Integer, nullable=True)
    new_purchase_amount = db.Column(db.Integer, nullable=True)
    realized_profit_loss = db.Column(db.Integer, nullable=True)
    purchase_amount_of_stocks_to_sell = db.Column(db.Integer, nullable=True)
    unit_currency = db.Column(db.String, nullable=True)
    update_dtim = db.Column(db.String, nullable=False)
    country = db.Column(db.String, nullable=True)


class UserPortfolioInfo(db.Model):
    __table_name__ = 'user_portfolio_info'

    user_code = db.Column(db.Integer, primary_key=True)
    portfolio_code = db.Column(db.Integer, primary_key=True)
    portfolio_nm = db.Column(db.String, nullable=False)
    portfolio_order = db.Column(db.Integer, nullable=False)
    lst_update_dtim = db.Column(db.String, nullable=False, default=time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))


class UserPortfolioMap(db.Model):
    __table_name__ = 'user_portfolio_map'

    user_code = db.Column(db.Integer, primary_key=True)
    account_number = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    portfolio_code = db.Column(db.Integer, primary_key=True)
    securities_code = db.Column(db.String, nullable=False)
    lst_update_dtim = db.Column(db.String, nullable=False, default=time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))


class TmpUserPortfolioMap(db.Model):
    __table_name__ = 'tmp_user_portfolio_map'

    user_code = db.Column(db.Integer, primary_key=True)
    account_number = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    portfolio_code = db.Column(db.Integer, primary_key=True)
    securities_code = db.Column(db.String, nullable=False)
    lst_update_dtim = db.Column(db.String, nullable=False, default=time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))


class UserPortfolioMst(db.Model):
    __table_name__ = 'user_portfolio_mst'

    user_code = db.Column(db.Integer, primary_key=True)
    portfolio_code = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    stock_nm = db.Column(db.String, nullable=True)
    holding_quantity = db.Column(db.Float, nullable=True)
    avg_purchase_price = db.Column(db.Float, nullable=True)
    close_price = db.Column(db.Float, nullable=True)
    prev_1w_close_price = db.Column(db.Float, nullable=True)
    total_value = db.Column(db.Integer, nullable=True)
    first_purchase_date = db.Column(db.String, nullable=True)
    retention_period = db.Column(db.Integer, nullable=True)
    new_purchase_amount = db.Column(db.Integer, nullable=True)
    realized_profit_loss = db.Column(db.Integer, nullable=True)
    purchase_amount_of_stocks_to_sell = db.Column(db.Integer, nullable=True)
    unit_currency = db.Column(db.String, nullable=True)
    update_dtim = db.Column(db.String, nullable=False)
    portfolio_nm = db.Column(db.String, nullable=True)
    market = db.Column(db.String, nullable=True)
    sector = db.Column(db.String, nullable=True)


class SecuritiesCode(db.Model):
    __table_name__ = 'securities_code'

    begin_date = db.Column(db.String, primary_key=True)
    end_date = db.Column(db.String, primary_key=True)
    securities_code = db.Column(db.String, primary_key=True)
    securities_nm = db.Column(db.String, nullable=False)
    available_flag = db.Column(db.Integer, nullable=False)
    login_type = db.Column(db.String, nullable=False)
    infotech_code = db.Column(db.String, nullable=False)


class StockAutocompleteNameList(db.Model):
    __table_name__ = 'stock_autocomplete_name_list'

    stock_nm = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String, nullable=False)
    market = db.Column(db.String, nullable=False)
    lst_update_date = db.Column(db.String, nullable=False)


class StockInfo(db.Model):
    __table_name__ = 'stock_info'

    stock_cd = db.Column(db.String, primary_key=True)
    begin_date = db.Column(db.Integer, nullable=False)
    end_date = db.Column(db.Integer, nullable=False)
    stock_nm = db.Column(db.String, nullable=False)
    market = db.Column(db.String, nullable=False)
    sector = db.Column(db.String, nullable=False)
    lst_update_date = db.Column(db.String, nullable=False)


class StockDailyTechnical(db.Model):
    __tablename__ = 'stock_daily_technical'

    date = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    stock_nm = db.Column(db.String, nullable=True)
    open_price = db.Column(db.Integer, nullable=True)
    high_price = db.Column(db.Integer, nullable=True)
    low_price = db.Column(db.Integer, nullable=True)
    close_price = db.Column(db.Integer, nullable=True)
    trading_volume = db.Column(db.Integer, nullable=True)
    transaction_amount = db.Column(db.Integer, nullable=True)
    market_capitalization = db.Column(db.Integer, nullable=True)
    share_of_market_cap = db.Column(db.Float, nullable=True)
    num_of_listed_stocks = db.Column(db.Integer, nullable=True)
    foreign_own_quantity = db.Column(db.Integer, nullable=True)
    share_of_foreign_own = db.Column(db.Float, nullable=True)


class StockDailyEtf(db.Model):
    __tablename__ = 'stock_daily_etf'

    date = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    stock_nm = db.Column(db.String, nullable=True)
    open_price = db.Column(db.Integer, nullable=True)
    high_price = db.Column(db.Integer, nullable=True)
    low_price = db.Column(db.Integer, nullable=True)
    close_price = db.Column(db.Integer, nullable=True)
    trading_volume = db.Column(db.Integer, nullable=True)
    transaction_amount = db.Column(db.Integer, nullable=True)
    nav = db.Column(db.Integer, nullable=True)
    base_index_nm = db.Column(db.String, nullable=True)
    base_index = db.Column(db.Float, nullable=True)


class StockDailyEtn(db.Model):
    __tablename__ = 'stock_daily_etn'

    date = db.Column(db.String, primary_key=True)
    isin_cd = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    stock_nm = db.Column(db.String, nullable=True)
    open_price = db.Column(db.Integer, nullable=True)
    high_price = db.Column(db.Integer, nullable=True)
    low_price = db.Column(db.Integer, nullable=True)
    close_price = db.Column(db.Integer, nullable=True)
    trading_volume = db.Column(db.Integer, nullable=True)
    transaction_amount = db.Column(db.Integer, nullable=True)
    iv = db.Column(db.Integer, nullable=True)
    base_index = db.Column(db.Float, nullable=True)


class DartFinancialRatio(db.Model):
    __tablename__ = 'dart_financial_ratio'

    stock_cd = db.Column(db.String, primary_key=True)
    quarter = db.Column(db.String, primary_key=True)
    roe = db.Column(db.Float, nullable=True)
    roa = db.Column(db.Float, nullable=True)
    current_ratio = db.Column(db.Float, nullable=True)
    debt_ratio = db.Column(db.Float, nullable=True)
    operating_margin = db.Column(db.Float, nullable=True)
    net_profit_margin = db.Column(db.Float, nullable=True)
    current_ratio_ttm = db.Column(db.Float, nullable=True)
    debt_ratio_ttm = db.Column(db.Float, nullable=True)
    operating_margin_ttm = db.Column(db.Float, nullable=True)
    net_profit_margin_ttm = db.Column(db.Float, nullable=True)


class StockDerivedVar(db.Model):
    __tablename__ = 'stock_derived_var'

    date = db.Column(db.Integer, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    stock_nm = db.Column(db.String, nullable=True)
    price_prev_1d = db.Column(db.Integer, nullable=True)
    price_prev_1w = db.Column(db.Integer, nullable=True)
    price_diff_1d = db.Column(db.Integer, nullable=True)
    price_diff_1w = db.Column(db.Integer, nullable=True)
    price_high_52w = db.Column(db.Integer, nullable=True)
    price_low_52w = db.Column(db.Integer, nullable=True)
    price_momt_1m = db.Column(db.Float, nullable=True)
    price_momt_3m = db.Column(db.Float, nullable=True)
    price_momt_6m = db.Column(db.Float, nullable=True)
    price_momt_12m = db.Column(db.Float, nullable=True)
    trading_vol_prev_1d = db.Column(db.Integer, nullable=True)


class UserStarredStock(db.Model):
    __tablename__ = 'user_starred_stock'

    user_code = db.Column(db.Integer, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    lst_update_dtim = db.Column(db.String, nullable=True, default=time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))


class StockViewCount(db.Model):
    __tablename__ = 'stock_view_count'

    date_hour = db.Column(db.Integer, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    view_count = db.Column(db.Integer, nullable=True)


class UserFilterPreset(db.Model):
    __tablename__ = 'user_filter_preset'

    user_code = db.Column(db.Integer, primary_key=True)
    preset_name = db.Column(db.String, primary_key=True)
    preset = db.Column(db.String, nullable=True)
    lst_update_dtim = db.Column(db.String, nullable=True, default=time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))


class StockAdjPrice(db.Model):
    __tablename__ = 'stock_adj_price'

    stock_cd = db.Column(db.String, primary_key=True)
    date = db.Column(db.String, primary_key=True)
    adj_open_price = db.Column(db.Integer, nullable=True)
    adj_high_price = db.Column(db.Integer, nullable=True)
    adj_low_price = db.Column(db.Integer, nullable=True)
    adj_close_price = db.Column(db.Integer, nullable=True)


class DateWorkingDay(db.Model):
    __tablename__ = 'date_working_day'

    seq = db.Column(db.Integer, primary_key=True)
    working_day = db.Column(db.Integer, primary_key=True)


class StockTruefrCd(db.Model):
    __tablename__ = 'stock_truefr_cd'

    stock_nm = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String)


class AppNotice(db.Model):
    __tablename__ = 'app_notice'

    dtim = db.Column(db.String, primary_key=True)
    notice_title = db.Column(db.String)
    notice_body = db.Column(db.String)
    valid_flag = db.Column(db.Integer)

    
class UsIsinCdMap(db.Model):
    __talbename__ = 'us_isin_cd_map'

    isin_cd = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String)


class UsTrfrStockNmSymbolMap(db.Model):
    __talbename__ = 'us_trfr_stock_nm_symbol_map'

    stock_nm = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String)


class UsStockInfo(db.Model):
    __talbename__ = 'us_stock_info'

    stock_cd = db.Column(db.String, primary_key=True)
    stock_nm = db.Column(db.String)
    exchange = db.Column(db.String)
    latest_date = db.Column(db.Integer)
    lst_update_date = db.Column(db.String)
    industry = db.Column(db.String)


class UsDateWorkingDay(db.Model):
    __talbename__ = 'us_date_working_day'

    seq = db.Column(db.Integer, primary_key=True)
    working_day = db.Column(db.Integer, primary_key=True)


class UsDateWorkingDayMapping(db.Model):
    __talbename__ = 'us_date_working_day_mapping'

    date = db.Column(db.Integer, primary_key=True)
    working_day = db.Column(db.Integer)


class UsStockDailyPrice(db.Model):
    __talbename__ = 'us_stock_daily_price'

    date = db.Column(db.Integer, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    open_price = db.Column(db.Integer)
    high_price = db.Column(db.Integer)
    low_price = db.Column(db.Integer)
    close_price = db.Column(db.Integer)
    adj_close_price = db.Column(db.Integer)
    trading_volume = db.Column(db.Integer)
    unadjusted_trading_volume = db.Column(db.Integer)
    market_cap = db.Column(db.Integer)


class UsStockDerivedVar(db.Model):
    __talbename__ = 'us_stock_derived_var'

    date = db.Column(db.Integer, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    price_prev_1d = db.Column(db.Float)
    price_prev_1w = db.Column(db.Float)
    price_diff_1d = db.Column(db.Float)
    price_diff_1w = db.Column(db.Float)
    price_high_52w = db.Column(db.Float)
    price_low_52w = db.Column(db.Float)
    price_momt_1m = db.Column(db.Float)
    price_momt_3m = db.Column(db.Float)
    price_momt_6m = db.Column(db.Float)
    price_momt_12m = db.Column(db.Float)
    trading_vol_prev_1d = db.Column(db.Integer)


class ExchangeRate(db.Model):
    __talbename__ = 'exchange_rate'

    date = db.Column(db.Integer, primary_key=True)
    usd_krw = db.Column(db.Float)


class UsStockDailyPriceRaw(db.Model):
    __talbename__ = 'us_stock_daily_price_raw'

    date = db.Column(db.Integer, primary_key=True)
    stock_cd = db.Column(db.String, primary_key=True)
    open_price = db.Column(db.Float)
    high_price = db.Column(db.Float)
    low_price = db.Column(db.Float)
    close_price = db.Column(db.Float)
    adj_close_price = db.Column(db.Float)
    trading_volume = db.Column(db.Integer)
    unadjusted_trading_volume = db.Column(db.Integer)
    market_cap = db.Column(db.Integer)
    split_coeff = db.Column(db.Float)


class TrfrStockNmStockCdMap(db.Model):
    __talbename__ = 'trfr_stock_nm_stock_cd_map'

    stock_nm = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String)


class SecuritiesTrCondition(db.Model):
    __talbename__ = 'securities_tr_condition'

    securities_code = db.Column(db.String, primary_key=True)
    no = db.Column(db.Integer)
    transaction_type = db.Column(db.String)
    transaction_detail_type = db.Column(db.String)
    country = db.Column(db.String)
    std_tr = db.Column(db.String)
    comment = db.Column(db.String)
    lst_update_dtim = db.Column(db.String)


class StockIsinCdMap(db.Model):
    __tablename__ = 'stock_isin_cd_map'

    isin_cd = db.Column(db.String, primary_key=True)
    stock_cd = db.Column(db.String, nullable=False)

    
class ApiStatusCode(db.Model):
    __talbename__ = 'api_status_code'

    status_code = db.Column(db.String, primary_key=True)
    msg = db.Column(db.String, primary_key=True)
    lst_update_dtim = db.Column(db.String)


class AppScrapLog(db.Model):
    __talbename__ = 'app_scrap_log'

    user_code = db.Column(db.Integer, primary_key=True)
    dtim = db.Column(db.String, primary_key=True)
    securities_code = db.Column(db.String)
    account_number = db.Column(db.String)
    status_code = db.Column(db.String)
    os = db.Column(db.String)
    input = db.Column(db.String)
    output = db.Column(db.String)


class AppVersion(db.Model):
    __talbename__ = 'app_version'

    platform = db.Column(db.String, primary_key=True)
    version = db.Column(db.String, primary_key=True)
    current_flag = db.Column(db.Integer)
    update_dtim = db.Column(db.String)


class UserDefaultPortfolio(db.Model):
    __talbename__ = 'user_default_portfolio'

    user_code = db.Column(db.Integer, primary_key=True)
    securities_code = db.Column(db.String, primary_key=True)
    portfolio_code = db.Column(db.Integer)
    lst_update_dtim = db.Column(db.String)


class PushNotiAcctStatus(db.Model):
    __talbename__ = 'push_noti_acct_status'

    user_code = db.Column(db.Integer, primary_key=True)
    account_number = db.Column(db.String, primary_key=True)
    sync_complete_flag = db.Column(db.Integer)
    fcm_token = db.Column(db.String)
    lst_update_dtim = db.Column(db.String)


class StockValuationIndicator(db.Model):
    __talbename__ = 'stock_valuation_indicator'

    stock_cd = db.Column(db.String, primary_key=True)
    quarter = db.Column(db.String, primary_key=True)
    stock_nm = db.Column(db.String)
    bps = db.Column(db.Float)
    eps = db.Column(db.Float)
    pbr = db.Column(db.Float)
    per = db.Column(db.Float)
    bps_ttm = db.Column(db.Float)
    eps_ttm = db.Column(db.Float)
    pbr_ttm = db.Column(db.Float)
    per_ttm = db.Column(db.Float)


class DartSimpleFinancialStatementsTtm(db.Model):
    __tablename__ = 'dart_simple_financial_statements_ttm'

    stock_cd = db.Column(db.String, primary_key=True)
    quarter = db.Column(db.String, primary_key=True)
    first_rcept_date = db.Column(db.String, primary_key=True)
    rcept_date = db.Column(db.String)
    next_first_rcept_date = db.Column(db.String)
    base_currency = db.Column(db.String)
    current_assets_ttm = db.Column(db.Integer)
    noncurrent_assets_ttm = db.Column(db.Integer)
    assets_ttm = db.Column(db.Integer)
    current_liabilities_ttm = db.Column(db.Integer)
    noncurrent_liabilities_ttm = db.Column(db.Integer)
    liabilities_ttm = db.Column(db.Integer)
    capital_ttm = db.Column(db.Integer)
    retained_earnings_ttm = db.Column(db.Integer)
    equity_ttm = db.Column(db.Integer)
    revenue_ttm = db.Column(db.Integer)
    operating_income_loss_ttm = db.Column(db.Integer)
    profit_loss_before_tax_ttm = db.Column(db.Integer)
    profit_loss_ttm = db.Column(db.Integer)
