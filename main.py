import os
from apis.api_user_info import UserCheck, SignUp, PasswordCheck, DeleteUser, \
    SendEmailVerificationCode, VerificationCodeCheck, GetUserLoginInfo, ChangePassword, \
    GetStarredStockList, SetStarredStock, DeleteStarredStock, CheckIsNewUser, ResetUserPassword, SetVerifInfo, GetReqNum
from apis.api_securities_info import GetTradingLog, SetUserSecuritiesInfo, GetUserSecuritiesInfoValidFlag, UpdateTradingLog, \
    ResetUserSecuritiesInfoValidFlag, GetAvailableSecuritiesList, DeleteUserSecuritiesInfo, \
    InsertTradingLog, ReviseTradingLog, DeleteTradingLog, GetPortfolioStockList, MultiInsertTradingLog, \
    GetRecentUpdateDate, DeleteTradingLogBySecurities, SetScrapLog
from apis.api_portfolio import GetUserPortfolio, GetPortfolioYield, GetPortfolioList, \
    SetStockPortfolio, CreateNewPortfolio, RenamePortfolio, DeleteUserPortfolio, GetPortfolioCompositionRatio, \
    GetPortfolioSectorCompositionRatio, SetPortfolioOrder, SetUserDefaultPortfolio, GetAllPortfolioYield2021
from apis.api_stocks import GetAutoCompletedStockNameList, GetStockPriceInfo, GetStockPriceList
from apis.api_financial_info import GetSingleFinancialInfo, GetFinancialInfo
from apis.api_finder import GetFilteredData, SetUserFilter, GetUserFilter, DeleteUserFilter, GetPopularStock, \
    GetUserStaredStock
from apis.api_app_info import GetAppNotice, SetAppNotice, DeleteAppNotice, GetCurrentVersion
import logging
from flask import current_app as app
import flask_monitoringdashboard as dashboard
from flask_restful import Api
from flask_cors import CORS
import pandas as pd
import sys
from logging.handlers import RotatingFileHandler


formatter2 = logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)s] >> %(message)s')

# file_handler
file_handler = RotatingFileHandler('./merlot_api.log', maxBytes=1024*1024*100, backupCount=5)
file_handler.setFormatter(formatter2)

# stdout handler
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter2)

app.logger.addHandler(stdout_handler)
app.logger.addHandler(file_handler)


## setting log_level through environment variable passed in values.yml
log_level = os.environ.get("LOGLEVEL")
if log_level == "info":
    app.logger.setLevel(logging.INFO)
elif log_level == "error":
    app.logger.setLevel(logging.ERROR)
elif log_level == "debug":
    app.logger.setLevel(logging.DEBUG)
else:
    app.logger.setLevel(logging.INFO)

cors = CORS(app, resources={r"/api/user/setverifinfo": {"origins": "*"}})
dashboard.bind(app)
api = Api(app)



api.add_resource(UserCheck, '/api/user/check')  # <- api 문서 구현 완료
api.add_resource(SignUp, '/api/user/signup')  # <- api 문서 구현 완료
api.add_resource(PasswordCheck, '/api/user/passcheck')  # <- api 문서 구현 완료
api.add_resource(ResetUserPassword, '/api/user/resetuserpassword')  # <- api 문서 구현 완료
api.add_resource(DeleteUser, '/api/user/deleteuser')  # <- api 문서 구현 완료
api.add_resource(SendEmailVerificationCode, '/api/user/sendemailverifcode')  # <- api 문서 구현 완료
api.add_resource(VerificationCodeCheck, '/api/user/emailverifcodecheck')  # <- api 문서 구현 완료
api.add_resource(GetUserLoginInfo, '/api/user/getuserlogininfo')  # <- api 문서 구현 완료
api.add_resource(ChangePassword, '/api/user/changepassword')  # <- api 문서 구현 완료
api.add_resource(GetStarredStockList, '/api/user/getstarredstocklist')
api.add_resource(SetStarredStock, '/api/user/setstarredstock')
api.add_resource(DeleteStarredStock, '/api/user/deletestarredstock')
api.add_resource(CheckIsNewUser, '/api/user/checkisnewuser')
api.add_resource(SetVerifInfo, '/api/user/setverifinfo')
api.add_resource(GetReqNum, '/api/user/getreqnum')

api.add_resource(GetPortfolioList, '/api/portfolio/getportfoliolist')  # <- api 문서 구현 완료
api.add_resource(SetPortfolioOrder, '/api/portfolio/setportfolioorder')  # <- api 문서 구현 완료
api.add_resource(CreateNewPortfolio, '/api/portfolio/createnewportfolio')  # <- api 문서 구현 완료
api.add_resource(RenamePortfolio, '/api/portfolio/renameportfolio')  # <- api 문서 구현 완료
api.add_resource(SetStockPortfolio, '/api/portfolio/setstockportfolio')  # <- api 문서 구현 완료
api.add_resource(GetUserPortfolio, '/api/portfolio/getuserportfolio')  # <- api 문서 구현 완료
api.add_resource(DeleteUserPortfolio, '/api/portfolio/deleteuserportfolio')  # <- api 문서 구현 완료
api.add_resource(GetPortfolioYield, '/api/portfolio/getportfolioyield')  # <- api문서 구현 완료
api.add_resource(GetPortfolioCompositionRatio, '/api/portfolio/getportfoliocompositionratio')  # <- api문서 구현 완료
api.add_resource(GetPortfolioSectorCompositionRatio, '/api/portfolio/getportfoliosectorcompositionratio')  # <- api문서 구현 완료
api.add_resource(GetPortfolioStockList, '/api/portfolio/getportfoliostocklist')  # <- api 문서 구현 완료
api.add_resource(SetUserDefaultPortfolio, '/api/portfolio/setuserdefaultportfolio')  # <- api 문서 구현 완료
api.add_resource(GetAllPortfolioYield2021, '/api/portfolio/getallportfolioyield2021')

api.add_resource(GetAvailableSecuritiesList, '/api/securities/getavailablesecuritieslist')  # <- api 문서 구현 완료
api.add_resource(SetUserSecuritiesInfo, '/api/securities/setusersecuritiesinfo')  # <- api 문서 구현 완료
api.add_resource(DeleteUserSecuritiesInfo, '/api/securities/deleteusersecuritiesinfo')
api.add_resource(GetUserSecuritiesInfoValidFlag, '/api/securities/getusersecuritiesinfovalidflag')  # <- api 문서 구현 완료
api.add_resource(UpdateTradingLog, '/api/securities/updatetradinglog')  # <- api 문서 구현 완료
api.add_resource(ResetUserSecuritiesInfoValidFlag, '/api/securities/resetusersecuritiesinfovalidflag')  # <- api 문서 구현 완료
api.add_resource(GetTradingLog, '/api/securities/gettradinglog')  # <- api 문서 구현 완료
api.add_resource(InsertTradingLog, '/api/securities/inserttradinglog')  # <- api 문서 구현 완료
api.add_resource(ReviseTradingLog, '/api/securities/revisetradinglog')  # <- api 문서 구현 완료
api.add_resource(DeleteTradingLog, '/api/securities/deletetradinglog')  # <- api 문서 구현 완료
api.add_resource(MultiInsertTradingLog, '/api/securities/multiinserttradinglog')  # <- api 문서 구현 완료
api.add_resource(GetRecentUpdateDate, '/api/securities/getrecentupdatedate')  # <- api 문서 구현 완료
api.add_resource(DeleteTradingLogBySecurities, '/api/securities/deletetradinglogbysecurities')  # <- api 문서 구현 완료
api.add_resource(SetScrapLog, '/api/securities/setscraplog')  # <- api 문서 구현 완료

api.add_resource(GetSingleFinancialInfo, '/api/financials/getsinglefinancialinfo')  # <- api 문서 구현 완료
api.add_resource(GetFinancialInfo, '/api/financials/getfinancialinfo')  # <- api 문서 구현 완료

api.add_resource(GetStockPriceInfo, '/api/stocks/getstockpriceinfo')  # <- api 문서 구현 완료
api.add_resource(GetStockPriceList, '/api/stocks/getstockpricelist')  # <- api 문서 구현 완료
api.add_resource(GetAutoCompletedStockNameList, '/api/stocks/getautocompletedstocknamelist')  # <- api문서 구현 완료
# api.add_resource(GetPopularStockList, '/api/stocks/getpopularstocklist')  # <- api문서 구현 완료

api.add_resource(GetFilteredData, '/api/finder/getfiltereddata')
api.add_resource(SetUserFilter, '/api/finder/setuserfilter')
api.add_resource(GetUserFilter, '/api/finder/getuserfilter')
api.add_resource(DeleteUserFilter, '/api/finder/deleteuserfilter')
api.add_resource(GetPopularStock, '/api/finder/getpopularstock')
api.add_resource(GetUserStaredStock, '/api/finder/getuserstaredstock')

api.add_resource(GetAppNotice, '/api/app/getappnotice')
api.add_resource(SetAppNotice, '/api/app/setappnotice')
api.add_resource(DeleteAppNotice, '/api/app/deleteappnotice')
api.add_resource(GetCurrentVersion, '/api/app/getcurrentversion')


if __name__ == '__main__':

    # pandas show 옵션
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    app.run(host="0.0.0.0", port=5000)

