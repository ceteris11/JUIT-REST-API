
from celery import Celery
# from securities.Mirae_windows import mirae_login


# celery 설정
app = Celery('securities_login', broker='pyamqp://username:pass123456@serverurl:port//')


# update 함수 정의
@app.task
def securities_update_log(user_code, securities_code, secret_key, begin_date, end_date, default_portfolio_code):
    return None
