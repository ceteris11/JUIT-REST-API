
import time
import datetime
from random import randint
from flask_restful import Resource
from flask_restful import reqparse
from util.email_sender import send_email
from db.db_model import UserInfo, UserEmailVerificationCode, UserPortfolioInfo, UserStarredStock, StockInfo, \
    UserSecuritiesInfo, UserPortfolioMap, UserMobileVerifInfo, session_scope, UsStockInfo
from module.merlot_logging import *
from util.util_update_portfolio import update_user_portfolio
from util.util_get_country import get_country
from util.util_get_status_msg import get_status_msg
import os
import hashlib
from base64 import b64encode
from flask import current_app as app
from flask import request


def check_user_validity(user_code, api_key):
    with session_scope() as session:
        checked_user = session.query(UserInfo).filter(UserInfo.user_code == user_code,
                                                      UserInfo.end_dtim == '99991231235959',
                                                      UserInfo.passwd != '').first()

        if checked_user is None:  # 회원번호 불일치
            return '106'
        elif checked_user.salt != api_key:  # key값이 틀린 경우
            return '105'
        else:
            return '000'


def get_verif_info(req_num):
    with session_scope() as session:
        verif_info = session.query(UserMobileVerifInfo).filter(UserMobileVerifInfo.req_num == req_num).first()

        if verif_info is None:
            return None
        else:
            return verif_info


class UserCheck(Resource):  # 회원 여부 확인
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('login_type', type=str)
        parser.add_argument('login_key', type=str)
        args = parser.parse_args()

        login_type = args['login_type']
        login_key = args['login_key']

        if login_type == 'mobile':
            with session_scope() as session:
                verif_info = session.query(UserMobileVerifInfo).filter(UserMobileVerifInfo.req_num == login_key).first()

                if verif_info is None:
                    return {'status': '110', 'msg': get_status_msg('110'), 'user_code': -99}
                else:
                    login_key = verif_info.user_di
        elif login_type in ['kakao', 'apple', 'email']:
            pass
        else:
            return {'status': '101', 'msg': get_status_msg('101'), 'user_code': -99}

        with session_scope() as session:
            checked_user = session.query(UserInfo).filter(UserInfo.login_type == login_type,
                                                          UserInfo.login_key == login_key,
                                                          UserInfo.end_dtim == '99991231235959',
                                                          UserInfo.passwd != '').all()

            if len(checked_user) == 0:  # 회원 아닌 경우: user_code로 -99 return
                return {'status': '000', 'msg': get_status_msg('000'), 'user_code': -99}

            else:  # 회원인 경우: user_code return
                return {'status': '000', 'msg': get_status_msg('000'), 'user_code': checked_user[0].user_code}


class ResetUserPassword(Resource):  # 비밀번호 초기화
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status != '000':
            return {'status': check_status, 'msg': get_status_msg(check_status), 'user_code': user_code}

        with session_scope() as session:
            checked_user = session.query(UserInfo).filter(UserInfo.user_code == user_code,
                                                             UserInfo.end_dtim == '99991231235959').all()

            if len(checked_user) == 0:  # 일치하는 회원이 없는 경우
                return {'status': '103', 'msg': get_status_msg('103'), 'user_code': user_code}

            else:  # 일치하는 회원이 있는 경우
                checked_user[0].passwd = ''
                return {'status': '000', 'msg': get_status_msg('000'), 'user_code': checked_user[0].user_code}


class SignUp(Resource):  # 회원 가입
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('login_type', type=str)
        parser.add_argument('login_key', type=str)
        parser.add_argument('passwd', type=str)
        args = parser.parse_args()

        login_type = args['login_type']
        login_key = args['login_key']
        passwd = args['passwd']

        with session_scope() as session:
            if login_type == 'mobile':
                with session_scope() as session:
                    verif_info = session.query(UserMobileVerifInfo).filter(UserMobileVerifInfo.req_num == login_key).first()

                    if verif_info is None:
                        return {'status': '110', 'msg': get_status_msg('110'), 'user_code': -99}
                    else:
                        login_key = verif_info.user_di
                    user_name = verif_info.user_name
                    user_birth = verif_info.user_birth
                    user_sex = verif_info.user_sex
                    user_foreign = verif_info.user_foreign
                    user_phone_number = verif_info.user_phone_number
            elif login_type in ['kakao', 'apple', 'email']:
                pass
            else:
                return {'status': '101', 'msg': get_status_msg('101'), 'user_code': -99, 'api_key': ''}
            checked_user = session.query(UserInfo).filter(UserInfo.login_type == login_type,
                                                          UserInfo.login_key == login_key,
                                                          UserInfo.end_dtim == '99991231235959').all()
            session.commit()

            if len(checked_user) == 0:  # 기존 회원 아닌 경우: 회원가입 후 user_code return
                salt = b64encode(os.urandom(32)).decode('utf-8')
                passwd = hashlib.sha256(salt.encode()+passwd.encode()).hexdigest()

                # new user 추가
                if login_type in ['kakao', 'apple', 'email']:  # <- 삭제 예정
                    new_user = UserInfo(login_type=login_type, login_key=login_key, passwd=passwd, salt=salt)
                else:
                    new_user = UserInfo(login_type=login_type, login_key=login_key, passwd=passwd, salt=salt,
                                        beg_dtim=datetime.datetime.now().strftime('%Y%m%d%H%M%S'),
                                        user_name=user_name, user_birth=user_birth,
                                        user_sex=user_sex, user_foreign=user_foreign,
                                        user_phone_number=user_phone_number)
                session.add(new_user)
                session.commit()

                checked_user = session.query(UserInfo).filter(UserInfo.login_type == login_type,
                                                              UserInfo.login_key == login_key,
                                                              UserInfo.end_dtim == '99991231235959').all()
                session.commit()

                # new portfolio 추가
                new_portfolilo = UserPortfolioInfo(user_code=checked_user[0].user_code,
                                                   portfolio_code=1,
                                                   portfolio_nm='my portfolio',
                                                   portfolio_order=1)
                session.add(new_portfolilo)
                session.commit()

                return {'status': '000', 'msg': get_status_msg('000'), 'user_code': checked_user[0].user_code, 'api_key': salt}

            else:  # 기존 회원인 경우
                if checked_user[0].passwd == '':
                    # 비밀번호 초기화된 경우
                    checked_user[0].passwd = hashlib.sha256(checked_user[0].salt.encode()+passwd.encode()).hexdigest()
                    session.commit()
                    return {'status': '000', 'msg': get_status_msg('000'), 'user_code': checked_user[0].user_code, 'api_key': checked_user[0].salt}
                else:
                    return {'status': '102', 'msg': get_status_msg('102'), 'user_code': -99, 'api_key': ''}


class PasswordCheck(Resource):  # 비밀번호 확인
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('passwd', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        passwd = args['passwd']

        with session_scope() as session:
            checked_user = session.query(UserInfo).filter(UserInfo.user_code == user_code,
                                                             UserInfo.end_dtim == '99991231235959').all()
            session.commit()

            if len(checked_user) == 0:  # 일치하는 회원이 없는 경우
                return {'status': '103', 'msg': get_status_msg('103'), 'user_code': user_code, 'api_key': ''}

            else:  # 일치하는 회원이 있는 경우
                salt = checked_user[0].salt
                passwd = hashlib.sha256(salt.encode() + passwd.encode()).hexdigest()
                if passwd == checked_user[0].passwd:
                    return {'status': '000', 'msg': get_status_msg('000'), 'user_code': checked_user[0].user_code, 'api_key': salt}
                else:
                    return {'status': '103', 'msg': get_status_msg('103'), 'user_code': user_code, 'api_key': ''}


class GetUserLoginInfo(Resource):  # 회원 여부 확인
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status != '000':
            return {'status': check_status, 'msg': get_status_msg(check_status), 'login_type': '', 'login_key': ''}

        with session_scope() as session:
            checked_user = session.query(UserInfo) \
                .filter(UserInfo.user_code == user_code, UserInfo.end_dtim == '99991231235959', UserInfo.passwd != '').all()
            session.commit()

            if len(checked_user) == 0:  # 회원 아닌 경우: user_code로 -99 return
                return {'status': '103', 'msg': get_status_msg('103'), 'login_type': "", 'login_key': ""}

            else:  # 회원인 경우: user_code return
                return {'status': '000', 'msg': get_status_msg('000'),
                        'login_type': checked_user[0].login_type,
                        'login_key': checked_user[0].login_key}


class DeleteUser(Resource):  # 회원 탈퇴
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('passwd', type=str)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        passwd = args['passwd']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '105':  # API Key 값이 틀린 경우
            return {'status': check_status, 'msg': get_status_msg(check_status), 'user_code': user_code}

        with session_scope() as session:
            checked_user = session.query(UserInfo).filter(UserInfo.user_code == user_code,
                                                          UserInfo.end_dtim == '99991231235959').all()
            session.commit()

            if len(checked_user) == 0:  # 일치하는 회원이 없는 경우
                return {'status': '103', 'msg': get_status_msg('103'), 'user_code': user_code}

            else:  # 일치하는 회원이 있는 경우
                salt = checked_user[0].salt
                passwd = hashlib.sha256(salt.encode() + passwd.encode()).hexdigest()
                if passwd == checked_user[0].passwd:  # password 일치
                    # user_info 유효기간을 현재 일시로 만료 처리
                    checked_user[0].end_dtim = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    session.commit()
                    return {'status': '000', 'msg': get_status_msg('000'), 'user_code': user_code}
                else:  # password 불일치
                    return {'status': '103', 'msg': get_status_msg('103'), 'user_code': user_code}


class SendEmailVerificationCode(Resource):  # 이메일 인증코드 발송
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('email', type=str)
        args = parser.parse_args()

        email = args['email']
        beg_dtim = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
        end_dtim = (datetime.datetime.now()+datetime.timedelta(minutes=10)).strftime('%Y%m%d%H%M%S%f')

        code = randint(100000, 999999)

        new_code = UserEmailVerificationCode(email=email, beg_dtim=beg_dtim, end_dtim=end_dtim, verification_code=code)
        with session_scope() as session:
            session.add(new_code)
            session.commit()

        send_email.apply_async((email, code), queue='email')

        return {'status': '000', 'msg': get_status_msg('000'), 'email': email}


class VerificationCodeCheck(Resource):  # 이메일 인증코드 확인
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('email', type=str)
        parser.add_argument('verification_code', type=int)
        args = parser.parse_args()

        email = args['email']
        verification_code = args['verification_code']

        with session_scope() as session:
            correct_email = session.query(UserEmailVerificationCode)\
                .filter(UserEmailVerificationCode.email == email,
                        UserEmailVerificationCode.end_dtim >= datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'))\
                .order_by(UserEmailVerificationCode.end_dtim.desc()).all()
            session.commit()

            if len(correct_email) == 0:  # 맞는 인증번호가 없는 경우
                return {'status': '104', 'msg': get_status_msg('104'), 'email': email}
            elif correct_email[0].verification_code != verification_code:  # 인증번호가 틀렸을 경우
                return {'status': '104', 'msg': get_status_msg('104'), 'email': email}
            else:  # 인증번호가 일치할 경우
                return {'status': '000', 'msg': get_status_msg('000'), 'email': email}


class ChangePassword(Resource):  # 비밀번호 변경
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('old_passwd', type=str)
        parser.add_argument('new_passwd', type=str)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        old_passwd = args['old_passwd']
        new_passwd = args['new_passwd']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '105':
            return {'status': check_status, 'msg': get_status_msg(check_status), 'user_code': user_code}

        with session_scope() as session:
            checked_user = session.query(UserInfo).filter(UserInfo.user_code == user_code,
                                                             UserInfo.end_dtim == '99991231235959').all()
            session.commit()

            if len(checked_user) == 0:  # 일치하는 회원이 없는 경우
                return {'status': '103', 'msg': get_status_msg('103'), 'user_code': user_code}

            salt = checked_user[0].salt
            old_passwd = hashlib.sha256(salt.encode() + old_passwd.encode()).hexdigest()
            if old_passwd == checked_user[0].passwd:
                new_passwd = hashlib.sha256(salt.encode() + new_passwd.encode()).hexdigest()
                checked_user[0].passwd = new_passwd
                session.commit()
                return {'status': '000', 'msg': get_status_msg('000'), 'user_code': user_code}  # 패스워드 변경 성공
            else:
                return {'status': '103', 'msg': get_status_msg('103'), 'user_code': user_code}  # 패스워드 불일치


class GetStarredStockList(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '105':
            return {'status': check_status, 'msg': get_status_msg(check_status), 'user_code': user_code}

        with session_scope() as session:
            user_starred_stock_list = session.query(UserStarredStock).\
                filter(UserStarredStock.user_code == user_code).\
                all()
            session.commit()

            return {'status': '000', 'msg': get_status_msg('000'),
                    'user_starred_stock_list': [{c.name: getattr(e, c.name) for c in e.__table__.columns} for e in user_starred_stock_list]}


class SetStarredStock(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('stock_cd', type=str)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        stock_cd = args['stock_cd']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '105':
            return {'status': check_status, 'msg': get_status_msg(check_status), 'user_code': user_code}

        # country 확인
        country = get_country(stock_cd)

        with session_scope() as session:
            if country == 'KR':
                stock_info = session.query(StockInfo). \
                    filter(StockInfo.stock_cd == f'A{stock_cd}',
                           StockInfo.end_date == 99991231). \
                    first()
                session.commit()
            elif country == 'US':
                stock_info = session.query(UsStockInfo). \
                    filter(UsStockInfo.stock_cd == f'{stock_cd}',
                           UsStockInfo.latest_date != -1). \
                    first()
                session.commit()
            else:
                stock_info = None

            if stock_info is None:
                return {'status': '403', 'msg': get_status_msg('403')}  # 해당 종목코드 존재하지 않음

            user_starred_stock = session.query(UserStarredStock). \
                filter(UserStarredStock.user_code == user_code,
                       UserStarredStock.stock_cd == stock_cd).\
                first()
            session.commit()

            if user_starred_stock is not None:
                return {'status': '107', 'msg': get_status_msg('107')}  # 이미 등록된 종목코드

            new_user_starred_stock = UserStarredStock(user_code=user_code,
                                                      stock_cd=stock_cd,
                                                      lst_update_dtim=datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            session.add(new_user_starred_stock)
            session.commit()

            return {'status': '000', 'msg': get_status_msg('000')}


class DeleteStarredStock(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('stock_cd', type=str)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        stock_cd = args['stock_cd']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '105':
            return {'status': check_status, 'msg': get_status_msg(check_status), 'user_code': user_code}

        with session_scope() as session:
            user_starred_stock_query = session.query(UserStarredStock). \
                filter(UserStarredStock.user_code == user_code,
                       UserStarredStock.stock_cd == stock_cd)
            user_starred_stock = user_starred_stock_query.first()
            session.commit()

            if user_starred_stock is None:
                return {'status': '202', 'msg': get_status_msg('202')}  # 해당 종목 없음

            user_starred_stock_query.delete()
            session.commit()

            return {'status': '000', 'msg': get_status_msg('000')}


class CheckIsNewUser(Resource):
    def post(self):
        if len(request.data) != 0:
            app.logger.info(make_log_msg("/api/user/checkisnewuser", request.data))  # 안드로이드 로깅
        else:
            app.logger.info(make_log_msg("/api/user/checkisnewuser", request.values))  # ios 로깅
        parser = reqparse.RequestParser()
        parser.add_argument('user_code', type=int)
        parser.add_argument('api_key', type=str)
        args = parser.parse_args()

        user_code = args['user_code']
        api_key = args['api_key']

        check_status = check_user_validity(user_code, api_key)
        if check_status == '105':
            return {'status': check_status, 'msg': get_status_msg(check_status), 'new_user_flag': -1}

        with session_scope() as session:
            # 등록된 증권사 계정이 있는지 확인
            user_securities_info = session.query(UserSecuritiesInfo).\
                filter(UserSecuritiesInfo.user_code == user_code).\
                first()
            session.commit()

            # 직접 입력된 거래내역이 있는지 확인
            user_portfolio_map = session.query(UserPortfolioMap). \
                filter(UserPortfolioMap.user_code == user_code).\
                first()
            session.commit()

        if user_securities_info is None and user_portfolio_map is None:
            return {'status': '000', 'msg': get_status_msg('000'), 'new_user_flag': 1}
        else:
            # 포트폴리오 업데이트
            update_user_portfolio(user_code=user_code)

            return {'status': '000', 'msg': get_status_msg('000'), 'new_user_flag': 0}


class GetReqNum(Resource):
    def post(self):
        with session_scope() as session:
            # req num 신규 생성
            new_user_mobile_verif_info = UserMobileVerifInfo()
            session.add(new_user_mobile_verif_info)
            session.commit()
            req_num = new_user_mobile_verif_info.req_num
        return {'status': '000', 'msg': get_status_msg('000'), 'req_num': req_num}


class SetVerifInfo(Resource):
    def post(self):
        # print(request.data)
        parser = reqparse.RequestParser()
        parser.add_argument('user_name', type=str)
        parser.add_argument('user_birth', type=str)
        parser.add_argument('user_sex', type=str)
        parser.add_argument('user_foreign', type=str)
        parser.add_argument('user_di', type=str)
        parser.add_argument('user_phone_number', type=str)
        args = parser.parse_args()
        # print(args)

        user_name = args['user_name']
        user_birth = args['user_birth']
        user_sex = args['user_sex']
        user_foreign = args['user_foreign']
        user_di = args['user_di']
        user_phone_number = args['user_phone_number']

        lst_update_dtim = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        with session_scope() as session:
            # 등록된 req_num이 없는 경우
            new_user_mobile_verif_info = UserMobileVerifInfo(user_name=user_name,
                                                             user_birth=user_birth,
                                                             user_sex=user_sex,
                                                             user_foreign=user_foreign,
                                                             user_di=user_di,
                                                             user_phone_number=user_phone_number,
                                                             lst_update_dtim=lst_update_dtim)
            session.add(new_user_mobile_verif_info)
            session.commit()
            return {'status': '000', 'msg': get_status_msg('000'), 'req_num': new_user_mobile_verif_info.req_num}
