from flask_restful import Resource
from flask_restful import reqparse
from db.db_model import session_scope, AppNotice, AppVersion
from datetime import datetime
from util.util_get_status_msg import get_status_msg


def get_admin_key():
    return '성공하고만다'


class GetAppNotice(Resource):  # 공지사항 조회
    def post(self):
        with session_scope() as session:
            app_notice = session.query(AppNotice). \
                filter(AppNotice.valid_flag == 1). \
                order_by(AppNotice.dtim.desc()).\
                first()
            session.commit()

            if app_notice is None:
                return {'status': '601', 'msg': get_status_msg('601'), 'title': '', 'body': ''}
            else:
                return {'status': '000', 'msg': get_status_msg('000'),
                        'title': app_notice.notice_title, 'body': app_notice.notice_body}


class SetAppNotice(Resource):  # 공지사항 설정
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('admin_key', type=str)
        parser.add_argument('title', type=str)
        parser.add_argument('body', type=str)
        args = parser.parse_args()

        admin_key = args['admin_key']
        title = args['title']
        body = args['body']

        if admin_key != get_admin_key():
            return {'status': '602', 'msg': get_status_msg('602')}

        with session_scope() as session:
            new_notice = AppNotice(dtim=datetime.today().strftime('%Y%m%d%H%M%S'),
                                   notice_title=title,
                                   notice_body=body,
                                   valid_flag=1)
            session.add(new_notice)
            session.commit()

            return {'status': '000', 'msg': get_status_msg('000')}


class DeleteAppNotice(Resource):  # 공지사항 삭제
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('admin_key', type=str)
        args = parser.parse_args()

        admin_key = args['admin_key']

        if admin_key != get_admin_key():
            return {'status': '602', 'msg': get_status_msg('602')}

        with session_scope() as session:
            app_notice = session.query(AppNotice). \
                filter(AppNotice.valid_flag == 1). \
                order_by(AppNotice.dtim.desc()).\
                all()
            for n in app_notice:
                n.valid_flag = 0
            session.commit()

            return {'status': '000', 'msg': get_status_msg('000')}


class GetCurrentVersion(Resource):  # 현재 앱 버전
    def post(self):
        with session_scope() as session:
            app_version = session.query(AppVersion). \
                filter(AppVersion.current_flag == 1). \
                all()
            session.commit()

            and_version = ''
            ios_version = ''

            for e in app_version:
                if e.platform == 'android':
                    and_version = e.version
                elif e.platform == 'ios':
                    ios_version = e.version

            return {'status': '000', 'msg': get_status_msg('000'),
                    'android_version': and_version, 'ios_version': ios_version}
