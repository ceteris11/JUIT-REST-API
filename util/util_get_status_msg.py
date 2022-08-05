import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from db.db_model import session_scope, ApiStatusCode


def get_status_msg(status_code):
    with session_scope() as session:
        status_info = session.query(ApiStatusCode). \
            filter(ApiStatusCode.status_code == status_code). \
            first()
        session.commit()

        if status_info is not None:
            msg = status_info.msg
        else:
            msg = "status code 정보 없음"

    return msg
