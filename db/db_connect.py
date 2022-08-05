from sqlalchemy import create_engine, text as sql_text
from datetime import datetime
import pandas as pd
from config.config_manager import *
from flask import current_app as app
from module.merlot_logging import *

def get_db_url():
    return get_config()['db_url']


def get_api_url():
    return get_config()['api_url']


# sqlalchemy 설정
engine = create_engine(f"mysql+pymysql://{get_config()['user']}:{get_config()['pwd']}@{get_config()['db_url']}:{get_config()['port']}/sauvignon"
                       f'?autocommit=true&charset=utf8mb4', pool_size=20, pool_pre_ping=True, pool_recycle=280, max_overflow=100)


def get_df_from_db(sql):
    # 커넥션 설정
    conn = engine.connect()
    trans = conn.begin()

    try:
        return_df = pd.read_sql_query(sql, conn)
        trans.commit()
    except Exception as e:
        app.logger.error(make_log_msg("flask: get_df_from_db", e))
        trans.rollback()
        return_df = None
    finally:
        conn.close()

    return return_df


def exec_query(sql, engine=engine):
    # MySQL Connection 연결
    conn = engine.connect()
    trans = conn.begin()
    # conn = connect(host='betterlife.duckdns.org', port=1231, user='betterlife', password='snail132',
    #                db='stock_db', charset='utf8')

    # Connection 으로부터 Cursor 생성
    sql = sql_text(sql)

    # SQL문 실행
    try:
        sql = sql
        result = conn.execute(sql)

        # 데이터 Fetch
        if result.returns_rows:
            result = result.fetchall()

        # commit
        trans.commit()
    except Exception as e:
        app.logger.error(make_log_msg("flask: exec_query", e))
        trans.rollback()
        result = None
    finally:
        # Connection 닫기
        conn.close()

    return result


def insert_data(data, table_name):
    conn = engine.connect()
    trans = conn.begin()
    # 임시 테이블에 저장
    tmp_table = f'tmp_table_{datetime.now().strftime("%Y%m%d_%H%M%S_%f")}'
    try:
        data.to_sql(tmp_table, con=conn, if_exists='append', index=False)
        trans.commit()

        # target 테이블에 임시 테이블 내용 insert(duplicated key error 발생 시, replace함)
        exec_query(f'replace into {table_name} select * from {tmp_table}')

        # 임시테이블 삭제
        exec_query(f'drop table {tmp_table}')
    except Exception as e:
        app.logger.error(make_log_msg("flask: insert_data", e))
        trans.rollback()
    finally:
        # 커넥션 종료
        conn.close()


if __name__ == '__main__':
    get_df_from_db(f'select * from ')
