from flask_restful import Resource
from flask_restful import reqparse
from db.db_connect import exec_query, get_df_from_db
import pandas as pd
from util.util_get_country import get_country
from util.util_get_status_msg import get_status_msg


def kr_get_financial_info(query_type, stock_cd):
    # 연간 기준일 경우
    if query_type == '연간':
        sql = f'select distinct quarter ' \
              f'from dart_simple_financial_statements_ttm ' \
              f'where quarter like "%4" ' \
              f'order by quarter desc ' \
              f'limit 4 '
        quarter_list = exec_query(sql)
        quarter_list = tuple([e[0] for e in quarter_list])

        sql = f'select stock_cd, quarter, ' \
              f'        revenue_ttm as revenue, operating_income_loss_ttm as operating_income_loss, ' \
              f'        profit_loss_ttm as profit_loss, operating_margin_ttm as operating_margin, ' \
              f'        net_profit_margin_ttm as net_profit_margin, ' \
              f'        roe as roe, debt_ratio_ttm as debt_ratio, eps_ttm as eps, per_ttm as per, ' \
              f'        bps_ttm as bps, pbr_ttm as pbr ' \
              f'from financial_info ' \
              f'where stock_cd = {stock_cd} ' \
              f'and quarter in {quarter_list} ' \
              f'order by quarter '
        financial_info_data = exec_query(sql)

    elif query_type == '분기':
        sql = f'select distinct quarter ' \
              f'from dart_simple_financial_statements ' \
              f'order by quarter desc ' \
              f'limit 4 '
        quarter_list = exec_query(sql)
        quarter_list = tuple([e[0] for e in quarter_list])

        sql = f'select stock_cd, quarter, ' \
              f'        revenue, operating_income_loss, ' \
              f'        profit_loss, operating_margin, ' \
              f'        net_profit_margin, ' \
              f'        roe, debt_ratio, eps, per_ttm as per, bps, pbr ' \
              f'from financial_info ' \
              f'where stock_cd = {stock_cd} ' \
              f'and quarter in {quarter_list} ' \
              f'order by quarter '
        financial_info_data = exec_query(sql)
    else:
        return None

    if len(financial_info_data) == 0:
        financial_info_data = pd.DataFrame([{'stock_cd': '0', 'quarter': '0', 'revenue': 0,
                                             'operating_income_loss': 0, 'profit_loss': 0,
                                             'operating_margin': 0, 'net_profit_margin': 0, 'roe': 0,
                                             'debt_ratio': 0, 'eps': 0, 'per': 0, 'bps': 0, 'pbr': 0}])

    quarter_list = list(quarter_list)
    quarter_list.reverse()
    financial_info_data = pd.DataFrame(financial_info_data)
    financial_info_data.columns = ['stock_cd', 'quarter', 'revenue', 'operating_income_loss', 'profit_loss',
                                   'operating_margin', 'net_profit_margin', 'roe', 'debt_ratio',
                                   'eps', 'per', 'bps', 'pbr']
    # print(financial_info_data)

    # na가 아닌 부분을 보여주고, na인 부분은 None으로 보여줌.
    financial_info_data = financial_info_data.where(pd.notna(financial_info_data), None)
    # print(financial_info_data)

    return financial_info_data


def us_get_financial_info(query_type, stock_cd):
    if query_type == '연간':
        sql = f"select a.stock_cd, a.date, a.quarter, " \
              f"		a.revenue_ttm as revenue, " \
              f"		a.operatingIncome_ttm as operating_income_loss, " \
              f"		a.netIncome_ttm as profit_loss, " \
              f"		b.operating_margin_ttm as operating_margin, " \
              f"		b.net_profit_margin_ttm as net_profit_margin, " \
              f"		b.roe as roe, " \
              f"		b.debt_ratio_ttm as debt_ratio, " \
              f"		c.eps_ttm as eps, " \
              f"		c.per_ttm as per, " \
              f"		c.bps_ttm as bps, " \
              f"		c.pbr_ttm as pbr " \
              f"from ( " \
              f"		select * " \
              f"		from us_financial_statements_ttm  " \
              f"		where stock_cd = '{stock_cd}' " \
              f"		and quarter = 'Q4' " \
              f"		order by date DESC  " \
              f"		limit 4) as a " \
              f"left join ( " \
              f"		select * " \
              f"		from us_financial_ratio " \
              f"		where stock_cd = '{stock_cd}') as b " \
              f"	on a.stock_cd = b.stock_cd " \
              f"	and a.date = b.date " \
              f"left join ( " \
              f"		select *  " \
              f"		from us_stock_valuation_indicator " \
              f"		where stock_cd = '{stock_cd}') as c  " \
              f"	on a.stock_cd = c.stock_cd " \
              f"	and a.date = c.date"
        financial_info_data = get_df_from_db(sql)

    elif query_type == '분기':
        sql = f"select a.stock_cd, a.date, a.quarter, " \
              f"		a.revenue as revenue, " \
              f"		a.operatingIncome as operating_income_loss, " \
              f"		a.netIncome as profit_loss, " \
              f"		b.operating_margin as operating_margin, " \
              f"		b.net_profit_margin as net_profit_margin, " \
              f"		b.roe as roe,  " \
              f"		b.debt_ratio as debt_ratio,  " \
              f"		c.eps as eps,  " \
              f"		c.per_ttm as per,  " \
              f"		c.bps as bps,  " \
              f"		c.pbr as pbr " \
              f"from ( " \
              f"		select * " \
              f"		from us_financial_statements  " \
              f"		where stock_cd = '{stock_cd}' " \
              f"		order by date DESC  " \
              f"		limit 4) as a " \
              f"left join ( " \
              f"		select * " \
              f"		from us_financial_ratio " \
              f"		where stock_cd = '{stock_cd}') as b " \
              f"	on a.stock_cd = b.stock_cd " \
              f"	and a.date = b.date " \
              f"left join ( " \
              f"		select *  " \
              f"		from us_stock_valuation_indicator " \
              f"		where stock_cd = '{stock_cd}') as c  " \
              f"	on a.stock_cd = c.stock_cd " \
              f"	and a.date = c.date"
        financial_info_data = get_df_from_db(sql)
        # per의 경우, TTM으로 계산함(분기값으로 계산하면 비상식적인 값이 나옴)
    else:
        return None

    financial_info_data['quarter'] = financial_info_data.apply(lambda x: str(x['date'])[:6] + '(' + x['quarter'] + ')', axis=1)

    if len(financial_info_data) == 0:
        financial_info_data = pd.DataFrame([{'stock_cd': '0', 'quarter': '0', 'revenue': 0,
                                             'operating_income_loss': 0, 'profit_loss': 0,
                                             'operating_margin': 0, 'net_profit_margin': 0, 'roe': 0,
                                             'debt_ratio': 0, 'eps': 0, 'per': 0, 'bps': 0, 'pbr': 0}])

    quarter_list = list(financial_info_data['quarter'])
    quarter_list.reverse()
    fd_columns = ['stock_cd', 'quarter', 'revenue', 'operating_income_loss', 'profit_loss',
                  'operating_margin', 'net_profit_margin', 'roe', 'debt_ratio', 'eps', 'per', 'bps', 'pbr']
    financial_info_data = financial_info_data.loc[:, fd_columns]
    # print(financial_info_data)

    # na가 아닌 부분을 보여주고, na인 부분은 None으로 보여줌.
    financial_info_data = financial_info_data.where(pd.notna(financial_info_data), None)
    # print(financial_info_data)

    return financial_info_data


class GetSingleFinancialInfo(Resource):  # 재무정보 단건 조회
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('info_cd', type=str)
        parser.add_argument('query_type', type=str)
        parser.add_argument('stock_cd', type=str)
        args = parser.parse_args()

        info_cd = args['info_cd']
        query_type = args['query_type']
        stock_cd = args['stock_cd']

        # country 정의
        country = get_country(stock_cd)

        # info_cd map dict 정의
        kr_info_cd_map_annual = {'roe': 'roe',
                                 'roa': 'roa',
                                 'eps': 'eps_ttm',
                                 'bps': 'bps_ttm',
                                 'per': 'per_ttm',
                                 'pbr': 'pbr_ttm',
                                 '부채비율': 'debt_ratio_ttm',
                                 '영업이익': 'operating_income_loss_ttm',
                                 '영업이익률': 'operating_margin_ttm',
                                 '순이익': 'profit_loss_ttm',
                                 '순이익률': 'net_profit_margin_ttm',
                                 '매출액': 'revenue_ttm'}

        kr_info_cd_map = {'roe': 'roe',
                          'roa': 'roa',
                          'eps': 'eps',
                          'bps': 'bps',
                          'per': 'per_ttm',
                          'pbr': 'pbr',
                          '부채비율': 'debt_ratio',
                          '영업이익': 'operating_income_loss_ttm',
                          '영업이익률': 'operating_margin',
                          '순이익': 'profit_loss_ttm',
                          '순이익률': 'net_profit_margin',
                          '매출액': 'revenue_ttm'}

        us_info_cd_map_annual = {'roe': 'roe',
                                 'roa': 'roa',
                                 'eps': 'eps_ttm',
                                 'bps': 'bps_ttm',
                                 'per': 'per_ttm',
                                 'pbr': 'pbr_ttm',
                                 '부채비율': 'debt_ratio_ttm',
                                 '영업이익': 'operatingIncome_ttm',
                                 '영업이익률': 'operating_margin_ttm',
                                 '순이익': 'netIncome_ttm',
                                 '순이익률': 'net_profit_margin_ttm',
                                 '매출액': 'revenue_ttm'}

        us_info_cd_map = {'roe': 'roe',
                          'roa': 'roa',
                          'eps': 'eps',
                          'bps': 'bps',
                          'per': 'per_ttm',
                          'pbr': 'pbr',
                          '부채비율': 'debt_ratio',
                          '영업이익': 'operatingIncome_ttm',
                          '영업이익률': 'operating_margin',
                          '순이익': 'netIncome_ttm',
                          '순이익률': 'net_profit_margin',
                          '매출액': 'revenue_ttm'}

        if country == 'KR':
            # 연간 기준일 경우
            if query_type == '연간':
                sql = f'select distinct quarter ' \
                      f'from dart_simple_financial_statements_ttm ' \
                      f'where quarter like "%4" ' \
                      f'order by quarter desc ' \
                      f'limit 4 '
                quarter_list = exec_query(sql)
                quarter_list = tuple([e[0] for e in quarter_list])

                sql = f'select stock_cd, quarter, stock_nm, {kr_info_cd_map_annual[info_cd]} ' \
                      f'from financial_info ' \
                      f'where stock_cd = {stock_cd} ' \
                      f'and quarter in {quarter_list} ' \
                      f'order by quarter '
                financial_info_data = exec_query(sql)

            elif query_type == '분기':
                sql = f'select distinct quarter ' \
                      f'from dart_simple_financial_statements_ttm ' \
                      f'order by quarter desc ' \
                      f'limit 4 '
                quarter_list = exec_query(sql)
                quarter_list = tuple([e[0] for e in quarter_list])

                sql = f'select stock_cd, quarter, stock_nm, {kr_info_cd_map[info_cd]} ' \
                      f'from financial_info ' \
                      f'where stock_cd = {stock_cd} ' \
                      f'and quarter in {quarter_list} ' \
                      f'order by quarter '
                financial_info_data = exec_query(sql)
            else:
                return {'status': '401', 'msg': get_status_msg('401')}
        elif country == 'US':
            if query_type == '연간':
                sql = f"select a.stock_cd, a.date, a.quarter, d.stock_nm, {us_info_cd_map_annual[info_cd]} " \
                      f"from ( " \
                      f"		select * " \
                      f"		from us_financial_statements_ttm  " \
                      f"		where stock_cd = '{stock_cd}' " \
                      f"		and quarter = 'Q4' " \
                      f"		order by date DESC  " \
                      f"		limit 4) as a " \
                      f"left join ( " \
                      f"		select * " \
                      f"		from us_financial_ratio " \
                      f"		where stock_cd = '{stock_cd}') as b " \
                      f"	on a.stock_cd = b.stock_cd " \
                      f"	and a.date = b.date " \
                      f"left join ( " \
                      f"		select *  " \
                      f"		from us_stock_valuation_indicator " \
                      f"		where stock_cd = '{stock_cd}') as c  " \
                      f"	on a.stock_cd = c.stock_cd " \
                      f"	and a.date = c.date " \
                      f"left join ( " \
                      f"        select *" \
                      f"        from us_stock_info" \
                      f"        where stock_cd = '{stock_cd}') as d " \
                      f"    on a.stock_cd = d.stock_cd"
                financial_info_data = get_df_from_db(sql)
            elif query_type == '분기':
                sql = f"select a.stock_cd, a.date, a.quarter, d.stock_nm, {us_info_cd_map[info_cd]} " \
                      f"from ( " \
                      f"		select * " \
                      f"		from us_financial_statements  " \
                      f"		where stock_cd = '{stock_cd}' " \
                      f"		order by date DESC  " \
                      f"		limit 4) as a " \
                      f"left join ( " \
                      f"		select * " \
                      f"		from us_financial_statements_ttm " \
                      f"		where stock_cd = '{stock_cd}') as ttm " \
                      f"	on a.stock_cd = ttm.stock_cd " \
                      f"	and a.date = ttm.date " \
                      f"left join ( " \
                      f"		select * " \
                      f"		from us_financial_ratio " \
                      f"		where stock_cd = '{stock_cd}') as b " \
                      f"	on a.stock_cd = b.stock_cd " \
                      f"	and a.date = b.date " \
                      f"left join ( " \
                      f"		select *  " \
                      f"		from us_stock_valuation_indicator " \
                      f"		where stock_cd = '{stock_cd}') as c  " \
                      f"	on a.stock_cd = c.stock_cd " \
                      f"	and a.date = c.date " \
                      f"left join ( " \
                      f"        select *" \
                      f"        from us_stock_info" \
                      f"        where stock_cd = '{stock_cd}') as d " \
                      f"    on a.stock_cd = d.stock_cd"
                financial_info_data = get_df_from_db(sql)
            else:
                return {'status': '401', 'msg': get_status_msg('401')}
            # quarter 정리
            financial_info_data['quarter'] = financial_info_data.apply(lambda x: str(x['date'])[:6] + '(' + x['quarter'] + ')', axis=1)
            quarter_list = financial_info_data['quarter']
            financial_info_data = financial_info_data.iloc[:, [0, 2, 3, 4]]
        else:
            # 2021.08.07 미국주식 추가 하면서 status 코드 추가: 문제시 삭제 가능
            return {'status': '402', 'msg': get_status_msg('402')}

        # print(financial_info_data)
        if len(financial_info_data) == 0:
            financial_info_data = pd.DataFrame([{'stock_cd': '0', 'quarter': '0', 'stock_nm': '0', 'value': 0}])

        quarter_list = list(quarter_list)
        quarter_list.reverse()
        financial_info_data = pd.DataFrame(financial_info_data)
        financial_info_data.columns = ['stock_cd', 'quarter', 'stock_nm', 'value']

        # na가 아닌 부분을 보여주고, na인 부분은 None으로 처리함.
        financial_info_data = financial_info_data.where(pd.notna(financial_info_data), None)

        # 영업이익, 순이익, 매출액 단위 처리
        if country == 'KR':
            unit_price = 100000000  # 억원 기준
        elif country == 'US':
            unit_price = 1000000  # 백만달러 기준
        else:
            unit_price = 1

        if info_cd in ['영업이익', '순이익', '매출액']:
            for i in range(financial_info_data.shape[0]):
                if pd.notna(financial_info_data.iloc[i, 3]):
                    financial_info_data.iloc[i, 3] = int(financial_info_data.iloc[i, 3]/unit_price)

        financial_info_list = []
        unit_currency = 'KRW' if country == 'KR' else 'USD'
        for q in quarter_list:
            current_row = financial_info_data.loc[financial_info_data['quarter'] == q, :]
            if current_row.shape[0] == 0 or current_row['value'].values[0] is None:
                financial_info_list.append({'stock_cd': stock_cd,
                                            'quarter': q,
                                            'stock_nm': '',
                                            'nodata_flag': 1,
                                            'value': 0,
                                            'unit_currency': unit_currency})
            else:
                financial_info_list.append({'stock_cd': stock_cd,
                                            'quarter': q,
                                            'stock_nm': current_row['stock_nm'].values[0],
                                            'nodata_flag': 0,
                                            'value': float(current_row['value'].values[0]),
                                            'unit_currency': unit_currency})

        # print(financial_info_list)
        return {'status': '000', 'msg': get_status_msg('000'),
                'financial_info': financial_info_list}


class GetFinancialInfo(Resource):  # 재무정보 조회
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('query_type', type=str)
        parser.add_argument('stock_cd', type=str)
        args = parser.parse_args()

        query_type = args['query_type']
        stock_cd = args['stock_cd']

        country = get_country(stock_cd)
        if country == 'KR':
            financial_info_data = kr_get_financial_info(query_type=query_type, stock_cd=stock_cd)
            quarter_list = financial_info_data['quarter']
        elif country == 'US':
            financial_info_data = us_get_financial_info(query_type=query_type, stock_cd=stock_cd)
            quarter_list = financial_info_data['quarter']
        else:
            financial_info_data = None
            quarter_list = None

        if financial_info_data is None:
            return {'status': '401', 'msg': get_status_msg('401')}

        financial_info_list = []
        unit_currency = 'KRW' if country == 'KR' else 'USD'
        for q in quarter_list:
            current_row = financial_info_data.loc[financial_info_data['quarter'] == q, :]
            if current_row.shape[0] == 0:
                financial_info_list.append({
                    'quarter': q,
                    'nodata_flag': 1,
                    'revenue': 0,
                    'operating_income_loss': 0,
                    'profit_loss': 0,
                    'operating_margin': 0,
                    'net_profit_margin': 0,
                    'roe': 0,
                    'debt_ratio': 0,
                    'eps': 0,
                    'per': 0,
                    'bps': 0,
                    'pbr': 0,
                    'unit_currency': unit_currency})
            else:
                financial_info_list.append({
                    'quarter': q,
                    'nodata_flag': 0,
                    'revenue': None if current_row['revenue'].values[0] is None else int(current_row['revenue'].values[0]),
                    'operating_income_loss': None if current_row['operating_income_loss'].values[0] is None else int(current_row['operating_income_loss'].values[0]),
                    'profit_loss': None if current_row['profit_loss'].values[0] is None else int(current_row['profit_loss'].values[0]),
                    'operating_margin': None if current_row['operating_margin'].values[0] is None else float(current_row['operating_margin'].values[0]),
                    'net_profit_margin': None if current_row['net_profit_margin'].values[0] is None else float(current_row['net_profit_margin'].values[0]),
                    'roe': None if current_row['roe'].values[0] is None else float(current_row['roe'].values[0]),
                    'debt_ratio': None if current_row['debt_ratio'].values[0] is None else float(current_row['debt_ratio'].values[0]),
                    'eps': None if current_row['eps'].values[0] is None else float(current_row['eps'].values[0]),
                    'per': None if current_row['per'].values[0] is None else float(current_row['per'].values[0]),
                    'bps': None if current_row['bps'].values[0] is None else float(current_row['bps'].values[0]),
                    'pbr': None if current_row['pbr'].values[0] is None else float(current_row['pbr'].values[0]),
                    'unit_currency': unit_currency})

        return {'status': '000', 'msg': get_status_msg('000'),
                'financial_info': financial_info_list}
