#  -*- coding: utf-8 -*-
# @author: zhangping

import pandas as pd
import datetime as dt
from .utils import DBUtil, IndexUtil


class DbClient:
    '''
    指标数据库客户类
    '''

    def __init__(self, dbuser, dbpass, host='81.69.0.75', port=1521, sid='orcl'):
        '''
        :param dbuser: 数据库用户
        :param dbpass: 数据库密码
        :param host: 数据库主机，默认：81.69.0.75
        :param port: 数据库端口，默认：1521
        :param sid: 数据库SID，默认：orcl
        '''
        self.conn_str = f'oracle+cx_oracle://{dbuser}:{dbpass}@{host}:{port}/{sid}'
        self.conn = DBUtil.get_conn(self.conn_str, echo=False)
        self.dicts = {}

    def get_dict(self, _id):
        '''
        获取指标信息
        :param _id: 指标id
        :return:
        '''
        _dict = self.dicts.get(_id)
        if _dict is None:
            self.dicts[_id] = _dict = IndexUtil.get_dict(_id, self.conn)
        return _dict

    def get_series(self, ids, start_dt='2010-01-01', end_dt=dt.datetime.today().strftime('%Y-%m-%d')):
        '''
        获取日期序列
        :param ids: 指标id或id列表
        :param start_dt: 开始日期，默认：2010年1月1日
        :param end_dt: 截至日期，默认：当天日期
        :return:
        '''
        ids = list(set(ids)) if type(ids) == list else [ids]
        # dfs = [self.__get_series(id, start_dt, end_dt) for id in ids]
        df = None
        for _id in ids:
            _df = self.__get_series(_id, start_dt, end_dt)
            df = _df if df is None else pd.merge(df, _df, on='date', how='outer')
        if df is not None and len(df) > 0:
            df.sort_index(inplace=True)

        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)
        return df

    def __get_series(self, _id, start_dt, end_dt):
        _dict = self.get_dict(_id)

        if _dict is None or _dict.get('table_name') is None:
            df = pd.DataFrame([], columns=[_id, 'date'])
        else:
            sql = f"select * from {_dict.get('table_name')} where index_id={_id} and index_date>=to_date('{start_dt}', 'yyyy-MM-dd') and index_date<=to_date('{end_dt}', 'yyyy-MM-dd')"
            df = pd.read_sql(sql, self.conn)
            df = df[['index_value', 'index_date']]
            df.rename(columns={'index_value': _id, 'index_date': 'date'}, inplace=True)

        return df

    def save_series(self, series, overwrite=False):
        '''
        保存指标数据
        :param series: 指标序列数据列表，列表元素格式：{'idx': 100001, 'date': datetime.datetime.date(), 'value': 100.05 }
        :param overwrite: 是否覆盖已有相同日期值，默认False
        :return:
        '''
        if series is not None and len(series) > 0:
            IndexUtil.save_items(series, self.conn, overwrite=overwrite)
            for _id in set([item['idx'] for item in series]): self.update_dict(_id)

    def delete_series(self, id):  # TODO
        '''
        清空指标数据
        '''
        pass

    def update_dict(self, _id):
        '''
        更新指标数据统计
        '''
        _dict = self.get_dict(_id)
        if _dict is not None or _dict.get('table_name') is None:
            sql_count = f'select count(id) as rows_count, min(index_date) as date_first, max(index_date) as date_last, max(update_time) as date_update from {_dict.get("table_name")} where index_id={_id}'
            sql_update = f'update dict_index set rows_count=(:rows_count), date_first=(:date_first), date_last=(:date_last), time_last_update=(:time_last_update), time_last_request=(:time_last_request) where id=(:id)'
            result = self.conn.execute(sql_count).fetchone()
            if result is not None and len(result) == 4:
                params = {
                    'id': _id,
                    'rows_count': result[0],
                    'date_first': result[1],
                    'date_last': result[2],
                    'time_last_update': result[3],
                    'time_last_request': dt.datetime.now()
                }
                self.conn.execute(sql_update, params)