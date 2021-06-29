#  -*- coding: utf-8 -*-
# @author: zhangping

import json
import datetime as dt
from urllib import request
from urllib import parse
from sqlalchemy import create_engine, Column, String
from sqlalchemy.types import VARCHAR, Date, TIMESTAMP, Integer, Float, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, MetaData


class HttpUtil:
    @classmethod
    def request_post(cls, url, params, headers={}, raw=False):
        if raw:
            headers['Content-Type'] = 'application/json'
            params = json.dumps(params).encode('utf-8')
        elif params is not None:
            params = parse.urlencode(params).encode('utf-8')

        req = request.Request(url, headers=headers, data=params)  # POST
        with request.urlopen(req) as result:
            data = result.read()
        return data

    @classmethod
    def request_down(cls, url, path, headers={}, params={}):
        params = parse.urlencode(params).encode('utf-8')
        req = request.Request(url, headers=headers, data=params)  # POST
        with request.urlopen(req) as f:
            data = f.read()
            fhandle = open(path, "wb")
            fhandle.write(data)
            fhandle.close()


class DBUtil:
    @classmethod
    def get_conn(cls, conn_str, echo=False):
        return create_engine(conn_str, echo=echo)

    @classmethod
    def get_df_type_dict(cls, df):
        type_dict = {}
        for i, j in zip(df.columns, df.dtypes):
            if "object" in str(j):
                type_dict.update({i: VARCHAR(20)})
            if "float" in str(j):
                type_dict.update({i: DECIMAL(20, 5)})
            if "date" in str(j):
                type_dict.update({i: Date()})
        return type_dict


class IndexUtil:
    @classmethod
    def get(cls, idx, date, value):
        return {'idx': idx, 'date': date, 'value': value}

    @classmethod
    def set_idx(cls, items, idx):
        items = items if type(items) == list else [items]
        for item in items:
            item['idx'] = idx
        return items

    @classmethod
    def get_dict(cls, idx, conn):
        sql_table = 'select * from dict_index where id={0}'
        record = conn.execute(sql_table.format(int(idx))).fetchone()
        return dict(record) if record is not None else None

    __pools = {}

    @classmethod
    def get_table_cls(cls, table_name):
        def fun__init(self, idx, date, value):
            self.id = None
            self.index_id = idx
            self.index_date = date
            self.index_value = value
            self.update_time = dt.datetime.now()

        def fun__repr(self):
            return "[id:" + str(self.id) + ", index_id:" + str(self.index_id) + ", index_date:" + str(
                self.index_date) + ", index_value=" + str(self.index_value) + "]"

        def fun_get_item(self):
            return {'id': self.id, 'idx': self.index_id, 'date': self.index_date, 'value': self.index_value}

        if cls.__pools.get(table_name) is None:
            tb = type(table_name, (declarative_base(),),
                      {'__table__': Table(table_name, MetaData(),
                                          Column('id', Integer, primary_key=True),
                                          Column('index_id', Integer),
                                          Column('index_date', Date),
                                          Column('index_value', Float),
                                          Column('update_time', TIMESTAMP)
                                          ),
                       '__table_args__': ({'autoload': True},),
                       '__init__': fun__init,
                       '__repr__': fun__repr,
                       'get_item': fun_get_item
                       }
                      )
            tb.metadata = tb.__table__.metadata
            cls.__pools[table_name] = tb
        return cls.__pools.get(table_name)

    @classmethod
    def save_items(cls, items, conn, overwrite=False):
        items = items if type(items) == list or items is None else [items]
        if items is None or len(items) == 0:
            return

        # for item in items:
        #     print(item)

        table_dict = {}
        session = sessionmaker(bind=conn)()
        for item in items:
            if table_dict.get(item['idx']) is None:
                _dict = cls.get_dict(item['idx'], conn)
                if _dict is not None and _dict['table_name'] is not None:
                    table_dict[item['idx']] = _dict['table_name']
            cls.__save_item(session, table_dict.get(item['idx']), item, overwrite=overwrite)
        session.commit()
        session.flush()

    # @classmethod
    # def save_item(cls, item):
    #     table = cls.get_dict(item['idx'])['table_name']
    #     session = sessionmaker(bind=DBUtil.get_conn('idbms'))()
    #     cls.__save_item(session, table, item)
    #     session.commit()
    #     session.flush()

    @classmethod
    def __save_item(cls, session, table, item, overwrite=False):
        tb = cls.get_table_cls(table)
        result = session.query(tb).filter_by(index_id=item['idx'], index_date=item['date'].date())
        if result.count() == 0:
            session.add(tb(item['idx'], item['date'].date(), item['value']))
        elif overwrite:
            result.update({"index_value": item['value'], "update_time": dt.datetime.now()})

    @classmethod
    def get_last_date(cls, idx_id, conn):
        sql_table = '''select table_name from dict_index where id={0}'''
        sql_last_date = '''select max(index_date) index_date from {0} where index_id={1}'''

        last_date = None
        result_table = conn.execute(sql_table.format(int(idx_id))).fetchone()
        if result_table is not None and len(result_table) == 1:
            table_name = str(result_table[0]).strip()
            last_date_result = conn.execute(sql_last_date.format(table_name, int(idx_id))).fetchone()
            if last_date_result is not None and len(last_date_result) == 1:
                last_date = last_date_result[0]
        return last_date
