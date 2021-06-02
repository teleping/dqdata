#  -*- coding: utf-8 -*-
# @author: zhangping

import json
import logging
import datetime as dt
import numpy as np
import pandas as pd
from .utils import HttpUtil


class ApiClient():
    '''
    指标数据接口客户类
    '''

    def __init__(self, token='', host='121.4.186.36', port=23588, log_level=logging.INFO, api_urls=None):
        '''
        ApiClient构造函数
        :param token: token字符串
        :param host: 接口服务地址，默认：121.4.186.36
        :param port: 接口服务端口，默认：23588
        :param log_level: 日志级别，默认：logging.INFO
        :param api_urls: 接口地址服务路径
        :return:
        '''
        api_urls = api_urls if api_urls is not None else {
            'api_query': '/updatemonitor/dict-index/exportData',  # query
            'api_import': '/updatemonitor/dict-index/importJson',  # insert
            'api_delete': '/updatemonitor/dict-index/deleteDictIndexData',  # delete
            'api_dict': '/updatemonitor/dict-index/queryDictIndex'  # index dict
        }
        self.token = token
        self.host = 'http://' + host + ':' + str(port)
        self.api_query = self.host + api_urls['api_query']
        self.api_import = self.host + api_urls['api_import']
        self.api_delete = self.host + api_urls['api_delete']
        self.api_dict = self.host + api_urls['api_dict']
        self.init_logger(log_level)

    def init_logger(self, log_level=logging.INFO):
        log_fmt = '%(asctime)s - %(filename)s[line:%(lineno)d]- %(levelname)s: %(message)s'
        log_handler = logging.StreamHandler()
        log_handler.setLevel(log_level)
        log_handler.setFormatter(logging.Formatter(fmt=log_fmt))
        self.logger = logging.getLogger()
        self.logger.setLevel(log_level)
        self.logger.addHandler(log_handler)

    def get_idx_dict(self, idx):
        '''
        获取指标信息
        :param idx: 指标id
        :return:
        '''
        if idx is None or type(idx) != int: raise Exception('指标id不可为空且为int类型')

        url = self.api_dict + f'?indexId={str(idx)}'
        self.logger.debug('request: ' + url)
        result = HttpUtil.request_post(url, None, headers={'token': self.token}, raw=False)
        result = json.loads(result.decode('utf-8'))
        self.logger.debug('result: ' + str(result))

        if 'code' not in result or result['code'] != 200:
            self.logger.error(result['msg'])
            raise Exception(result['msg'])
        self.logger.info(result['msg'])
        return result['info']

    def get_series(self, ids, start_dt='2015-01-01', end_dt=None, column='id'):
        '''
        获取日期序列
        :param ids: 指标id或id列表
        :param start_dt: 开始日期，默认：2015年1月1日
        :param end_dt: 截至日期，默认：当日日期
        :param column: 列名字段：id/name
        :return:
        '''
        # ids = list(set(ids)) if type(ids) == list else [ids]
        if column not in ['id', 'name']: raise Exception('列名仅支持id和name')
        if ids is None or len(ids) == 0: raise Exception('指标id列表不可为空')

        ids = ids if type(ids) == list else [int(ids)]
        params = {'rows': [{'id': _id} for _id in ids],
                  'startDate': start_dt if start_dt is not None else '2021-01-01',
                  'endDate': end_dt if end_dt is not None else dt.datetime.today().strftime('%Y-%m-%d')}

        self.logger.debug('request: ' + self.api_query)
        self.logger.debug('params: ' + str(params))
        result = HttpUtil.request_post(self.api_query, params, headers={'token': self.token}, raw=True)
        result = json.loads(result.decode('utf-8'))
        self.logger.debug('result: ' + str(result))
        if 'code' not in result or result['code'] != 200:
            self.logger.error(result['msg'])
            raise Exception(result['msg'])

        df = pd.DataFrame({'date': []})
        for i, idx in enumerate(result['info']):
            _df = pd.DataFrame({'date': [dt.datetime.utcfromtimestamp(a[0] / 1000) for a in idx['data']],
                                idx[column]: [a[1] for a in idx['data']]})
            if _df is None or len(_df) == 0:
                df[idx[column]] = np.nan
            else:
                df = pd.merge(df, _df, on='date', how='outer')
        df.set_index(['date'], inplace=True)
        return df

    def save_series(self, items, overwrite=False):
        '''
        保存指标数据
        :param items: 指标数据列表，格式：[{'idx': 100001, 'date': datetime.datetime.today(), 'value': 100.05 }]
        :param overwrite: 是否覆盖已有相同日期值，默认False
        :return:
        '''
        if items is None or len(items) == 0:
            return False

        rows_dict = {}
        for item in items:
            if item['idx'] is None or item['date'] is None or item['value'] is None: continue
            if item['idx'] not in rows_dict: rows_dict[item['idx']] = []
            rows = rows_dict[item['idx']]
            rows.append([item['date'].strftime('%Y-%m-%d'), item['value']])

        series = [{'ID': key, 'ROWS': rows_dict[key]} for key in rows_dict]
        params = {'jsonObj': series, 'importPara': 0 if overwrite else 1}

        self.logger.debug('request: ' + self.api_import)
        self.logger.debug('params: ' + str(params))
        result = HttpUtil.request_post(self.api_import, params, headers={'token': self.token}, raw=False)
        result = json.loads(result.decode('utf-8'))
        self.logger.debug('result: ' + str(result))

        if 'code' not in result or result['code'] != 200:
            self.logger.error(result['msg'])
            raise Exception(result['msg'])

        self.logger.info(result['msg'])
        return True

    def del_series(self, ids, start_dt='1900-01-01', end_dt='2999-01-01'):
        '''
        删除指标数据
        :param ids: 指标id或id列表
        :param start_dt: 开始日期，默认：1900-01-01
        :param end_dt: 截至日期，默认：2999-01-01
        :return:
        '''
        ids = ids if type(ids) == list else [int(ids)]
        for idx in ids:
            url = self.api_delete + f'?indexId={str(idx)}&startDate={start_dt}&endDate={end_dt}'
            # params = {'indexId': idx_id, 'startDate': start_dt, 'endDate': end_dt}
            self.logger.debug('request: ' + url)
            result = HttpUtil.request_post(url, None, headers={'token': self.token}, raw=False)
            result = json.loads(result.decode('utf-8'))
            self.logger.debug('result: ' + str(result))

            if 'code' not in result or result['code'] != 200:
                self.logger.error(result['msg'])
                raise Exception(result['msg'])
            self.logger.info(result['msg'])

        return True
