# dqdata

Data sdk for Ducheng Quant Database.

## 1、安装：

```
pip install dqdata
```

```buildoutcfg
依赖包：numpy, pandas, urllib3, sqlalchemy
```

## 2、ApiClient实例创建

### 方法说明：

```buildoutcfg
ApiClient(token='', host='121.4.186.36', port=23588, log_level=logging.INFO, api_urls=None)

token: token字符串
host: 接口服务地址，默认：121.4.186.36
port: 接口服务端口，默认：23588
log_level: 日志级别，默认：logging.INFO
api_urls: 接口地址服务路径
```

### 示例：

```
api = ApiClient(token='xxxx-xxxx-xxxx')
```

## 3、查询指标信息

### 方法说明：

```buildoutcfg
get_idx_dict(idx)

idx: 指标id
```

### 示例：通过id查询指标信息

```
info = api.get_idx_dict(109646)
print(info)
```

### 执行结果

```
{'id': 109646, 'code': 'RB_DC', 'name': '河钢承钢：天津市场价格：螺纹钢：HRB400E：Ф18-25mm（日）', 'unit': '元/吨', 'frequency': '日', 'description': None, 'tableName': 'T_STEEL', 'sourceType': 'mysteel', 'sourceCode': 'ST_0001246521', 'sourceDescription': None, 'industry': '黑色', 'type': '现货价格', 'commodity': 'RB', 'sjbId': None, 'userId': None, 'rowsCount': 1191, 'dateFirst': '2016-07-21T00:00:00', 'dateLast': '2021-04-28T00:00:00', 'timeLastUpdate': '2021-04-28T19:56:44.783', 'timeLastRequest': None, 'priority': None, 'status': None, 'shortName': None, 'updateDescription': None, 'indexPropertiesList': None, 'categoryId': None, 'indexName': None}
```

## 4、通过类型查询指标列表

### 方法说明：

```buildoutcfg
get_dict_list(source_type):

source_type: 指标来源类型
```

### 示例：通过类型查询指标列表

```
df = api.get_dict_list('wind')
print(df)
```

### 执行结果

```
          id    code       name  ... categoryIdList commodityName sorting
0     102786  EXC_JY      美元中间价  ...           None          None    None
1     102804  PTA_JY  FOB鹿特丹 MX  ...           None          None    None
2     102805  PTA_JY    国产MX：华东  ...           None          None    None
3     102818  PTA_JY  国产PX：镇海炼化  ...           None          None    None
4     102835   PF_JY  TA01M.CZC  ...           None          None    None
...      ...     ...        ...  ...            ...           ...     ...
```

## 5、查询指标数据

### 方法说明：

```buildoutcfg
get_series(ids, start_dt='2015-01-01', end_dt=None, column='id')

ids: 指标id或id列表
start_dt: 开始日期，默认：2015年1月1日
end_dt: 截至日期，默认：当日日期
column: 列名字段：id/name
```

### 示例：通过id列表查询指标数据

```
df = api.get_series([109645, 109646], start_dt='2021-05-15')
print(df)
```

### 执行结果

```
            109645  109646
date                      
2021-05-17  5910.0  5920.0
2021-05-18  5880.0  5910.0
2021-05-19  5610.0  5640.0
2021-05-20  5500.0  5530.0
2021-05-21  5450.0  5500.0
2021-05-24  5260.0  5310.0
2021-05-25  5210.0  5260.0
2021-05-26  5080.0  5130.0
2021-05-27  4980.0  5030.0
2021-05-28  5070.0  5120.0
```

### 示例：使用指标名称作为列名

```
df = api.get_series([109645, 109646], column='name', start_dt='2021-05-15')
print(df)
```

### 执行结果

```
            鑫达：天津市场价格：螺纹钢：HRB400E：Ф18-25mm（日）  河钢承钢：天津市场价格：螺纹钢：HRB400E：Ф18-25mm（日）
date                                                                              
2021-05-17                             5910.0                               5920.0
2021-05-18                             5880.0                               5910.0
2021-05-19                             5610.0                               5640.0
2021-05-20                             5500.0                               5530.0
2021-05-21                             5450.0                               5500.0
2021-05-24                             5260.0                               5310.0
2021-05-25                             5210.0                               5260.0
2021-05-26                             5080.0                               5130.0
2021-05-27                             4980.0                               5030.0
2021-05-28                             5070.0                               5120.0
```

## 6、插入指标数据

### 方法说明：

```buildoutcfg
save_series(self, items, overwrite=False):

items: 指标数据列表，格式：[{'idx': 100001, 'date': datetime.datetime.today(), 'value': 100.05 }]
overwrite: 是否覆盖已有相同日期值，默认False
```

### 示例：插入数据

```
items = []

for i in range(11):
    items.append({'idx': 114278, 'date': dt.datetime.today() - dt.timedelta(days=i + 10), 'value': i * 100})
    items.append({'idx': 109646, 'date': dt.datetime.today() - dt.timedelta(days=i + 10), 'value': i * 1000})

api.save_series(items, overwrite=False)
```

### 执行结果

```
2021-06-01 17:12:05,983 - api_client.py[line:116]- INFO: T_STEEL 更新或新增了22条记录
```

### 查询数据

```
df = api.get_series([109645, 109646], column='id', start_dt='2021-05-15')
print(df)
```

### 执行结果

```
            109645  109646
date                      
2021-05-15   700.0  7000.0
2021-05-16   600.0  6000.0
2021-05-17   500.0  5000.0
2021-05-18   400.0  4000.0
2021-05-19   300.0  3000.0
2021-05-20   200.0  2000.0
2021-05-21   100.0  1000.0
2021-05-22     0.0     0.0
```

## 7、删除指标数据

### 方法说明：

```buildoutcfg
del_series(ids, start_dt='1900-01-01', end_dt='2999-01-01')

ids: 指标id或id列表
start_dt: 开始日期，默认：1900-01-01
end_dt: 截至日期，默认：2999-01-01
```

### 示例：清空数据

```
api.del_series([109645, 109646])
```

### 执行结果

```
2021-06-01 14:55:42,583 - api_client.py[line:131]- INFO: 删除了728条数据。
2021-06-01 14:55:42,810 - api_client.py[line:131]- INFO: 删除了1191条数据。
```

### 示例：删除指定起始日期数据

```
api.del_series([109645, 109646], start_dt='2021-04-15')
```

### 执行结果

```
2021-06-01 14:59:28,538 - api_client.py[line:131]- INFO: 删除了15条数据。
2021-06-01 14:59:28,585 - api_client.py[line:131]- INFO: 删除了15条数据。
```

