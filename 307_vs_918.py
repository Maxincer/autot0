"""
检查307账户的指定情况
1. 以918未标杆
"""


import pandas as pd
from pymongo import MongoClient

server_mongodb = MongoClient('mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe')
col_post_trade_position = server_mongodb['post_trade_data']['post_trade_position']
iter_post_trade_position = col_post_trade_position.find({'DataDate': '20201229'})

# 获取307可交易股票集合
set_secid_in_position_307 = set()
for dict_position in iter_post_trade_position:
    if not dict_position['LongQty']:  # 取决于上传至内网的股票
        continue
    secid_in_postion = dict_position['SecurityID']
    set_secid_in_position_307.add(secid_in_postion)

# 获取918可交易股票集合
fpath_xlsx_918_post_trade_data = (
    r'D:\projects\autot0\data\918_20201229_holding.csv'
)
df_918_holding = pd.read_csv(fpath_xlsx_918_post_trade_data, converters={'stock_code': lambda x: str(x).zfill(6)})
list_dicts_918_holding = df_918_holding.to_dict('records')
set_secid_in_position_918 = set()
for dict_918_holding in list_dicts_918_holding:
    if not dict_918_holding['current_amount']:  # 取决于上传至内网的股票
        continue
    secid_in_postion = dict_918_holding['current_amount']
    set_secid_in_position_918.add(secid_in_postion)


# 获取918股票委托标的集合： 买集合与卖集合
fpath_xlsx_compare_draft = 'data/compare_draft.xlsx'
df_918_order = pd.read_excel(
    fpath_xlsx_compare_draft,
    sheet_name='918_order',
    dtype={'OrderQty': float},
    converters={'SecurityID': lambda x: str(x).zfill(6)}
)
list_dicts_918_order = df_918_order.to_dict('records')

set_secids_buy_918 = set()
set_secids_sell_918 = set()
for dict_918_order in list_dicts_918_order:
    secid = dict_918_order['SecurityID']
    order_qty = dict_918_order['OrderQty']
    if order_qty > 0:
        set_secids_buy_918.add(secid)
    elif order_qty < 0:
        set_secids_sell_918.add(secid)
    else:
        print(f"{secid}委托数量为0")

# 获取307委托标的集合
df_307_order = pd.read_excel(
    fpath_xlsx_compare_draft,
    sheet_name='307_order',
    dtype={'OrderQty': float},
    converters={'SecurityID': lambda x: str(x).zfill(6)}
)
list_dicts_307_order = df_307_order.to_dict('records')

set_secids_buy_307 = set()
set_secids_sell_307 = set()
for dict_307_trade in list_dicts_307_order:
    secid = dict_307_trade['SecurityID']
    order_qty = dict_307_trade['OrderQty']
    if order_qty > 0:
        set_secids_buy_307.add(secid)
    elif order_qty < 0:
        set_secids_sell_307.add(secid)
    else:
        print(f"{secid}委托数量为0")


# 获取918股票exposure标的集合： 买集合与卖集合
fpath_xlsx_compare_draft = 'data/compare_draft.xlsx'
df_918_position = pd.read_excel(
    fpath_xlsx_compare_draft,
    sheet_name='918_position',
    dtype={'NetQty': float},
    converters={'SecurityID': lambda x: str(x).zfill(6)}
)
list_dicts_918_position = df_918_position.to_dict('records')

set_secids_longqty_918 = set()
set_secids_shortqty_918 = set()
for dict_918_position in list_dicts_918_position:
    secid = dict_918_position['SecurityID']
    netqty = dict_918_position['NetQty']
    if netqty > 0:
        set_secids_longqty_918.add(secid)
    elif netqty < 0:
        set_secids_shortqty_918.add(secid)
    else:
        print(f"{secid}委托数量为0")

# 获取307exposure标的集合
df_307_position = pd.read_excel(
    fpath_xlsx_compare_draft,
    sheet_name='307_position',
    dtype={'NetQty': float},
    converters={'SecurityID': lambda x: str(x).zfill(6)}
)
list_dicts_307_position = df_307_position.to_dict('records')

set_secids_longqty_307 = set()
set_secids_shortqty_307 = set()
for dict_307_position in list_dicts_307_position:
    secid = dict_307_position['SecurityID']
    netqty = dict_307_position['NetQty']
    if netqty > 0:
        set_secids_longqty_307.add(secid)
    elif netqty < 0:
        set_secids_shortqty_307.add(secid)
    else:
        print(f"{secid}委托数量为0")

# print(f'set_intersection_buy: {set_intersection_buy}')
# print(f'set_intersection_sell: {set_intersection_sell}')


print(f"'307_longqty - 918_longqty': '{len(set_secids_longqty_307 - set_secids_longqty_918)}'")
print(f"'307_shortqty - 918_shortqty': '{len(set_secids_shortqty_307 - set_secids_shortqty_918)}'")
print(f"'918_longqty - 307_longqty': '{len(set_secids_longqty_918 - set_secids_longqty_307)}'")
set_aim = (set_secids_longqty_918 - set_secids_longqty_307) & set_secid_in_position_307
# print(f"set_aim: {set_aim}")


print(f"'918_shortqty - 307_shortqty': '{len(set_secids_shortqty_918 - set_secids_shortqty_307)}'")

set_aim_short = (set_secids_shortqty_307 - set_secids_shortqty_918) & set_secid_in_position_918
print(f'1:{set_aim_short}')

# 多空比
print(f"307 actual: {len(set_secids_longqty_307)} : {len(set_secids_shortqty_307)}")
print(f"918 actual: {len(set_secids_longqty_918)} : {len(set_secids_shortqty_918)}")











