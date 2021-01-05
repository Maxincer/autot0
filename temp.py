from pymongo import MongoClient
import pandas as pd
#
# fpath_918_holding = 'D:\data\A_trading_data\A_result\918hao_MS\holding_haitong.xlsx'
#
# df_holding_918_c_haitong = pd.read_excel(
#     fpath_918_holding,
#     skiprows=3,
#     converters={
#         '代码': lambda x: str(x).zfill(6)
#     }
# )
# list_dicts_holding = df_holding_918_c_haitong.to_dict('records')
#
# set_secids_in_918_c_haitong = set()
# for dict_holding in list_dicts_holding:
#     secid = dict_holding['代码']
#     set_secids_in_918_c_haitong.add(secid)
#
# server_mongodb = MongoClient('mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe')
# db_trade_data = server_mongodb['post_trade_data']
# col_post_trade_position = db_trade_data['post_trade_position']
#
# iter_post_trade_position = col_post_trade_position.find({'DataDate': '20201224'})
# set_secids_in_307 = set()
# for dict_post_trade_position in iter_post_trade_position:
#     set_secids_in_307.add(dict_post_trade_position['SecurityID'])
# print(len(set_secids_in_307))
#
#
# print(set_secids_in_307.intersection(set_secids_in_918_c_haitong))
# print(len(set_secids_in_307.intersection(set_secids_in_918_c_haitong)))

df_11 = pd.read_html('鸣石满天星4号每日交割单-20201228.htm', header=0, converters={
    '证券代码': str
})[0]

print(df_11)
print('11')

