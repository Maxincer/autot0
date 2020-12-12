import pandas as pd
from pymongo import MongoClient

server_mongodb = MongoClient('mongodb://localhost:27017/')
col_ = server_mongodb['post_trade_data']['post_trade_position']
list_ = list(col_.find({'DataDate': '20201208'}))
df_ = pd.DataFrame(list_)
df_.to_csv('20201208.csv', encoding='ansi')

list_ = list(col_.find({'DataDate': '20201209'}))
df_ = pd.DataFrame(list_)
df_.to_csv('20201209.csv', encoding='ansi')



