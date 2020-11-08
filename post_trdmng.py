#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20201104T110000

"""
This script is to deal with the post-trading management.

Function:
    1. Generate AutoT0 files:
        1. 192.168.5.8/accounts/python/YZJ/xujie/accounts-307/autot0_holding.csv
        2. 192.168.5.8/accounts/python/YZJ/xujie/accounts-307/autot0_short_selling_quota.csv
    2. upload to ftp

Assumption:
    1. T日处理T-1日的post_trddata

Steps:
    1. 从邮件中获取交割单数据，并更新数据库。

    1. 在T日计算T-1日的PNL:
        1. Input Data:
            模式1: T-1日的持仓, T-1日的交割单; T-1日的融券费用; 行情数据_收盘价
            模式2: 策略开始日至今的交割单, 计算T-2日清算后position, 在结合T-1日交割单计算T-1日PNL。
        2. PNL = 持仓盈亏 + 平仓盈亏 + trdfee + interest
        3. Output Data:

    2. check:
        1. 使用资金明细与交割单对账
        2. 注意融券占用费扣款为1个月1次

Note:
    1. post-trade 的input data为清算后数据, 具体为:
        1. 交割单数据
        2. 估值表数据
    2. trade 的 input data 为资金数据、委托数据、持仓数据， 这个是实时数据
    3. 交割单数据需要使用营业部柜台数据，否则少费用科目

Naming Convention:
    1. jgd: 交割单, 尚未找到标准英文

# todo
    1. 目前假设只有一个账户需要处理，需要扩展到多账户

"""

from pymongo import MongoClient

from globals import Globals


class PostTrdMng:
    def __init__(self):
        self.gl = Globals()
        server_mongo = MongoClient('mongodb://localhost:27017/')
        db_basicinfo = server_mongo['basicinfo']
        self.col_acctinfo = db_basicinfo['acctinfo']
        db_post_trddata = server_mongo['post_trddata']
        self.col_post_trddata = db_post_trddata['rawdata_jgd']

        # 交割单数据下载
        fpath_xlsx_jgd_patchdata =


    def update_rawdata(self):
        # todo 扩展到多账户
        pass


if __name__ == '__main__':
    print('Done')












