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
    1. T日处理T-1日的post_trddata.
    2. 本项目中约定 T 即为 gl.str_today

Steps:
    1. 维护交割单数据库 - 真准完
        1. Mode1, 全量写入。
        2. Mode2, 增量写入。
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
    4. 邮件数据的处理，一定是先下载为文件， 后读取处理

Naming Convention:
    1. jgd: 交割单, 尚未找到标准英文

# todo
    1. 目前假设只有一个账户需要处理，需要扩展到多账户

"""
import os

import pandas as pd
from pymongo import MongoClient
from WindPy import w

from globals import Globals


class PostTrdMng:
    def __init__(self):
        self.gl = Globals()
        server_mongo = MongoClient('mongodb://localhost:27017/')
        db_basicinfo = server_mongo['basicinfo']
        self.col_acctinfo = db_basicinfo['acctinfo']
        db_post_trddata = server_mongo['post_trddata']
        self.col_post_trddata = db_post_trddata['rawdata_jgd']
        self.str_prdalias = '鸣石满天星7号'
        # 交割单数据下载
        fpath_xlsx_jgd_20201019_20201104_patchdata = (
            f"/data/input/post_trddata/patch_data/{self.str_prdalias}7号交割单20201019-20201104.xlsx"
        )
        self.list_fpaths_jgd_patchdata = [fpath_xlsx_jgd_20201019_20201104_patchdata]
        self.str_email_subject = (
            f"{self.str_prdalias}每日交割单-{self.gl.str_today}",  # 邮件主题中日期为20201106, 为20201105的成交数据
        )
        self.fpath_xlsx_jgd_raw_data = (
            f"data/input/post_trddata/{self.gl.str_last_trddate}/{self.str_prdalias}每日交割单-{self.gl.str_last_trddate}"
        )
        if not os.path.exists(f"data/input/post_trddata/{self.gl.str_last_trddate}"):
            os.makedirs(f"data/input/post_trddata/{self.gl.str_last_trddate}")

    def dld_emails(self, str_date):
        # 下载邮件
        if not os.path.exists(self.fpath_xlsx_jgd_raw_data):
            self.gl.update_attachments_from_email(
                self.str_email_subject, self.fpath_xlsx_jgd_raw_data
            )

    def update_jgddata_under_complete_algo(self, str_startdate, str_enddate):
        """
        全量算法:
        1. 将能获取到的所有交割单放在一堆，按照交易日期列表进行遍历，逐日update到mongodb中
        2. todo 待确认，关键字为流水号
        :param str_startdate:
        :param str_enddate:
        :return:
        """
        list_str_trddates = self.gl.get_list_str_trddate(str_startdate, str_enddate)
        # 读取交割单数据
        df_xlsx_jgd_rawdata = pd.DataFrame()
        for fpath_jgd_patchdata in self.list_fpaths_jgd_patchdata:
            df_xlsx_jgd_rawdata = pd.read_excel(
                self.fpath_xlsx_jgd_raw_data,
                skiprows=1,
                dtype={
                    '成交日期': str,
                    '成交时间': str,
                    '客户号': str,
                    '证券账户': str,
                    '证券代码': str,
                    '成交数量': float,
                    '证券余额': float,
                    '成交价格': float,
                    '成交金额': float,
                    '实际收费': float,
                    '资金余额': float,
                    '成交净额': float,
                    '结算价': float,
                    '清算周期': str,
                    '报盘时间': str,
                    '发生日期': str,
                    '流水号': str,
                    '成交编号': str,
                    '经手费': float,
                    '证管费': float,
                    '佣金': float,
                    '印花税': float,
                    '过户费': float,
                    '标准收费': float,
                    '合约号': str
                }
            )
            df_xlsx_jgd_rawdata.append(df_xlsx_jgd_rawdata)

        # todo 以此为基础，逐日添加逐日数据







if __name__ == '__main__':
    print('Done')












