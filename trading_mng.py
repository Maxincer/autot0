#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20201126T160000

"""
Function:
    1. exposure monitor
    2. real-time database update

Todo:
    1. 多进程并发下的mongodb 读写
    2. 有限CPU核数下的进程管理
    3. 出于成本考虑， 直接使用源数据中的收盘价
    4. 对于order的side的理解和抽象

Note:
    1. balance: 余额， 在本项目中特指清算后的数据

"""

from datetime import datetime
from threading import Thread
from time import sleep

import pandas as pd
from pymongo import MongoClient

from globals import Globals, STR_TODAY, os


class UpdateTradeRawDataFund(Thread):
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = acctidbymxz

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_basic_info = server_mongodb['basicinfo']
        col_acctinfo = db_basic_info['acctinfo']
        dict_acctinfo = col_acctinfo.find_one({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
        acctidbybroker = dict_acctinfo['AcctIDByBroker']
        data_srctype = dict_acctinfo['DataSourceType']
        list_fpaths_trade_data = [_.strip() for _ in dict_acctinfo['DataFilePath'][1:-1].split(',')]
        fpath_input_csv_margin_account_fund = list_fpaths_trade_data[0].replace('<YYYYMMDD>', self.str_today)
        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_fund = db_trade_data['trade_rawdata_fund']

        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trade_rawdata_fund = []
            if data_srctype in ['hait_ehfz']:
                with open(fpath_input_csv_margin_account_fund) as f:
                    list_datalines_of_file = f.readlines()
                    list_fields = list_datalines_of_file[0].strip().split(',')
                    list_datalines = [_.strip() for _ in list_datalines_of_file[1:]]
                    for dataline in list_datalines:
                        list_data = [str(_).strip() for _ in dataline.split(',')]
                        dict_fields2data = dict(zip(list_fields, list_data))
                        dict_fields2data.update({'DataDate': self.str_today})
                        dict_fields2data.update({'UpdateTime': str_update_time})
                        dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                        list_dicts_trade_rawdata_fund.append(dict_fields2data)
            elif data_srctype in ['huat_matic_tsi']:
                if os.path.exists(fpath_input_csv_margin_account_fund):
                    with open(fpath_input_csv_margin_account_fund) as f:
                        list_datalines_of_file = f.readlines()
                        list_fields = list_datalines_of_file[0].strip().split(',')
                        list_datalines = [_.strip() for _ in list_datalines_of_file[1:]]
                        for dataline in list_datalines:
                            list_data = [str(_).strip() for _ in dataline.split(',')]
                            dict_fields2data = dict(zip(list_fields, list_data))
                            dict_fields2data.update({'DataDate': self.str_today})
                            dict_fields2data.update({'UpdateTime': str_update_time})
                            dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                            if dict_fields2data['fund_account'] == acctidbybroker:
                                list_dicts_trade_rawdata_fund.append(dict_fields2data)
            else:
                raise ValueError('Unknown data source type.')
            col_trade_rawdata_fund.delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
            if list_dicts_trade_rawdata_fund:
                col_trade_rawdata_fund.insert_many(list_dicts_trade_rawdata_fund)
            print('update_trade_rawdata_fund finished, sleep 30s')
            sleep(30)


class UpdateTradeRawDataHolding(Thread):
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = acctidbymxz
        self.gl = gl

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_basic_info = server_mongodb['basicinfo']
        col_acctinfo = db_basic_info['acctinfo']
        dict_acctinfo = col_acctinfo.find_one({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
        acctidbybroker = dict_acctinfo['AcctIDByBroker']
        data_srctype = dict_acctinfo['DataSourceType']
        list_fpaths_trade_data = [_.strip() for _ in dict_acctinfo['DataFilePath'][1:-1].split(',')]

        fpath_input_csv_margin_account_holding = list_fpaths_trade_data[1].replace('<YYYYMMDD>', self.str_today)
        fpath_input_csv_margin_account_holding_last_trddate = list_fpaths_trade_data[1].replace(
            '<YYYYMMDD>', self.gl.str_last_trddate
        )

        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_holding = db_trade_data['trade_rawdata_holding']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trade_rawdata_holding = []
            if data_srctype in ['hait_ehfz']:
                with open(fpath_input_csv_margin_account_holding) as f:
                    list_datalines_of_file = f.readlines()
                    list_fields = list_datalines_of_file[0].strip().split(',')
                    list_datalines = [_.strip() for _ in list_datalines_of_file[1:]]
                    for dataline in list_datalines:
                        list_data = [str(_).strip() for _ in dataline.split(',')]
                        dict_fields2data = dict(zip(list_fields, list_data))
                        dict_fields2data.update({'DataDate': self.str_today})
                        dict_fields2data.update({'UpdateTime': str_update_time})
                        dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                        list_dicts_trade_rawdata_holding.append(dict_fields2data)

            elif data_srctype in ['huat_matic_tsi']:
                if os.path.exists(fpath_input_csv_margin_account_holding):
                    fpath_input_csv_margin_account_holding = fpath_input_csv_margin_account_holding
                else:
                    fpath_input_csv_margin_account_holding = fpath_input_csv_margin_account_holding_last_trddate
                with open(fpath_input_csv_margin_account_holding) as f:
                    list_datalines_of_file = f.readlines()
                    list_fields = list_datalines_of_file[0].strip().split(',')
                    list_datalines = [_.strip() for _ in list_datalines_of_file[1:]]
                    for dataline in list_datalines:
                        list_data = [str(_).strip() for _ in dataline.split(',')]
                        dict_fields2data = dict(zip(list_fields, list_data))
                        dict_fields2data.update({'DataDate': self.str_today})
                        dict_fields2data.update({'UpdateTime': str_update_time})
                        dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                        if dict_fields2data['fund_account'] == acctidbybroker:
                            list_dicts_trade_rawdata_holding.append(dict_fields2data)
            else:
                raise ValueError('Unknown data source type.')

            col_trade_rawdata_holding.delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
            if list_dicts_trade_rawdata_holding:
                col_trade_rawdata_holding.insert_many(list_dicts_trade_rawdata_holding)
            print('update_trade_rawdata_holding finished, sleep 30s')
            sleep(30)


class UpdateTradeRawDataOrder(Thread):
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = acctidbymxz

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_basic_info = server_mongodb['basicinfo']
        col_acctinfo = db_basic_info['acctinfo']
        dict_acctinfo = col_acctinfo.find_one({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
        list_fpaths_trade_data = [_.strip() for _ in dict_acctinfo['DataFilePath'][1:-1].split(',')]
        acctidbybroker = dict_acctinfo['AcctIDByBroker']
        data_srctype = dict_acctinfo['DataSourceType']
        fpath_input_csv_margin_account_order = list_fpaths_trade_data[2].replace('<YYYYMMDD>', self.str_today)
        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_order = db_trade_data['trade_rawdata_order']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trade_rawdata_order = []
            if data_srctype in ['hait_ehfz']:
                with open(fpath_input_csv_margin_account_order) as f:
                    list_datalines_of_file = f.readlines()
                    list_fields = list_datalines_of_file[0].strip().split(',')
                    list_datalines = [_.strip() for _ in list_datalines_of_file[1:]]
                    for dataline in list_datalines:
                        list_data = [str(_).strip() for _ in dataline.split(',')]
                        dict_fields2data = dict(zip(list_fields, list_data))
                        dict_fields2data.update({'DataDate': self.str_today})
                        dict_fields2data.update({'UpdateTime': str_update_time})
                        dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                        list_dicts_trade_rawdata_order.append(dict_fields2data)

            elif data_srctype in ['huat_matic_tsi']:
                if os.path.exists(fpath_input_csv_margin_account_order):
                    with open(fpath_input_csv_margin_account_order) as f:
                        list_datalines_of_file = f.readlines()
                        list_fields = list_datalines_of_file[0].strip().split(',')
                        list_datalines = [_.strip() for _ in list_datalines_of_file[1:]]
                        for dataline in list_datalines:
                            list_data = [str(_).strip() for _ in dataline.split(',')]
                            dict_fields2data = dict(zip(list_fields, list_data))
                            dict_fields2data.update({'DataDate': self.str_today})
                            dict_fields2data.update({'UpdateTime': str_update_time})
                            dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                            if dict_fields2data['fund_account'] == acctidbybroker:
                                list_dicts_trade_rawdata_order.append(dict_fields2data)
            else:
                raise ValueError('Unknown data source type.')

            col_trade_rawdata_order.delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
            if list_dicts_trade_rawdata_order:
                col_trade_rawdata_order.insert_many(list_dicts_trade_rawdata_order)
            print('update_trade_rawdata_order finished, sleep 30s')
            sleep(30)


class UpdateTradeRawDataRQMX(Thread):
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = acctidbymxz
        self.gl = gl

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_basic_info = server_mongodb['basicinfo']
        col_acctinfo = db_basic_info['acctinfo']
        dict_acctinfo = col_acctinfo.find_one({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
        acctidbybroker = dict_acctinfo['AcctIDByBroker']
        data_srctype = dict_acctinfo['DataSourceType']
        list_fpaths_trade_data = [_.strip() for _ in dict_acctinfo['DataFilePath'][1:-1].split(',')]
        fpath_input_csv_margin_account_rqmx = list_fpaths_trade_data[3].replace('<YYYYMMDD>', self.str_today)
        fpath_input_csv_margin_account_rqmx_last_trddate = list_fpaths_trade_data[3].replace('<YYYYMMDD>',
                                                                                             self.gl.str_last_trddate)
        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_rqmx = db_trade_data['trade_rawdata_rqmx']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trade_rawdata_rqmx = []
            if data_srctype in ['huat_matic_tsi']:
                if os.path.exists(fpath_input_csv_margin_account_rqmx):
                    fpath_input_csv_margin_account_rqmx = fpath_input_csv_margin_account_rqmx
                else:
                    fpath_input_csv_margin_account_rqmx = fpath_input_csv_margin_account_rqmx_last_trddate
                with open(fpath_input_csv_margin_account_rqmx) as f:
                    list_datalines_of_file = f.readlines()
                    list_fields = list_datalines_of_file[0].strip().split(',')
                    list_datalines = [_.strip() for _ in list_datalines_of_file[1:]]
                    for dataline in list_datalines:
                        list_data = [str(_).strip() for _ in dataline.split(',')]
                        dict_fields2data = dict(zip(list_fields, list_data))
                        dict_fields2data.update({'DataDate': self.str_today})
                        dict_fields2data.update({'UpdateTime': str_update_time})
                        dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                        if dict_fields2data['fund_account'] == acctidbybroker:
                            list_dicts_trade_rawdata_rqmx.append(dict_fields2data)
            col_trade_rawdata_rqmx.delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
            if list_dicts_trade_rawdata_rqmx:
                col_trade_rawdata_rqmx.insert_many(list_dicts_trade_rawdata_rqmx)
            print('update_trade_rawdata_rqmx finished, sleep 30s')
            sleep(30)


class UpdateTradeRawDataPrivateSecLoan(Thread):
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = acctidbymxz

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_basic_info = server_mongodb['basicinfo']
        col_acctinfo = db_basic_info['acctinfo']
        dict_acctinfo = col_acctinfo.find_one({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
        acctidbybroker = dict_acctinfo['AcctIDByBroker']
        data_srctype = dict_acctinfo['DataSourceType']
        fpath_input_xlsx_private_secloan = (
            'D:/projects/autot0/data/input/trade_data_patch/huat/private_security_loan.xlsx'
        )
        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_private_secloan = db_trade_data['trade_rawdata_private_security_loan']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trade_rawdata_secloan = []
            if data_srctype in ['huat_matic_tsi']:
                df_xlsx_private_security_loan = pd.read_excel(
                    fpath_input_xlsx_private_secloan,
                    dtype={'资产账号': str, '合约数量': float, '合约编号': str},
                    converters={'证券代码': lambda x: str(x).zfill(6)}
                )
                list_dicts_private_secloan = df_xlsx_private_security_loan.to_dict('records')
                for dict_private_secloan in list_dicts_private_secloan:
                    if dict_private_secloan['资产账号'] == acctidbybroker:
                        dict_private_secloan['DataDate'] = self.str_today
                        dict_private_secloan['UpdateTime'] = str_update_time
                        dict_private_secloan['AcctIDByMXZ'] = self.acctidbymxz
                        list_dicts_trade_rawdata_secloan.append(dict_private_secloan)
            col_trade_rawdata_private_secloan.delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
            if list_dicts_trade_rawdata_secloan:
                col_trade_rawdata_private_secloan.insert_many(list_dicts_trade_rawdata_secloan)
            print('update trade_rawdata_private_security_loan finished, sleep 30s')
            sleep(30)


class UpdateTradeFmtDataFund(Thread):
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = acctidbymxz

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )

        db_basic_info = server_mongodb['basicinfo']
        col_acctinfo = db_basic_info['acctinfo']
        dict_acctinfo = col_acctinfo.find_one({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
        data_srctype = dict_acctinfo['DataSourceType']
        accttype = dict_acctinfo['AcctType']
        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_fund = db_trade_data['trade_rawdata_fund']
        col_trade_fmtdata_fund = db_trade_data['trade_fmtdata_fund']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            iter_trade_rawdata_fund = col_trade_rawdata_fund.find(
                {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz}
            )

            list_dicts_trade_fmtdata_fund = []
            if data_srctype in ['hait_ehfz']:
                for dict_trade_rawdata_fund in iter_trade_rawdata_fund:
                    cash_available_for_collateral_trade = float(dict_trade_rawdata_fund['可用余额'])
                    tt_asset = float(dict_trade_rawdata_fund['资产总值'])
                    tt_mv = float(dict_trade_rawdata_fund['证券市值'])
                    cash = tt_asset - tt_mv
                    dict_trade_fmtdata_fund = {
                        'DataDate': self.str_today,
                        'UpdateTime': str_update_time,
                        'AcctIDByMXZ': self.acctidbymxz,
                        'Cash': cash,
                        'CashAvailableForCollateralTrade': cash_available_for_collateral_trade
                    }
                    list_dicts_trade_fmtdata_fund.append(dict_trade_fmtdata_fund)

            elif data_srctype in ['huat_matic_tsi'] and accttype in ['m']:
                for dict_trade_rawdata_fund in iter_trade_rawdata_fund:
                    cash = float(dict_trade_rawdata_fund['fund_asset'])
                    # tt_asset = float(dict_trade_rawdata_fund['assure_asset'])
                    # tt_mv = float(dict_trade_rawdata_fund['market_value'])
                    dict_trade_fmtdata_fund = {
                        'DataDate': self.str_today,
                        'UpdateTime': str_update_time,
                        'AcctIDByMXZ': self.acctidbymxz,
                        'Cash': cash,
                    }
                    list_dicts_trade_fmtdata_fund.append(dict_trade_fmtdata_fund)

            else:
                raise ValueError('Unknown data_srctype.')

            col_trade_fmtdata_fund.delete_many({'DataDate': self.str_today})
            if list_dicts_trade_fmtdata_fund:
                col_trade_fmtdata_fund.insert_many(list_dicts_trade_fmtdata_fund)
            print('update_trade_fmtdata_fund finished, sleep 30s')
            sleep(30)


class UpdateTradeFmtDataHolding(Thread):
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = acctidbymxz

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_basic_info = server_mongodb['basicinfo']
        col_acctinfo = db_basic_info['acctinfo']
        dict_acctinfo = col_acctinfo.find_one({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
        data_srctype = dict_acctinfo['DataSourceType']

        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_holding = db_trade_data['trade_rawdata_holding']
        col_trade_fmtdata_holding = db_trade_data['trade_fmtdata_holding']

        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            iter_trade_rawdata_holding = col_trade_rawdata_holding.find(
                {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz}
            )
            list_dicts_trade_fmtdata_holding = []
            if data_srctype in ['hait_ehfz']:
                for dict_trade_rawdata_holding in iter_trade_rawdata_holding:
                    secid = str(dict_trade_rawdata_holding['证券代码']).zfill(6)
                    dict_exg = {'1': 'SZSE', '2': 'SSE'}
                    secidsrc = dict_exg[dict_trade_rawdata_holding['市场类型']]
                    str_code = f"{secid}.{secidsrc}"
                    sectype = Globals.get_mingshi_sectype_from_code(str_code)
                    longqty = float(dict_trade_rawdata_holding['当前拥股数量'])
                    longqtybalance = float(dict_trade_rawdata_holding['昨日持仓量'])
                    longamt = float(dict_trade_rawdata_holding['证券市值'])

                    dict_trade_fmtdata_holding = {
                        'DataDate': self.str_today,
                        'UpdateTime': str_update_time,
                        'AcctIDByMXZ': self.acctidbymxz,
                        'SecurityID': secid,
                        'SecurityIDSource': secidsrc,
                        'LongQty': longqty,
                        'LongQtyBalance': longqtybalance,
                        'LongAmt': longamt,
                        'SecurityType': sectype,
                        }
                    list_dicts_trade_fmtdata_holding.append(dict_trade_fmtdata_holding)

            elif data_srctype in ['huat_matic_tsi']:
                for dict_trade_rawdata_holding in iter_trade_rawdata_holding:
                    secid = str(dict_trade_rawdata_holding['stock_code']).zfill(6)
                    dict_exg = {'1': 'SSE', '2': 'SZSE'}
                    secidsrc = dict_exg[str(dict_trade_rawdata_holding['exchange_type'])]
                    str_code = f"{secid}.{secidsrc}"
                    sectype = Globals.get_mingshi_sectype_from_code(str_code)
                    longqty = float(dict_trade_rawdata_holding['current_amount'])
                    longqtybalance = float(dict_trade_rawdata_holding['hold_amount'])
                    longamt = float(dict_trade_rawdata_holding['market_value'])

                    dict_trade_fmtdata_holding = {
                        'DataDate': self.str_today,
                        'UpdateTime': str_update_time,
                        'AcctIDByMXZ': self.acctidbymxz,
                        'SecurityID': secid,
                        'SecurityIDSource': secidsrc,
                        'LongQty': longqty,
                        'LongQtyBalance': longqtybalance,
                        'LongAmt': longamt,
                        'SecurityType': sectype,
                        }
                    list_dicts_trade_fmtdata_holding.append(dict_trade_fmtdata_holding)
            else:
                raise ValueError('Unknown data_srctype.')

            col_trade_fmtdata_holding.delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
            if list_dicts_trade_fmtdata_holding:
                col_trade_fmtdata_holding.insert_many(list_dicts_trade_fmtdata_holding)
            print('Update_trade_fmtdata_holding finished, sleep 30s')
            sleep(30)


class UpdateTradeFmtDataOrder(Thread):
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = acctidbymxz

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_basic_info = server_mongodb['basicinfo']
        col_acctinfo = db_basic_info['acctinfo']
        dict_acctinfo = col_acctinfo.find_one({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
        data_srctype = dict_acctinfo['DataSourceType']

        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_order = db_trade_data['trade_rawdata_order']
        col_trade_fmtdata_order = db_trade_data['trade_fmtdata_order']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            iter_trade_rawdata_order = col_trade_rawdata_order.find(
                {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz}
            )
            # 确认side:
            # todo fix 协议中只有3个方向： 1. 1 = buy, 2. 2 = sell, 5 = Sell Short, 字符串: str
            # todo 但国内在交易环节可以提供这部分信息： BC: 买券还券 和 现券还券 B: 担保品买入
            # todo 委托状态OrdStatus: 每家券商不一样
            list_dicts_trade_fmtdata_order = []
            if data_srctype in ['hait_xtpb']:
                for dict_trade_rawdata_order in iter_trade_rawdata_order:
                    secid = dict_trade_rawdata_order['证券代码']
                    symbol = dict_trade_rawdata_order['证券名称']
                    # todo 已知买卖标记字段值包括： 现券还券划拨，限价担保品买入，限价担保品卖出，限价融券卖出
                    trade_mark = dict_trade_rawdata_order['买卖标记']
                    exchange = dict_trade_rawdata_order['交易市场']
                    secidsrc = {'上证所': 'SSE', '深交所': 'SZSE'}[exchange]
                    contract_id = dict_trade_rawdata_order['合同编号']
                    lastqty = float(dict_trade_rawdata_order['成交数量'])
                    lastpx = float(dict_trade_rawdata_order['成交均价'])
                    if '买' in trade_mark or '卖' in trade_mark:  # 交易事项
                        dict_trade_mark2str_side = {'限价担保品买入': '1', '限价担保品卖出': '2', '限价融券卖出': '5'}
                        side = dict_trade_mark2str_side[trade_mark]
                    elif '划拨' in trade_mark:  # todo 抽象现券还券划转和买券还券划转， 目前只看到了现券还券划转
                        side = trade_mark
                    else:
                        raise ValueError('Unknown trade mark.')
                    order_time = dict_trade_rawdata_order['委托时间'].replace(':', '')
                    dict_trade_fmtdata_order = {
                        'DataDate': self.str_today,
                        'UpdateTime': str_update_time,
                        'AcctIDByMXZ': self.acctidbymxz,
                        'SecurityID': secid,
                        'SecurityIDSource': secidsrc,
                        'Symbol': symbol,
                        'ContractID': contract_id,
                        'LastQty': lastqty,
                        'LastPx': lastpx,
                        'Side': side,
                        'OrderTime': order_time,
                    }
                    list_dicts_trade_fmtdata_order.append(dict_trade_fmtdata_order)

            elif data_srctype in ['hait_ehfz']:
                for dict_trade_rawdata_order in iter_trade_rawdata_order:
                    secid = str(dict_trade_rawdata_order['证券代码']).zfill(6)
                    symbol = dict_trade_rawdata_order['证券名称']
                    trade_mark = dict_trade_rawdata_order['@交易类型']
                    exg = dict_trade_rawdata_order['市场类型']
                    secidsrc = {'1': 'SSE', '2': 'SZSE'}[exg]
                    cumqty = float(dict_trade_rawdata_order['成交数量'])
                    avgpx = float(dict_trade_rawdata_order['成交价格'])
                    # 海通的style:
                    # ①待撤：撤单指令还未报到场内。
                    # ②正撤：撤单指令已送达公司，正在等待处理，此时不能确定是否已进场；
                    # ③部撤：下单指令中的一部份数量已被撤消；
                    # ④已撤：委托指令全部被撤消；
                    # ⑤未报：下单指令还未送入数据处理；
                    # ⑥待报：下单指令还未被数据处理报到场内；
                    # ⑦正报：下单指令已送达公司，正在等待处理，此时不能确定是否已进场；
                    # ⑧已报：已收到下单反馈；
                    # ⑨部成：下单指令部份成交；
                    # ⑩已成：下单指令全部成交；
                    # ⑾撤废：撤单废单，表示撤单指令失败，原因可能是被撤的下单指令已经成交了或场内无法找到这条下单记录；
                    # ⑿废单：交易所反馈的信息，表示该定单无效。
                    ordstatus = dict_trade_rawdata_order['@委托状态']
                    if trade_mark in ['1', '2']:
                        side = trade_mark
                    elif trade_mark in ['12']:
                        side = '5'
                    elif trade_mark in ['15', '0']:
                        side = 'XQHQ'
                    else:
                        raise ValueError('Unknown trade mark.')
                    order_time = dict_trade_rawdata_order['委托时间'].replace(':', '')
                    dict_trade_fmtdata_order = {
                        'DataDate': self.str_today,
                        'UpdateTime': str_update_time,
                        'AcctIDByMXZ': self.acctidbymxz,
                        'SecurityID': secid,
                        'SecurityIDSource': secidsrc,
                        'Symbol': symbol,
                        'CumQty': cumqty,
                        'AvgPx': avgpx,
                        'Side': side,
                        'OrdStatus': ordstatus,
                        'OrderTime': order_time,
                    }
                    list_dicts_trade_fmtdata_order.append(dict_trade_fmtdata_order)

            elif data_srctype in ['huat_matic_tsi']:
                pass

            else:
                raise ValueError('Unknown data_srctype.')
            col_trade_fmtdata_order.delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
            if list_dicts_trade_fmtdata_order:
                col_trade_fmtdata_order.insert_many(list_dicts_trade_fmtdata_order)
            print('update_trade_fmtdata_order finished, sleep 30s')
            sleep(30)


class UpdateTradeFmtDataPublicSecLoan(Thread):
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = acctidbymxz

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_basic_info = server_mongodb['basicinfo']
        col_acctinfo = db_basic_info['acctinfo']
        dict_acctinfo = col_acctinfo.find_one({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
        data_srctype = dict_acctinfo['DataSourceType']

        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_rqmx = db_trade_data['trade_rawdata_rqmx']
        col_trade_fmtdata_public_secloan = db_trade_data['trade_fmtdata_public_security_loan']

        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            iter_trade_rawdata_rqmx = col_trade_rawdata_rqmx.find(
                {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz}
            )
            list_dicts_trade_fmtdata_public_secloan = []
            if data_srctype in ['hait_ehfz']:
                pass

            elif data_srctype in ['huat_matic_tsi']:
                for dict_trade_rawdata_rqmx in iter_trade_rawdata_rqmx:
                    secloan_source = int(dict_trade_rawdata_rqmx['compact_source'])
                    if not secloan_source:
                        contract_id = str(dict_trade_rawdata_rqmx['compact_id'])
                        secid = str(dict_trade_rawdata_rqmx['stock_code']).zfill(6)
                        expiration_days = 180
                        maturity_date = str(dict_trade_rawdata_rqmx['ret_end_date'])
                        open_date = str(dict_trade_rawdata_rqmx['open_date'])
                        qty = float(dict_trade_rawdata_rqmx['real_compact_amount'])
                        ir = float(dict_trade_rawdata_rqmx['year_rate'])

                        dict_trade_fmtdata_public_secloan = {
                            'DataDate': self.str_today,
                            'UpdateTime': str_update_time,
                            'AcctIDByMXZ': self.acctidbymxz,
                            'ContractID': contract_id,
                            'SecurityID': secid,
                            'ExpirationDays': expiration_days,
                            'MaturityDate': maturity_date,
                            'QtyToBeChargedInterest': qty,
                            'Rate': ir,
                            'StartDate': open_date,
                            }
                        list_dicts_trade_fmtdata_public_secloan.append(dict_trade_fmtdata_public_secloan)
            else:
                raise ValueError('Unknown data_srctype.')

            col_trade_fmtdata_public_secloan.delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
            if list_dicts_trade_fmtdata_public_secloan:
                col_trade_fmtdata_public_secloan.insert_many(list_dicts_trade_fmtdata_public_secloan)
            print('Update_trade_fmtdata_holding finished, sleep 30s')
            sleep(30)


class UpdateTradeFmtDataPrivateSecLoan(Thread):
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = acctidbymxz

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_basic_info = server_mongodb['basicinfo']
        col_acctinfo = db_basic_info['acctinfo']
        dict_acctinfo = col_acctinfo.find_one({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
        data_srctype = dict_acctinfo['DataSourceType']

        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_private_secloan = db_trade_data['trade_rawdata_private_security_loan']
        col_trade_fmtdata_private_secloan = db_trade_data['trade_fmtdata_private_security_loan']

        while True:
            str_update_time = datetime.now().strftime('%H%M%S')

            list_dicts_trade_fmtdata_private_secloan = []
            if data_srctype in ['hait_ehfz']:
                pass

            elif data_srctype in ['huat_matic_tsi']:
                for dict_trade_rawdata_private_secloan in col_trade_rawdata_private_secloan.find(
                        {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz}
                ):
                    contract_id = str(dict_trade_rawdata_private_secloan['合约编号'])
                    secid = str(dict_trade_rawdata_private_secloan['证券代码']).zfill(6)
                    expiration_days = int(dict_trade_rawdata_private_secloan['剩余期限'])
                    maturity_date = str(dict_trade_rawdata_private_secloan['到期日'].replace('-', ''))
                    qty = float(dict_trade_rawdata_private_secloan['合约数量'])
                    if maturity_date == self.str_today:
                        qty = 0
                    open_date = str(dict_trade_rawdata_private_secloan['起始日'])
                    ir = float(dict_trade_rawdata_private_secloan['占用利率'])
                    if ir > 1:
                        ir = ir / 10

                    dict_trade_fmtdata_private_secloan = {
                        'DataDate': self.str_today,
                        'UpdateTime': str_update_time,
                        'AcctIDByMXZ': self.acctidbymxz,
                        'ContractID': contract_id,
                        'SecurityID': secid,
                        'ExpirationDays': expiration_days,
                        'MaturityDate': maturity_date,
                        'QtyToBeChargedInterest': qty,
                        'Rate': ir,
                        'StartDate': open_date,
                        }
                    list_dicts_trade_fmtdata_private_secloan.append(dict_trade_fmtdata_private_secloan)
            else:
                raise ValueError('Unknown data_srctype.')

            col_trade_fmtdata_private_secloan.delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
            if list_dicts_trade_fmtdata_private_secloan:
                col_trade_fmtdata_private_secloan.insert_many(list_dicts_trade_fmtdata_private_secloan)
            print('Update_trade_fmtdata_private_secloan finished, sleep 30s')
            sleep(30)


class UpdateTradeSSQuotaFromSecLoan(Thread):
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = acctidbymxz
        self.gl = gl

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_trade_data = server_mongodb['trade_data']
        col_trade_fmtdata_public_secloan = db_trade_data['trade_fmtdata_public_security_loan']
        col_trade_fmtdata_private_secloan = db_trade_data['trade_fmtdata_private_security_loan']
        col_trade_position = db_trade_data['trade_position']
        col_trade_ssquota_from_secloan = db_trade_data['trade_ssquota_from_security_loan']

        set_secids_nic = set()
        for dict_excluded_secids in self.gl.col_posttrd_fmtdata_excluded_secids.find(
                {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.acctidbymxz, 'Composite': 'NotInComposite'}
        ):
            set_secids_nic.add(dict_excluded_secids['SecurityID'])

        while True:
            set_secid_from_public_secloan = set()
            set_secid_from_private_secloan = set()
            iter_col_trade_fmtdata_public_secloan = col_trade_fmtdata_public_secloan.find(
                {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz}
            )
            iter_col_trade_fmtdata_private_secloan = col_trade_fmtdata_private_secloan.find(
                {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz}
            )
            for dict_trade_fmtdata_public_secloan in iter_col_trade_fmtdata_public_secloan:
                secid = dict_trade_fmtdata_public_secloan['SecurityID']
                set_secid_from_public_secloan.add(secid)

            for dict_trade_fmtdata_private_secloan in iter_col_trade_fmtdata_private_secloan:
                secid = dict_trade_fmtdata_private_secloan['SecurityID']
                set_secid_from_private_secloan.add(secid)

            set_secid_in_ssquota = set_secid_from_public_secloan | set_secid_from_private_secloan

            # 将 secloan_from_public_secloan 表中的QtyToBeChargedInterest数据按照 股票代码 汇总
            dict_secid2ssquota_from_public_secloan = {}
            for secid_in_ssquota in set_secid_in_ssquota:
                ssquota_from_public_secloan = 0
                for dict_trade_fmtdata_public_secloan in col_trade_fmtdata_public_secloan.find(
                        {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz, 'SecurityID': secid_in_ssquota}
                ):
                    ssquota_from_public_secloan += dict_trade_fmtdata_public_secloan['QtyToBeChargedInterest']
                dict_secid2ssquota_from_public_secloan[secid_in_ssquota] = ssquota_from_public_secloan

            dict_secid2ssquota_from_private_secloan = {}
            for secid_in_ssquota in set_secid_in_ssquota:
                ssquota_from_private_secloan = 0
                for dict_trade_fmtdata_private_secloan in col_trade_fmtdata_private_secloan.find(
                        {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz, 'SecurityID': secid_in_ssquota}
                ):
                    ssquota_from_private_secloan += dict_trade_fmtdata_private_secloan['QtyToBeChargedInterest']
                dict_secid2ssquota_from_private_secloan[secid_in_ssquota] = ssquota_from_private_secloan

            # # 计算ssquota_from_public_secpool 与 ssquota_from_private_secpool
            list_dicts_trade_ssquota_from_secloan = []
            for secid_in_ssquota in set_secid_in_ssquota:
                # if secid_in_ssquota in self.gl.dict_secid2composite:
                #     cpssrc = self.gl.dict_secid2composite[secid_in_ssquota]
                # else:
                #     cpssrc = 'AutoT0'
                if secid_in_ssquota in set_secids_nic:
                    cpssrc = 'NotInComposite'
                else:
                    cpssrc = 'AutoT0'
                ssquota_from_public_secloan = dict_secid2ssquota_from_public_secloan[secid_in_ssquota]
                ssquota_from_private_secloan = dict_secid2ssquota_from_private_secloan[secid_in_ssquota]
                dict_trade_position = col_trade_position.find_one(
                    {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz, 'SecurityID': secid_in_ssquota}
                )
                if dict_trade_position:
                    shortqty = dict_trade_position['ShortQty']
                else:
                    shortqty = 0

                ssquota = ssquota_from_public_secloan + ssquota_from_private_secloan
                str_update_time = datetime.now().strftime('%H%M%S')
                dict_trade_fmtdata_ssquota_from_secloan = {
                    'DataDate': self.str_today,
                    'AcctIDByMXZ': self.acctidbymxz,
                    'UpdateTime': str_update_time,
                    'SecurityID': secid_in_ssquota,
                    'SSQuota': ssquota,
                    'ShortQty': shortqty,
                    'SSQuotaFromPublicSecPool': ssquota_from_public_secloan,
                    'SSQuotaFromPrivateSecPool': ssquota_from_private_secloan,
                    'AvailableSSQuota': ssquota - shortqty,
                    'CompositeSource': cpssrc,
                }
                list_dicts_trade_ssquota_from_secloan.append(dict_trade_fmtdata_ssquota_from_secloan)

            col_trade_ssquota_from_secloan.delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz})
            if list_dicts_trade_ssquota_from_secloan:
                col_trade_ssquota_from_secloan.insert_many(list_dicts_trade_ssquota_from_secloan)
            print('Update_trade_ssquota_from_secloan finished, sleep 10s')
            sleep(10)


class UpdateTradePosition(Thread):
    """
    1. 计算标准格式的position
    2. LongQty 与 ShortQty 分别计算
    3. Assumption
        1. 融券空头，受昨日余额和今日融券卖出额影响，并由此计算
        2. 其他直接读取
        3. 划转事项客观准确的执行时间为盘后清算时，在盘中无法得到这部分数据， 但将划转事项视为立即生效合理。
    """
    def __init__(self, gl, acctidbymxz):
        super().__init__()
        self.gl = gl
        self.acctidbymxz = acctidbymxz

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_basicinfo = server_mongodb['basicinfo']
        col_acctinfo = db_basicinfo['acctinfo']
        dict_acctinfo = col_acctinfo.find_one({'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.acctidbymxz})
        data_srctype = dict_acctinfo['DataSourceType']
        db_trade_data = server_mongodb['trade_data']
        db_posttrd_data = server_mongodb['post_trade_data']
        col_posttrd_position = db_posttrd_data['post_trade_position']
        col_trade_position = db_trade_data['trade_position']
        col_trade_fmtdata_order = db_trade_data['trade_fmtdata_order']
        col_trade_fmtdata_holding = db_trade_data['trade_fmtdata_holding']
        col_trade_rawdata_rqmx = db_trade_data['trade_rawdata_rqmx']

        while True:
            # 获取position的secid全集
            # # 昨夜清算后持仓col_posttrd_position
            list_dicts_trade_position = []
            if data_srctype in ['hait_ehfz']:  # 方舟的short position 不能实时更新
                set_secids_in_position_predate = set()
                for dict_posttrd_position in col_posttrd_position.find(
                    {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.acctidbymxz}
                ):
                    secid_in_predate_position = dict_posttrd_position['SecurityID']
                    set_secids_in_position_predate.add(secid_in_predate_position)

                # # 今日交易记录中的所有secid: todo 此处使用hait_xtpb格式(视野受限，假设划款事项与交易的完全记录都在同一张表里)：
                list_dicts_trade_fmtdata_order = list(
                    col_trade_fmtdata_order.find({'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.acctidbymxz})
                )
                set_secids_in_trade_fmtdata_order = set()
                for dict_trade_fmtdata_order in list_dicts_trade_fmtdata_order:
                    secid_in_trade_order = dict_trade_fmtdata_order['SecurityID']
                    set_secids_in_trade_fmtdata_order.add(secid_in_trade_order)

                # # 获取trade_data_holding的所有secid
                set_secids_in_trade_fmtdata_holding = set()
                for dict_trade_fmtdata_holding in col_trade_fmtdata_holding.find(
                    {'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.acctidbymxz}
                ):
                    secid = dict_trade_fmtdata_holding['SecurityID']
                    set_secids_in_trade_fmtdata_holding.add(secid)

                # # 并集: 清算后持仓 + 交易中的持仓 + 当日交易
                set_secids_in_trade_position = (
                    set_secids_in_position_predate | set_secids_in_trade_fmtdata_order | set_secids_in_trade_fmtdata_holding
                )
                list_secids_in_trade_position = list(set_secids_in_trade_position)
                list_secids_in_trade_position.sort()
                for secid in list_secids_in_trade_position:
                    # 计算longqty 取当前持仓： todo 注意 hait_xtpb 的 当日拥股字段 为扣除划转的余额
                    longqty = 0
                    for dict_trade_fmtdata_holding in col_trade_fmtdata_holding.find(
                        {'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.acctidbymxz, 'SecurityID': secid}
                    ):
                        longqty += dict_trade_fmtdata_holding['LongQty']

                    # 计算shortqty： 昨日shortqty余额 + 今融券卖出 - 还券(现券还券，买入还券)
                    dict_posttrd_position_last_trddate = col_posttrd_position.find_one(
                        {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.acctidbymxz, 'SecurityID': secid}
                    )
                    if dict_posttrd_position_last_trddate:
                        shortqty_last_trddate = dict_posttrd_position_last_trddate['ShortQty']
                    else:
                        shortqty_last_trddate = 0

                    shortqty_delta_today = 0
                    longqty_delta_from_xqhq = 0
                    for dict_trade_fmtdata_order in list_dicts_trade_fmtdata_order:
                        if dict_trade_fmtdata_order['SecurityID'] == secid and dict_trade_fmtdata_order['Side'] in ['5']:
                            shortqty_delta_today += dict_trade_fmtdata_order['CumQty']

                        # todo 需要添加买券还券

                        if (
                                dict_trade_fmtdata_order['SecurityID'] == secid
                                and dict_trade_fmtdata_order['Side'] in ['XQHQ']
                                and dict_trade_fmtdata_order['OrdStatus'] in ['8', '10']  # xtpb是2和8
                        ):
                            shortqty_delta_today += -dict_trade_fmtdata_order['CumQty']
                            longqty_delta_from_xqhq += dict_trade_fmtdata_order['CumQty']

                    shortqty = shortqty_last_trddate + shortqty_delta_today
                    longqty = longqty - longqty_delta_from_xqhq
                    str_update_time = datetime.now().strftime('%H%M%S')
                    dict_trade_position = {
                        'DataDate': self.gl.str_today,
                        'UpdateTime': str_update_time,
                        'AcctIDByMXZ': self.acctidbymxz,
                        'SecurityID': secid,
                        'LongQty': longqty,
                        'ShortQty': shortqty,
                        'NetQty': longqty - shortqty,
                    }
                    list_dicts_trade_position.append(dict_trade_position)

            elif data_srctype in ['huat_matic_tsi']:
                iter_posttrd_position_last_trddate = col_posttrd_position.find(
                    {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.acctidbymxz}
                )
                set_secids_in_position_predate = set()
                for dict_posttrd_position in iter_posttrd_position_last_trddate:
                    secid_in_predate_position = dict_posttrd_position['SecurityID']
                    set_secids_in_position_predate.add(secid_in_predate_position)

                # # 今日rqmx中的所有secid:
                list_dicts_trade_rawdata_rqmx = list(
                    col_trade_rawdata_rqmx.find({'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.acctidbymxz})
                )
                set_secids_in_trade_rawdata_rqmx = set()
                for dict_trade_rawdata_rqmx in list_dicts_trade_rawdata_rqmx:
                    secid_in_rawdata_rqmx = str(dict_trade_rawdata_rqmx['stock_code'])  # todo fmt_rqmx
                    set_secids_in_trade_rawdata_rqmx.add(secid_in_rawdata_rqmx)

                # # 获取trade_fmtdata_holding的所有secid
                set_secids_in_trade_fmtdata_holding = set()
                for dict_trade_fmtdata_holding in col_trade_fmtdata_holding.find(
                    {'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.acctidbymxz}
                ):
                    secid = dict_trade_fmtdata_holding['SecurityID']
                    set_secids_in_trade_fmtdata_holding.add(secid)

                # # 并集: 清算后持仓 + 交易中的持仓 + 当日交易
                set_secids_in_trade_position = (
                    set_secids_in_position_predate
                    | set_secids_in_trade_rawdata_rqmx
                    | set_secids_in_trade_fmtdata_holding
                )
                list_secids_in_trade_position = list(set_secids_in_trade_position)
                list_secids_in_trade_position.sort()
                for secid in list_secids_in_trade_position:
                    # 计算longqty 取当前持仓：
                    longqty = 0
                    for dict_trade_fmtdata_holding in col_trade_fmtdata_holding.find(
                        {'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.acctidbymxz, 'SecurityID': secid}
                    ):
                        longqty += dict_trade_fmtdata_holding['LongQty']

                    # 计算shortqty： rqmx 汇总
                    shortqty = 0
                    for dict_trade_rawdata_rqmx in list_dicts_trade_rawdata_rqmx:  # todo fmt_rqmx
                        if str(dict_trade_rawdata_rqmx['stock_code']) == secid:
                            shortqty += float(dict_trade_rawdata_rqmx['real_compact_amount'])

                    str_update_time = datetime.now().strftime('%H%M%S')
                    dict_trade_position = {
                        'DataDate': self.gl.str_today,
                        'UpdateTime': str_update_time,
                        'AcctIDByMXZ': self.acctidbymxz,
                        'SecurityID': secid,
                        'LongQty': longqty,
                        'ShortQty': shortqty,
                        'NetQty': longqty - shortqty,
                    }
                    list_dicts_trade_position.append(dict_trade_position)

            else:
                raise ValueError('Unknown data source type.')

            col_trade_position.delete_many({'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.acctidbymxz})
            if list_dicts_trade_position:
                col_trade_position.insert_many(list_dicts_trade_position)
            print('Update_trade_position finished, sleep 10s')
            sleep(10)


class UpdateTradeDataBase:
    def __init__(self, str_trddate=STR_TODAY):
        self.gl = Globals(str_today=str_trddate)
        self.list_acctidsbymxz = ['8111_m_huat_9239', '3004_m_hait_8866']

    def update_raw_data_and_fmtdata(self, acctidbymxz):
        # rawdata
        t1_update_trade_rawdata_fund = UpdateTradeRawDataFund(self.gl, acctidbymxz)
        t2_update_trade_rawdata_holding = UpdateTradeRawDataHolding(self.gl, acctidbymxz)
        t3_update_trade_rawdata_order = UpdateTradeRawDataOrder(self.gl, acctidbymxz)
        t4_update_trade_rawdata_rqmx = UpdateTradeRawDataRQMX(self.gl, acctidbymxz)
        t5_update_trade_rawdata_private_secloan = UpdateTradeRawDataPrivateSecLoan(self.gl, acctidbymxz)
        t1_update_trade_rawdata_fund.start()
        t2_update_trade_rawdata_holding.start()
        t3_update_trade_rawdata_order.start()
        t4_update_trade_rawdata_rqmx.start()
        t5_update_trade_rawdata_private_secloan.start()

        # # fmtdata
        t5_update_trade_fmtdata_fund = UpdateTradeFmtDataFund(self.gl, acctidbymxz)
        t6_update_trade_fmtdata_holding = UpdateTradeFmtDataHolding(self.gl, acctidbymxz)
        t7_update_trade_fmtdata_order = UpdateTradeFmtDataOrder(self.gl, acctidbymxz)
        t8_update_trade_fmtdata_public_secloan = UpdateTradeFmtDataPublicSecLoan(self.gl, acctidbymxz)
        t9_update_trade_fmtdata_private_secloan = UpdateTradeFmtDataPrivateSecLoan(self.gl, acctidbymxz)

        t5_update_trade_fmtdata_fund.start()
        t6_update_trade_fmtdata_holding.start()
        t7_update_trade_fmtdata_order.start()
        t8_update_trade_fmtdata_public_secloan.start()
        t9_update_trade_fmtdata_private_secloan.start()

        # # business data
        t8_update_trade_position = UpdateTradePosition(self.gl, acctidbymxz)
        t8_update_trade_position.start()
        t9_update_trade_ssquota_from_secloan = UpdateTradeSSQuotaFromSecLoan(self.gl, acctidbymxz)
        t9_update_trade_ssquota_from_secloan.start()

    def run(self):
        for acctidbymxz in self.list_acctidsbymxz:
            self.update_raw_data_and_fmtdata(acctidbymxz)


if __name__ == '__main__':
    task = UpdateTradeDataBase()
    task.run()











