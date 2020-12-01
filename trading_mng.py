#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20201126T160000

"""
Function:
    1. Exposure Monitor
"""

from datetime import datetime

from globals import Globals, STR_TODAY
import json
from multiprocessing import Process
from pymongo import MongoClient

from time import sleep


class UpdateTradingRawDataFund(Process):
    def __init__(self, gl):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = gl.acctidbymxz
        self.fpath_input_csv_margin_account_fund = gl.fpath_input_csv_margin_account_fund

    def run(self):
        server_mongodb = MongoClient('mongodb://localhost:27017/')
        db_trading_data = server_mongodb['trading_data']
        col_trading_rawdata_fund = db_trading_data['trading_rawdata_fund']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trading_rawdata_fund = []
            with open(self.fpath_input_csv_margin_account_fund) as f:
                list_datalines = f.readlines()
                list_fields = list_datalines[0].strip().split(',')
                for dataline in list_datalines[1:]:
                    list_data = dataline.split(',')
                    dict_fields2data = dict(zip(list_fields, list_data))
                    dict_fields2data.update({'DataDate': self.str_today})
                    dict_fields2data.update({'UpdateTime': str_update_time})
                    dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                    list_dicts_trading_rawdata_fund.append(dict_fields2data)
            col_trading_rawdata_fund.delete_many({'DataDate': self.str_today})
            col_trading_rawdata_fund.insert_many(list_dicts_trading_rawdata_fund)
            print('update_trading_rawdata_fund finished, sleep 30s')
            sleep(30)


class UpdateTradingRawDataHolding(Process):
    def __init__(self, gl):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = gl.acctidbymxz
        self.fpath_input_csv_margin_account_holding = gl.fpath_input_csv_margin_account_holding

    def run(self):
        server_mongodb = MongoClient('mongodb://localhost:27017/')
        db_trading_data = server_mongodb['trading_data']
        col_trading_rawdata_holding = db_trading_data['trading_rawdata_holding']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trading_rawdata_holding = []
            with open(self.fpath_input_csv_margin_account_holding) as f:
                list_datalines = f.readlines()
                list_fields = list_datalines[0].strip().split(',')
                for dataline in list_datalines[1:]:
                    list_data = dataline.split(',')
                    dict_fields2data = dict(zip(list_fields, list_data))
                    dict_fields2data.update({'DataDate': self.str_today})
                    dict_fields2data.update({'UpdateTime': str_update_time})
                    dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                    list_dicts_trading_rawdata_holding.append(dict_fields2data)
            col_trading_rawdata_holding.delete_many({'DataDate': self.str_today})
            col_trading_rawdata_holding.insert_many(list_dicts_trading_rawdata_holding)
            print('update_trading_rawdata_holding finished, sleep 30s')
            sleep(30)


class UpdateTradingRawDataEntrust(Process):
    def __init__(self, gl):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = gl.acctidbymxz
        self.fpath_input_csv_margin_account_entrust = gl.fpath_input_csv_margin_account_entrust

    def run(self):
        server_mongodb = MongoClient('mongodb://localhost:27017/')
        db_trading_data = server_mongodb['trading_data']
        col_trading_rawdata_entrust = db_trading_data['trading_rawdata_entrust']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trading_rawdata_entrust = []
            with open(self.fpath_input_csv_margin_account_entrust) as f:
                list_datalines = f.readlines()
                list_fields = list_datalines[0].strip().split(',')
                for dataline in list_datalines[1:]:
                    list_data = dataline.split(',')
                    dict_fields2data = dict(zip(list_fields, list_data))
                    dict_fields2data.update({'DataDate': self.str_today})
                    dict_fields2data.update({'UpdateTime': str_update_time})
                    dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                    list_dicts_trading_rawdata_entrust.append(dict_fields2data)
            col_trading_rawdata_entrust.delete_many({'DataDate': self.str_today})
            col_trading_rawdata_entrust.insert_many(list_dicts_trading_rawdata_entrust)
            print('update_trading_rawdata_entrust finished, sleep 30s')
            sleep(30)


class UpdateTradingRawDataSecLoan(Process):
    def __init__(self, gl):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = gl.acctidbymxz
        self.fpath_input_csv_margin_account_secloan = gl.fpath_input_csv_margin_account_secloan

    def run(self):
        server_mongodb = MongoClient('mongodb://localhost:27017/')
        db_trading_data = server_mongodb['trading_data']
        col_trading_rawdata_secloan = db_trading_data['trading_rawdata_secloan']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trading_rawdata_secloan = []
            with open(self.fpath_input_csv_margin_account_secloan) as f:
                list_datalines = f.readlines()
                list_fields = list_datalines[0].strip().split(',')
                for dataline in list_datalines[1:]:
                    list_data = dataline.split(',')
                    dict_fields2data = dict(zip(list_fields, list_data))
                    dict_fields2data.update({'DataDate': self.str_today})
                    dict_fields2data.update({'UpdateTime': str_update_time})
                    dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                    list_dicts_trading_rawdata_secloan.append(dict_fields2data)
            col_trading_rawdata_secloan.delete_many({'DataDate': self.str_today})
            col_trading_rawdata_secloan.insert_many(list_dicts_trading_rawdata_secloan)
            print('update_trading_rawdata_secloan finished, sleep 30s')
            sleep(30)


class TrdMonitor:
    def __init__(self, str_trddate=STR_TODAY):
        self.gl = Globals(str_today=str_trddate)

    def update_raw_data(self):
        p1_update_trading_rawdata_fund = UpdateTradingRawDataFund(self.gl)
        p2_update_trading_rawdata_holding = UpdateTradingRawDataHolding(self.gl)
        p3_update_trading_rawdata_entrust = UpdateTradingRawDataEntrust(self.gl)
        p4_update_trading_rawdata_secloan = UpdateTradingRawDataSecLoan(self.gl)
        p1_update_trading_rawdata_fund.start()
        p2_update_trading_rawdata_holding.start()
        p3_update_trading_rawdata_entrust.start()
        p4_update_trading_rawdata_secloan.start()

    def run(self):
        self.update_raw_data()


if __name__ == '__main__':
    task = TrdMonitor()
    task.run()











