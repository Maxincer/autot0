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
    2.
"""

from datetime import datetime
from threading import Thread
from time import sleep

from pymongo import MongoClient

from globals import Globals, STR_TODAY


class UpdateTradeRawDataFund(Thread):
    def __init__(self, gl):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = gl.acctidbymxz
        self.fpath_input_csv_margin_account_fund = gl.fpath_input_csv_margin_account_fund

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )

        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_fund = db_trade_data['trade_rawdata_fund']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trade_rawdata_fund = []
            with open(self.fpath_input_csv_margin_account_fund) as f:
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
            col_trade_rawdata_fund.delete_many({'DataDate': self.str_today})
            if list_dicts_trade_rawdata_fund:
                col_trade_rawdata_fund.insert_many(list_dicts_trade_rawdata_fund)
            print('update_trade_rawdata_fund finished, sleep 30s')
            sleep(30)


class UpdateTradeRawDataHolding(Thread):
    def __init__(self, gl):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = gl.acctidbymxz
        self.fpath_input_csv_margin_account_holding = gl.fpath_input_csv_margin_account_holding

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_holding = db_trade_data['trade_rawdata_holding']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trade_rawdata_holding = []
            with open(self.fpath_input_csv_margin_account_holding) as f:
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
            col_trade_rawdata_holding.delete_many({'DataDate': self.str_today})
            if list_dicts_trade_rawdata_holding:
                col_trade_rawdata_holding.insert_many(list_dicts_trade_rawdata_holding)
            print('update_trade_rawdata_holding finished, sleep 30s')
            sleep(30)


class UpdateTradeRawDataOrder(Thread):
    def __init__(self, gl):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = gl.acctidbymxz
        self.fpath_input_csv_margin_account_order = gl.fpath_input_csv_margin_account_order

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_order = db_trade_data['trade_rawdata_order']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trade_rawdata_order = []
            with open(self.fpath_input_csv_margin_account_order) as f:
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
            col_trade_rawdata_order.delete_many({'DataDate': self.str_today})
            if list_dicts_trade_rawdata_order:
                col_trade_rawdata_order.insert_many(list_dicts_trade_rawdata_order)
            print('update_trade_rawdata_order finished, sleep 30s')
            sleep(30)


class UpdateTradeRawDataSecLoan(Thread):
    def __init__(self, gl):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = gl.acctidbymxz
        self.fpath_input_csv_margin_account_secloan = gl.fpath_input_csv_margin_account_secloan

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_secloan = db_trade_data['trade_rawdata_secloan']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            list_dicts_trade_rawdata_secloan = []
            with open(self.fpath_input_csv_margin_account_secloan) as f:
                list_datalines_of_file = f.readlines()
                list_fields = list_datalines_of_file[0].strip().split(',')
                list_datalines = [_.strip() for _ in list_datalines_of_file[1:]]
                for dataline in list_datalines:
                    list_data = [str(_).strip() for _ in dataline.split(',')]
                    dict_fields2data = dict(zip(list_fields, list_data))
                    dict_fields2data.update({'DataDate': self.str_today})
                    dict_fields2data.update({'UpdateTime': str_update_time})
                    dict_fields2data.update({'AcctIDByMXZ': self.acctidbymxz})
                    list_dicts_trade_rawdata_secloan.append(dict_fields2data)
            col_trade_rawdata_secloan.delete_many({'DataDate': self.str_today})
            if list_dicts_trade_rawdata_secloan:
                col_trade_rawdata_secloan.insert_many(list_dicts_trade_rawdata_secloan)
            print('update_trade_rawdata_secloan finished, sleep 30s')
            sleep(30)


class UpdateTradeFmtDataFund(Thread):
    def __init__(self, gl):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = gl.acctidbymxz

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_fund = db_trade_data['trade_rawdata_fund']
        col_trade_fmtdata_fund = db_trade_data['trade_fmtdata_fund']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            iter_trade_rawdata_fund = col_trade_rawdata_fund.find(
                {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz}
            )
            list_dicts_trade_fmtdata_fund = []
            for dict_trade_rawdata_fund in iter_trade_rawdata_fund:
                cash_available_for_collateral_trade = float(dict_trade_rawdata_fund['可用余额'])
                tt_asset = float(dict_trade_rawdata_fund['资产总值'])
                tt_mv = float(dict_trade_rawdata_fund['证券市值'])

                # ehtc
                # net_asset = float(dict_trade_rawdata_fund['净资产'])
                # cash_from_ss = float(dict_trade_rawdata_fund['剩余融券卖出资金'])
                # cash_available_for_collateral_trade = float(dict_trade_rawdata_fund['可用金额'])
                # tt_asset = float(dict_trade_rawdata_fund['总资产'])
                # tt_mv = float(dict_trade_rawdata_fund['总市值'])
                cash = tt_asset - tt_mv
                dict_trade_fmtdata_fund = {
                    'DataDate': self.str_today,
                    'UpdateTime': str_update_time,
                    'AcctIDByMXZ': self.acctidbymxz,
                    'Cash': cash,
                    'CashAvailableForCollateralTrade': cash_available_for_collateral_trade
                }
                list_dicts_trade_fmtdata_fund.append(dict_trade_fmtdata_fund)

            col_trade_fmtdata_fund.delete_many({'DataDate': self.str_today})
            if list_dicts_trade_fmtdata_fund:
                col_trade_fmtdata_fund.insert_many(list_dicts_trade_fmtdata_fund)
            print('update_trade_fmtdata_fund finished, sleep 30s')
            sleep(30)


class UpdateTradeFmtDataHolding(Thread):
    def __init__(self, gl):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = gl.acctidbymxz

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_holding = db_trade_data['trade_rawdata_holding']
        col_trade_fmtdata_holding = db_trade_data['trade_fmtdata_holding']

        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            iter_trade_rawdata_holding = col_trade_rawdata_holding.find(
                {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz}
            )
            list_dicts_trade_fmtdata_holding = []
            for dict_trade_rawdata_holding in iter_trade_rawdata_holding:
                secid = str(dict_trade_rawdata_holding['代码']).zfill(6)
                # shareholder_id = dict_trade_rawdata_holding['股东账号']
                # if shareholder_id[0].isalpha():
                #     secidsrc = 'SSE'
                # elif shareholder_id[0].isdigit():
                #     secidsrc = 'SZSE'
                # else:
                #     raise ValueError(f'Wrong shareholder ID: {shareholder_id}')
                dict_exg = {'1': 'SZSE', '2': 'SSE'}
                secidsrc = dict_exg[dict_trade_rawdata_holding['市场']]
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

            col_trade_fmtdata_holding.delete_many({'DataDate': self.str_today})
            if list_dicts_trade_fmtdata_holding:
                col_trade_fmtdata_holding.insert_many(list_dicts_trade_fmtdata_holding)
            print('Update_trade_fmtdata_holding finished, sleep 30s')
            sleep(30)


class UpdateTradeFmtDataOrder(Thread):
    def __init__(self, gl):
        super().__init__()
        self.str_today = gl.str_today
        self.acctidbymxz = gl.acctidbymxz

    def run_xtpb(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_trade_data = server_mongodb['trade_data']
        col_trade_rawdata_order = db_trade_data['trade_rawdata_order']
        col_trade_fmtdata_order = db_trade_data['trade_fmtdata_order']
        while True:
            str_update_time = datetime.now().strftime('%H%M%S')
            iter_trade_rawdata_order = col_trade_rawdata_order.find(
                {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz}
            )
            # todo 格式化不同来源的不同格式, 以下为hait_xtpb

            # 确认side:
            # todo fix 协议中只有3个方向： 1. 1 = buy, 2. 2 = sell, 5 = Sell Short, 字符串: str
            # todo 但国内在交易环节可以提供这部分信息： BC: 买券还券 和 现券还券 B: 担保品买入
            list_dicts_trade_fmtdata_order = []
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
            col_trade_fmtdata_order.delete_many({'DataDate': self.str_today})
            if list_dicts_trade_fmtdata_order:
                col_trade_fmtdata_order.insert_many(list_dicts_trade_fmtdata_order)
            print('update_trade_fmtdata_order finished, sleep 30s')
            sleep(30)

    def run_ehfz(self):  # ehfz
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
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
            list_dicts_trade_fmtdata_order = []
            for dict_trade_rawdata_order in iter_trade_rawdata_order:
                secid = str(dict_trade_rawdata_order['证券代码']).zfill(6)
                symbol = dict_trade_rawdata_order['证券名称']
                trade_mark = dict_trade_rawdata_order['@交易类型']
                exg = dict_trade_rawdata_order['市场类型']
                secidsrc = {'1': 'SSE', '2': 'SZSE'}[exg]
                cumqty = float(dict_trade_rawdata_order['成交数量'])
                avgpx = float(dict_trade_rawdata_order['成交价格'])
                ordstatus = dict_trade_rawdata_order['@委托状态']
                if trade_mark in ['1', '2']:
                    side = int(trade_mark)
                elif trade_mark in ['15'] and ordstatus in ['2', '8']:  # todo 抽象现券还券划转和买券还券划转， 目前只看到了现券还券划转
                    side = 5
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
                    'OrderTime': order_time,
                }
                list_dicts_trade_fmtdata_order.append(dict_trade_fmtdata_order)
            col_trade_fmtdata_order.delete_many({'DataDate': self.str_today})
            if list_dicts_trade_fmtdata_order:
                col_trade_fmtdata_order.insert_many(list_dicts_trade_fmtdata_order)
            print('update_trade_fmtdata_order finished, sleep 30s')
            sleep(30)


class UpdateTradePosition(Thread):
    """
    1. 计算标准格式的position
    2. LongQty 与 ShortQty 分别计算
    3. Assumption
        1. 融券空头，受昨日余额和今日融券卖出额影响，并由此计算
        2. 其他直接读取
        3. 划转事项客观准确的执行时间为盘后清算时，在盘中无法得到这部分数据， 但将划转事项视为立即生效合理。
    """
    def __init__(self, gl):
        super().__init__()
        self.gl = gl

    def run(self):
        server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        db_trade_data = server_mongodb['trade_data']
        db_posttrd_data = server_mongodb['post_trade_data']
        col_posttrd_position = db_posttrd_data['post_trade_position']
        col_trade_position = db_trade_data['trade_position']
        col_trade_fmtdata_order = db_trade_data['trade_fmtdata_order']
        col_trade_fmtdata_holding = db_trade_data['trade_fmtdata_holding']
        while True:
            # 获取position的secid全集
            # # 昨夜清算后持仓col_posttrd_position
            iter_posttrd_position_last_trddate = col_posttrd_position.find(
                {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
            set_secids_in_position_predate = set()
            for dict_posttrd_position in iter_posttrd_position_last_trddate:
                secid_in_predate_position = dict_posttrd_position['SecurityID']
                set_secids_in_position_predate.add(secid_in_predate_position)

            # # 今日交易记录中的所有secid: todo 此处使用hait_xtpb格式(视野受限，假设划款事项与交易的完全记录都在同一张表里)：
            list_dicts_trade_fmtdata_order = list(
                col_trade_fmtdata_order.find({'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.gl.acctidbymxz})
            )
            set_secids_in_trade_fmtdata_order = set()
            for dict_trade_fmtdata_order in list_dicts_trade_fmtdata_order:
                secid_in_trade_order = dict_trade_fmtdata_order['SecurityID']
                set_secids_in_trade_fmtdata_order.add(secid_in_trade_order)

            # # 获取trade data_holding的所有secid
            iter_trade_fmtdata_holding = col_trade_fmtdata_holding.find(
                {'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
            set_secids_in_trade_fmtdata_holding = set()
            for dict_trade_fmtdata_holding in iter_trade_fmtdata_holding:
                secid = dict_trade_fmtdata_holding['SecurityID']
                set_secids_in_trade_fmtdata_holding.add(secid)

            # # 并集: 清算后持仓 + 交易中的持仓 + 当日交易
            set_secids_in_trade_position = (
                set_secids_in_position_predate | set_secids_in_trade_fmtdata_order | set_secids_in_trade_fmtdata_holding
            )
            list_secids_in_trade_position = list(set_secids_in_trade_position)
            list_secids_in_trade_position.sort()
            list_dicts_trade_position = []
            for secid in list_secids_in_trade_position:
                # 计算longqty 取当前持仓： todo 注意 hait_xtpb 的 当日拥股字段 为扣除划转的余额
                longqty = 0
                iter_trade_fmtdata_holding_find_secid = col_trade_fmtdata_holding.find(
                    {'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.gl.acctidbymxz, 'SecurityID': secid}
                )
                for dict_trade_fmtdata_holding in iter_trade_fmtdata_holding_find_secid:
                    longqty += dict_trade_fmtdata_holding['LongQty']

                # 计算shortqty： 昨日shortqty余额 + 今融券卖出 - 还券(现券还券，买入还券)
                dict_posttrd_position_last_trddate = col_posttrd_position.find_one(
                    {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz, 'SecurityID': secid}
                )
                if dict_posttrd_position_last_trddate:
                    shortqty_last_trddate = dict_posttrd_position_last_trddate['ShortQty']
                else:
                    shortqty_last_trddate = 0
                shortqty_delta_today = 0
                for dict_trade_fmtdata_order in list_dicts_trade_fmtdata_order:
                    if dict_trade_fmtdata_order['SecurityID'] == secid and dict_trade_fmtdata_order['Side'] in ['5']:
                        shortqty_delta_today += dict_trade_fmtdata_order['LastQty']
                    if (dict_trade_fmtdata_order['SecurityID'] == secid
                            and dict_trade_fmtdata_order['Side'] in ['现券还券划拨', '买券还券划拨']):
                        shortqty_delta_today += -dict_trade_fmtdata_order['LastQty']
                shortqty = shortqty_last_trddate + shortqty_delta_today
                str_update_time = datetime.now().strftime('%H%M%S')
                dict_trade_position = {
                    'DataDate': self.gl.str_today,
                    'UpdateTime': str_update_time,
                    'AcctIDByMXZ': self.gl.acctidbymxz,
                    'SecurityID': secid,
                    'LongQty': longqty,
                    'ShortQty': shortqty,
                    'NetQty': longqty - shortqty,
                }
                list_dicts_trade_position.append(dict_trade_position)
            col_trade_position.delete_many({'DataDate': self.gl.str_today})
            if list_dicts_trade_position:
                col_trade_position.insert_many(list_dicts_trade_position)
            print('Update_trade_position finished, sleep 30s')
            sleep(30)


class UpdateTradeDataBase:
    def __init__(self, str_trddate=STR_TODAY):
        self.gl = Globals(str_today=str_trddate)

    def update_raw_data_and_fmtdata(self):
        # rawdata
        t1_update_trade_rawdata_fund = UpdateTradeRawDataFund(self.gl)
        t2_update_trade_rawdata_holding = UpdateTradeRawDataHolding(self.gl)
        t3_update_trade_rawdata_order = UpdateTradeRawDataOrder(self.gl)
        t4_update_trade_rawdata_secloan = UpdateTradeRawDataSecLoan(self.gl)
        t1_update_trade_rawdata_fund.start()
        t2_update_trade_rawdata_holding.start()
        t3_update_trade_rawdata_order.start()
        t4_update_trade_rawdata_secloan.start()

        # fmtdata
        t5_update_trade_fmtdata_fund = UpdateTradeFmtDataFund(self.gl)
        t6_update_trade_fmtdata_holding = UpdateTradeFmtDataHolding(self.gl)
        t7_update_trade_fmtdata_order = UpdateTradeFmtDataOrder(self.gl)
        t5_update_trade_fmtdata_fund.start()
        t6_update_trade_fmtdata_holding.start()
        t7_update_trade_fmtdata_order.run_ehfz()

        # business data
        t8_update_trade_position = UpdateTradePosition(self.gl)
        t8_update_trade_position.start()

    def run(self):
        self.update_raw_data_and_fmtdata()


if __name__ == '__main__':
    task = UpdateTradeDataBase()
    task.run()











