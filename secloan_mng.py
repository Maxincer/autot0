#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20201024T220000

"""
This script is to manage the security loan contracts pool.
Steps:
    1. Format the file.
    2. Filter the available securities by '两融标的'.
    3. Match the filtered demand with the available securities pools and marking the source.
    4. Output the report about how to reach the target.
Assumption:
    1. 在公共券池和私用券池都有AvlQty的情况下，prefer使用私用券池(成本低)。(20201027)。核实：融券卖出会优先卖出锁券数量
    2. 目标券池会根据是否为两融标的筛选
    3. 最新收盘价小于5元的股票不借入 -> TgtQty 重置为0
    4. 融券利率超过9%的股票不借

"""
import json
import os

import pandas as pd
from WindPy import *

from globals import Globals


class SecLoanMng:
    def __init__(self):
        self.gl = Globals()
        self.gl.update_attachments_from_email()
        df_csv_tgtsecids = pd.read_csv(
            self.gl.fpath_input_csv_target_secids,
            converters={'SecurityID': lambda x: str(x).zfill(6)}
        )
        self.list_tgtsecids = df_csv_tgtsecids['SecurityID'].to_list()
        self.list_tgtsecids.sort()
        self.list_tgtwindcodes = [self.gl.get_secid2windcode(_) for _ in self.list_tgtsecids]

        df_private_secpool_ready = pd.read_excel(
            self.gl.fpath_input_xlsx_marginable_secpools_from_hait,
            sheet_name='即时可用券池',
            dtype={'未分配数量': float},
            converters={'证券代码': lambda x: str(x).zfill(6)}
        )
        self.list_dicts_private_secpool_ready = df_private_secpool_ready.to_dict('records')

        df_public_secpool = pd.read_excel(
            self.gl.fpath_input_xlsx_marginable_secpools_from_hait,
            sheet_name='公共券池',
            dtype={
                '数量': float,
            },
            converters={
                '证券代码': lambda x: str(x).zfill(6)
            }
        )
        self.list_dicts_public_secpool = df_public_secpool.to_dict('records')

        # todo 添加外部询券反馈表, 需要讨论工作流。目前自定义
        df_input_xlsx_secpool_from_outside_src = pd.read_excel(
            self.gl.fpath_input_xlsx_secpool_from_outside_src,
            dtype={
                'DataDate': str,
                'AvlQty': float,
            },
            converters={
                'SecurityID': lambda x: str(x).zfill(6)
            }
        )
        df_input_xlsx_secpool_from_outside_src = (
            df_input_xlsx_secpool_from_outside_src[
                df_input_xlsx_secpool_from_outside_src['DataDate'] == self.gl.str_today
            ]
        )
        self.list_dicts_secpool_from_outside_src = df_input_xlsx_secpool_from_outside_src.to_dict('records')

        df_csv_ssquota_last_trddate = pd.read_csv(
            self.gl.fpath_input_csv_ssquota,
            dtype={'Quota': float, 'ShortQty': float, 'Holding': float},
            converters={'Id': lambda x: str(x).zfill(6)}
        )
        self.list_dicts_ssquota_last_trddate = df_csv_ssquota_last_trddate.to_dict('records')
        self.list_windcodes_ssquota_last_trddate = [
            self.gl.get_secid2windcode(_['Id']) for _ in self.list_dicts_ssquota_last_trddate
        ]

        # 证券简称与两融标志数据, 从Wind下载
        list_windcodes_to_query = list(set(self.list_tgtwindcodes) | set(self.list_windcodes_ssquota_last_trddate))
        list_windcodes_to_query.sort()

        if os.path.exists(self.gl.fpath_json_dict_windcode2wssdata):
            with open(self.gl.fpath_json_dict_windcode2wssdata) as f:
                self.dict_windcode2wssdata = json.load(f)
        else:
            str_list_windcodes_to_query = ','.join(list_windcodes_to_query)
            wss_winddata = w.wss(
                str_list_windcodes_to_query, 'sec_name,marginornot,pre_close', f'tradeDate={self.gl.str_today}'
            )
            self.dict_windcode2wssdata = {
                'sec_name': dict(zip(wss_winddata.Codes, wss_winddata.Data[0])),
                'marginornot': dict(zip(wss_winddata.Codes, wss_winddata.Data[1])),
                'pre_close': dict(zip(wss_winddata.Codes, wss_winddata.Data[2]))
            }
            with open(self.gl.fpath_json_dict_windcode2wssdata, 'w') as f:
                json.dump(self.dict_windcode2wssdata, f)

    def get_secloanmng_draft(self):
        # 先遍历tgtsecpool, 添加新增加的券
        # 再遍历已锁定合约列表, 得到应减少的券
        list_dicts_secpool_analysis = []
        for tgtwindcode in self.list_tgtwindcodes:
            tgtsecid = tgtwindcode[:6]
            margin_or_not = self.dict_windcode2wssdata['marginornot'][tgtwindcode]
            secname = self.dict_windcode2wssdata['sec_name'][tgtwindcode]
            pre_close = self.dict_windcode2wssdata['pre_close'][tgtwindcode]
            if margin_or_not in ['是']:
                margin_or_not_mark = 1
            elif margin_or_not in ['否']:
                margin_or_not_mark = 0
            else:
                raise ValueError('Unknown MarginOrNot Value.')

            private_secpool_ready_avlqty = 0
            for dict_private_secpool_ready in self.list_dicts_private_secpool_ready:
                secid_in_private_secpool_ready = dict_private_secpool_ready['证券代码']
                if tgtsecid == secid_in_private_secpool_ready:
                    private_secpool_ready_avlqty = dict_private_secpool_ready['未分配数量']

            public_secpool_avlqty = 0
            for dict_public_secpool in self.list_dicts_public_secpool:
                secid_in_public_secpool = dict_public_secpool['证券代码']
                if tgtsecid == secid_in_public_secpool:
                    public_secpool_avlqty = dict_public_secpool['数量']

            quota_last_trddate = 0
            for dict_ssquota_last_trddate in self.list_dicts_ssquota_last_trddate:
                secid_ssquota_last_trddate = dict_ssquota_last_trddate['Id']
                if tgtsecid == secid_ssquota_last_trddate:
                    quota_last_trddate = dict_ssquota_last_trddate['Quota']
            tgtqty = 1000
            if pre_close <= 5:
                tgtqty = 0
            qty2lock = min(tgtqty - quota_last_trddate, private_secpool_ready_avlqty)
            qty2borrow_from_public_secpool = min(tgtqty - quota_last_trddate - qty2lock, public_secpool_avlqty)

            # 计算外部询券数量
            if margin_or_not_mark:
                qty2borrow_from_outside_source = max(
                        tgtqty - quota_last_trddate - private_secpool_ready_avlqty - public_secpool_avlqty, 0
                )
            else:
                qty2borrow_from_outside_source = 0

            # 计算需要终止的融券额度(由于目标券池发生变化导致): 目标数量 - 最新已持有数量 todo 目前为quota_last_trddate
            qty2terminate = max(quota_last_trddate - tgtqty, 0)

            dict_secpool_analysis = {
                'DataDate': self.gl.str_today,
                'SecurityID': tgtsecid,
                'WindCode': tgtwindcode,
                'SecName': secname,
                'TgtQty': tgtqty,
                'MarginOrNotMark': margin_or_not_mark,
                'AvlQtyInPrivatePool': private_secpool_ready_avlqty,
                'AvlQtyInPublicPool': public_secpool_avlqty,
                'QuotaOnLastTrdDate': quota_last_trddate,
                'QtyToTerminate': qty2terminate,
                'QtyToLock': qty2lock,
                'QtyToBorrowFromPublicSecPool': qty2borrow_from_public_secpool,
                'QtyToBorrowFromOutsideSource': qty2borrow_from_outside_source,
            }
            list_dicts_secpool_analysis.append(dict_secpool_analysis)

        for dict_ssquota_last_trddate in self.list_dicts_ssquota_last_trddate:
            secid_ssquota_last_trddate = dict_ssquota_last_trddate['Id']
            windcode_ssquota_last_trddate = self.gl.get_secid2windcode(secid_ssquota_last_trddate)
            quota_last_trddate = dict_ssquota_last_trddate['Quota']

            if secid_ssquota_last_trddate not in self.list_tgtsecids:
                private_secpool_ready_avlqty = 0
                for dict_private_secpool_ready in self.list_dicts_private_secpool_ready:
                    secid_in_private_secpool_ready = dict_private_secpool_ready['证券代码']
                    if secid_ssquota_last_trddate == secid_in_private_secpool_ready:
                        private_secpool_ready_avlqty = dict_private_secpool_ready['未分配数量']
                tgtqty = 0
                qty2lock = min(tgtqty - quota_last_trddate, private_secpool_ready_avlqty)
                dict_secpool_analysis = {
                    'DataDate': self.gl.str_today,
                    'SecurityID': secid_ssquota_last_trddate,
                    'WindCode': windcode_ssquota_last_trddate,
                    'SecName': self.dict_windcode2wssdata['sec_name'][windcode_ssquota_last_trddate],
                    'TgtQty': tgtqty,
                    'MarginOrNotMark': 1,
                    'AvlQtyInPrivatePool': private_secpool_ready_avlqty,
                    'QuotaOnLastTrdDate': quota_last_trddate,
                    'QtyToLock': qty2lock,
                    'QtyToBorrowFromPublicSecPool': 0,  # todo 目前为0。当已有数量未达到tgt数量时，需要区分合约来源
                    'QtyToBorrowFromOutsideSource': 0,
                }
                list_dicts_secpool_analysis.append(dict_secpool_analysis)

        df_secpool_analysis = pd.DataFrame(list_dicts_secpool_analysis)
        df_secpool_analysis.to_csv(self.gl.fpath_output_csv_tgtsecloan_mngdraft, index=False, encoding='ansi')
        print(f"{self.gl.str_today}_tgtsecloan_mngdraft Finished.")

    def get_and_send_xlsx_demand_of_secpool_from_outside_src(self):
        """
        1.生成每日给海通发送的"需要预约的券源需求.xlsx"
        2.按指定格式发送到指定邮箱
        """
        df_csv_tgtsecloan_mngdraft = pd.read_csv(
            self.gl.fpath_output_csv_tgtsecloan_mngdraft,
            dtype={'QtyToBorrowFromOutSideSource': float},
            converters={'SecurityID': lambda x: str(x).zfill(6)},
            encoding='ansi'
        )
        list_dicts_tgtsecloan_mngdraft = df_csv_tgtsecloan_mngdraft.to_dict('records')

        str_time_now = datetime.now().strftime('%H%M')
        if str_time_now < '0900':
            str_date = self.gl.str_today
        else:
            str_date = self.gl.str_next_trddate

        list_dicts_demand_of_secpool_from_outside_src = []
        for dict_tgtsecloan_mngdraft in list_dicts_tgtsecloan_mngdraft:
            if dict_tgtsecloan_mngdraft['QtyToBorrowFromOutsideSource'] > 0:
                secid = dict_tgtsecloan_mngdraft['SecurityID']
                secname = dict_tgtsecloan_mngdraft['SecName']
                dict_demand_of_secpool_from_outside_src = {
                    '证券代码': secid,
                    '证券名称': secname,
                    '需求数量（股）': 10000,
                    '期限（天）': 28,
                    '营业部名称': '贵阳长岭北路',
                    '客户号': '1882842000',
                    '客户名称': '鸣石满天星七号',
                    '筹券日期': str_date,
                    '特殊备注': '',
                }
                list_dicts_demand_of_secpool_from_outside_src.append(dict_demand_of_secpool_from_outside_src)
        df_demand_of_secpool_from_outside_src = pd.DataFrame(list_dicts_demand_of_secpool_from_outside_src)
        df_demand_of_secpool_from_outside_src.to_excel(
            self.gl.fpath_output_xlsx_demand_of_secpool_from_outside_src,
            index=False,
            sheet_name='券源需求',
        )
        print('券源需求已生成.')

        # 邮件发送
        self.gl.send_file_via_email(
            self.gl.email_to_addr_haitsecpool_from_outside_src,
            self.gl.email_subject_haitsecpool_from_outside_src,
            self.gl.fpath_output_xlsx_demand_of_secpool_from_outside_src,
            '需要预约的券源需求.xlsx'
        )

    def run(self):
        self.get_secloanmng_draft()
        self.get_and_send_xlsx_demand_of_secpool_from_outside_src()
        print('SecLoanMng Finished.')


if __name__ == '__main__':
    task = SecLoanMng()
    task.run()
