#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20201024T220000

"""
This script is to deal with the pre-trade management:
    1. manage the security loan contracts pool.

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

Todo:
    1. 区分ManuT0与AutoT0
    2. 根据目标融券金额估算：
        1. 保证金占用金额
    3. 建设rawdata -> fmtdata

Note:
    1. 融券完全对冲交易策略下，required minimum net asset、保证金、融券卖出市值的初始关系（没有市值波动，融券卖出证券市值=融券卖出金额=融券卖出所得资金）。
        0. 假设所有的担保品折算率为0
        1. 净资产 = 3 * 保证金 = 3 * (0.5 * 融券卖出市值)
        2. 总资产 = 现金 + 担保品市值 = 融券卖出所得资金 + 非融券卖出所得资金 + 担保品市值
        3. 保证金 = 融券卖出所得资金 + 非融券卖出所得资金 - 融券卖出金额
        4. 保证金占用 = 融券卖出证券市值 * 融券保证金比例 + 利息及费用 = 0.5 * 融券卖出证券市值
        5. 可用保证金 = 保证金 - 保证金占用 = 非融券卖出所得资金 - 融券卖出证券市值 * 0.5 + 0
        6. 令可用保证金 = 0
            得： 非融券卖出所得资金 = 0.5融券卖出证券市值
            即： 总资产 = 融券卖出所得资金 + 0.5 * 融券卖出所得资金 + 担保品市值 = 2.5 * 融券卖出所得资金 = 2.5 * 融券卖出证券市值
                净资产 = 总资产 - 融券卖出证券市值 = 1.5 * 融券卖出证券市值 = 3 * 保证金占用
"""
from datetime import datetime
import json
from math import floor
import os

import pandas as pd

from globals import Globals, STR_TODAY, w


class PreTrdMng:
    def __init__(self, str_trddate=STR_TODAY, download_winddata_mark=0):
        self.gl = Globals(str_trddate, download_winddata_mark)
        self.gl.update_attachments_from_email(
            '每日券池信息', self.gl.str_today, self.gl.dirpath_input_xlsx_marginable_secpools_from_hait
        )

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

        # todo 自定义券池， 未使用
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

        self.list_dicts_posttrd_fmtdata_ssquota_from_secloan_last_trddate = list(
            self.gl.col_posttrd_fmtdata_ssquota_from_secloan.find(
               {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
        )
        self.list_windcodes_ssquota_last_trddate = [
            self.gl.get_secid2windcode(_['SecurityID'])
            for _ in self.list_dicts_posttrd_fmtdata_ssquota_from_secloan_last_trddate
        ]

        iter_dict_fmtted_wssdata = self.gl.col_fmtted_wssdata.find({'DataDate': self.gl.str_today}, {'_id': 0})
        df_fmtted_wssdata_today = pd.DataFrame(iter_dict_fmtted_wssdata)
        self.dict_fmtted_wssdata_of_today = df_fmtted_wssdata_today.set_index('WindCode').to_dict()

    def upload_pretrd_rawdata(self):
        # grp_tgtsecids_by_cps
        # todo 升级到fmtdata
        df_grp_tgtsecids_by_cps = pd.read_csv(
            self.gl.fpath_input_csv_grp_tgtsecids_by_cps, converters={'SecurityID': lambda x: str(x).zfill(6)}
        )
        df_grp_tgtsecids_by_cps['DataDate'] = self.gl.str_today
        df_grp_tgtsecids_by_cps['AcctIDByMXZ'] = self.gl.acctidbymxz
        list_dicts_grp_tgtsecids_by_cps = df_grp_tgtsecids_by_cps.to_dict('records')
        self.gl.col_pretrd_grp_tgtsecids_by_cps.delete_many(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_grp_tgtsecids_by_cps:
            self.gl.col_pretrd_grp_tgtsecids_by_cps.insert_many(list_dicts_grp_tgtsecids_by_cps)

    def get_secloanmng_draft(self):
        # todo 业务假设： 从机器T0目标持仓中剔除人工T0所需股票
        # 获取非机器T0股票
        iter_dicts_grp_tgtsecids_by_cps_not_autot0 = self.gl.col_pretrd_grp_tgtsecids_by_cps.find(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        set_secids_in_grp_tgtsecids_by_cps_not_autot0 = set()
        for dict_grp_tgtsecids_by_cps_manut0 in iter_dicts_grp_tgtsecids_by_cps_not_autot0:
            tgtsecids = dict_grp_tgtsecids_by_cps_manut0['SecurityID']
            set_secids_in_grp_tgtsecids_by_cps_not_autot0.add(tgtsecids)

        # 先遍历tgtsecpool, 添加新增加的券
        # 再遍历已锁定合约列表, 得到应减少的券
        list_dicts_secpool_analysis = []
        for tgtwindcode in self.list_tgtwindcodes:
            tgtsecid = tgtwindcode[:6]
            if tgtsecid in set_secids_in_grp_tgtsecids_by_cps_not_autot0:
                continue
            margin_or_not_mark = self.gl.dict_fmtted_wssdata['MarginOrNotMark'][tgtwindcode]
            symbol = self.gl.dict_fmtted_wssdata['Symbol'][tgtwindcode]
            pre_close = self.gl.dict_fmtted_wssdata['PreClose'][tgtwindcode]
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

            quota_last_trddate = 0  # 清算后Quota
            for dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate in (
                    self.list_dicts_posttrd_fmtdata_ssquota_from_secloan_last_trddate
            ):
                if tgtsecid == dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate['SecurityID']:
                    quota_last_trddate = dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate['SSQuota']

            # 根据tgtamt计算tgtqty
            # 每只股票标杆10万元。单只股票上限30万。（800万券池， 80支股票 * 10万）
            # 借券单位最小为200股（考虑到科创板）
            # 近似后取ceiling
            tgtamt = 200000
            tgtqty = floor((tgtamt / pre_close) / 200) * 200
            # todo 由于现有交易系统无法进行挂单成交，故对盘口价差大的股票交易成本很大，导致实际收益与策略收益偏离。暂时不建仓。
            if pre_close <= 5 or pre_close > 300:
                tgtqty = 0
            # todo 由于现有交易系统无法交易200股以下的科创板股票，故在规模小的情况下，剔除科创板
            if tgtsecid[:3] in ['688'] and pre_close > 100:
                tgtqty = 0
            if not margin_or_not_mark:
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
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'SecurityID': tgtsecid,
                'WindCode': tgtwindcode,
                'SecName': symbol,
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

        for dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate in (
                self.list_dicts_posttrd_fmtdata_ssquota_from_secloan_last_trddate
        ):
            secid_ssquota_last_trddate = dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate['SecurityID']
            if secid_ssquota_last_trddate in set_secids_in_grp_tgtsecids_by_cps_not_autot0:
                continue
            windcode_ssquota_last_trddate = self.gl.get_secid2windcode(secid_ssquota_last_trddate)
            quota_last_trddate = dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate['SSQuota']
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
                    'AcctIDByMXZ': self.gl.acctidbymxz,
                    'SecurityID': secid_ssquota_last_trddate,
                    'WindCode': windcode_ssquota_last_trddate,
                    'SecName': self.dict_fmtted_wssdata_of_today['Symbol'][windcode_ssquota_last_trddate],
                    'TgtQty': tgtqty,
                    'MarginOrNotMark': 1,
                    'AvlQtyInPrivatePool': private_secpool_ready_avlqty,
                    'QuotaOnLastTrdDate': quota_last_trddate,
                    'QtyToLock': qty2lock,
                    'QtyToBorrowFromPublicSecPool': 0,
                    'QtyToBorrowFromOutsideSource': 0,
                }
                list_dicts_secpool_analysis.append(dict_secpool_analysis)
        df_secpool_analysis = pd.DataFrame(list_dicts_secpool_analysis)
        df_secpool_analysis.to_csv(self.gl.fpath_output_csv_tgtsecloan_mngdraft, index=False, encoding='ansi')
        self.gl.col_pretrd_tgtsecloan_mngdraft.delete_many(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        self.gl.col_pretrd_tgtsecloan_mngdraft.insert_many(list_dicts_secpool_analysis)
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
            qty2borrow_from_outside_source = dict_tgtsecloan_mngdraft['QtyToBorrowFromOutsideSource']
            if qty2borrow_from_outside_source > 0:
                secid = dict_tgtsecloan_mngdraft['SecurityID']
                # 筛选最小数量限制：外界询券状态下，非双创最少10000股，双创最少1000股
                if secid[:3] in ['688']:
                    continue
                if (secid[:3] not in ['688', '300']) and qty2borrow_from_outside_source < 10000:
                    continue
                if (secid[:3] in ['688', '300']) and qty2borrow_from_outside_source < 1000:
                    continue
                secname = dict_tgtsecloan_mngdraft['SecName']
                dict_demand_of_secpool_from_outside_src = {
                    '证券代码': secid,
                    '证券名称': secname,
                    '需求数量（股）': qty2borrow_from_outside_source,
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
        # 先运行T日的post_trddata_mng, 将T-1的清算数据上传
        self.upload_pretrd_rawdata()
        self.get_secloanmng_draft()
        self.get_and_send_xlsx_demand_of_secpool_from_outside_src()
        print('PreTrdMng Finished.')


if __name__ == '__main__':
    task = PreTrdMng(download_winddata_mark=0)
    task.run()
