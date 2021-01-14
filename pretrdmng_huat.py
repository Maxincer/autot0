#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20211012T220000

"""
This script is to deal with the pre-trade management for broker huat
    1. manage the security loan contracts pool.
    2. generate csv according to 融券通-批量融券模板.xlsx

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

    2. huat的约券周期从16:00开始
"""
from math import ceil

import pandas as pd

from globals import Globals, STR_TODAY


class PreTrdMng:
    def __init__(self, str_trddate=STR_TODAY, download_winddata_mark=0):
        self.gl = Globals(str_trddate, download_winddata_mark)
        self.gl.acctidbymxz = '8111_m_huat_9239'
        df_csv_tgtsecids = pd.read_csv(
            self.gl.fpath_input_csv_target_secids,
            converters={'SecurityID': lambda x: str(x).zfill(6)}
        )
        self.list_tgtsecids = df_csv_tgtsecids['SecurityID'].to_list()
        self.list_tgtsecids.sort()
        self.list_tgtwindcodes = [self.gl.get_secid2windcode(_) for _ in self.list_tgtsecids]

        iter_dict_fmtted_wssdata = self.gl.col_fmtted_wssdata.find({'DataDate': self.gl.str_today}, {'_id': 0})
        df_fmtted_wssdata_today = pd.DataFrame(iter_dict_fmtted_wssdata)
        self.dict_fmtted_wssdata_of_today = df_fmtted_wssdata_today.set_index('WindCode').to_dict()

    def upload_pretrd_rawdata(self):
        # grp_tgtsecids_by_cps
        df_grp_tgtsecids_by_cps = pd.read_csv(
            self.gl.fpath_input_csv_grp_tgtsecids_by_cps, converters={'SecurityID': lambda x: str(x).zfill(6)}
        )
        df_grp_tgtsecids_by_cps['DataDate'] = self.gl.str_today
        df_grp_tgtsecids_by_cps['AcctIDByMXZ'] = self.gl.acctidbymxz  # todo
        list_dicts_grp_tgtsecids_by_cps = df_grp_tgtsecids_by_cps.to_dict('records')
        self.gl.col_pretrd_grp_tgtsecids_by_cps.delete_many(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_grp_tgtsecids_by_cps:
            self.gl.col_pretrd_grp_tgtsecids_by_cps.insert_many(list_dicts_grp_tgtsecids_by_cps)

    def get_secloanmng_draft(self):
        # 获取非机器T0股票
        iter_dicts_grp_tgtsecids_by_cps_not_autot0 = self.gl.col_pretrd_grp_tgtsecids_by_cps.find(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.gl.acctidbymxz}
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
            close = self.gl.dict_fmtted_wssdata['Close'][tgtwindcode]
            # private_secpool_ready_avlqty = 0
            # for dict_private_secpool_ready in self.list_dicts_private_secpool_ready:
            #     secid_in_private_secpool_ready = dict_private_secpool_ready['证券代码']
            #     if tgtsecid == secid_in_private_secpool_ready:
            #         private_secpool_ready_avlqty = dict_private_secpool_ready['未分配数量']
            #
            # public_secpool_avlqty = 0
            # for dict_public_secpool in self.list_dicts_public_secpool:
            #     secid_in_public_secpool = dict_public_secpool['证券代码']
            #     if tgtsecid == secid_in_public_secpool:
            #         public_secpool_avlqty = dict_public_secpool['数量']
            #
            # quota_last_trddate = 0  # 清算后Quota
            # for dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate in (
            #         self.list_dicts_posttrd_fmtdata_ssquota_from_secloan_last_trddate
            # ):
            #     if tgtsecid == dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate['SecurityID']:
            #         quota_last_trddate = dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate['SSQuota']

            # 根据tgtamt计算tgtqty
            # 每只股票标杆20万元
            # 处于公司集中交易系统无法交易200股以下考虑，不借入科创板股票
            # 近似后取ceiling
            tgtamt = 200000
            tgtqty = ceil((tgtamt / close) / 100) * 100
            # 由于现有交易系统无法进行挂单成交，故对盘口价差大的股票交易成本很大，导致实际收益与策略收益偏离。暂时不建仓。
            if close <= 5:
                tgtqty = 0
            # todo 由于现有交易系统无法交易200股以下的科创板股票，故在规模小的情况下，剔除科创板
            if tgtsecid[:3] in ['688']:
                tgtqty = 0
            if not margin_or_not_mark:
                tgtqty = 0
            #
            # 首发假设
            quota_last_trddate = 0
            private_secpool_ready_avlqty = 0
            public_secpool_avlqty = 0
            qty2terminate = 0
            qty2lock = 0
            qty2borrow_from_public_secpool = 0
            qty2borrow_from_outside_source = 0
            # qty2lock = min(tgtqty - quota_last_trddate, private_secpool_ready_avlqty)
            # qty2borrow_from_public_secpool = min(tgtqty - quota_last_trddate - qty2lock, public_secpool_avlqty)
            #
            # # 计算外部询券数量
            # if margin_or_not_mark:
            #     qty2borrow_from_outside_source = max(
            #             tgtqty - quota_last_trddate - private_secpool_ready_avlqty - public_secpool_avlqty, 0
            #     )
            # else:
            #     qty2borrow_from_outside_source = 0
            #
            # # 计算需要终止的融券额度(由于目标券池发生变化导致): 目标数量 - 最新已持有数量 todo 目前为quota_last_trddate
            # qty2terminate = max(quota_last_trddate - tgtqty, 0)

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
        #
        df_secpool_analysis = pd.DataFrame(list_dicts_secpool_analysis)
        df_secpool_analysis.to_csv(self.gl.fpath_output_csv_tgtsecloan_mngdraft, index=False, encoding='ansi')
        self.gl.col_pretrd_tgtsecloan_mngdraft.delete_many(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        self.gl.col_pretrd_tgtsecloan_mngdraft.insert_many(list_dicts_secpool_analysis)
        print(f"{self.gl.str_today}_tgtsecloan_mngdraft Finished.")

    def get_csv_secloan_order(self):
        df_csv_tgtsecloan_mngdraft = pd.read_csv(
            self.gl.fpath_output_csv_tgtsecloan_mngdraft,
            dtype={'QtyToBorrowFromOutSideSource': float},
            converters={'SecurityID': lambda x: str(x).zfill(6)},
            encoding='ansi'
        )
        list_dicts_tgtsecloan_mngdraft = df_csv_tgtsecloan_mngdraft.to_dict('records')

        fpath_marginable_secloan = r'D:\projects\autot0\data\input\pretrddata\marginable_secpools\huat\每日券池-20210114.xlsx'
        df_marginable_secloan = pd.read_excel(
            fpath_marginable_secloan,
            dtype={'委托数量': float, '委托期限': int, '委托利率': float},
            converters={'证券代码': lambda x: str(x.strip()).zfill(6)}
        )
        list_dicts_marginable_secloans = df_marginable_secloan.to_dict('records')

        list_dicts_secloan_order = []
        for dict_tgtsecloan_mngdraft in list_dicts_tgtsecloan_mngdraft:
            tgtqty = dict_tgtsecloan_mngdraft['TgtQty']
            acctid = '960000039239'
            qty2lock = tgtqty  # todo qty2lock 改为tgtqty
            # 若当日需求大于0，则到行情数据中询券
            if qty2lock > 0:
                tgtsecid = dict_tgtsecloan_mngdraft['SecurityID']
                if tgtsecid[0] in ['6']:
                    exchange = 'SH'
                elif tgtsecid[0] in ['3', '0']:
                    exchange = 'SZ'
                else:
                    raise ValueError('Unknown exchange.')
                list_dicts_avlsecloan_by_secid = []
                list_int_ordterm = []
                for dict_marginable_secloan in list_dicts_marginable_secloans:
                    secid_in_marginable_secloan = dict_marginable_secloan['证券代码']
                    avlqty_in_marginable_secloan = dict_marginable_secloan['委托数量']
                    int_avlterm = dict_marginable_secloan['委托期限']
                    if tgtsecid == secid_in_marginable_secloan and avlqty_in_marginable_secloan and int_avlterm <= 31:
                        list_dicts_avlsecloan_by_secid.append(dict_marginable_secloan)
                        list_int_ordterm.append(int_avlterm)
                list_int_ordterm.sort(reverse=False)

                for int_ordterm in list_int_ordterm:
                    for dict_avlsecloan in list_dicts_avlsecloan_by_secid:
                        avlqty = dict_avlsecloan['委托数量']
                        if qty2lock > avlqty:
                            ordqty = avlqty
                        else:
                            ordqty = qty2lock
                        if not ordqty:
                            continue
                        ir = dict_avlsecloan['委托利率']
                        if dict_avlsecloan['委托期限'] == int_ordterm:
                            dict_secloan_order = {
                                '资产账号': acctid,
                                '证券代码': tgtsecid,
                                '市场': exchange,
                                '申报类型': '非约定申报',
                                '约定号': '',
                                '期限': int_ordterm,
                                '利率': ir,
                                '数量': ordqty,
                            }
                            list_dicts_secloan_order.append(dict_secloan_order)
                            qty2lock = qty2lock - ordqty

        df_secloan_order = pd.DataFrame(list_dicts_secloan_order)
        df_secloan_order.to_csv('secloan_order.csv', index=False, encoding='ansi')
        print('批量融券单已生成.')

    def run(self):
        # 先运行T日的post_trddata_mng, 将T-1的清算数据上传
        self.upload_pretrd_rawdata()
        self.get_secloanmng_draft()
        self.get_csv_secloan_order()
        print('PreTrdMng Finished.')


if __name__ == '__main__':
    task = PreTrdMng(download_winddata_mark=0)
    task.run()
