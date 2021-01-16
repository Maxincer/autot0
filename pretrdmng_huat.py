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
    3. 信号券池: 由张博士产生并发出，最原始的信号标的组成的券池

Abbreviation:
    1. MarketData: md

"""
from math import ceil

import pandas as pd

from globals import Globals, STR_TODAY


class PreTrdMng:
    def __init__(self, str_trddate=STR_TODAY, download_winddata_mark=0):
        self.gl = Globals(str_trddate, download_winddata_mark)
        self.list_acctidsbymxz = ['8111_m_huat_9239']
        iter_dict_fmtted_wssdata = self.gl.col_fmtted_wssdata.find({'DataDate': self.gl.str_today}, {'_id': 0})
        df_fmtted_wssdata_today = pd.DataFrame(iter_dict_fmtted_wssdata)
        self.dict_fmtted_wssdata_of_today = df_fmtted_wssdata_today.set_index('WindCode').to_dict()

    def upload_pretrd_rawdata(self, acctidbymxz):
        # 上传信号券池原始数据
        df_csv_tgtsecids = pd.read_csv(
            self.gl.fpath_input_csv_target_secids,
            converters={'SecurityID': lambda x: str(x).zfill(6)}
        )
        df_csv_tgtsecids['DataDate'] = self.gl.str_today
        df_csv_tgtsecids['AcctIDByMXZ'] = acctidbymxz

        list_dicts_tgtsecids = df_csv_tgtsecids.to_dict('records')
        self.gl.col_pretrd_rawdata_tgtsecids.delete_many({'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz})
        if list_dicts_tgtsecids:
            self.gl.col_pretrd_rawdata_tgtsecids.insert_many(list_dicts_tgtsecids)

        df_secids_excluded = pd.read_csv(
            self.gl.fpath_input_csv_excluded_secids, converters={'SecurityID': lambda x: str(x).zfill(6)}
        )
        df_secids_excluded = df_secids_excluded[df_secids_excluded['AcctIDByMXZ'] == acctidbymxz]
        df_secids_excluded = df_secids_excluded.copy()
        df_secids_excluded['DataDate'] = self.gl.str_today
        list_dicts_secids_excluded = df_secids_excluded.to_dict('records')
        self.gl.col_pretrd_rawdata_excluded_secids.delete_many(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        )
        if list_dicts_secids_excluded:
            self.gl.col_pretrd_rawdata_excluded_secids.insert_many(list_dicts_secids_excluded)

        # 上传专项券池-专项融券市场行情
        dict_acctinfo = self.gl.col_acctinfo.find_one(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        )
        fpath_pretrd_md_private_secloan = dict_acctinfo['PreTradeDataFilePath']['PrivateSecurityLoanMarketData']
        df_xlsx_md_private_secloan = pd.read_excel(fpath_pretrd_md_private_secloan, dtype='str')
        list_dicts_md_private_secloan = df_xlsx_md_private_secloan.to_dict('records')
        for dict_md_private_secloan in list_dicts_md_private_secloan:
            dict_md_private_secloan['DataDate'] = self.gl.str_today
            dict_md_private_secloan['AcctIDByMXZ'] = acctidbymxz

        self.gl.col_pretrd_rawdata_md_private_secloan.delete_many(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        )
        if list_dicts_md_private_secloan:
            self.gl.col_pretrd_rawdata_md_private_secloan.insert_many(list_dicts_md_private_secloan)

        # 上传专项券池-普通融券市场行情
        fpath_pretrd_md_public_secloan = dict_acctinfo['PreTradeDataFilePath']['PublicSecurityLoanMarketData']
        df_xlsx_md_public_secloan = pd.read_excel(fpath_pretrd_md_public_secloan, dtype='str')
        list_dicts_md_public_secloan = df_xlsx_md_public_secloan.to_dict('records')
        for dict_md_public_secloan in list_dicts_md_public_secloan:
            dict_md_public_secloan['DataDate'] = self.gl.str_today
            dict_md_public_secloan['AcctIDByMXZ'] = acctidbymxz

        self.gl.col_pretrd_rawdata_md_public_secloan.delete_many(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        )
        if list_dicts_md_public_secloan:
            self.gl.col_pretrd_rawdata_md_public_secloan.insert_many(list_dicts_md_public_secloan)

    def upload_pretrd_fmtdata(self, acctidbymxz):
        list_excluded_secids = []
        for dict_excluded_secid in self.gl.col_pretrd_rawdata_excluded_secids.find(
                {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        ):
            list_excluded_secids.append(dict_excluded_secid['SecurityID'])

        iter_dicts_pretrd_rawdata_tgtsecids = self.gl.col_pretrd_rawdata_tgtsecids.find(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        )
        list_pretrd_fmtdata_tgtsecids = []
        for dict_pretrd_rawdata_tgtsecids in iter_dicts_pretrd_rawdata_tgtsecids:
            secid = dict_pretrd_rawdata_tgtsecids['SecurityID']
            windcode = self.gl.get_secid2windcode(secid)
            if secid in list_excluded_secids:
                excluded_mark = 1
            else:
                excluded_mark = 0
            dict_pretrd_fmtdata_tgtsecids = {
                'DataDate': self.gl.str_today,
                'AcctIDByMXZ': acctidbymxz,
                'SecurityID': secid,
                'WindCode': windcode,
                'ExcludedMark': excluded_mark
            }
            list_pretrd_fmtdata_tgtsecids.append(dict_pretrd_fmtdata_tgtsecids)
        self.gl.col_pretrd_fmtdata_tgtsecids.delete_many({'DataDate': self.gl.str_today})
        if list_pretrd_fmtdata_tgtsecids:
            self.gl.col_pretrd_fmtdata_tgtsecids.insert_many(list_pretrd_fmtdata_tgtsecids)

        broker_abbr = acctidbymxz.split('_')[2]
        if broker_abbr in ['huat']:
            # todo 默认普通券池的MD原格式默认为专业版3导出格式，专项券池的MD原格式为matic导出的格式
            # todo 将普通券池与专项券池合并，并以secloan_src加以区分: public/private
            list_pretrd_fmtdata_md_security_loan = []
            for dict_rawdata_public_secloan in self.gl.col_pretrd_rawdata_md_public_secloan.find(
                    {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
            ):
                secid = dict_rawdata_public_secloan['证券代码'].zfill(6)
                avlqty = float(dict_rawdata_public_secloan['融券限额'])
                dict_pretrd_fmtdata_md_security_loan = {
                    'DataDate': self.gl.str_today,
                    'AcctIDByMXZ': acctidbymxz,
                    'SecurityID': secid,
                    'AvlQty': avlqty,
                    'SecurityLoanSource': 'Public'
                }
                list_pretrd_fmtdata_md_security_loan.append(dict_pretrd_fmtdata_md_security_loan)

            for dict_rawdata_private_secloan in self.gl.col_pretrd_rawdata_md_private_secloan.find(
                    {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
            ):
                secid = dict_rawdata_private_secloan['证券名称']
                avlqty = float(dict_rawdata_private_secloan['委托数量'])
                if dict_rawdata_private_secloan['券源类型'] in ['竞拍券']:
                    secloan_src = 'Private_JPQ'
                elif dict_rawdata_private_secloan['券源类型'] in ['实时券']:
                    secloan_src = 'Private_SSQ'
                else:
                    raise ValueError('Unknown security loan type in private security loan')
                dict_pretrd_fmtdata_md_security_loan = {
                    'DataDate': self.gl.str_today,
                    'AcctIDByMXZ': acctidbymxz,
                    'SecurityID': secid,
                    'AvlQty': avlqty,
                    'SecurityLoanSource': secloan_src
                }
                list_pretrd_fmtdata_md_security_loan.append(dict_pretrd_fmtdata_md_security_loan)

            self.gl.col_pretrd_fmtdata_md_security_loan.delete_many(
                {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
            )
            if list_pretrd_fmtdata_md_security_loan:
                self.gl.col_pretrd_fmtdata_md_security_loan.insert_many(list_pretrd_fmtdata_md_security_loan)

    def upload_secloan_demand_analysis(self, acctidbymxz):
        # 先遍历tgtsecpool, 添加新增加的券
        # 再遍历已锁定合约列表, 得到应减少的券

        iter_pretrd_fmtdata_tgtsecids = self.gl.col_pretrd_fmtdata_tgtsecids.find(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz, 'ExcludedMark': 0}
        )
        list_tgtwindcodes = [_['WindCode'] for _ in iter_pretrd_fmtdata_tgtsecids]

        list_dicts_secloan_demand_analysis = []
        for tgtwindcode in list_tgtwindcodes:
            tgtsecid = tgtwindcode.split('.')[0]
            margin_or_not_mark = self.gl.dict_fmtted_wssdata['MarginOrNotMark'][tgtwindcode]
            symbol = self.gl.dict_fmtted_wssdata['Symbol'][tgtwindcode]
            preclose = self.gl.dict_fmtted_wssdata['PreClose'][tgtwindcode]

            # 专项券池可用分析
            # 华泰的专项券池分为: 实时券池(日内交易)，竞拍券池(盘后竞价)。
            private_secloan_avlqty = 0
            for dict_private_secpool_ready in self.list_dicts_private_secpool_ready:
                secid_in_private_secpool_ready = dict_private_secpool_ready['证券代码']
                if tgtsecid == secid_in_private_secpool_ready:
                    private_secpool_ready_avlqty = dict_private_secpool_ready['未分配数量']


            # 普通券池可用分析
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
        for acctidbymxz in self.list_acctidsbymxz:
            self.upload_pretrd_rawdata(acctidbymxz)
            self.get_secloanmng_draft(acctidbymxz)
            self.get_csv_secloan_order(acctidbymxz)
        print('PreTrdMng Finished.')


if __name__ == '__main__':
    task = PreTrdMng(download_winddata_mark=1)
    task.run()
