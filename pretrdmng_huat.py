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
    4. 从期限和首轮开仓成本考虑，先约public secloan, 再约private secloan： public券池期限长，可最大程度上减少换手次数
    5. 市场行情 market data 算是 pre_trade data

Abbreviation:

Todo:
    1. 在为不同策略的目标券池做区分的时候，目前只按照股票标的进行区分，不能按照数量区分

"""
from math import ceil

import pandas as pd

from globals import Globals, STR_TODAY, datetime, os


class PreTrdMng:
    def __init__(self, str_trddate=STR_TODAY, download_winddata_mark=0):
        self.gl = Globals(str_trddate, download_winddata_mark)
        self.list_acctidsbymxz = self.gl.list_acctidsbymxz
        df_fmtted_wssdata_today = pd.DataFrame(
            self.gl.col_fmtted_wssdata.find({'DataDate': self.gl.str_today}, {'_id': 0})
        )
        self.dict_fmtted_wssdata_of_today = df_fmtted_wssdata_today.set_index('WindCode').to_dict()

    def dld_data_from_email(self, acctidbymxz):
        dict_acctinfo = self.gl.col_acctinfo.find_one({'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz})
        broker_abbr = dict_acctinfo['BrokerAbbr']
        dirpath_pretrddata_md = f'D:/projects/autot0/data/input/pretrddata/market_data/{broker_abbr}'

        if broker_abbr in ['hait']:
            email_subject = '每日券池信息'
            self.gl.update_attachments_from_email(
                email_subject, f'{self.gl.str_today}', dirpath_pretrddata_md, date_in_fn=1
            )
        elif broker_abbr in ['huat']:
            pass
        elif broker_abbr in ['zx']:
            pass
        else:
            raise ValueError('Unknown broker_abbr.')

    def upload_pretrd_rawdata(self, acctidbymxz):
        broker_abbr = self.gl.col_acctinfo.find_one(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        )['BrokerAbbr']

        # 上传信号券池原始数据: 目标券池 - 排除券池
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
        self.gl.col_pretrd_rawdata_excluded_secids.delete_many({'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz})
        if list_dicts_secids_excluded:
            self.gl.col_pretrd_rawdata_excluded_secids.insert_many(list_dicts_secids_excluded)

        dict_acctinfo = self.gl.col_acctinfo.find_one(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        )
        fpath_pretrd_md_private_secloan = dict_acctinfo['PreTradeDataFilePath']['PrivateSecurityLoanMarketData']
        fpath_pretrd_md_public_secloan = dict_acctinfo['PreTradeDataFilePath']['PublicSecurityLoanMarketData']

        if broker_abbr in ['huat']:
            # 上传融券券池-普通融券市场行情
            with open(fpath_pretrd_md_public_secloan, 'rb') as f:
                list_datalines = f.readlines()
                list_fields = [_.decode('ansi').replace('"', '').replace('=', '') for _ in list_datalines[0].split(b'\t')]
                list_dicts_md_public_secloan = []
                for datalines in list_datalines[1:]:
                    list_data = [_.decode('ansi').replace('"', '').replace('=', '') for _ in datalines.split(b'\t')]
                    dict_md_public_secloan = dict(zip(list_fields, list_data))
                    dict_md_public_secloan['DataDate'] = self.gl.str_today
                    dict_md_public_secloan['AcctIDByMXZ'] = acctidbymxz
                    list_dicts_md_public_secloan.append(dict_md_public_secloan)

            # 上传融券券池-专项融券市场行情
            df_csv_md_private_secloan = pd.read_excel(
                fpath_pretrd_md_private_secloan,
                dtype={'委托数量': float, '委托期限': int, '委托利率': float},
                converters={'证券代码': lambda x: str(x).zfill(6)}
            )
            list_dicts_md_private_secloan = df_csv_md_private_secloan.to_dict('records')
            for dict_md_private_secloan in list_dicts_md_private_secloan:
                dict_md_private_secloan['DataDate'] = self.gl.str_today
                dict_md_private_secloan['AcctIDByMXZ'] = acctidbymxz

        elif broker_abbr in ['hait']:
            # 上传融券券池-普通融券市场行情
            fpath_pretrd_md_public_secloan = fpath_pretrd_md_public_secloan.replace('<YYYYMMDD>', self.gl.str_today)
            df_xlsx_public_secloan = pd.read_excel(fpath_pretrd_md_public_secloan, sheet_name='公共券池', dtype='str')
            list_dicts_md_public_secloan = df_xlsx_public_secloan.to_dict('records')
            
            # 上传融券券池-专项融券市场行情
            fpath_pretrd_md_private_secloan = fpath_pretrd_md_private_secloan.replace('<YYYYMMDD>', self.gl.str_today)
            df_xlsx_private_secloan = pd.read_excel(
                fpath_pretrd_md_private_secloan, sheet_name='即时可用券池', dtype='str'
            )
            list_dicts_md_private_secloan = df_xlsx_private_secloan.to_dict('records')

        else:
            raise ValueError('Unknown broker alias.')

        self.gl.col_pretrd_rawdata_md_private_secloan.delete_many(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        )
        if list_dicts_md_private_secloan:
            self.gl.col_pretrd_rawdata_md_private_secloan.insert_many(list_dicts_md_private_secloan)

        self.gl.col_pretrd_rawdata_md_public_secloan.delete_many(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        )
        if list_dicts_md_public_secloan:
            self.gl.col_pretrd_rawdata_md_public_secloan.insert_many(list_dicts_md_public_secloan)

    def upload_pretrd_fmtdata(self, acctidbymxz):
        # tgtsecids
        dict_secid2cpssrc = {}
        for dict_excluded_secid in self.gl.col_pretrd_rawdata_excluded_secids.find(
                {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        ):
            dict_secid2cpssrc.update({dict_excluded_secid['SecurityID']: dict_excluded_secid['Composite']})

        list_pretrd_fmtdata_tgtsecids = []
        for dict_pretrd_rawdata_tgtsecids in self.gl.col_pretrd_rawdata_tgtsecids.find(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        ):
            secid = dict_pretrd_rawdata_tgtsecids['SecurityID']
            windcode = self.gl.get_secid2windcode(secid)
            if secid in dict_secid2cpssrc:
                excluded_mark = 1
                cpssrc = dict_secid2cpssrc[secid]
            else:
                excluded_mark = 0
                cpssrc = 'AutoT0'

            dict_pretrd_fmtdata_tgtsecids = {
                'DataDate': self.gl.str_today,
                'AcctIDByMXZ': acctidbymxz,
                'SecurityID': secid,
                'WindCode': windcode,
                'ExcludedMark': excluded_mark,
                'CompositeSource': cpssrc,
            }
            list_pretrd_fmtdata_tgtsecids.append(dict_pretrd_fmtdata_tgtsecids)
        self.gl.col_pretrd_fmtdata_tgtsecids.delete_many({'DataDate': self.gl.str_today})
        if list_pretrd_fmtdata_tgtsecids:
            self.gl.col_pretrd_fmtdata_tgtsecids.insert_many(list_pretrd_fmtdata_tgtsecids)

        # col_pretrd_fmtdata_md_secloan
        broker_abbr = acctidbymxz.split('_')[2]
        list_pretrd_fmtdata_md_security_loan = []

        if broker_abbr in ['huat']:
            # 默认普通券池的MD原格式默认为高级版导出格式，专项券池的MD原格式为matic导出的格式
            # 将普通券池与专项券池合并，并以secloan_src加以区分: public/private
            for dict_rawdata_public_secloan in self.gl.col_pretrd_rawdata_md_public_secloan.find(
                    {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
            ):
                secid = dict_rawdata_public_secloan['证券代码'].zfill(6)
                avlqty = float(dict_rawdata_public_secloan['可卖数量'])
                if avlqty:
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
                secid = dict_rawdata_private_secloan['证券代码']
                avlqty = float(dict_rawdata_private_secloan['委托数量'])
                secloan_term = int(dict_rawdata_private_secloan['委托期限'])
                secloan_ir = float(dict_rawdata_private_secloan['委托利率'])
                if secloan_ir > 1:
                    secloan_ir = secloan_ir / 100
                if avlqty:
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
                        'SecurityLoanTerm': secloan_term,
                        'SecurityLoanInterestRate': secloan_ir,
                        'SecurityLoanSource': secloan_src
                    }
                    list_pretrd_fmtdata_md_security_loan.append(dict_pretrd_fmtdata_md_security_loan)

            self.gl.col_pretrd_fmtdata_md_security_loan.delete_many(
                {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
            )
            if list_pretrd_fmtdata_md_security_loan:
                self.gl.col_pretrd_fmtdata_md_security_loan.insert_many(list_pretrd_fmtdata_md_security_loan)

        elif broker_abbr in ['hait']:
            for dict_rawdata_public_secloan in self.gl.col_pretrd_rawdata_md_public_secloan.find(
                    {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
            ):
                secid = dict_rawdata_public_secloan['证券代码'].zfill(6)
                avlqty = float(dict_rawdata_public_secloan['数量'])
                if avlqty:
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
                secid = dict_rawdata_private_secloan['证券代码']
                avlqty = float(dict_rawdata_private_secloan['未分配数量'])
                secloan_term = (
                        dict_rawdata_private_secloan['合约到期日'].strftime('%Y%m%d')
                        - self.gl.str_today.strftime('%Y%m%d')
                        + 1
                )
                secloan_ir = float(dict_rawdata_private_secloan['客户费率'])

                if secloan_ir > 1:
                    secloan_ir = secloan_ir / 100
                if avlqty:
                    secloan_src = 'Private_SSQ'  # 海通实际叫做“即时可用券池”
                    dict_pretrd_fmtdata_md_security_loan = {
                        'DataDate': self.gl.str_today,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'AvlQty': avlqty,
                        'SecurityLoanTerm': secloan_term,
                        'SecurityLoanInterestRate': secloan_ir,
                        'SecurityLoanSource': secloan_src
                    }
                    list_pretrd_fmtdata_md_security_loan.append(dict_pretrd_fmtdata_md_security_loan)

            self.gl.col_pretrd_fmtdata_md_security_loan.delete_many(
                {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
            )
            if list_pretrd_fmtdata_md_security_loan:
                self.gl.col_pretrd_fmtdata_md_security_loan.insert_many(list_pretrd_fmtdata_md_security_loan)

        else:
            raise ValueError('Unknown broker abbr.')

    def upload_secloan_demand_analysis(self, acctidbymxz):
        # 1. 海通: 公用券池，私用券池(即使可用券池，预约券池(核准制))； 2. 华泰: 普通券池，专项券池（实时券池，竞拍券池）
        broker_abbr = acctidbymxz.split('_')[2]
        list_tgtwindcodes = [
            _['WindCode'] for _ in self.gl.col_pretrd_fmtdata_tgtsecids.find(
                {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz, 'ExcludedMark': 0}
            )
        ]

        list_dicts_secloan_demand_analysis = []
        # 1. 遍历tgtwindcodes:
        for tgtwindcode in list_tgtwindcodes:
            tgtsecid = tgtwindcode.split('.')[0]
            margin_or_not_mark = self.gl.dict_fmtted_wssdata_today['MarginOrNotMark'][tgtwindcode]
            symbol = self.gl.dict_fmtted_wssdata_today['Symbol'][tgtwindcode]
            preclose = self.gl.dict_fmtted_wssdata_today['PreClose'][tgtwindcode]
            # 先遍历tgtwindcodes, 添加新增加的券
            # 再遍历已锁定合约列表, 得到应减少的券
            if broker_abbr in ['huat']:
                # market_data
                # # 华泰融券策略: 先约实时券（时间优先，价格优先），再约竞拍券(价格优先)
                public_secloan_avlqty = 0
                private_ssq_secloan_avlqty = 0
                private_jpq_secloan_avlqty = 0
                for dict_pretrd_fmtdata_md_secloan in self.gl.col_pretrd_fmtdata_md_security_loan.find(
                        {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz, 'SecurityID': tgtsecid}
                ):
                    secloan_src = dict_pretrd_fmtdata_md_secloan['SecurityLoanSource']
                    if secloan_src in ['Private_SSQ']:
                        private_ssq_secloan_avlqty += dict_pretrd_fmtdata_md_secloan['AvlQty']
                    elif secloan_src in ['Private_JPQ']:
                        private_jpq_secloan_avlqty += dict_pretrd_fmtdata_md_secloan['AvlQty']
                    elif secloan_src in ['Public']:
                        public_secloan_avlqty += dict_pretrd_fmtdata_md_secloan['AvlQty']
                    else:
                        raise ValueError('Unknown security loan avlqty.')

                private_secloan_avlqty = private_ssq_secloan_avlqty + private_jpq_secloan_avlqty

                # Quota:
                # # RealTimeQuota:
                public_secloan_quota = 0

                for dict_trade_fmtdata_public_secloan in self.gl.col_trade_fmtdata_public_secloan.find(
                    {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz, 'SecurityID': tgtsecid}
                ):
                    public_secloan_quota += dict_trade_fmtdata_public_secloan['QtyToBeChargedInterest']

                private_secloan_quota = 0
                for dict_trade_fmtdata_private_secloan in self.gl.col_trade_fmtdata_private_secloan.find(
                    {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz, 'SecurityID': tgtsecid}
                ):
                    private_secloan_quota += dict_trade_fmtdata_private_secloan['QtyToBeChargedInterest']

                secloan_quota = public_secloan_quota + private_secloan_quota

                # # QuotaOnLastTrdDate
                quota_last_trddate = 0

                for dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate in (
                        self.gl.col_posttrd_fmtdata_ssquota_from_secloan.find(
                            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': acctidbymxz, 'SecurityID': tgtsecid}
                        )
                ):
                    quota_last_trddate += dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate['SSQuota']

                # 根据tgtamt计算tgtqty
                tgtamt = 200000  # 每只股票标杆20万元，近似后取ceiling
                tgtqty = ceil((tgtamt / preclose) / 100) * 100
                # 由于现有交易系统无法进行挂单成交，故对盘口价差大的股票交易成本很大，导致实际收益与策略收益偏离。暂时不建仓。
                if preclose <= 5:
                    tgtqty = 0
                if tgtsecid[:3] in ['688']:
                    tgtqty = 0  # 公司集中交易系统无法交易200股以下，不借入科创板股票
                if not margin_or_not_mark:
                    tgtqty = 0

                # 借券顺序: 1. public secloan 2. private secloan(SSQ -> JPQ) 3. outside secloan
                ordqty_from_public_secloan = max(min(tgtqty - secloan_quota, public_secloan_avlqty), 0)
                ordqty_from_private_ssq_secloan = max(
                    min(tgtqty - secloan_quota - ordqty_from_public_secloan, private_ssq_secloan_avlqty), 0
                )
                ordqty_from_private_jpq_secloan = max(
                    min(
                        tgtqty - secloan_quota - ordqty_from_public_secloan - ordqty_from_private_ssq_secloan,
                        private_jpq_secloan_avlqty
                    ),
                    0
                )
                ordqty_from_private_secloan = ordqty_from_private_ssq_secloan + ordqty_from_private_jpq_secloan
                ordqty_from_outside_secloan = max(
                    tgtqty - secloan_quota - ordqty_from_public_secloan - ordqty_from_private_secloan, 0
                )

                # 计算需要终止的融券额度(由于目标券池发生变化导致): 目标数量 - 最新已持有数量
                quota2terminate = max(secloan_quota - tgtqty, 0)

                dict_secloan_demand_analysis = {
                    'DataDate': self.gl.str_today,
                    'UpdateTime': datetime.now().strftime('%H%M%S'),
                    'AcctIDByMXZ': acctidbymxz,
                    'SecurityID': tgtsecid,
                    'WindCode': tgtwindcode,
                    'Symbol': symbol,
                    'TgtQty': tgtqty,
                    'MarginOrNotMark': margin_or_not_mark,
                    'AvlQtyInPrivatePool': private_secloan_avlqty,
                    'AvlQtyInPublicPool': public_secloan_avlqty,
                    'QuotaRealTime': secloan_quota,
                    'QuotaToTerminate': quota2terminate,
                    'QuotaOnLastTrdDate': quota_last_trddate,
                    'OrdQtyFromPublicSecLoan': ordqty_from_public_secloan,
                    'OrdQtyFromPrivateSecLoan': ordqty_from_private_secloan,
                    'OrdQtyFromPrivateSSQSecLoan': ordqty_from_private_ssq_secloan,
                    'OrdQtyFromPrivateJPQSecLoan': ordqty_from_private_jpq_secloan,
                    'OrdQtyFromOutsideSource': ordqty_from_outside_secloan,
                }
                list_dicts_secloan_demand_analysis.append(dict_secloan_demand_analysis)

            elif broker_abbr in ['hait']:
                # market_data
                # 海通融券策略:
                # # 1. 公用券池(不约) → 私用券池(即时可用券)
                # # 2. 数据与交易都是日更新
                public_secloan_avlqty = 0
                private_ssq_secloan_avlqty = 0
                private_jpq_secloan_avlqty = 0
                for dict_pretrd_fmtdata_md_secloan in self.gl.col_pretrd_fmtdata_md_security_loan.find(
                        {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz, 'SecurityID': tgtsecid}
                ):
                    secloan_src = dict_pretrd_fmtdata_md_secloan['SecurityLoanSource']
                    if secloan_src in ['Private_SSQ']:
                        private_ssq_secloan_avlqty += dict_pretrd_fmtdata_md_secloan['AvlQty']
                    elif secloan_src in ['Private_JPQ']:
                        private_jpq_secloan_avlqty += dict_pretrd_fmtdata_md_secloan['AvlQty']
                    elif secloan_src in ['Public']:
                        public_secloan_avlqty += dict_pretrd_fmtdata_md_secloan['AvlQty']
                    else:
                        raise ValueError('Unknown security loan avlqty.')

                private_secloan_avlqty = private_ssq_secloan_avlqty + private_jpq_secloan_avlqty

                # # QuotaOnLastTrdDate
                quota_last_trddate = 0
                for dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate in (
                        self.gl.col_posttrd_fmtdata_ssquota_from_secloan.find(
                            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': acctidbymxz, 'SecurityID': tgtsecid}
                        )
                ):
                    quota_last_trddate += dict_posttrd_fmtdata_ssquota_from_secloan_last_trddate['SSQuota']

                # 根据tgtamt计算tgtqty
                tgtamt = 200000  # 每只股票标杆20万元，近似后取ceiling
                tgtqty = ceil((tgtamt / preclose) / 100) * 100
                # 由于现有交易系统无法进行挂单成交，故对盘口价差大的股票交易成本很大，导致实际收益与策略收益偏离。暂时不建仓。
                if preclose <= 5:
                    tgtqty = 0
                if tgtsecid[:3] in ['688']:
                    tgtqty = 0  # 公司集中交易系统无法交易200股以下，不借入科创板股票
                if not margin_or_not_mark:
                    tgtqty = 0

                # 借券顺序: 1. public secloan 2. private secloan(SSQ) 3. outside secloan
                ordqty_from_public_secloan = max(min(tgtqty - quota_last_trddate, public_secloan_avlqty), 0)
                ordqty_from_private_ssq_secloan = max(
                    min(tgtqty - quota_last_trddate - ordqty_from_public_secloan, private_ssq_secloan_avlqty), 0
                )
                ordqty_from_private_jpq_secloan = max(
                    min(
                        tgtqty - quota_last_trddate - ordqty_from_public_secloan - ordqty_from_private_ssq_secloan,
                        private_jpq_secloan_avlqty
                    ),
                    0
                )
                ordqty_from_private_secloan = ordqty_from_private_ssq_secloan + ordqty_from_private_jpq_secloan
                ordqty_from_outside_secloan = max(
                    tgtqty - quota_last_trddate - ordqty_from_public_secloan - ordqty_from_private_secloan, 0
                )

                # 计算需要终止的融券额度(由于目标券池发生变化导致): 目标数量 - 最新已持有数量
                quota2terminate = max(quota_last_trddate - tgtqty, 0)

                dict_secloan_demand_analysis = {
                    'DataDate': self.gl.str_today,
                    'UpdateTime': datetime.now().strftime('%H%M%S'),
                    'AcctIDByMXZ': acctidbymxz,
                    'SecurityID': tgtsecid,
                    'WindCode': tgtwindcode,
                    'Symbol': symbol,
                    'TgtQty': tgtqty,
                    'MarginOrNotMark': margin_or_not_mark,
                    'AvlQtyInPrivatePool': private_secloan_avlqty,
                    'AvlQtyInPublicPool': public_secloan_avlqty,
                    'QuotaRealTime': 'N/A',
                    'QuotaToTerminate': quota2terminate,
                    'QuotaOnLastTrdDate': quota_last_trddate,
                    'OrdQtyFromPublicSecLoan': ordqty_from_public_secloan,
                    'OrdQtyFromPrivateSecLoan': ordqty_from_private_secloan,
                    'OrdQtyFromPrivateSSQSecLoan': ordqty_from_private_ssq_secloan,
                    'OrdQtyFromPrivateJPQSecLoan': ordqty_from_private_jpq_secloan,
                    'OrdQtyFromOutsideSource': ordqty_from_outside_secloan,
                }
                list_dicts_secloan_demand_analysis.append(dict_secloan_demand_analysis)

            else:
                raise ValueError('Unknown broker abbr.')

        # 2. 遍历已持有合约
        if broker_abbr in ['huat']:
            iter_secloan = self.gl.col_trade_ssquota_from_secloan.find(
                {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
            )
        elif broker_abbr in ['hait']:
            iter_secloan = self.gl.col_posttrd_fmtdata_ssquota_from_secloan.find(
                {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': acctidbymxz}
            )
        else:
            raise ValueError('Unknown broker abbr.')

        for dict_secloan in iter_secloan:
            secid = dict_secloan['SecurityID']
            if secid[0] in ['6', '5']:
                windcode = f"{secid}.SH"
            elif secid[0] in ['3', '0']:
                windcode = f"{secid}.SZ"
            else:
                raise ValueError('Unknown security id')
            symbol = self.gl.dict_fmtted_wssdata_today['Symbol'][windcode]
            margin_or_not_mark = self.gl.dict_fmtted_wssdata_today['MarginOrNotMark'][windcode]

            if windcode not in list_tgtwindcodes:
                ssquota = dict_secloan['SSQuota']
                quota2terminate = ssquota

                dict_secloan_demand_analysis = {
                    'DataDate': self.gl.str_today,
                    'UpdateTime': datetime.now().strftime('%H%M%S'),
                    'AcctIDByMXZ': acctidbymxz,
                    'SecurityID': secid,
                    'WindCode': windcode,
                    'Symbol': symbol,
                    'TgtQty': 0,
                    'MarginOrNotMark': margin_or_not_mark,
                    'AvlQtyInPrivatePool': 'N/A',
                    'AvlQtyInPublicPool': 'N/A',
                    'QuotaRealTime': ssquota,
                    'QuotaToTerminate': quota2terminate,
                    'QuotaOnLastTrdDate': 'N/A',
                    'OrdQtyFromPublicSecLoan': - dict_secloan['SSQuotaFromPublicSecPool'],
                    'OrdQtyFromPrivateSecLoan': - dict_secloan['SSQuotaFromPrivateSecPool'],
                    'OrdQtyFromPrivateSSQSecLoan': 'N/A',
                    'OrdQtyFromPrivateJPQSecLoan': 'N/A',
                    'OrdQtyFromOutsideSource': 'N/A',
                }
                list_dicts_secloan_demand_analysis.append(dict_secloan_demand_analysis)

        df_secloan_demand_analysis = pd.DataFrame(list_dicts_secloan_demand_analysis)
        df_secloan_demand_analysis.to_csv(
            self.gl.fpath_output_csv_secloan_demand_analysis, index=False, encoding='ansi'
        )

        self.gl.col_pretrd_secloan_demand_analysis.delete_many(
            {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
        )
        if list_dicts_secloan_demand_analysis:
            self.gl.col_pretrd_secloan_demand_analysis.insert_many(list_dicts_secloan_demand_analysis)
        print(f"{self.gl.str_today}_secloan_demand_analysis Finished.")

    def output_secloan_order(self, acctidbymxz):
        dict_acctinfo = self.gl.col_acctinfo.find_one({'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz})
        acctidbybroker = dict_acctinfo['AcctIDByBroker']
        broker_abbr = acctidbymxz.split('_')[2]
        if broker_abbr in ['huat']:
            # 华泰的order是报在融券通中的order。别忘了还有普通券池的order
            iter_col_pretrd_secloan_demand_analysis = self.gl.col_pretrd_secloan_demand_analysis.find(
                {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
            )

            list_dicts_secloan_order = []
            for dict_pretrd_secloan_demand_analysis in iter_col_pretrd_secloan_demand_analysis:
                tgtqty = dict_pretrd_secloan_demand_analysis['TgtQty']
                if tgtqty:
                    secid = dict_pretrd_secloan_demand_analysis['SecurityID']
                    if secid[0] in ['6']:
                        exchange = 'SH'
                    elif secid[0] in ['3', '0']:
                        exchange = 'SZ'
                    else:
                        raise ValueError('Unknown exchange.')

                    # SSQ
                    tgtordqty_from_private_ssq_secloan = (
                        dict_pretrd_secloan_demand_analysis['OrdQtyFromPrivateSSQSecLoan']
                    )
                    if tgtordqty_from_private_ssq_secloan:
                        # 期限匹配
                        iter_col_pretrd_md_private_ssq_secloan = (
                            self.gl.col_pretrd_fmtdata_md_security_loan.find(
                                {
                                    'DataDate': self.gl.str_today,
                                    'AcctIDByMXZ': acctidbymxz,
                                    'SecurityID': secid,
                                    'SecurityLoanSource': 'Private_SSQ'
                                }
                            ).sort('SecurityLoanTerm', -1)
                        )
                        # 从期限最长的开始借
                        for dict_md_private_ssq_secloan in iter_col_pretrd_md_private_ssq_secloan:
                            secloan_term = dict_md_private_ssq_secloan['SecurityLoanTerm']
                            if secloan_term >= 31:
                                continue
                            avlqty = dict_md_private_ssq_secloan['AvlQty']
                            ordqty = ceil(min(tgtordqty_from_private_ssq_secloan, avlqty)/100)*100
                            # todo 华泰的order最少1000，根据30万元上限，将700的order近似为1000
                            if 700 <= ordqty < 1000:
                                ordqty = 1000

                            if ordqty < 700:
                                continue
                            ir = dict_md_private_ssq_secloan['SecurityLoanInterestRate']
                            dict_secloan_order = {
                                '资产账号': acctidbybroker,
                                '证券代码': secid,
                                '市场': exchange,
                                '申报类型': '非约定申报',
                                '约定号': '',
                                '期限': secloan_term,
                                '利率': ir,
                                '数量': ordqty,
                            }
                            list_dicts_secloan_order.append(dict_secloan_order)
                            tgtordqty_from_private_ssq_secloan = tgtordqty_from_private_ssq_secloan - ordqty

                    # JPQ
                    tgtordqty_from_private_jpq_secloan = (
                        dict_pretrd_secloan_demand_analysis['OrdQtyFromPrivateJPQSecLoan']
                    )
                    if tgtordqty_from_private_jpq_secloan:
                        # 期限匹配
                        iter_col_pretrd_md_private_jpq_secloan = (
                            self.gl.col_pretrd_fmtdata_md_security_loan.find(
                                {
                                    'DataDate': self.gl.str_today,
                                    'AcctIDByMXZ': acctidbymxz,
                                    'SecurityID': secid,
                                    'SecurityLoanSource': 'Private_JPQ'
                                }
                            ).sort('SecurityLoanTerm', -1)
                        )
                        # 从期限最长的开始借
                        for dict_md_private_jpq_secloan in iter_col_pretrd_md_private_jpq_secloan:
                            secloan_term = dict_md_private_jpq_secloan['SecurityLoanTerm']
                            if secloan_term >= 31:
                                continue
                            avlqty = dict_md_private_jpq_secloan['AvlQty']
                            ordqty = ceil(min(tgtordqty_from_private_jpq_secloan, avlqty)/100)*100

                            if 700 <= ordqty < 1000:
                                ordqty = 1000

                            if ordqty < 700:
                                continue
                            ir = 0.09  # todo 最多接受0.09 但需要改进，需要获得融入券息报价，然后参考之报价
                            dict_secloan_order = {
                                '资产账号': acctidbybroker,
                                '证券代码': secid,
                                '市场': exchange,
                                '申报类型': '非约定申报',
                                '约定号': '',
                                '期限': secloan_term,
                                '利率': ir,
                                '数量': ordqty,
                            }
                            list_dicts_secloan_order.append(dict_secloan_order)
                            tgtordqty_from_private_jpq_secloan = tgtordqty_from_private_jpq_secloan - ordqty

                    # OrdQtyFromOutsideSource
                    tgtordqty_from_outside_secloan = (
                        dict_pretrd_secloan_demand_analysis['OrdQtyFromOutsideSource']
                    )
                    if tgtordqty_from_outside_secloan:
                        # 报14天与28天的期限，0.085的利率，对半数量申请
                        ordqty_from_outside_src = dict_pretrd_secloan_demand_analysis['OrdQtyFromOutsideSource']
                        if 1000 <= ordqty_from_outside_src < 2000:
                            dict_secloan_order = {
                                '资产账号': acctidbybroker,
                                '证券代码': secid,
                                '市场': exchange,
                                '申报类型': '非约定申报',
                                '约定号': '',
                                '期限': 14,
                                '利率': 0.085,
                                '数量': ordqty_from_outside_src,
                            }
                            list_dicts_secloan_order.append(dict_secloan_order)

                        elif 700 <= ordqty_from_outside_src < 1000:
                            dict_secloan_order = {
                                '资产账号': acctidbybroker,
                                '证券代码': secid,
                                '市场': exchange,
                                '申报类型': '非约定申报',
                                '约定号': '',
                                '期限': 14,
                                '利率': 0.085,
                                '数量': 1000,
                            }
                            list_dicts_secloan_order.append(dict_secloan_order)

                        elif ordqty_from_outside_src <= 700:
                            continue

                        else:
                            list_secloan_terms = [14, 28]
                            for secloan_term in list_secloan_terms:
                                ordqty = (
                                        (round(dict_pretrd_secloan_demand_analysis['OrdQtyFromOutsideSource']/200, 0))
                                        * 100
                                )
                                dict_secloan_order = {
                                    '资产账号': acctidbybroker,
                                    '证券代码': secid,
                                    '市场': exchange,
                                    '申报类型': '非约定申报',
                                    '约定号': '',
                                    '期限': secloan_term,
                                    '利率': 0.085,
                                    '数量': ordqty,
                                }
                                list_dicts_secloan_order.append(dict_secloan_order)

            df_secloan_order = pd.DataFrame(list_dicts_secloan_order)
            fn_secloan_order = f'{acctidbymxz}_secloan_order.csv'
            fpath_output_secloan_order = os.path.join(self.gl.dirpath_output_secloan_order, fn_secloan_order)
            df_secloan_order.to_csv(fpath_output_secloan_order, index=False, encoding='ansi')
            df_secloan_order['DataDate'] = self.gl.str_today
            df_secloan_order['AcctIDByMXZ'] = acctidbymxz
            list_dicts_secloan_order_2b_updated = df_secloan_order.to_dict('records')
            print('批量融券单已生成.')
            self.gl.col_trade_secloan_order.delete_many({'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz})
            if list_dicts_secloan_order_2b_updated:
                self.gl.col_trade_secloan_order.insert_many(list_dicts_secloan_order_2b_updated)

        elif broker_abbr in ['hait']:
            str_time_now = datetime.now().strftime('%H%M')
            if str_time_now < '0900':
                str_date = self.gl.str_today
            else:
                str_date = self.gl.str_next_trddate

            list_dicts_secloan_order = []
            for dict_pretrd_secloan_demand_analysis in self.gl.col_pretrd_secloan_demand_analysis.find(
                    {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
            ):
                ordqty_from_outside_secloan = dict_pretrd_secloan_demand_analysis['OrdQtyFromOutsideSource']
                if ordqty_from_outside_secloan > 0:
                    secid = dict_pretrd_secloan_demand_analysis['SecurityID']
                    # 筛选最小数量限制：外界询券状态下，非双创最少10000股，双创最少1000股
                    if secid[:3] in ['688']:
                        continue
                    if (secid[:3] not in ['688', '300']) and ordqty_from_outside_secloan < 10000:
                        continue
                    if (secid[:3] in ['688', '300']) and ordqty_from_outside_secloan < 1000:
                        continue
                    symbol = dict_pretrd_secloan_demand_analysis['Symbol']
                    dict_secloan_order = {
                        '证券代码': secid,
                        '证券名称': symbol,
                        '需求数量（股）': ordqty_from_outside_secloan,
                        '期限（天）': 28,
                        '营业部名称': '南京常府街',
                        '客户号': '1880921905',
                        '客户名称': '鸣石满天星三号1期',
                        '筹券日期': str_date,
                        '特殊备注': '',
                    }
                    list_dicts_secloan_order.append(dict_secloan_order)

            fn_secloan_order = f'{acctidbymxz}_secloan_order.xlsx'
            fpath_output_secloan_order = os.path.join(self.gl.dirpath_output_secloan_order, fn_secloan_order)
            df_demand_of_secpool_from_outside_src = pd.DataFrame(list_dicts_secloan_order)
            df_demand_of_secpool_from_outside_src.to_excel(
                fpath_output_secloan_order,
                index=False,
                sheet_name='券源需求',
            )
            print('券源需求已生成.')

            # 邮件发送
            self.gl.send_file_via_email(
                self.gl.email_addr_to_hait,
                self.gl.email_subject_to_hait,
                fpath_output_secloan_order,
                '需要预约的券源需求.xlsx'
            )

        else:
            raise ValueError('Unknown broker abbr.')

    def run(self):
        # 在运行pretrd之前，需要先运行T日的post_trddata_mng, 将T-1的清算数据上传
        for acctidbymxz in self.list_acctidsbymxz:
            self.dld_data_from_email(acctidbymxz)
            self.upload_pretrd_rawdata(acctidbymxz)
            self.upload_pretrd_fmtdata(acctidbymxz)
            self.upload_secloan_demand_analysis(acctidbymxz)
            self.output_secloan_order(acctidbymxz)
        print('PreTrdMng Finished.')


if __name__ == '__main__':
    task = PreTrdMng(download_winddata_mark=0)
    task.run()
