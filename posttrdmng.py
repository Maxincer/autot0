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
    1. T日处理T-1日的post_trddata。
    2. 本项目中约定 T 即为 gl.str_today。
    3. 重要业务假设, 人工T0策略拟定目标券池后，机器T0策略将从机器T0目标券池中剔除属于人工T0券池的标的。

Steps:
    1. 维护交割单数据库 - 真准完
        1. Mode1, 全量写入。
        2. Mode2, 增量写入。
    1. 在T日计算T-1日的PNL:
        1. Input Data:
            模式1: T-1日的持仓, T-1日的融券明细(未了结合约), T-1日的交割单; T-1日的融券费用; 行情数据_收盘价
            模式2: 策略开始日至今的交割单, 计算T-2日清算后position, 在结合T-1日交割单计算T-1日PNL。
        2. PNL
            1. PNL = 持仓盈亏 + 平仓盈亏 + fee_from_trdactivity + interest
            2. CF = 资金明细
            PNL与CF核对
        3. Output Data:

    2. check:
        1. 使用资金明细与交割单对账
        2. 注意融券占用费扣款为1个月1次

Input Data: post trading data 清算后数据
    1. Source:
        1. 融券明细 - 未了结合约
        2. 担保品持仓
        3. 当日委托
        4. 交割单

Note:
    1. post-trade 的input data为清算后数据, 具体为:
        1. 对账单数据
        2. 估值表数据
        3. 券息发生额及应付利息数据
    2. trade 的 input data 为资金数据、委托数据、持仓数据， 这个是实时数据
    3. 交割单数据需要使用营业部柜台数据，否则少费用科目
    4. 邮件数据的处理，一定是先下载为文件， 后读取处理
    5. 注意区分 position 与 holding
    6. 重要假设： 公用券池的未平仓合约由未结利息推出
    7. 重要约定： QtyToBeChargedInterest，在公用合约中为未还数量，在私用合约中为占用额度数量
    8. 重要假定： 盘前运行（T日清算T-1的损益）

Warning:
    1. 海通证券e海通财在晚上10点导出的清算后的私用券源合约是不准确的，应该以次日8:30发送的邮件中的信息为准。
        这使得只能T日早上处理T-1日的posttrddata。

Naming Convention:
    1. jgd: 交割单, 尚未找到标准英文
    2. DataDate: 为数据的发生日期，非录入日期。如T日录入的T-1日清算后数据，DataDate应为T-1日。
# todo
    1. 目前假设只有一个账户需要处理，需要扩展到多账户
    2. 目前人工区分公用合约与私用合约。替代方案：可在T+1日收到的利息结算单中根据流水号，反推T日的合约构成。
    3. position 中 添加SecurityType 和 SecurityIDSource 字段
"""
import matplotlib.pyplot as plt
import pandas as pd

from globals import Globals, STR_TODAY


class PostTrdMng:
    def __init__(self, str_trddate=STR_TODAY, download_winddata_mark=0):
        self.gl = Globals(str_trddate, download_winddata_mark=download_winddata_mark)
        # self.gl.update_attachments_from_email(
        #     f'鸣石满天星7号清算后数据{self.gl.str_today}', f'{self.gl.str_today}', self.gl.dirpath_posttrddata_from_email
        # )

    def upload_posttrd_rawdata(self, acctidbymxz):
        """
        将需要的原始数据上传至数据库：
            1. holding
            2. secloan from private secpool： 注，该表中的收回数量指的是额度，不是空头数量。
            3. shortqty from secloan
            4. fee_from_secloan
        """

        dict_acctinfo = self.gl.col_acctinfo.find_one({'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz})
        data_srctype = dict_acctinfo['DataSourceType']
        dldfilter = dict_acctinfo['DownloadDataFilter']
        if not dldfilter:
            dldfilter = dict_acctinfo['AcctIDByBroker']

        list_fpaths_trade_data = [_.strip() for _ in dict_acctinfo['DataFilePath'][1:-1].split(',')]
        fpath_input_csv_margin_account_fund = list_fpaths_trade_data[0].replace('<YYYYMMDD>', self.gl.str_today)
        fpath_input_csv_margin_account_holding = list_fpaths_trade_data[1].replace(
            '<YYYYMMDD>', self.gl.str_last_trddate
        )
        fpath_input_csv_margin_account_order = list_fpaths_trade_data[2].replace(
            '<YYYYMMDD>', self.gl.str_last_trddate
        )
        fpath_input_csv_margin_account_rqmx = list_fpaths_trade_data[3].replace(
            '<YYYYMMDD>', self.gl.str_last_trddate
        )

        # posttrd_rawdata_fund
        if data_srctype in ['hait_ehtc']:
            df_input_rawdata_fund = pd.read_excel(
                self.gl.fpath_input_xlsx_fund,
                skiprows=1,
                nrows=1,
                dtype={
                    '资金余额': float,
                    '可用金额': float,
                    '融资市值': float,
                    '融券金额': float,
                    '总市值(公允价)': float,
                    '总资产(公允价)': float,
                    '净资产(公允价)': float,
                    '总负债': float,
                    '资金负债': float,
                    '融券负债': float,
                    '利息/费用': float,
                    '现金资产': float,
                    '担保证券市值': float,
                    '可用保证金': float,
                    '融资已用保证金': float,
                    '融券已用保证金': float,
                    '授信额度': float,
                    '融资保证金比例': float,
                    '融券保证金比例': float,
                    '维持担保比例(扣除非担保品)': str,
                },
            )

        elif data_srctype in ['hait_ehfz']:
            df_input_rawdata_fund = pd.read_csv(
                self.gl.fpath_input_rawdata_fund,
                dtype={
                    '资金账户': str,
                    '币种': str,
                    '可用金额': float,
                    '可取余额': float,
                    '冻结金额': float,
                    '证券市值': float,
                    '资金余额': float,
                    '资产总值': float,
                    '总盈亏': float
                },
            )

        elif data_srctype in ['huat_matic_tsi']:
            df_input_rawdata_fund = pd.read_csv(
                fpath_input_csv_margin_account_fund,
                dtype={'fund_account': str, 'assure_asset': float, 'market_value': float}
            )
            df_input_rawdata_fund = df_input_rawdata_fund[df_input_rawdata_fund['fund_account'] == dldfilter].copy()

        else:
            raise ValueError('Wrong data source type.')

        df_input_rawdata_fund = df_input_rawdata_fund.where(df_input_rawdata_fund.notnull(), None)
        df_input_rawdata_fund['DataDate'] = self.gl.str_last_trddate
        df_input_rawdata_fund['AcctIDByMXZ'] = self.gl.acctidbymxz
        list_dicts_rawdata_fund = df_input_rawdata_fund.to_dict('records')
        self.gl.col_posttrd_rawdata_fund.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_rawdata_fund:
            self.gl.col_posttrd_rawdata_fund.insert_many(list_dicts_rawdata_fund)

        # posttrd_rawdata_holding
        if data_srctype in ['hait_ehtc']:
            df_input_rawdata_holding = pd.read_excel(
                self.gl.fpath_input_xlsx_holding,
                skiprows=4,
                type={
                    '证券余额': float,
                    '证券可用': float,
                    '交易市场': str,
                    '股东账号': str
                },
                converters={'证券代码': lambda x: str(x).zfill(6)}
            )
        elif data_srctype in ['hait_ehfz']:
            df_input_rawdata_holding = pd.read_csv(
                self.gl.fpath_input_rawdata_holding,
                dtype={
                    '资金账号': str,
                    '市场': float,
                    '昨日持仓量': float,
                    '股份余额': float,
                    '可卖数量': float,
                    '可申购数量': float,
                    '当前拥股数量': float,
                    '成本价格': float,
                    '证券市值': float,
                    '浮动盈亏': float,
                },
                converters={'代码': lambda x: str(x).zfill(6)}
            )
        elif data_srctype in ['huat_matic_tsi']:
            df_input_rawdata_holding = pd.read_csv(
                fpath_input_csv_margin_account_holding,
                dtype={'fund_account': str, 'current_amount': float},
                converters={'stock_code': lambda x: str(x).zfill(6)}
            )
            df_input_rawdata_holding = (
                df_input_rawdata_holding[df_input_rawdata_holding['fund_account'] == dldfilter].copy()
            )

        else:
            raise ValueError('Wrong data source type.')


        df_input_rawdata_holding = df_input_rawdata_holding.where(df_input_rawdata_holding.notnull(), None)
        df_input_rawdata_holding['DataDate'] = self.gl.str_last_trddate
        df_input_rawdata_holding['AcctIDByMXZ'] = self.gl.acctidbymxz
        list_dicts_rawdata_holding = df_input_rawdata_holding.to_dict('records')
        self.gl.col_posttrd_rawdata_holding.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_rawdata_holding:
            self.gl.col_posttrd_rawdata_holding.insert_many(list_dicts_rawdata_holding)

        # posttrd_rawdata_secloan_from_private_secpool
        if data_srctype in ['hait_ehtc']:
            df_input_rawdata_rqmx = pd.read_excel(
                fpath_input_csv_margin_account_rqmx,
                dtype={
                    '营业部代码': str,
                    '客户号': str,
                    '证券账号': str,
                    '证券代码': str,
                    '私用转融券费率': float,
                    '发生日期': str,
                    '合约数量': float,
                    '收回数量': float,
                    '合约预计利息': float,
                    '合约理论到期日期': str,
                    '合约流水号': str,
                    '借入人合约号': str,
                    '出借人合约号': str,
                }
            )

        elif data_srctype in ['huat_matic_tsi']:
            df_input_rawdata_rqmx = pd.read_csv(fpath_input_csv_margin_account_rqmx, dtype='str')
        
        else: 
            raise ValueError('Wrong data source type.')

        df_input_rawdata_rqmx = df_input_rawdata_rqmx.where(df_input_rawdata_rqmx.notnull(), None)
        df_input_rawdata_rqmx['DataDate'] = self.gl.str_last_trddate
        df_input_rawdata_rqmx['AcctIDByMXZ'] = self.gl.acctidbymxz
        list_dicts_rawdata_rqmx = df_input_rawdata_rqmx.to_dict('records')
        self.gl.col_posttrd_rawdata_rqmx.delete_many({'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz})
        if list_dicts_rawdata_rqmx:
            self.gl.col_posttrd_rawdata_rqmx.insert_many(list_dicts_rawdata_rqmx)

        # posttrd_rawdata_secloan_from_public_secpool
        df_input_xlsx_secloan_from_public_secpool = pd.read_excel(
            self.gl.fpath_input_xlsx_secloan_from_public_secpool,
            dtype={
                '证券代码': str,
                '尚欠数量': float,
                '流水号': str,
                '发生日期': str,
                '到期日期': str,
                '展期次数': float,
                '年利率': float,
                '已还数量': float,
                '已还费用': float,
                '已计费用': float,
                '已还罚息': float,
                '融券负债市值': float,
                '融券卖出股数': float,
                '融券卖出价': float,
            }
        )
        df_input_xlsx_secloan_from_public_secpool = (
            df_input_xlsx_secloan_from_public_secpool.where(df_input_xlsx_secloan_from_public_secpool.notnull(), None)
        )
        df_input_xlsx_secloan_from_public_secpool['DataDate'] = self.gl.str_last_trddate
        df_input_xlsx_secloan_from_public_secpool['AcctIDByMXZ'] = self.gl.acctidbymxz
        list_dicts_secloan_from_public_secpool = df_input_xlsx_secloan_from_public_secpool.to_dict('records')
        self.gl.col_posttrd_rawdata_secloan_from_public_secpool.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_secloan_from_public_secpool:
            self.gl.col_posttrd_rawdata_secloan_from_public_secpool.insert_many(
                list_dicts_secloan_from_public_secpool
            )

        # posttrd_rawdata_shortqty_from_secloan
        df_xlsx_shortqty = pd.read_excel(
            self.gl.fpath_input_xlsx_shortqty, skiprows=1, dtype={'证券代码': str, '尚欠数量': float, '流水号': str}
        )
        df_xlsx_shortqty = df_xlsx_shortqty.where(df_xlsx_shortqty.notnull(), None)
        df_xlsx_shortqty['DataDate'] = self.gl.str_last_trddate
        df_xlsx_shortqty['AcctIDByMXZ'] = self.gl.acctidbymxz
        list_dicts_shortqty = df_xlsx_shortqty.to_dict('records')
        self.gl.col_posttrd_rawdata_shortqty_from_secloan.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_shortqty:
            self.gl.col_posttrd_rawdata_shortqty_from_secloan.insert_many(list_dicts_shortqty)

        # posttrd_rawdata_fee_from_secloan
        df_xls_fee_from_secloan = pd.read_excel(
            self.gl.fpath_input_xls_fee_from_secloan,
            dtype={
                '合约号': str,
                '发生日期': str,
                '流水号': str,
                '客户号': str,
                '计息日期': str,
                '利息': float,
                '固定额度费': float,
                '占用额度费': float,
                '提前还贷手续费': float,
                '逾期罚息': float,
                '坏账罚息': float,
                '未结总利息': float,
                '未结总固定额度费': float,
                '未结总占用额度费': float,
                '未结总提前还贷手续费': float,
                '未结总逾期罚息': float,
                '未结总坏账罚息': float,
            }
        )
        df_xls_fee_from_secloan = df_xls_fee_from_secloan.where(
            df_xls_fee_from_secloan.notnull(), None
        )
        df_xls_fee_from_secloan['DataDate'] = self.gl.str_last_trddate
        df_xls_fee_from_secloan['AcctIDByMXZ'] = self.gl.acctidbymxz
        list_dicts_fee_from_secloan = df_xls_fee_from_secloan.to_dict('records')
        self.gl.col_posttrd_rawdata_fee_from_secloan.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_fee_from_secloan:
            self.gl.col_posttrd_rawdata_fee_from_secloan.insert_many(list_dicts_fee_from_secloan)

        # posttrd_rawdata_jgd
        df_xlsx_jgd = pd.read_excel(
            self.gl.fpath_input_xlsx_jgd,
            dtype={
                '成交日期': str,
                '成交时间': str,
                '客户号': str,
                '计息日期': str,
                '证券账户': str,
                '证券代码': str,
                '证券数量': float,
                '证券余额': float,
                '成交价格': float,
                '成交金额': float,
                '资金余额': float,
                '结算价': float,
                '清算周期': str,
                '报盘席位': str,
                '报盘时间': str,
                '报盘合同号': str,
                '发生日期': str,
                '流水号': str,
                '成交编号': str,
                '经手费': float,
                '过户费': float,
                '证管费': float,
                '佣金': float,
                '印花税': float,
            }
        )
        df_xlsx_jgd = df_xlsx_jgd.where(df_xlsx_jgd.notnull(), None)
        df_xlsx_jgd['DataDate'] = self.gl.str_last_trddate
        df_xlsx_jgd['AcctIDByMXZ'] = self.gl.acctidbymxz
        list_dicts_posttrd_rawdata_jgd = df_xlsx_jgd.to_dict('records')
        self.gl.col_posttrd_rawdata_jgd.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_rawdata_jgd:
            self.gl.col_posttrd_rawdata_jgd.insert_many(list_dicts_posttrd_rawdata_jgd)

    def upload_posttrd_fmtdata(self):
        """
        将源数据格式format成为统一格式
        Output:
            1. position
            2. ssquota
        """
        # col_posttrd_fmtdata_fund
        list_dicts_posttrd_rawdata_fund = list(
            self.gl.col_posttrd_rawdata_fund.find(
                {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
        )
        list_dicts_posttrd_fmtdata_fund = []
        for dict_posttrd_rawdata_fund in list_dicts_posttrd_rawdata_fund:
            cash_balance = dict_posttrd_rawdata_fund['资金余额']
            cash_from_ss = dict_posttrd_rawdata_fund['融券金额']
            available_fund = dict_posttrd_rawdata_fund['可用金额']
            finance_amt = dict_posttrd_rawdata_fund['融资市值']
            ssamt = dict_posttrd_rawdata_fund['融券负债']
            secmv = dict_posttrd_rawdata_fund['总市值(公允价)']
            ttasset = dict_posttrd_rawdata_fund['总资产(公允价)']
            ttliability = dict_posttrd_rawdata_fund['总负债']
            liability_from_finance = dict_posttrd_rawdata_fund['资金负债']  # 资金负债与利息相等
            liability_from_secloan = dict_posttrd_rawdata_fund['融券负债'] + dict_posttrd_rawdata_fund['利息/费用']
            net_asset = dict_posttrd_rawdata_fund['净资产(公允价)']
            collateral_secmv = dict_posttrd_rawdata_fund['担保证券市值']
            margin_ratio_finance = dict_posttrd_rawdata_fund['融资保证金比例']
            margin_ratio_secloan = dict_posttrd_rawdata_fund['融券保证金比例']
            avl_margin_amt = dict_posttrd_rawdata_fund['可用保证金']  # todo 猜测可能是margin excess
            margin_deposit_from_finance = dict_posttrd_rawdata_fund['融资已用保证金']
            margin_deposit_from_secloan = dict_posttrd_rawdata_fund['融券已用保证金']
            credit_quota = dict_posttrd_rawdata_fund['授信额度']
            maintenance_ratio = dict_posttrd_rawdata_fund['维持担保比例(扣除非担保品)']

            dict_posttrd_fmtdata_fund = {
                'DataDate': self.gl.str_last_trddate,
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'CashBalance': cash_balance,
                'CashFromSS': cash_from_ss,
                'AvailableFund': available_fund,
                'FinanceAmt': finance_amt,
                'ShortSellAmt': ssamt,
                'SecurityMarketValue': secmv,
                'TotalAsset': ttasset,
                'TotalLiability': ttliability,
                'LiabilityFromFinance': liability_from_finance,
                'LiabilityFromSecLoan': liability_from_secloan,
                'NetAsset': net_asset,
                'CollateralMarketValue': collateral_secmv,
                'MarginRatioOfFinance': margin_ratio_finance,
                'MarginRatioOfSecLoan': margin_ratio_secloan,
                'AvailableMarginAmt': avl_margin_amt,
                'MarginDepositFromFinance': margin_deposit_from_finance,
                'MarginDepositFromSecLoan': margin_deposit_from_secloan,
                'CreditQuota': credit_quota,
                'MaintenanceRatio': maintenance_ratio,
            }
            list_dicts_posttrd_fmtdata_fund.append(dict_posttrd_fmtdata_fund)

        self.gl.col_posttrd_fmtdata_fund.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_fmtdata_fund:
            self.gl.col_posttrd_fmtdata_fund.insert_many(list_dicts_posttrd_fmtdata_fund)

        # col_posttrd_fmtdata_holding
        list_dicts_posttrd_rawdata_holding = list(
            self.gl.col_posttrd_rawdata_holding.find(
                {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
        )
        list_dicts_posttrd_fmtdata_holding = []
        set_secid_in_rawdata_holding = set()
        for dict_posttrd_rawdata_holding in list_dicts_posttrd_rawdata_holding:
            secid = dict_posttrd_rawdata_holding['证券代码']
            if secid in self.gl.dict_secid2composite:
                cpssrc = self.gl.dict_secid2composite[secid]
            else:
                cpssrc = 'AutoT0'
            symbol = dict_posttrd_rawdata_holding['证券名称']
            longqty = dict_posttrd_rawdata_holding['证券余额']
            secidsrc = self.gl.dict_exchange2secidsrc[dict_posttrd_rawdata_holding['交易市场']]
            dict_posttrd_fmtdata_holding = {
                'DataDate': self.gl.str_last_trddate,
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'SecurityID': secid,
                'SecurityIDSource': secidsrc,
                'Symbol': symbol,
                'LongQty': longqty,
                'CompositeSource': cpssrc,
            }
            list_dicts_posttrd_fmtdata_holding.append(dict_posttrd_fmtdata_holding)
            set_secid_in_rawdata_holding.add(secid)
        self.gl.col_posttrd_fmtdata_holding.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_fmtdata_holding:
            self.gl.col_posttrd_fmtdata_holding.insert_many(list_dicts_posttrd_fmtdata_holding)

        # col_posttrd_fmtdata_jgd
        list_dicts_posttrd_rawdata_jgd = list(
            self.gl.col_posttrd_rawdata_jgd.find(
                {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
        )
        list_dicts_posttrd_fmtdata_jgd = []
        for dict_posttrd_rawdata_jgd in list_dicts_posttrd_rawdata_jgd:
            serial_number = dict_posttrd_rawdata_jgd['流水号']
            if not serial_number:
                continue
            str_trddate = dict_posttrd_rawdata_jgd['成交日期']
            secid = dict_posttrd_rawdata_jgd['证券代码']
            exchange = dict_posttrd_rawdata_jgd['市场']
            secidsrc = self.gl.dict_exchange2secidsrc[exchange]
            symbol = dict_posttrd_rawdata_jgd['证券简称'].split()[0]
            business_type = dict_posttrd_rawdata_jgd['业务类别']
            if business_type in ['买入']:
                side = 1
            elif business_type in ['卖出']:
                side = 2
            else:
                raise ValueError('交割单读取错误，未知业务类型。')
            cumqty = dict_posttrd_rawdata_jgd['成交数量']
            avgpx = dict_posttrd_rawdata_jgd['成交价格']
            cumamt = dict_posttrd_rawdata_jgd['成交金额']
            cash_balance = dict_posttrd_rawdata_jgd['资金余额']
            str_entrust_time = dict_posttrd_rawdata_jgd['报盘时间']
            if str_entrust_time:
                str_entrust_datetime = f"{str_trddate}{str_entrust_time.replace(':', '')}"
            else:
                str_entrust_datetime = ''

            # todo 海通307备忘录：佣金包括证管费和经手费
            fee_from_trdactivity = (
                + dict_posttrd_rawdata_jgd['佣金']
                + dict_posttrd_rawdata_jgd['印花税']
                + dict_posttrd_rawdata_jgd['过户费']
            )

            if secid in self.gl.dict_secid2composite:
                cpssrc = self.gl.dict_secid2composite[secid]
            else:
                cpssrc = 'AutoT0'

            dict_posttrd_fmtdata_jgd = {
                'DataDate': self.gl.str_last_trddate,
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'TradeDate': str_trddate,
                'SecurityID': secid,
                'SecurityIDSource': secidsrc,
                'CompositeSource': cpssrc,
                'Symbol': symbol,
                'Side': side,
                'CumQty': cumqty,
                'AvgPx': avgpx,
                'CumAmt': cumamt,
                'CashBalance': cash_balance,
                'EntrustDateTime': str_entrust_datetime,
                'SerialNumber': serial_number,
                '经手费': dict_posttrd_rawdata_jgd['经手费'],
                '证管费': dict_posttrd_rawdata_jgd['证管费'],
                '佣金': dict_posttrd_rawdata_jgd['佣金'],
                '印花税': dict_posttrd_rawdata_jgd['印花税'],
                '过户费': dict_posttrd_rawdata_jgd['过户费'],
                'FeeFromTradingActivity': fee_from_trdactivity,
            }
            list_dicts_posttrd_fmtdata_jgd.append(dict_posttrd_fmtdata_jgd)

        self.gl.col_posttrd_fmtdata_jgd.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_fmtdata_jgd:
            self.gl.col_posttrd_fmtdata_jgd.insert_many(list_dicts_posttrd_fmtdata_jgd)

        # col_posttrd_fmtdata_shortqty_from_secloan  note: 未平仓合约
        list_dicts_posttrd_rawdata_shortqty_from_secloan = list(
            self.gl.col_posttrd_rawdata_shortqty_from_secloan.find(
                {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
        )
        list_dicts_posttrd_fmtdata_shortqty_from_secloan = []
        set_secid_in_rawdata_shortqty_from_secloan = set()
        for dict_posttrd_rawdata_shortqty_from_secloan in list_dicts_posttrd_rawdata_shortqty_from_secloan:
            secid = dict_posttrd_rawdata_shortqty_from_secloan['证券代码']
            if secid in self.gl.dict_secid2composite:
                cpssrc = self.gl.dict_secid2composite[secid]
            else:
                cpssrc = 'AutoT0'
            symbol = dict_posttrd_rawdata_shortqty_from_secloan['证券名称']
            shortqty = dict_posttrd_rawdata_shortqty_from_secloan['尚欠数量']
            serial_number = dict_posttrd_rawdata_shortqty_from_secloan['流水号']
            dict_posttrd_fmtdata_shortqty_from_secloan = {
                'DataDate': self.gl.str_last_trddate,
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'SecurityID': secid,
                'Symbol': symbol,
                'ShortQty': shortqty,
                'SerialNumber': serial_number,
                'CompositeSource': cpssrc,
            }
            list_dicts_posttrd_fmtdata_shortqty_from_secloan.append(dict_posttrd_fmtdata_shortqty_from_secloan)
            set_secid_in_rawdata_shortqty_from_secloan.add(secid)
        self.gl.col_posttrd_fmtdata_shortqty_from_secloan.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_fmtdata_shortqty_from_secloan:
            self.gl.col_posttrd_fmtdata_shortqty_from_secloan.insert_many(
                list_dicts_posttrd_fmtdata_shortqty_from_secloan
            )

        # col_posttrd_fmtdata_secloan_from_private_secpool
        list_dicts_posttrd_rawdata_secloan_from_private_secpool = list(
            self.gl.col_posttrd_rawdata_secloan_from_private_secpool.find(
                {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
        )
        list_dicts_posttrd_fmtdata_secloan_from_private_secpool = []
        set_secid_in_rawdata_secloan_from_private_secpool = set()
        for dict_posttrd_rawdata_secloan_from_private_secpool in list_dicts_posttrd_rawdata_secloan_from_private_secpool:
            secid = dict_posttrd_rawdata_secloan_from_private_secpool['证券代码']
            if secid in self.gl.dict_secid2composite:
                cpssrc = self.gl.dict_secid2composite[secid]
            else:
                cpssrc = 'AutoT0'

            serial_number = dict_posttrd_rawdata_secloan_from_private_secpool['合约流水号']
            qty_to_be_charged_interest = (
                dict_posttrd_rawdata_secloan_from_private_secpool['合约数量']
                - dict_posttrd_rawdata_secloan_from_private_secpool['收回数量']
            )
            interest = dict_posttrd_rawdata_secloan_from_private_secpool['私用转融券费率']
            str_startdate = dict_posttrd_rawdata_secloan_from_private_secpool['发生日期']
            str_maturity_date = dict_posttrd_rawdata_secloan_from_private_secpool['合约理论到期日期']
            expiration_days = dict_posttrd_rawdata_secloan_from_private_secpool['期限']
            set_secid_in_rawdata_secloan_from_private_secpool.add(secid)
            dict_posttrd_fmtdata_secloan_from_private_secpool = {
                'DataDate': self.gl.str_last_trddate,
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'SecurityID': secid,
                'SerialNumber': serial_number,
                'QtyToBeChargedInterest': qty_to_be_charged_interest,
                'Rate': interest,
                'ExpirationDays': expiration_days,
                'StartDate': str_startdate,
                'MaturityDate': str_maturity_date,
                'CompositeSource': cpssrc,
            }
            list_dicts_posttrd_fmtdata_secloan_from_private_secpool.append(
                dict_posttrd_fmtdata_secloan_from_private_secpool
            )
        self.gl.col_posttrd_fmtdata_secloan_from_private_secpool.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_fmtdata_secloan_from_private_secpool:
            self.gl.col_posttrd_fmtdata_secloan_from_private_secpool.insert_many(
                list_dicts_posttrd_fmtdata_secloan_from_private_secpool
            )

        # col_posttrd_fmtdata_secloan_from_public_secpool
        list_dicts_posttrd_rawdata_secloan_from_public_secpool = list(
            self.gl.col_posttrd_rawdata_secloan_from_public_secpool.find(
                {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
        )
        list_dicts_posttrd_fmtdata_secloan_from_public_secpool = []
        set_secid_in_rawdata_secloan_from_public_secpool = set()
        for dict_posttrd_rawdata_secloan_from_public_secpool in list_dicts_posttrd_rawdata_secloan_from_public_secpool:
            secid = dict_posttrd_rawdata_secloan_from_public_secpool['证券代码']
            if secid in self.gl.dict_secid2composite:
                cpssrc = self.gl.dict_secid2composite[secid]
            else:
                cpssrc = 'AutoT0'
            contract_id = dict_posttrd_rawdata_secloan_from_public_secpool['流水号']
            qty_to_be_charged_interest = dict_posttrd_rawdata_secloan_from_public_secpool['尚欠数量']
            interest = dict_posttrd_rawdata_secloan_from_public_secpool['年利率']
            str_startdate = dict_posttrd_rawdata_secloan_from_public_secpool['发生日期']
            str_maturity_date = dict_posttrd_rawdata_secloan_from_public_secpool['到期日期']
            expiration_days = 180  # 6个月，假设为180天
            set_secid_in_rawdata_secloan_from_public_secpool.add(secid)
            dict_posttrd_fmtdata_secloan_from_public_secpool = {
                'DataDate': self.gl.str_last_trddate,
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'SecurityID': secid,
                'ContractID': contract_id,
                'QtyToBeChargedInterest': qty_to_be_charged_interest,
                'Rate': interest,
                'ExpirationDays': expiration_days,
                'StartDate': str_startdate,
                'MaturityDate': str_maturity_date,
                'CompositeSource': cpssrc,
            }
            list_dicts_posttrd_fmtdata_secloan_from_public_secpool.append(
                dict_posttrd_fmtdata_secloan_from_public_secpool
            )
        self.gl.col_posttrd_fmtdata_secloan_from_public_secpool.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_fmtdata_secloan_from_public_secpool:
            self.gl.col_posttrd_fmtdata_secloan_from_public_secpool.insert_many(
                list_dicts_posttrd_fmtdata_secloan_from_public_secpool
            )

        # col_posttrd_fmtdata_ssquota_from_secloan
        set_secid_in_ssquota = (
                set_secid_in_rawdata_shortqty_from_secloan | set_secid_in_rawdata_secloan_from_private_secpool
        )

        # col_posttrd_position  仓位
        set_secids_in_position = set_secid_in_rawdata_holding | set_secid_in_rawdata_shortqty_from_secloan
        # # 将shortqty_from_secloan表中shortqty数据按照股票代码汇总
        dict_secid2shortqty = {}
        for secid_in_position in set_secids_in_position:
            shortqty = 0
            for dict_posttrd_fmtdata_shortqty_from_secloan in list_dicts_posttrd_fmtdata_shortqty_from_secloan:
                if secid_in_position == dict_posttrd_fmtdata_shortqty_from_secloan['SecurityID']:
                    shortqty += dict_posttrd_fmtdata_shortqty_from_secloan['ShortQty']
            dict_secid2shortqty[secid_in_position] = shortqty

        list_dicts_posttrd_position = []
        for secid_in_position in set_secids_in_position:
            longqty = 0
            shortqty = 0
            symbol = ''
            for dict_posttrd_fmtdata_holding in list_dicts_posttrd_fmtdata_holding:
                secid_in_holding = dict_posttrd_fmtdata_holding['SecurityID']
                if secid_in_position == secid_in_holding:
                    symbol = dict_posttrd_fmtdata_holding['Symbol']
                    longqty = dict_posttrd_fmtdata_holding['LongQty']

            for dict_posttrd_fmtdata_shortqty_from_secloan in list_dicts_posttrd_fmtdata_shortqty_from_secloan:
                secid_in_shortqty = dict_posttrd_fmtdata_shortqty_from_secloan['SecurityID']
                if secid_in_position == secid_in_shortqty:
                    symbol = dict_posttrd_fmtdata_shortqty_from_secloan['Symbol']
                    shortqty = dict_secid2shortqty[secid_in_shortqty]
            longamt = longqty * self.gl.dict_fmtted_wssdata['PreClose'][self.gl.get_secid2windcode(secid_in_position)]
            shortamt = shortqty * self.gl.dict_fmtted_wssdata['PreClose'][self.gl.get_secid2windcode(secid_in_position)]

            if secid_in_position in self.gl.dict_secid2composite:
                cpssrc = self.gl.dict_secid2composite[secid_in_position]
            else:
                cpssrc = 'AutoT0'

            dict_posttrd_position = {
                'DataDate': self.gl.str_last_trddate,
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'SecurityID': secid_in_position,
                'Symbol': symbol,
                'LongQty': longqty,
                'ShortQty': shortqty,
                'NetQty': longqty - shortqty,
                'LongAmt': longamt,
                'ShortAmt': shortamt,
                'NetAmt': longamt - shortamt,
                'CompositeSource': cpssrc,
            }
            list_dicts_posttrd_position.append(dict_posttrd_position)
        self.gl.col_posttrd_position.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_position:
            self.gl.col_posttrd_position.insert_many(list_dicts_posttrd_position)

        # # 将 secloan_from_private_secpool 表中的QtyToBeChargedInterest数据按照 股票代码 汇总
        dict_secid2ssquota_from_private_secpool = {}
        for secid_in_ssquota in set_secid_in_ssquota:
            ssquota_from_private_secpool = 0
            for dict_posttrd_fmtdata_secloan_from_private_secpool in \
                    list_dicts_posttrd_fmtdata_secloan_from_private_secpool:
                if secid_in_ssquota == dict_posttrd_fmtdata_secloan_from_private_secpool['SecurityID']:
                    ssquota_from_private_secpool += (
                        dict_posttrd_fmtdata_secloan_from_private_secpool['QtyToBeChargedInterest']
                    )
            dict_secid2ssquota_from_private_secpool[secid_in_ssquota] = ssquota_from_private_secpool

        # # 将 secloan_from_public_secpool 表中的QtyToBeChargedInterest数据按照 股票代码 汇总
        dict_secid2ssquota_from_public_secpool = {}
        for secid_in_ssquota in set_secid_in_ssquota:
            ssquota_from_public_secpool = 0
            for dict_posttrd_fmtdata_secloan_from_public_secpool in (
                    list_dicts_posttrd_fmtdata_secloan_from_public_secpool
            ):
                if secid_in_ssquota == dict_posttrd_fmtdata_secloan_from_public_secpool['SecurityID']:
                    ssquota_from_public_secpool += (
                        dict_posttrd_fmtdata_secloan_from_public_secpool['QtyToBeChargedInterest']
                    )
            dict_secid2ssquota_from_public_secpool[secid_in_ssquota] = ssquota_from_public_secpool

        # # 计算ssquota_from_public_secpool 与 ssquota_from_private_secpool
        list_dicts_posttrd_fmtdata_ssquota_from_secloan = []
        for secid_in_ssquota in set_secid_in_ssquota:
            if secid_in_ssquota in self.gl.dict_secid2composite:
                cpssrc = self.gl.dict_secid2composite[secid_in_ssquota]
            else:
                cpssrc = 'AutoT0'
            ssquota_from_private_secpool = dict_secid2ssquota_from_private_secpool[secid_in_ssquota]
            ssquota_from_public_secpool = dict_secid2ssquota_from_public_secpool[secid_in_ssquota]
            shortqty = 0
            for dict_posttrd_position in list_dicts_posttrd_position:
                if secid_in_ssquota == dict_posttrd_position['SecurityID']:
                    shortqty = dict_posttrd_position['ShortQty']
            ssquota = ssquota_from_public_secpool + ssquota_from_private_secpool
            dict_posttrd_fmtdata_ssquota_from_secloan = {
                'DataDate': self.gl.str_last_trddate,
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'SecurityID': secid_in_ssquota,
                'SSQuota': ssquota,
                'ShortQty': shortqty,
                'SSQuotaFromPrivateSecPool': ssquota_from_private_secpool,
                'SSQuotaFromPublicSecPool': ssquota_from_public_secpool,
                'AvailableSSQuota': ssquota - shortqty,
                'CompositeSource': cpssrc,
            }
            list_dicts_posttrd_fmtdata_ssquota_from_secloan.append(dict_posttrd_fmtdata_ssquota_from_secloan)

        self.gl.col_posttrd_fmtdata_ssquota_from_secloan.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_fmtdata_ssquota_from_secloan:
            self.gl.col_posttrd_fmtdata_ssquota_from_secloan.insert_many(
                list_dicts_posttrd_fmtdata_ssquota_from_secloan)

        # col_posttrd_fmtdata_fee_from_secloan
        list_dicts_posttrd_rawdata_fee_from_secloan = list(
            self.gl.col_posttrd_rawdata_fee_from_secloan.find(
                {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
        )
        list_dicts_posttrd_fmtdata_fee_from_secloan = []
        for dict_posttrd_rawdata_fee_from_secloan in list_dicts_posttrd_rawdata_fee_from_secloan:
            serial_number = dict_posttrd_rawdata_fee_from_secloan['流水号']
            loan_type = dict_posttrd_rawdata_fee_from_secloan['合约类型'].strip()
            # 流水号在时间维度上唯一
            if loan_type in ['融券']:
                dict_shortqty_from_secloan = self.gl.col_posttrd_fmtdata_secloan_from_public_secpool.find_one(
                    {'AcctIDByMXZ': self.gl.acctidbymxz, 'ContractID': serial_number,}
                )
                if serial_number in ['000000046148']:
                    continue
                secid = dict_shortqty_from_secloan['SecurityID']
                loan_type = 'secloan_from_public'
            elif loan_type in ['私用融券', '私用券源']:
                dict_posttrd_fmtdata_secloan_from_private_secpool = (
                    self.gl.col_posttrd_fmtdata_secloan_from_private_secpool.find_one(
                        {'AcctIDByMXZ': self.gl.acctidbymxz, 'SerialNumber': serial_number}
                    )
                )

                if dict_posttrd_fmtdata_secloan_from_private_secpool is None:
                    raise ValueError(f'{serial_number}合约无法按股票代码归集。')
                else:
                    secid = dict_posttrd_fmtdata_secloan_from_private_secpool['SecurityID']
                    loan_type = 'secloan_from_private'
            else:
                raise ValueError('Unknown loan_type.')
            interest = dict_posttrd_rawdata_fee_from_secloan['利息']
            quota_locking_fee = dict_posttrd_rawdata_fee_from_secloan['占用额度费']
            fixed_quota_fee = dict_posttrd_rawdata_fee_from_secloan['固定额度费']
            penalty_from_default = dict_posttrd_rawdata_fee_from_secloan['坏账罚息']
            penalty_from_prepay = dict_posttrd_rawdata_fee_from_secloan['提前还贷手续费']
            penalty_from_overdue = dict_posttrd_rawdata_fee_from_secloan['逾期罚息']

            accrued_interest = dict_posttrd_rawdata_fee_from_secloan['未结总利息']
            accrued_quota_locking_fee = dict_posttrd_rawdata_fee_from_secloan['未结总占用额度费']
            accrued_fixed_quota_fee = dict_posttrd_rawdata_fee_from_secloan['未结总固定额度费']
            accrued_penalty_from_default = dict_posttrd_rawdata_fee_from_secloan['未结总坏账罚息']
            accrued_penalty_from_prepay = dict_posttrd_rawdata_fee_from_secloan['未结总提前还贷手续费']
            accrued_penalty_from_overdue = dict_posttrd_rawdata_fee_from_secloan['未结总逾期罚息']

            fee_from_secloan = (
                    interest
                    + quota_locking_fee
                    + fixed_quota_fee
                    + penalty_from_default
                    + penalty_from_prepay
                    + penalty_from_overdue
            )

            accrued_fee_from_secloan = (
                accrued_interest
                + accrued_quota_locking_fee
                + accrued_fixed_quota_fee
                + accrued_penalty_from_default
                + accrued_penalty_from_prepay
                + accrued_penalty_from_overdue
            )

            if secid in self.gl.dict_secid2composite:
                cpssrc = self.gl.dict_secid2composite[secid]
            else:
                cpssrc = 'AutoT0'

            dict_posttrd_fmtdata_fee_from_secloan = {
                'DataDate': self.gl.str_last_trddate,
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'SecurityID': secid,
                'CompositeSource': cpssrc,
                'LoanType': loan_type,
                'InterestFromPublicSecLoan': interest,
                'InterestFromPrivateSecLoan': quota_locking_fee + fixed_quota_fee,
                'PenaltyFromDefault': penalty_from_default,
                'PenaltyFromPrepay': penalty_from_prepay,
                'PenaltyFromOverdue': penalty_from_overdue,
                'FeeFromSecLoan': fee_from_secloan,
                'AccruedInterestFromPublicSecLoan': accrued_interest,
                'AccruedInterestFromPrivateSecLoan': accrued_quota_locking_fee + accrued_fixed_quota_fee,
                'AccruedPenaltyFromDefault': accrued_penalty_from_default,
                'AccruedPenaltyFromPrepay': accrued_penalty_from_prepay,
                'AccruedPenaltyFromOverdue': accrued_penalty_from_overdue,
                'AccruedFeeFromSecLoan': accrued_fee_from_secloan
            }
            list_dicts_posttrd_fmtdata_fee_from_secloan.append(dict_posttrd_fmtdata_fee_from_secloan)
        self.gl.col_posttrd_fmtdata_fee_from_secloan.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_fmtdata_fee_from_secloan:
            self.gl.col_posttrd_fmtdata_fee_from_secloan.insert_many(list_dicts_posttrd_fmtdata_fee_from_secloan)

    def get_and_upload_col_post_trddata_pnl(self):
        """
        根据清算后数据计算T日PNL
        Input：
            1. col_posttrd_fmtdata_position: str_last_last_trddate: T-2, str_last_trddate: T-1
            2. col_posttrd_fmtdata_jgd: str_last_trddate
            3. col_posttrd_fmtdata_fee_from_secloan: str_last_trddate

        Algo:
            PNL_Sum
            = PNL_Part1 + PNL_Part2 + PNL_Part3
            = ∑[(卖出成交价－当日结算价)×卖出量]
              +∑[(当日结算价－买入成交价)×买入量]
              +(上一交易日结算价－当日结算价)×(上一交易日卖出持仓量－上一交易日买入持仓量)

            FeeFromSecLoan: 券息计提费用，归集而来

            FeeFromTradingActivity
            = 佣金(包括经手费、证管费和风险金) + 过户费 + 印花税

        Output:
            1. col_posttrd_pnl
        """
        # 1. 取需清算数据中所有股票的集合
        set_secids_in_jgd = set()
        for _ in self.gl.col_posttrd_fmtdata_jgd.find(
                {'DataDate': self.gl.str_last_trddate, 'TradeDate': self.gl.str_last_trddate}
        ):
            set_secids_in_jgd.add(_['SecurityID'])

        set_secids_in_position_last_trddate = set()
        for _ in self.gl.col_posttrd_position.find({'DataDate': self.gl.str_last_trddate}):
            set_secids_in_position_last_trddate.add(_['SecurityID'])

        set_secids_in_position_last_last_trddate = set()
        for _ in self.gl.col_posttrd_position.find({'DataDate': self.gl.str_last_last_trddate}):
            set_secids_in_position_last_last_trddate.add(_['SecurityID'])

        set_secids_in_fee_from_secloan = set()
        for _ in self.gl.col_posttrd_fmtdata_fee_from_secloan.find({'DataDate': self.gl.str_last_trddate}):
            set_secids_in_fee_from_secloan.add(_['SecurityID'])

        set_secids_in_position_and_jgd_and_fee_from_secloan = (
                set_secids_in_position_last_last_trddate
                | set_secids_in_position_last_trddate
                | set_secids_in_jgd
                | set_secids_in_fee_from_secloan
        )

        iter_dict_fmtted_wssdata = self.gl.col_fmtted_wssdata.find({'DataDate': self.gl.str_last_trddate}, {'_id': 0})
        df_fmtted_wssdata = pd.DataFrame(iter_dict_fmtted_wssdata)
        dict_fmtted_wssdata = df_fmtted_wssdata.set_index('WindCode').to_dict()
        dict_windcodes2close = dict_fmtted_wssdata['Close']
        dict_windcodes2preclose = dict_fmtted_wssdata['PreClose']

        # 2. 计算: 按股票归集  # todo 添加时间参数
        # todo 重要假设： 1. 目前由于按照股票进行策略区分，不涉及股数，所以出于成本考虑，直接从pnl层次进行分组;
        # todo 重要假设： 2. 约定: 不在人工T0 tgtlist 的股票，为机器T0策略;

        # # 从pre_trddata中读取数据， 获取非AutoT0证券
        iter_dicts_grp_tgtsecids_by_cps_manut0 = self.gl.col_pretrd_grp_tgtsecids_by_cps.find(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz, 'Composite': 'ManuT0'}
        )

        set_secids_in_grp_tgtsecids_by_cps_manut0 = set()
        for dict_grp_tgtsecids_by_cps_manut0 in iter_dicts_grp_tgtsecids_by_cps_manut0:
            tgtsecids = dict_grp_tgtsecids_by_cps_manut0['SecurityID']
            set_secids_in_grp_tgtsecids_by_cps_manut0.add(tgtsecids)

        iter_dicts_grp_tgtsecids_by_cps_nic = self.gl.col_pretrd_grp_tgtsecids_by_cps.find(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz, 'Composite': 'NotInComposite'}
        )
        set_secids_in_grp_tgtsecids_by_cps_nic = set()
        for dict_grp_tgtsecids_by_cps_manut0 in iter_dicts_grp_tgtsecids_by_cps_nic:
            tgtsecids = dict_grp_tgtsecids_by_cps_manut0['SecurityID']
            set_secids_in_grp_tgtsecids_by_cps_nic.add(tgtsecids)

        list_dicts_posttrd_jgd = list(self.gl.col_posttrd_fmtdata_jgd.find(
            {'DataDate': self.gl.str_last_trddate, 'TradeDate': self.gl.str_last_trddate})
        )
        list_dicts_posttrd_pnl_by_secid = []
        for secid_in_position_and_jgd_and_fee_from_secloan in set_secids_in_position_and_jgd_and_fee_from_secloan:
            if secid_in_position_and_jgd_and_fee_from_secloan in set_secids_in_grp_tgtsecids_by_cps_manut0:
                cpssrc = 'ManuT0'
            elif secid_in_position_and_jgd_and_fee_from_secloan in set_secids_in_grp_tgtsecids_by_cps_nic:
                cpssrc = 'NotInComposite'
            else:
                cpssrc = 'AutoT0'

            # # 1. pnl_part1 = ∑[(卖出成交价－当日结算价)×卖出量]
            # # 2. pnl_part2 = ∑[(当日结算价－买入成交价)×买入量]
            pnl_part1 = 0
            pnl_part2 = 0
            close = dict_windcodes2close[self.gl.get_secid2windcode(secid_in_position_and_jgd_and_fee_from_secloan)]
            preclose = dict_windcodes2preclose[self.gl.get_secid2windcode(secid_in_position_and_jgd_and_fee_from_secloan)]
            for dict_posttrd_jgd in list_dicts_posttrd_jgd:
                secid = dict_posttrd_jgd['SecurityID']
                if secid_in_position_and_jgd_and_fee_from_secloan == secid:
                    avgpx = dict_posttrd_jgd['AvgPx']
                    cumqty = dict_posttrd_jgd['CumQty']
                    side = dict_posttrd_jgd['Side']
                    if side in [2]:
                        pnl_part1 += (avgpx - close) * cumqty
                    elif side in [1]:
                        pnl_part2 += (close - avgpx) * cumqty
                    else:
                        raise ValueError('Unknown side when deal with jgd_data.')

            # # 3. pnl_part3 = (上一交易日结算价－当日结算价)×(上一交易日卖出持仓量－上一交易日买入持仓量) (历史持仓盈亏)
            dict_posttrd_position_last_last_trddate = (
                self.gl.col_posttrd_position.find_one(
                    {
                        'DataDate': self.gl.str_last_last_trddate,
                        'AcctIDByMXZ': self.gl.acctidbymxz,
                        'SecurityID': secid_in_position_and_jgd_and_fee_from_secloan
                    }
                )
            )
            shortqty = 0
            longqty = 0
            if dict_posttrd_position_last_last_trddate:
                shortqty = dict_posttrd_position_last_last_trddate['ShortQty']
                longqty = dict_posttrd_position_last_last_trddate['LongQty']
            pnl_part3 = (preclose - close) * (shortqty - longqty)
            pnl_123 = pnl_part1 + pnl_part2 + pnl_part3

            # # 4. fee_from_trading_activity
            list_dicts_fmtdata_jgd = list(
                self.gl.col_posttrd_fmtdata_jgd.find(
                    {
                        'DataDate': self.gl.str_last_trddate,
                        'AcctIDByMXZ': self.gl.acctidbymxz,
                        'SecurityID': secid_in_position_and_jgd_and_fee_from_secloan,
                        'TradeDate': self.gl.str_last_trddate
                    }
                )
            )

            fee_from_trdactivity = 0
            for dict_fmtdata_jgd in list_dicts_fmtdata_jgd:
                fee_from_trdactivity += dict_fmtdata_jgd['FeeFromTradingActivity']

            # # 5. security_loan_interest
            list_dicts_fmtdata_fee_from_secloan = list(
                self.gl.col_posttrd_fmtdata_fee_from_secloan.find(
                    {
                        'DataDate': self.gl.str_last_trddate,
                        'AcctIDByMXZ': self.gl.acctidbymxz,
                        'SecurityID': secid_in_position_and_jgd_and_fee_from_secloan,
                    }
                )
            )
            fee_from_secloan = 0
            for dict_fmtdata_fee_from_secloan in list_dicts_fmtdata_fee_from_secloan:
                fee_from_secloan += dict_fmtdata_fee_from_secloan['FeeFromSecLoan']

            pnl_by_secid = pnl_123 - fee_from_trdactivity - fee_from_secloan
            dict_posttrd_pnl_by_secid = {
                'DataDate': self.gl.str_last_trddate,  # todo 改为可变参数
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'SecurityID': secid_in_position_and_jgd_and_fee_from_secloan,
                'CompositeSource': cpssrc,
                'PNL_Part1': pnl_part1,
                'PNL_Part2': pnl_part2,
                'PNL_Part3': pnl_part3,
                'PNL_PartSum': pnl_123,
                'FeeFromTradingActivity': fee_from_trdactivity,
                'FeeFromSecLoan': fee_from_secloan,
                'PNLBySecID': pnl_by_secid
            }
            list_dicts_posttrd_pnl_by_secid.append(dict_posttrd_pnl_by_secid)
        self.gl.col_posttrd_pnl_by_secid.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_pnl_by_secid:
            self.gl.col_posttrd_pnl_by_secid.insert_many(list_dicts_posttrd_pnl_by_secid)

        #  3. 计算: 按账户-策略归集
        if list_dicts_posttrd_pnl_by_secid:
            df_posttrd_pnl_by_secid = pd.DataFrame(list_dicts_posttrd_pnl_by_secid)
            df_posttrd_pnl_by_acctidbymxz_cps = (
                df_posttrd_pnl_by_secid.groupby(by=['DataDate', 'AcctIDByMXZ', 'CompositeSource']).sum().reset_index()
            )
            list_dicts_posttrd_pnl_by_acctidbymxz_cps = df_posttrd_pnl_by_acctidbymxz_cps.to_dict('records')
        else:
            list_dicts_posttrd_pnl_by_acctidbymxz_cps = []

        self.gl.col_posttrd_pnl_by_acctidbymxz_cps.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_pnl_by_acctidbymxz_cps:
            self.gl.col_posttrd_pnl_by_acctidbymxz_cps.insert_many(list_dicts_posttrd_pnl_by_acctidbymxz_cps)

        # 4. 计算： 按账户归集
        if list_dicts_posttrd_pnl_by_secid:
            df_posttrd_pnl_by_secid = pd.DataFrame(list_dicts_posttrd_pnl_by_secid)
            df_posttrd_pnl_by_acctidbymxz = (
                df_posttrd_pnl_by_secid.groupby(by=['DataDate', 'AcctIDByMXZ']).sum().reset_index()
            )
            list_dicts_posttrd_pnl_by_acctidbymxz = df_posttrd_pnl_by_acctidbymxz.to_dict('records')
        else:
            list_dicts_posttrd_pnl_by_acctidbymxz = []

        self.gl.col_posttrd_pnl_by_acctidbymxz.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if list_dicts_posttrd_pnl_by_acctidbymxz:
            self.gl.col_posttrd_pnl_by_acctidbymxz.insert_many(list_dicts_posttrd_pnl_by_acctidbymxz)

    def get_col_secloan_utility_analysis(self):
        """
        分析融券券池的使用效率
        """
        # todo 仅分析了autot0策略的
        # 计算策略交易额与交易股票的支数
        iter_dicts_postfmtdata_jgd = self.gl.col_posttrd_fmtdata_jgd.find(
            {
                'DataDate': self.gl.str_last_trddate,
                'AcctIDByMXZ': self.gl.acctidbymxz,
                'CompositeSource': 'AutoT0',
                'TradeDate': self.gl.str_last_trddate

            }
        )
        trdamt_autot0 = 0
        set_secids_from_trading = set()
        for dict_postfmtdata_jgd in iter_dicts_postfmtdata_jgd:
            secid = dict_postfmtdata_jgd['SecurityID']
            if secid not in self.gl.dict_secid2composite:
                trdamt_autot0_delta = abs(dict_postfmtdata_jgd['CumAmt'])
            else:
                trdamt_autot0_delta = 0
            trdamt_autot0 += trdamt_autot0_delta
            set_secids_from_trading.add(secid)
        i_secids_from_trading = len(set_secids_from_trading)

        # 计算目标融券支数
        i_secids_from_tgtsecloan = 0
        iter_dicts_pretrd_tgtsecloan_mngdraft = self.gl.col_pretrd_tgtsecloan_mngdraft.find(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        for dict_pretrd_tgtsecloan_mngdraft in iter_dicts_pretrd_tgtsecloan_mngdraft:
            tgtqty = dict_pretrd_tgtsecloan_mngdraft['TgtQty']
            if tgtqty:
                i_secids_from_tgtsecloan += 1

        # 计算融券额度中股票的支数
        iter_dicts_posttrd_fmtdata_ssquota_from_secloan = self.gl.col_posttrd_fmtdata_ssquota_from_secloan.find(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz, 'CompositeSource': 'AutoT0'}
        )
        ssquota_mv = 0
        set_secids_from_ssquota_from_secloan = set()
        for dict_posttrd_fmtdata_ssquota_from_secloan in iter_dicts_posttrd_fmtdata_ssquota_from_secloan:
            secid = dict_posttrd_fmtdata_ssquota_from_secloan['SecurityID']
            set_secids_from_ssquota_from_secloan.add(secid)
            ssquota_mv_delta = (
                    dict_posttrd_fmtdata_ssquota_from_secloan['SSQuota']
                    * self.gl.dict_fmtted_wssdata['PreClose'][self.gl.get_secid2windcode(secid)]
            )
            ssquota_mv += ssquota_mv_delta
        i_secids_from_ssquota_from_secloan = len(set_secids_from_ssquota_from_secloan)

        # 计算敞口相关指标
        long_exposure = 0
        short_exposure = 0
        iter_dicts_posttrd_position = self.gl.col_posttrd_position.find(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz, 'CompositeSource': 'AutoT0'}
        )
        for dict_posttrd_position in iter_dicts_posttrd_position:
            long_exposure_delta = dict_posttrd_position['LongAmt']
            short_exposure_delta = dict_posttrd_position['ShortAmt']
            long_exposure += long_exposure_delta
            short_exposure += short_exposure_delta
        net_exposure = long_exposure - short_exposure
        gross_exposure = long_exposure + short_exposure

        if gross_exposure:
            pct_trdamt2gross_exposure = round(trdamt_autot0 / gross_exposure, 2)
        else:
            pct_trdamt2gross_exposure = 999999

        pct_i = round(i_secids_from_trading / i_secids_from_ssquota_from_secloan, 2)
        if i_secids_from_tgtsecloan:
            pct_i_secids_from_ssquota_from_secloan2i_secids_from_tgtsecloan = (
                    round(i_secids_from_ssquota_from_secloan / i_secids_from_tgtsecloan, 2)
            )
        else:
            pct_i_secids_from_ssquota_from_secloan2i_secids_from_tgtsecloan = 999999999
        if ssquota_mv:
            pct_short_exposure2ssquota_mv = round(short_exposure / ssquota_mv, 2)
        else:
            pct_short_exposure2ssquota_mv = 999999999
        dict_secloan_utility_analysis = {
            'DataDate': self.gl.str_last_trddate,
            'AcctIDByMXZ': self.gl.acctidbymxz,
            'CompositeSource': 'AutoT0',  # todo  假设只统计AutoT0
            'ExpectedNetAsset': 1.5 * short_exposure,
            'TradingAmt': trdamt_autot0,
            'SecuritiesCountInTrading': i_secids_from_trading,
            'SecuritiesCountInSecLoanQuota': i_secids_from_ssquota_from_secloan,
            'SecuritiesCountInTargetSecurityLoanPool': i_secids_from_tgtsecloan,
            'LongExposure': long_exposure,
            'ShortExposure': short_exposure,
            'NetExposure': net_exposure,
            'GrossExposure': gross_exposure,
            'SecLoanQuotaMarketValue': ssquota_mv,
            'PctOfTradingAmtByGrossExposure': pct_trdamt2gross_exposure,
            'PctOfSecCountInTradingBySecCountInSecLoanQuota': pct_i,
            'PctOfSecCountInSecLoanQuotaBySecCountInTgtSecLoan': (
                pct_i_secids_from_ssquota_from_secloan2i_secids_from_tgtsecloan
            ),
            'PctOfShortExposureBySecLoanQuotaMarketValue': pct_short_exposure2ssquota_mv,
        }
        print(
            f'昨日交易股票数量{i_secids_from_trading}支，'
            f'借入股票实际数量{i_secids_from_ssquota_from_secloan}支，占比{pct_i:.0%}.; '
            f'交易额共{round(trdamt_autot0/10000)}万，总暴露{round(gross_exposure/10000)}万，'
            f'占比{pct_trdamt2gross_exposure:.0%}。;'
            f'空头暴露为{round(short_exposure/10000)}万，借入股票{round(ssquota_mv/10000)}万，'
            f'占比{pct_short_exposure2ssquota_mv:.0%}。; '
            f'筛选后的目标券池数量为{i_secids_from_tgtsecloan}支。'
        )
        self.gl.col_posttrd_secloan_utility_analysis.delete_many(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz, 'CompositeSource': 'AutoT0'}
        )
        self.gl.col_posttrd_secloan_utility_analysis.insert_one(dict_secloan_utility_analysis)

    def output_xlsx_posttrd_analysis(self):
        # 输出pnl_analysis （应当改名， 由于已经使用，故不改名）
        iter_dicts_posttrd_pnl_by_acctidbymxz_cps = self.gl.col_posttrd_pnl_by_acctidbymxz_cps.find(
            {'AcctIDByMXZ': self.gl.acctidbymxz}, {'_id': 0}
        )
        df_output_sheet_pnl_analysis = pd.DataFrame(iter_dicts_posttrd_pnl_by_acctidbymxz_cps)
        df_output_sheet_pnl_analysis['PNL_Part1'] = df_output_sheet_pnl_analysis['PNL_Part1'].apply(lambda x:
                                                                                                    round(x, 2))
        df_output_sheet_pnl_analysis['PNL_Part2'] = df_output_sheet_pnl_analysis['PNL_Part2'].apply(lambda x:
                                                                                                    round(x, 2))
        df_output_sheet_pnl_analysis['PNL_Part3'] = df_output_sheet_pnl_analysis['PNL_Part3'].apply(lambda x:
                                                                                                    round(x, 2))
        df_output_sheet_pnl_analysis['PNL_PartSum'] = (
            df_output_sheet_pnl_analysis['PNL_PartSum'].apply(lambda x: round(x, 2))
        )
        df_output_sheet_pnl_analysis['FeeFromTradingActivity'] = (
            df_output_sheet_pnl_analysis['FeeFromTradingActivity'].apply(lambda x: round(x, 2))
        )
        df_output_sheet_pnl_analysis['FeeFromSecLoan'] = (
            df_output_sheet_pnl_analysis['FeeFromSecLoan'].apply(lambda x: round(x, 2))
        )
        df_output_sheet_pnl_analysis['PNLBySecID'] = (
            df_output_sheet_pnl_analysis['PNLBySecID'].apply(lambda x: round(x, 2))
        )
        df_output_sheet_pnl_analysis.sort_values(by=['CompositeSource', 'DataDate'], inplace=True)

        # df_output_sheet_pnl_analysis_by_acctidbymxz
        iter_dicts_posttrd_pnl_by_acctidbymxz = self.gl.col_posttrd_pnl_by_acctidbymxz.find(
            {'AcctIDByMXZ': self.gl.acctidbymxz}, {'_id': 0}
        )
        list_output_sheet_pnl_analysis_by_acctidbymxz = []
        for dict_posttrd_pnl_by_acctidbymxz in iter_dicts_posttrd_pnl_by_acctidbymxz:
            _datadate = dict_posttrd_pnl_by_acctidbymxz['DataDate']
            pnl_part_sum = round(dict_posttrd_pnl_by_acctidbymxz['PNL_PartSum'], 2)
            fee_from_trading_activity = round(dict_posttrd_pnl_by_acctidbymxz['FeeFromTradingActivity'], 2)
            fee_from_secloan = round(dict_posttrd_pnl_by_acctidbymxz['FeeFromSecLoan'], 2)
            pnl_by_acctidbymxz = round(dict_posttrd_pnl_by_acctidbymxz['PNLBySecID'], 2)
            dict_posttrd_cf_from_indirect_method = self.gl.col_posttrd_cf_from_indirect_method.find_one(
                {'DataDate': _datadate}
            )
            if dict_posttrd_cf_from_indirect_method:
                dif_cf_from_indirect_method = dict_posttrd_cf_from_indirect_method['DifCFFromIndirectMethod']
            else:
                dif_cf_from_indirect_method = None

            dict_output_posttrd_pnl_by_acctidbymxz = {
                'DataDate': _datadate,
                'AcctIDByMXZ': dict_posttrd_pnl_by_acctidbymxz['AcctIDByMXZ'],
                'PNL_PartSum': pnl_part_sum,
                'FeeFromTradingActivity': fee_from_trading_activity,
                'FeeFromSecLoan': fee_from_secloan,
                'PNLByAcctIDByMXZ': pnl_by_acctidbymxz,
                'DifCFFromIndirectMethod': dif_cf_from_indirect_method
            }
            list_output_sheet_pnl_analysis_by_acctidbymxz.append(dict_output_posttrd_pnl_by_acctidbymxz)
        df_output_sheet_pnl_analysis_by_acctidbymxz = pd.DataFrame(list_output_sheet_pnl_analysis_by_acctidbymxz)
        df_output_sheet_pnl_analysis_by_acctidbymxz.sort_values(by=['DataDate'], inplace=True)

        # 输出fund
        iter_dicts_posttrd_fund_by_acctidbymxz = self.gl.col_posttrd_fmtdata_fund.find(
            {'AcctIDByMXZ': self.gl.acctidbymxz}, {'_id': 0}
        )
        df_output_sheet_fund_rpt = pd.DataFrame(iter_dicts_posttrd_fund_by_acctidbymxz)
        df_output_sheet_fund_rpt.sort_values(by=['DataDate'], inplace=True)

        # 输出security_loan_utility_analysis
        iter_dicts_posttrd_security_loan_utility_analysis = self.gl.col_posttrd_secloan_utility_analysis.find(
            {'AcctIDByMXZ': self.gl.acctidbymxz}, {'_id': 0}
        )
        df_output_sheet_security_loan_utility_analysis = pd.DataFrame(iter_dicts_posttrd_security_loan_utility_analysis)
        df_output_sheet_security_loan_utility_analysis.sort_values(by=['DataDate'], inplace=True)

        with pd.ExcelWriter(self.gl.fpath_output_xlsx_posttrd_analysis) as writer:
            df_output_sheet_pnl_analysis.to_excel(writer, sheet_name='pnl_analysis', index=False)
            df_output_sheet_pnl_analysis_by_acctidbymxz.to_excel(
                writer, sheet_name='pnl_analysis_by_acctidbymxz', index=False
            )
            df_output_sheet_fund_rpt.to_excel(writer, sheet_name='fund', index=False)
            df_output_sheet_security_loan_utility_analysis.to_excel(
                writer, sheet_name='security_loan_utility_analysis', index=False
            )
            writer.save()
        print('Output posttrd_analysis.xlsx Finished')

    def get_longamt_histogram(self):
        iter_position = self.gl.col_posttrd_position.find(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz, 'CompositeSource': 'AutoT0'},
            {'_id': 0}
        )
        df_position = pd.DataFrame(iter_position)
        s_longamt = df_position['LongAmt']
        s_longamt.plot(kind='hist')
        plt.show()

    def check_pnl_via_indirect_method(self):
        """
        检查机制，根据PNL计算CF
        # todo 账户级别 到 区分 策略
        """
        # 获取PNL
        dict_posttrd_pnl_by_acctidbymxz = self.gl.col_posttrd_pnl_by_acctidbymxz.find_one(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        pnl = dict_posttrd_pnl_by_acctidbymxz['PNLBySecID']

        # 获取清算数据 资产变动和负债变动
        # # CF_T = PNL + Liability_delta - non-current_Asset_delta
        # todo non-current_asset 还包括保证金利息
        # # last_last_trddate
        # # # liability_last_last_trddate = shortamt + liability_from_secloan
        list_posttrd_position_last_last_trddate = list(
            self.gl.col_posttrd_position.find(
                {'DataDate': self.gl.str_last_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
        )
        shortamt_last_last_trddate = 0
        for dict_posttrd_position_last_last_trddate in list_posttrd_position_last_last_trddate:
            shortamt_last_last_trddate += dict_posttrd_position_last_last_trddate['ShortAmt']

        longamt_last_last_trddate = 0
        for dict_posttrd_position_last_last_trddate in list_posttrd_position_last_last_trddate:
            longamt_last_last_trddate += dict_posttrd_position_last_last_trddate['LongAmt']

        iter_posttrd_fee_from_secloan_last_last_trddate = self.gl.col_posttrd_fmtdata_fee_from_secloan.find(
            {'DataDate': self.gl.str_last_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        liability_from_secloan_last_last_trddate = 0
        for dict_posttrd_fee_from_secloan_last_last_trddate in iter_posttrd_fee_from_secloan_last_last_trddate:
            liability_from_secloan_last_last_trddate += (
                dict_posttrd_fee_from_secloan_last_last_trddate['AccruedFeeFromSecLoan']
            )
        liability_last_last_trddate = shortamt_last_last_trddate + liability_from_secloan_last_last_trddate
        non_current_asset_last_last_trddate = longamt_last_last_trddate

        # # last_trddate
        list_dicts_posttrd_position_last_trddate = list(
            self.gl.col_posttrd_position.find(
                {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
            )
        )
        shortamt_last_trddate = 0
        for dict_posttrd_position_last_trddate in list_dicts_posttrd_position_last_trddate:
            shortamt_last_trddate += dict_posttrd_position_last_trddate['ShortAmt']

        longamt_last_trddate = 0
        for dict_posttrd_position_last_trddate in list_dicts_posttrd_position_last_trddate:
            longamt_last_trddate += dict_posttrd_position_last_trddate['LongAmt']

        iter_posttrd_fee_from_secloan_last_trddate = self.gl.col_posttrd_fmtdata_fee_from_secloan.find(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        liability_from_secloan_last_trddate = 0
        for dict_posttrd_fee_from_secloan_last_trddate in iter_posttrd_fee_from_secloan_last_trddate:
            liability_from_secloan_last_trddate += (
                dict_posttrd_fee_from_secloan_last_trddate['AccruedFeeFromSecLoan']
            )
        liability_last_trddate = shortamt_last_trddate + liability_from_secloan_last_trddate
        non_current_asset_last_trddate = longamt_last_trddate

        cf_from_indirect_method = (
                pnl
                + liability_last_trddate
                - liability_last_last_trddate
                - non_current_asset_last_trddate
                + non_current_asset_last_last_trddate
        )

        # 现金流
        dict_posttrd_fmtdata_fund_last_trddate = self.gl.col_posttrd_fmtdata_fund.find_one(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        cash_ending_last_trddate = dict_posttrd_fmtdata_fund_last_trddate['CashBalance']

        dict_posttrd_fmtdata_fund_last_last_trddate = self.gl.col_posttrd_fmtdata_fund.find_one(
            {'DataDate': self.gl.str_last_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        if dict_posttrd_fmtdata_fund_last_last_trddate is None:
            cf_posttrd = 'N/A'
            dif_cf_from_indirect_method = 'N/A'
        else:
            cash_ending_last_last_trddate = dict_posttrd_fmtdata_fund_last_last_trddate['CashBalance']
            cf_posttrd = cash_ending_last_trddate - cash_ending_last_last_trddate
            dif_cf_from_indirect_method = cf_from_indirect_method - cf_posttrd

        dict_posttrd_cf_from_indirect_method = {
            'DataDate': self.gl.str_last_trddate,
            'AcctIDByMXZ': self.gl.acctidbymxz,
            'PNL': pnl,
            'LiabilityChange': liability_last_trddate - liability_last_last_trddate,
            'NonCurrentAssetChange': non_current_asset_last_trddate - non_current_asset_last_last_trddate,
            'CFFromIndirectMethod': cf_from_indirect_method,
            'CFFromPostTradeData': cf_posttrd,
            'DifCFFromIndirectMethod': dif_cf_from_indirect_method,
        }
        self.gl.col_posttrd_cf_from_indirect_method.delete_one(
            {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': self.gl.acctidbymxz}
        )
        self.gl.col_posttrd_cf_from_indirect_method.insert_one(dict_posttrd_cf_from_indirect_method)

    def run(self):
        for acctidbymxz in self.gl.list_acctidsbymxz:
            self.upload_posttrd_rawdata(acctidbymxz)
            self.upload_posttrd_fmtdata(acctidbymxz)
            self.get_and_upload_col_post_trddata_pnl(acctidbymxz)
            self.get_col_secloan_utility_analysis(acctidbymxz)
            self.get_longamt_histogram(acctidbymxz)
            self.check_pnl_via_indirect_method(acctidbymxz)
            self.output_xlsx_posttrd_analysis(acctidbymxz)


if __name__ == '__main__':
    task = PostTrdMng(download_winddata_mark=0)
    task.run()
    print('Done')
