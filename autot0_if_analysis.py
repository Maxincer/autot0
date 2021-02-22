#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20210221T220000

"""
分析AutoT0_IF组合的交易执行情况

# 课题分析与问询反馈
1. 利用率
    1. 产品的股票池AUM: 换仓前 日内策略交易账户 中 所有股票市值之和
    2. number of stock:  换仓前 日内策略交易账户 中 所有的 number of stock
    3. 股票池中有多少比例的股票开了多仓，有多少比例的股票开了空仓
        1. number of stock 的比值
        2. 金额的比值

2. 交易性能
    1. 撤单率 = (已报委托笔数 + 部成委托笔数 + 已撤委托笔数 + 部撤委托笔数)/(委托总笔数 - 废单委托笔数 - 划转委托笔数)
    2. 废单率 = 废单数 / 委托总笔数

3. 持股集中度统计: 对账户中存在的证券进行统计(而不是日内策略驱动的发生交易的证券)；
    1. 金额比值(top10)
    2. 金额比值(top30)

"""

from globals import Globals, pd
from pymongo import ASCENDING


class AutoT0TradingPerformanceAnalysis:
    def __init__(self):
        self.gl = Globals()
        self.dict_acctidbyxxq2acctidbymxz = {}
        for _ in self.gl.col_acctinfo.find({'DataDate': self.gl.str_today}):
            if _['StrategiesAllocationByAcct']:
                list_strategies = _['StrategiesAllocationByAcct'].split(';')
                if 'AutoT0_IF' in list_strategies or 'AutoT0_SecurityLoan' in list_strategies:
                    self.dict_acctidbyxxq2acctidbymxz.update({_['AcctIDByXuXiaoQiang4Trd']: _['AcctIDByMXZ']})
            else:
                continue

        # 上传kdb原始委托数据
        self.fpath_order_from_kdb_102_116 = f'D:/projects/autot0/data/kdb_102_116/q_{self.gl.str_today}.csv'
        with open(self.fpath_order_from_kdb_102_116, encoding='utf-8') as f:
            list_dicts_order_from_kdb = []
            list_list_datalines = f.readlines()
            list_fields = list_list_datalines[0].strip().split(',')
            list_list_values = [_.strip().split(',') for _ in list_list_datalines[1:]]
            for list_value in list_list_values:
                dict_order_from_kdb = dict(zip(list_fields, list_value))
                dict_order_from_kdb['DataDate'] = self.gl.str_today
                if dict_order_from_kdb['accountname'] not in self.dict_acctidbyxxq2acctidbymxz:
                    dict_order_from_kdb['AcctIDByMXZ'] = ''
                else:
                    dict_order_from_kdb['AcctIDByMXZ'] = (
                        self.dict_acctidbyxxq2acctidbymxz[dict_order_from_kdb['accountname']]
                    )
                list_dicts_order_from_kdb.append(dict_order_from_kdb)
            self.gl.col_trade_rawdata_order_from_kdb_102_116.delete_many({'DataDate': self.gl.str_today})
            if list_dicts_order_from_kdb:
                self.gl.col_trade_rawdata_order_from_kdb_102_116.insert_many(list_dicts_order_from_kdb)

    def update_col_autot0_tpa(self):
        list_dicts_autot0_tpa = []
        for acctidbyxxq, acctidbymxz in self.dict_acctidbyxxq2acctidbymxz.items():
            # 计算 # of stocks
            longamt = 0
            i_cs = 0
            list_longamt_sort_reversed = []
            for _ in self.gl.col_posttrd_fmtdata_holding.find(
                    {'DataDate': self.gl.str_last_trddate, 'AcctIDByMXZ': acctidbymxz, 'SecurityType': 'CS'}
            ).sort('time', ASCENDING):
                longamt_delta = _['LongAmt']
                list_longamt_sort_reversed.append(longamt_delta)
                longamt += longamt_delta
                i_cs += 1
            list_longamt_sort_reversed.sort(reverse=True)
            dict_secid2olamt = {}
            dict_secid2osamt = {}
            i_order = 0
            i_order_withdraw = 0
            i_order_fd = 0
            for dict_order_from_kdb in self.gl.col_trade_rawdata_order_from_kdb_102_116.find(
                    {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
            ).sort('time', ASCENDING):
                sym = dict_order_from_kdb['sym']
                if 'AutoTrade' in sym:
                    secid = str(dict_order_from_kdb['stockcode']).zfill(6)
                    if (secid not in dict_secid2olamt.keys()) and (secid not in dict_secid2osamt.keys()):
                        cumqty_vec = int(dict_order_from_kdb['bidvol'])
                        close = self.gl.dict_fmtted_wssdata_last_trddate['Close'][self.gl.get_secid2windcode(secid)]
                        cumamt = close * abs(cumqty_vec)
                        if cumqty_vec > 0:
                            dict_secid2olamt.update({secid: cumamt})
                        elif cumqty_vec < 0:
                            dict_secid2osamt.update({secid: cumamt})
                        else:
                            pass

                    # 计算撤单与废单数量
                    i_order += 1
                    ordstatus = str(dict_order_from_kdb['status'])
                    if ordstatus in ['1', '2', '5']:
                        i_order_withdraw += 1
                    elif ordstatus in ['6']:
                        i_order_fd += 1
                    else:
                        pass

            # 计算 开多/空仓的股票数量与金额
            i_ol_cs = len(dict_secid2olamt)
            i_os_cs = len(dict_secid2osamt)
            if i_cs:
                flt_i_ol_cs_by_i_cs = round(i_ol_cs / i_cs, 2)
                flt_i_os_cs_by_i_cs = round(i_os_cs / i_cs, 2)
            else:
                flt_i_ol_cs_by_i_cs = 9
                flt_i_os_cs_by_i_cs = 9
            olamt_cs = 0
            for olamt_cs_delta in dict_secid2olamt.values():
                olamt_cs += olamt_cs_delta

            osamt_cs = 0
            for osamt_cs_delta in dict_secid2osamt.values():
                osamt_cs += osamt_cs_delta

            if longamt:
                flt_olamt_by_longamt = round(olamt_cs / longamt, 2)
            else:
                flt_olamt_by_longamt = 999999999

            if longamt:
                flt_osamt_by_longamt = round(osamt_cs / longamt, 2)
            else:
                flt_osamt_by_longamt = 999999999

            # 计算撤单率与废单率
            if i_order:
                ratio_order_withdraw = i_order_withdraw / i_order
                ratio_order_fd = i_order_fd / i_order
            else:
                ratio_order_withdraw = 999999999
                ratio_order_fd = 999999999

            # 计算持股集中度
            sum_longamt_top10 = sum(list_longamt_sort_reversed[:10])
            sum_longamt_top30 = sum(list_longamt_sort_reversed[:30])

            if longamt:
                ratio_longamt_top10 = sum_longamt_top10 / longamt
                ratio_longamt_top30 = sum_longamt_top30 / longamt
            else:
                ratio_longamt_top10 = 999999999
                ratio_longamt_top30 = 999999999

            dict_autot0_tpa = {
                'DataDate': self.gl.str_today,
                'AcctIDByMXZ': acctidbymxz,
                '股票市值(万元)': round(longamt/10000),
                '股票支数': i_cs,
                '多开支数/股票支数': flt_i_ol_cs_by_i_cs,
                '空开支数/股票支数': flt_i_os_cs_by_i_cs,
                '多开市值/股票市值': round(flt_olamt_by_longamt, 2),
                '空开市值/股票市值': round(flt_osamt_by_longamt, 2),
                '撤单率': round(ratio_order_withdraw, 2),
                '废单率': round(ratio_order_fd, 2),
                '股票市值集中度-top10': round(ratio_longamt_top10, 4),
                '股票市值集中度-top30': round(ratio_longamt_top30, 4),
            }
            list_dicts_autot0_tpa.append(dict_autot0_tpa)
        self.gl.col_tpa_autot0.delete_many({'DataDate': self.gl.str_today})
        if list_dicts_autot0_tpa:
            self.gl.col_tpa_autot0.insert_many(list_dicts_autot0_tpa)

        df_tpa_autot0 = pd.DataFrame(
            self.gl.col_tpa_autot0.find({}, {'_id': 0})
        )
        df_tpa_autot0.to_excel(self.gl.fpath_output_xlsx_tpa_autot0, index=False)

    def run(self):
        self.update_col_autot0_tpa()


if __name__ == '__main__':
    task = AutoT0TradingPerformanceAnalysis()
    task.run()














