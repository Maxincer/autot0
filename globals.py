#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20201031T180000

"""
This script provides global variables, constants, functions in this project
"""

from datetime import datetime
from email import encoders
from email.header import Header, decode_header
from email.parser import Parser
from email.utils import parseaddr, formataddr
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
import os
from poplib import POP3_SSL
import smtplib

import pandas as pd
from pymongo import MongoClient

from WindPy import w

STR_TODAY = datetime.today().strftime('%Y%m%d')


class Globals:
    def __init__(self, str_today=STR_TODAY, download_winddata_mark=0):
        # 日期部分
        self.str_today = str_today
        self.server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        self.db_global = self.server_mongodb['global']
        self.col_trdcalendar = self.db_global['trade_calendar']

        self.list_str_trdcalendar = []
        for _ in self.col_trdcalendar.find():
            self.list_str_trdcalendar += _['Data']
        idx_str_today = self.list_str_trdcalendar.index(self.str_today)
        self.str_last_trddate = self.list_str_trdcalendar[idx_str_today - 1]
        self.str_next_trddate = self.list_str_trdcalendar[idx_str_today + 1]
        self.str_last_last_trddate = self.list_str_trdcalendar[idx_str_today - 2]
        self.str_next_next_trddate = self.list_str_trdcalendar[idx_str_today + 2]

        # 配置文件部分: basicinfo
        self.db_basicinfo = self.server_mongodb['basicinfo']
        self.col_acctinfo = self.db_basicinfo['acctinfo']
        self.col_prdinfo = self.db_basicinfo['prdinfo']
        self.list_acctinfo = list(self.col_acctinfo.find({'DataDate': self.str_today}))
        # self.list_acctidsbymxz_autot0 = ['307_m_hait_2000', '8111_m_huat_9239']
        self.acctidbymxz = '8111_m_huat_9239'
        # 路径部分
        # # basicinfo
        dict_acctinfo = self.col_acctinfo.find_one(
            {'DataDate': self.str_today, 'AcctIDByMXZ': self.acctidbymxz}
        )
        str_path = '(Y:\investment_manager_products\hait_ehfz_api\YYYYMMDD_1882842000_fund.csv,Y:\investment_manager_products\hait_ehfz_api\YYYYMMDD_1882842000_holding.csv,Y:\investment_manager_products\hait_ehfz_api\YYYYMMDD_1882842000_order.csv,Y:\investment_manager_products\hait_ehfz_api\YYYYMMDD_1882842000_security_loan.csv)'
        list_fpaths_data_file_path = [
            _.strip() for _ in str_path.replace('YYYYMMDD', self.str_today)[1:-1].split(',')
        ]

        # # SecLoanContractsPoolMng
        self.fpath_input_csv_target_secids = f'data/input/tgt_secpools/{self.str_today}_target_secids.csv'
        if not os.path.exists(self.fpath_input_csv_target_secids):
            self.fpath_input_csv_target_secids = f'data/input/tgt_secpools/{self.str_last_trddate}_target_secids.csv'
            if not os.path.exists(self.fpath_input_csv_target_secids):
                self.fpath_input_csv_target_secids = (
                    f'data/input/tgt_secpools/{self.str_last_last_trddate}_target_secids.csv'
                )

        self.fpath_input_xlsx_private_secpool_ready = (
            f"data/input/marginable_secpools/{self.str_today}即时可用券池-发布.xlsx"
        )
        self.fpath_input_csv_ssquota = (
            f"data/output/192.168.5.8_accounts_python_YZJ_xujie_accounts_307/"
            f"{self.str_last_trddate}_autot0_short_selling_quota.csv"
        )
        self.fpath_input_xls_public_secpool = f"data/input/marginable_secpools/公共券池{self.str_today}.xls"
        self.fpath_output_csv_tgtsecloan_mngdraft = (
            f"data/output/tgtsecloan_mngdraft/{self.str_today}_tgtsecloan_mngdraft.csv"
        )
        self.fpath_json_dict_windcode2wssdata = (
            f"data/input/windcode2wssdata/{self.str_today}_dict_windcode2wssdata.json"
        )

        self.fpath_input_xlsx_unfulfill_secloan_contract = f"data/input/trddata/融券明细_未了结仓单_{self.str_today}.xlsx"
        self.fpath_input_xlsx_private_secloan_contract = f"data/input/trddata/融券私用合约_{self.str_today}.xlsx"
        if not os.path.exists(self.fpath_input_xlsx_unfulfill_secloan_contract):
            self.fpath_input_xlsx_unfulfill_secloan_contract = (
                f'data/input/trddata/融券明细_未了结仓单_{self.str_last_trddate}.xlsx'
            )
            self.fpath_input_xlsx_private_secloan_contract = (
                f"data/input/trddata/融券私用合约_{self.str_last_trddate}.xlsx"
            )


        # 外部券源(预约券池)： 目前为自定义格式： 为提供给我们的可锁券数量（日更，全量）。主要来自于外部询券
        self.fpath_input_xlsx_secpool_from_outside_src = (
            f"data/input/pretrddata/marginable_secpools/ts_secpool_from_outside_source.xlsx"
        )
        self.fpath_input_xlsx_marginable_secpools_from_hait = (
            f"data/input/pretrddata/marginable_secpools/hait/每日券池-{self.str_today}.xlsx"
        )

        self.dirpath_input_xlsx_marginable_secpools_from_hait = "data/input/pretrddata/marginable_secpools/hait"
        self.dirpath_posttrddata_from_email = f'data/input/post_trddata/{self.str_last_trddate}'
        # 预约券申请文件，模板 -> plan text version
        self.fpath_output_xlsx_demand_of_secpool_from_outside_src = (
            f"data/output/mode_files/{self.str_today}_demand_of_secpool_from_outside_src.xlsx"
        )
        # 预约券申请文件发送邮箱参数信息
        self.email_from_addr = 'maxinzhe@mingshiim.com'
        self.email_pwd = 'D3cqJ7GpDiPNCubu'
        self.email_to_addr_haitsecpool_from_outside_src = '009995@htsec.com'
        self.email_subject_haitsecpool_from_outside_src = f'【券源需求】鸣石满天星七号-{self.str_today}'

        # pre-trade
        self.fpath_input_csv_grp_tgtsecids_by_cps = 'data/input/pretrddata/group_tgtsecids_by_composite.csv'
        self.fpath_output_xlsx_provided_secloan_analysis = (
            'data/output/security_loan/provided_security_loan_analysis.xlsx'
        )
        # # database
        self.db_pretrddata = self.server_mongodb['pre_trade_data']
        self.col_pretrd_grp_tgtsecids_by_cps = self.db_pretrddata['group_target_secids_by_composite']
        self.col_pretrd_tgtsecloan_mngdraft = self.db_pretrddata['tgtsecloan_mngdraft']
        self.col_provided_secloan_analysis = self.db_pretrddata['provided_secloan_analysis']

        # trading
        # # filepath
        self.fpath_input_csv_margin_account_fund = list_fpaths_data_file_path[0]
        self.fpath_input_csv_margin_account_holding = list_fpaths_data_file_path[1]
        self.fpath_input_csv_margin_account_order = list_fpaths_data_file_path[2]
        self.fpath_input_csv_margin_account_secloan = list_fpaths_data_file_path[3]

        # # database
        self.db_trading_data = self.server_mongodb['trading_data']
        self.col_trading_rawdata_fund = self.db_trading_data['trading_rawdata_fund']
        self.col_trading_rawdata_holding = self.db_trading_data['trading_rawdata_holding']
        self.col_trading_rawdata_order = self.db_trading_data['trading_rawdata_order']
        self.col_trading_rawdata_secloan = self.db_trading_data['trading_rawdata_secloan']

        # post-trade
        # # dict_map
        # # # todo 重要假设： 根据股票代码区分策略归属，不计数量
        self.dict_secid2composite = {}
        for dict_pretrddata_grp_tgt_secids_by_composite in (
                self.col_pretrd_grp_tgtsecids_by_cps.find(
                    {'DataDate': self.str_last_trddate, 'AcctIDByMXZ': self.acctidbymxz}
                )
        ):
            secid = dict_pretrddata_grp_tgt_secids_by_composite['SecurityID']
            composite = dict_pretrddata_grp_tgt_secids_by_composite['Composite']
            self.dict_secid2composite.update({secid: composite})

        # # database
        self.db_posttrddata = self.server_mongodb['post_trade_data']
        self.col_shortqty_from_secloan = self.db_posttrddata['shortqty_from_secloan']
        self.col_posttrd_fmtdata_secloan_from_private_secpool = (
            self.db_posttrddata['post_trade_fmtdata_secloan_from_private_secpool']
        )
        self.col_posttrd_fmtdata_secloan_from_public_secpool = (
            self.db_posttrddata['post_trade_fmtdata_secloan_from_public_secpool']
        )
        self.col_ssquota_by_secid_from_private_secpool = self.db_posttrddata['ssquota_by_secid_from_private_secpool']
        self.col_posttrd_rawdata_holding = self.db_posttrddata['post_trade_rawdata_holding']
        self.col_posttrd_fmtdata_holding = self.db_posttrddata['post_trade_fmtdata_holding']
        self.col_posttrd_rawdata_secloan_from_private_secpool = (
            self.db_posttrddata['post_trade_rawdata_secloan_from_private_secpool']
        )
        self.col_posttrd_rawdata_secloan_from_public_secpool = (
            self.db_posttrddata['post_trade_rawdata_secloan_from_public_secpool']
        )
        self.col_posttrd_rawdata_shortqty_from_secloan = (
            self.db_posttrddata['post_trade_rawdata_shortqty_from_secloan']
        )
        self.col_posttrd_fmtdata_shortqty_from_secloan = (
            self.db_posttrddata['post_trade_fmtdata_shortqty_from_secloan']
        )
        self.col_posttrd_position = self.db_posttrddata['post_trade_position']
        self.col_posttrd_fmtdata_fee_from_secloan = self.db_posttrddata['post_trade_fmtdata_fee_from_secloan']
        self.col_posttrd_fmtdata_ssquota_from_secloan = self.db_posttrddata['post_trade_fmtdata_ssquota_from_secloan']
        self.col_posttrd_rawdata_fee_from_secloan = self.db_posttrddata['post_trade_rawdata_fee_from_secloan']
        self.col_posttrd_rawdata_jgd = self.db_posttrddata['post_trade_rawdata_jgd']
        self.col_posttrd_fmtdata_jgd = self.db_posttrddata['post_trade_fmtdata_jgd']
        self.col_posttrd_pnl = self.db_posttrddata['post_trade_pnl']
        self.col_posttrd_pnl_by_secid = self.db_posttrddata['post_trade_pnl_by_secid']
        self.col_posttrd_pnl_by_acctidbymxz_cps = self.db_posttrddata['post_trade_pnl_by_acctidbymxz_cps']
        self.col_posttrd_rawdata_fund = self.db_posttrddata['post_trade_rawdata_fund']
        self.col_posttrd_fmtdata_fund = self.db_posttrddata['post_trade_fmtdata_fund']
        self.col_posttrd_secloan_utility_analysis = self.db_posttrddata['post_trade_secloan_utility_analysis']
        self.col_posttrd_pnl_by_acctidbymxz = self.db_posttrddata['post_trade_pnl_by_acctidbymxz']
        self.col_posttrd_cf_from_indirect_method = self.db_posttrddata['post_trade_cf_from_indirect_method']

        # # posttrd_holding: 程序在T日运行，清算T-1日数据，部分文件名为T-1，文件包日期为T-1日期
        self.fpath_input_rawdata_fund = list_fpaths_data_file_path[0]
        self.fpath_input_rawdata_holding = list_fpaths_data_file_path[1]
        self.fpath_input_rawdata_secloan_shortqty = list_fpaths_data_file_path[3]

        # todo: 成本考虑, 特殊指定hait_ehtc数据
        self.fpath_input_xlsx_fund = (
            f"data/input/post_trddata/{self.str_last_trddate}/两融_资产导出_{self.str_last_trddate}.xlsx"
        )

        self.fpath_input_xlsx_holding = (
            f"data/input/post_trddata/{self.str_last_trddate}/两融_资产导出_{self.str_last_trddate}.xlsx"
        )

        self.fpath_input_xlsx_shortqty = (
            f"data/input/post_trddata/{self.str_last_trddate}/融券明细_未了结仓单_{self.str_last_trddate}.xlsx"
        )
        self.fpath_input_xlsx_secloan_from_private_secpool = (
            f"data/input/post_trddata/{self.str_last_trddate}/私用券源合约{self.str_today}.xls"
        )
        self.fpath_input_xlsx_secloan_from_public_secpool = (
            f"data/input/post_trddata/{self.str_last_trddate}/融券公用合约_{self.str_last_trddate}.xlsx"
        )
        # self.fpath_input_xls_fee_from_secloan = (
        #     f"data/input/post_trddata/{self.str_last_trddate}/{self.prdalias}未结利息{self.str_today}.xls"
        # )
        # self.fpath_input_xlsx_jgd = (
        #     f"data/input/post_trddata/{self.str_last_trddate}/{self.prdalias}每日交割单-{self.str_today}.xlsx"
        # )
        self.fpath_output_xlsx_posttrd_analysis = "data/output/report/posttrd_analysis.xlsx"

        # # wind的公共数据下载， 下载的时间为自然时间， 交易日为查询日当天
        self.col_fmtted_wssdata = self.db_global['fmtted_wssdata']
        if download_winddata_mark:
            w.start(showmenu=False)
            wset = w.wset("sectorconstituent", f"date={self.str_today};sectorid=a001010100000000")
            list_windcodes = wset.Data[1]
            list_windcodes_patch = ['511990.SH']
            str_windcodes = ','.join(list_windcodes + list_windcodes_patch)
            wss_wssdata = w.wss(str_windcodes, "sec_name, close, pre_close, marginornot",
                                f"tradeDate={self.str_today};priceAdj=U;cycle=D")
            df_wssdata = pd.DataFrame(
                wss_wssdata.Data,
                index=['Symbol', 'Close', 'PreClose', 'MarginOrNot'],
                columns=wss_wssdata.Codes
            ).T.reset_index()
            df_wssdata['MarginOrNotMark'] = df_wssdata['MarginOrNot'].map({'是': 1, '否': 0})
            df_wssdata['DataDate'] = self.str_today
            df_wssdata = df_wssdata.rename(columns={'index': 'WindCode'})
            df_fmtted_wssdata = (
                df_wssdata.loc[:, ['DataDate', 'WindCode', 'Symbol', 'Close', 'PreClose', 'MarginOrNotMark']].copy()
            )
            list_dicts_fmtted_wssdata = df_fmtted_wssdata.to_dict('records')
            self.col_fmtted_wssdata.delete_many({'DataDate': self.str_today})
            self.col_fmtted_wssdata.insert_many(list_dicts_fmtted_wssdata)
        iter_fmtted_wssdata = self.col_fmtted_wssdata.find({'DataDate': self.str_today}, {'_id': 0})
        df_fmtted_wssdata = pd.DataFrame(iter_fmtted_wssdata)
        df_fmtted_wssdata = df_fmtted_wssdata.set_index('WindCode')
        self.dict_fmtted_wssdata = df_fmtted_wssdata.to_dict()
        # 其他
        self.dict_exchange2secidsrc = {'深A': 'SZSE', '沪A': 'SSE'}

    @classmethod
    def get_secid2windcode(cls, str_secid):
        # 将沪深交易所标的代码转为windcode
        if len(str_secid) == 6:
            if str_secid[0] in ['0', '3']:
                __windcode = str_secid + '.SZ'
                return __windcode
            elif str_secid[0] in ['6']:
                __windcode = str_secid + '.SH'
                return __windcode
            elif str_secid[0] in ['5']:
                __windcode = str_secid + '.SH'
                return __windcode
            else:
                raise ValueError('Wrong secid first letter: Cannot get windcode suffix according to SecurityID.')
        else:
            raise ValueError('The length of the security ID is not 6.')

    @staticmethod
    def decode_str(__str):
        value, charset = decode_header(__str)[0]
        if charset:
            value = value.decode(charset)
        return value

    @staticmethod
    def fmt_email_addr(__str):
        name, addr = parseaddr(__str)
        ret = formataddr((Header(name, 'utf-8').encode(), addr))
        return ret

    def get_attachment(self, msg, dirpath_output_attachment):
        for part in msg.walk():
            # 获取附件名称类型
            file_name = part.get_filename()
            if file_name:
                h = Header(file_name)
                # 对附件名称进行解码
                dh = decode_header(h)
                fn = dh[0][0]
                if dh[0][1]:
                    # 将附件名称可读化
                    fn = self.decode_str(str(fn, dh[0][1]))
                # 下载附件
                data = part.get_payload(decode=True)
                # 在指定目录下创建文件，注意二进制文件需要用wb模式打开
                fpath_output_attachment = os.path.join(dirpath_output_attachment, fn)
                if not os.path.exists(dirpath_output_attachment):
                    os.mkdir(dirpath_output_attachment)
                with open(fpath_output_attachment, 'wb') as f:
                    f.write(data)  # 保存附件
                    print(f"The attachment {fn} downloaded to {dirpath_output_attachment}.")

    def update_attachments_from_email(self, str_email_subject, str_email_tgtdate, dirpath_output_attachment):
        # todo 下载指定邮件中的所有附件到指定文件夹中，如果文件夹不存在，则新建文件夹
        # 从邮件中下载数据
        addr_pop3_server = 'pop.exmail.qq.com'
        server_pop3 = POP3_SSL(addr_pop3_server, 995)
        server_pop3.user(self.email_from_addr)
        server_pop3.pass_(self.email_pwd)
        resp, mails, octets = server_pop3.list()
        index = len(mails)  # 若有30封邮件，第30封邮件为最新的邮件
        mark_tgtmail_exist = 0
        for i in range(index, 0, -1):  # 倒叙遍历邮件
            resp, lines, octets = server_pop3.retr(i)
            msg_content = b'\r\n'.join(lines).decode('utf-8')
            msg = Parser().parsestr(msg_content)
            subject = self.decode_str(msg.get('Subject')).strip()
            str_date_in_msg = self.decode_str(msg.get('date')).strip()
            list_str_date_in_msg_split = str_date_in_msg.split()
            i_split_str_date = len(list_str_date_in_msg_split)
            if i_split_str_date == 2:
                dt_email_recvdate = datetime.strptime(list_str_date_in_msg_split[0], '%Y-%m-%d')
            elif i_split_str_date == 6:
                try:
                    dt_email_recvdate = datetime.strptime(str_date_in_msg, '%a, %d %b %Y %H:%M:%S +0800')
                except ValueError:
                    dt_email_recvdate = datetime.strptime(str_date_in_msg, '%a, %d %b %Y %H:%M:%S -0000')

            elif i_split_str_date == 7:
                try:
                    dt_email_recvdate = datetime.strptime(str_date_in_msg, '%a, %d %b %Y %H:%M:%S +0800 (GMT+08:00)')
                except ValueError:
                    dt_email_recvdate = datetime.strptime(str_date_in_msg, '%a, %d %b %Y %H:%M:%S +0800 (CST)')

            else:
                raise ValueError(f'Unknown date format from email: {str_date_in_msg}')

            str_email_recvdate = dt_email_recvdate.strftime('%Y%m%d')
            if subject == str_email_subject and str_email_recvdate == str_email_tgtdate:
                self.get_attachment(msg, dirpath_output_attachment)
                mark_tgtmail_exist = 1
                break
        if not mark_tgtmail_exist:
            print(f'Warning: {str_email_subject} not found.')
        # server_pop3.quit()
        print('The attachment from email download finished.')

    def send_file_via_email(self, email_to_addr, subject, fpath_file, fn_attachment):
        # 通过邮件发送文件
        # 发送的文件为单一附件
        addr_smtp_server = 'smtp.exmail.qq.com'
        msg = MIMEMultipart()
        msg['From'] = self.fmt_email_addr(f'马新哲<{self.email_from_addr}>')
        msg['To'] = self.fmt_email_addr(f'<{email_to_addr}>')
        msg['Subject'] = Header(subject, 'utf-8').encode()
        with open(fpath_file, 'rb') as f:
            mime = MIMEBase('application', 'octet-stream')
            mime.set_payload(f.read())
            encoders.encode_base64(mime)
            mime.add_header('Content-Disposition', 'attachment', filename=fn_attachment)
            msg.attach(mime)
        server_smtp = smtplib.SMTP(addr_smtp_server)
        server_smtp.starttls()
        server_smtp.login(self.email_from_addr, self.email_pwd)
        server_smtp.sendmail(self.email_from_addr, email_to_addr, msg.as_string())
        print('The mail has been sent.')
        server_smtp.quit()

    @staticmethod
    def get_list_str_trddate(str_startdate, str_enddate):
        """
        获得期间自然日期间的交易日，参数为闭区间两端
        :param str_startdate: 起始日期，包括
        :param str_enddate: 终止日期，包括
        :return: list, 指定期间内交易日的列表
        """
        wtdays = w.tdays(str_startdate, str_enddate)
        list_str_trddates = [x.strftime('%Y%m%d') for x in wtdays.Data[0]]
        return list_str_trddates

    @classmethod
    def get_mingshi_sectype_from_code(cls, str_code):
        """
        实际使用的函数
        :param str_code: SecurityID.SecurityIDSource
            1. SecuritySource:
                1. SSE: 上交所
                2. SZSE: 深交所
                3. ITN: internal id
        :return:
            1. CE, Cash Equivalent, 货基，质押式国债逆回购
            2. CS, Common Stock, 普通股
            3. ETF, ETF, 注意：货币类ETF归类为CE，而不是ETF
            4. SWAP, swap
        """

        list_split_wcode = str_code.split('.')
        secid = list_split_wcode[0]
        exchange = list_split_wcode[1]
        if exchange in ['SH', 'SSE'] and len(secid) == 6:
            if secid in ['511990', '511830', '511880', '511850', '511660', '511810', '511690']:
                return 'CE'
            elif secid in ['204001']:
                return 'CE'
            elif secid[:3] in ['600', '601', '603', '688']:
                return 'CS'
            elif secid in ['510500', '000905', '512500']:
                return 'ETF'
            else:
                return 'IrrelevantItem'

        elif exchange in ['SZ', 'SZSE'] and len(secid) == 6:
            if secid[:3] in ['000', '001', '002', '003', '004', '300', '301', '302', '303', '304', '305', '306', '307',
                             '308', '309']:
                return 'CS'
            elif secid[:3] in ['115', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129']:
                return '可转债'
            elif secid[:3] in ['131']:
                return 'CE'
            elif secid in ['159001', '159005', '159003']:
                return 'CE'
            else:
                return 'IrrelevantItem'
        elif exchange in ['CFE', 'CFFEX']:
            return 'Index Future'

        elif exchange == 'ITN':
            sectype = secid.split('_')[0]
            return sectype

        else:
            raise ValueError(f'{str_code} has unknown exchange or digit number is not 6.')

    @staticmethod
    def secloan_match(fpath_input_csv_target_secids, fpath_xlsx_secloan_secids_2b_matched):
        """
        两个券池进行匹配，求交集
        """
        df_secloan_secids_tgt = pd.read_csv(
            fpath_input_csv_target_secids,
            converters={'SecurityID': lambda x: str(x).zfill(6)}
        )
        list_secloan_secids_tgt = df_secloan_secids_tgt['SecurityID'].to_list()
        set_secloan_secids_tgt = set(list_secloan_secids_tgt)
        if os.path.exists(fpath_xlsx_secloan_secids_2b_matched):
            dict_dfs_secloan_secids_2b_matched = pd.read_excel(
                fpath_xlsx_secloan_secids_2b_matched,
                sheet_name=None,
                converters={
                    'code': lambda x: str(x).zfill(6),
                    '证券代码': lambda x: str(x).zfill(6),
                }
            )

            list_secid_fields_name = ['SecurityID', 'code', '证券代码']  # 标题栏
            set_secloan_secids_2b_matched = set()
            for sheet_name, df_ in dict_dfs_secloan_secids_2b_matched.items():
                for secid_field_name in list_secid_fields_name:
                    if secid_field_name in df_.columns:
                        for secid in df_[secid_field_name].to_list():
                            set_secloan_secids_2b_matched.add(secid)

            set_matched_secloan_secids = set_secloan_secids_2b_matched & set_secloan_secids_tgt
            list_matched_secloan_secids = list(set_matched_secloan_secids)
            list_matched_secloan_secids.sort()
            print(fpath_xlsx_secloan_secids_2b_matched, list_matched_secloan_secids)
            i_secloan_secids = len(list_matched_secloan_secids)
        else:
            i_secloan_secids = 'DataFileNotExists'
        return i_secloan_secids

    def output_provided_secloan_analysis_xlsx(self):
        list_broker_alias_for_secloan_analysis = ['hait', 'huat', 'swhy', 'gtja']
        list_dicts_secloan_analysis_among_brokers = []
        for broker_alias in list_broker_alias_for_secloan_analysis:
            i_secloan_secids = task.secloan_match(
                f'D:/projects/autot0/data/input/tgt_secpools/{self.str_today}_target_secids.csv',
                f'D:/projects/autot0/data/input/pretrddata/marginable_secpools/{broker_alias}/'
                f'每日券池-{self.str_today}.xlsx'
            )
            dict_secloan_analysis_among_brokers = {
                'DataDate': self.str_today,
                'BrokerAlias': broker_alias,
                'IntersectionSecurityCount': i_secloan_secids
            }
            list_dicts_secloan_analysis_among_brokers.append(dict_secloan_analysis_among_brokers)
        self.col_provided_secloan_analysis.delete_many({'DataDate': self.str_today})
        if list_dicts_secloan_analysis_among_brokers:
            self.col_provided_secloan_analysis.insert_many(list_dicts_secloan_analysis_among_brokers)

        iter_secloan_analysis = self.col_provided_secloan_analysis.find({}, {'_id': 0})
        df_secloan_analysis = pd.DataFrame(iter_secloan_analysis)
        df_secloan_analysis.to_excel(self.fpath_output_xlsx_provided_secloan_analysis, index=False)

        print('Output provided_security_loan_analysis.xlsx Finished.')


if __name__ == '__main__':
    # 盘后运行, 更新数据库
    task = Globals(download_winddata_mark=0)
    task.output_provided_secloan_analysis_xlsx()

    print('Done')
