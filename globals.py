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

from pymongo import MongoClient

from WindPy import w

STR_TODAY = datetime.today().strftime('%Y%m%d')


class Globals:
    def __init__(self, str_today=STR_TODAY):
        # 日期部分
        self.str_today = str_today  # 该日必为交易日
        self.server_mongodb = MongoClient('mongodb://localhost:27017/')
        self.db_global = self.server_mongodb['global']
        self.col_trdcalendar = self.db_global['trade_calendar']
        self.list_str_trdcalendar = list(self.col_trdcalendar.find({'Year': '2020'}))[0]['Data']
        idx_str_today = self.list_str_trdcalendar.index(self.str_today)
        self.str_last_trddate = self.list_str_trdcalendar[idx_str_today - 1]
        self.str_next_trddate = self.list_str_trdcalendar[idx_str_today + 1]
        self.str_last_last_trddate = self.list_str_trdcalendar[idx_str_today - 2]
        self.str_next_next_trddate = self.list_str_trdcalendar[idx_str_today + 2]
        self.acctidbymxz = '307_m_hait_2000'
        self.prdalias = '鸣石满天星7号'

        # 路径部分
        # # SecLoanContractsPoolMng
        self.fpath_input_csv_target_secids = f'data/input/tgt_secpools/{self.str_today}_target_secids.csv'
        if not os.path.exists(self.fpath_input_csv_target_secids):
            self.fpath_input_csv_target_secids = f'data/input/tgt_secpools/{self.str_last_trddate}_target_secids.csv'

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
            f"data/input/marginable_secpools/ts_secpool_from_outside_source.xlsx"
        )
        self.fpath_input_xlsx_marginable_secpools_from_hait = (
            f"data/input/marginable_secpools/{self.str_today}_marginable_secpools_from_hait.xlsx"
        )

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
        # # database
        self.db_pretrddata = self.server_mongodb['pre_trade_data']
        self.col_pretrd_grp_tgtsecids_by_cps = self.db_pretrddata['group_target_secids_by_composite']

        # post-trade
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

        # # posttrd_holding: 程序在T日运行，清算T-1日数据，部分文件名为T-1，文件包日期为T-1日期
        self.fpath_input_xlsx_fund = (
            f'data/input/post_trddata/{self.str_last_trddate}/两融_资产导出_{self.str_last_trddate}.xlsx'
        )
        self.fpath_input_xlsx_holding = (
            f'data/input/post_trddata/{self.str_last_trddate}/两融_资产导出_{self.str_last_trddate}.xlsx'
        )
        self.fpath_output_csv_autot0_holding = (
            f'data/output/192.168.5.8_accounts_python_YZJ_xujie_accounts_307/{self.str_today}_autot0_holding.csv'
        )  # 尚未使用
        self.fpath_input_xlsx_shortqty = (
            f"data/input/post_trddata/{self.str_last_trddate}/融券明细_未了结仓单_{self.str_last_trddate}.xlsx"
        )
        self.fpath_input_xlsx_secloan_from_private_secpool = (
            f"data/input/post_trddata/{self.str_last_trddate}/私用券源合约{self.str_today}.xls"
        )
        self.fpath_input_xlsx_secloan_from_public_secpool = (
            f"data/input/post_trddata/{self.str_last_trddate}/融券公用合约_{self.str_last_trddate}.xlsx"
        )
        self.fpath_input_xls_fee_from_secloan = (
            f"data/input/post_trddata/{self.str_last_trddate}/{self.prdalias}未结利息{self.str_today}.xls"
        )
        self.fpath_input_xlsx_jgd = (
            f"data/input/post_trddata/{self.str_last_trddate}/{self.prdalias}每日交割单-{self.str_today}.xlsx"
        )
        self.fpath_output_xlsx_pnl_analysis = "data/output/pnl/pnl_analysis.xlsx"

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

    def get_attachment(self, msg, fpath_output_attachment):
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
                with open(fpath_output_attachment, 'wb') as f:
                    f.write(data)  # 保存附件
                    print(f"The attachment {fn} download finished.")

    def update_attachments_from_email(self, str_email_subject, fpath_output_attachment):
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
            if subject in [str_email_subject]:
                self.get_attachment(msg, fpath_output_attachment)
                mark_tgtmail_exist = 1
                break
        if not mark_tgtmail_exist:
            print(f'Warning: {str_email_subject} not found.')
        server_pop3.quit()
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

    @staticmethod
    def get_mingshi_sectype_from_code(str_code):
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


if __name__ == '__main__':
    task = Globals()
    a = task.get_list_str_trddate('20201019', '20201023')
    print(a)



