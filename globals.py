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

from WindPy import w


class Globals:
    def __init__(self):
        # 日期部分
        self.dt_today = datetime.today()

        w.start()
        self.str_today = self.dt_today.strftime('%Y%m%d')
        self.dt_last_trddate = w.tdaysoffset(-1, self.str_today).Data[0][0]
        self.str_last_trddate = self.dt_last_trddate.strftime('%Y%m%d')
        self.dt_next_trddate = w.tdaysoffset(1, self.str_today).Data[0][0]
        self.str_next_trddate = self.dt_next_trddate.strftime('%Y%m%d')

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

    def get_attachment(self, msg):
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
                with open(self.fpath_input_xlsx_marginable_secpools_from_hait, 'wb') as f:
                    f.write(data)  # 保存附件
                    print(f"The attachment {fn} download finished.")

    def update_attachments_from_email(self):
        # 从邮件中下载数据
        addr_pop3_server = 'pop.exmail.qq.com'
        server_pop3 = POP3_SSL(addr_pop3_server, 995)
        server_pop3.user(self.email_from_addr)
        server_pop3.pass_(self.email_pwd)
        resp, mails, octets = server_pop3.list()
        index = len(mails)  # 若有30封邮件，第30封邮件为最新的邮件

        for i in range(index, 0, -1):  # 倒叙遍历邮件
            resp, lines, octets = server_pop3.retr(i)
            msg_content = b'\r\n'.join(lines).decode('utf-8')
            msg = Parser().parsestr(msg_content)
            subject = self.decode_str(msg.get('Subject')).strip()
            if subject in ['每日券池信息']:
                dt_email_datetime = datetime.strptime(msg.get('Date')[0:19], '%Y-%m-%d %H:%M:%S')
                str_email_date = datetime.strftime(dt_email_datetime, '%Y%m%d')
                if str_email_date in [self.str_today]:
                    self.get_attachment(msg)
                    break
        server_pop3.quit()

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


if __name__ == '__main__':
    task = Globals()
    task.update_attachments_from_email()
