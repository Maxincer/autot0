import pandas as pd

from WindPy import w

w.start()

fpath_ = (
    r"D:\projects\autot0\data\output\192.168.5.8_accounts_python_YZJ_xujie_accounts_307\20201109_autot0_short_selling_quota.csv"
)

df_csv = pd.read_csv(fpath_, converters={'Id': lambda x: x.zfill(6)}, dtype={'Quota': float})


def get_secid2windcode(str_secid):
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


df_csv['WindCode'] = df_csv['Id'].apply(get_secid2windcode)
list_windcodes = df_csv['WindCode'].to_list()
str_windcodes = ','.join(list_windcodes)
wss_close = w.wss(str_windcodes, 'close', 'tradeDate=20201106')
dict_windcode2close = dict(zip(wss_close.Codes, wss_close.Data[0]))
df_csv['Close'] = df_csv['WindCode'].map(dict_windcode2close)
df_csv['MarketValue'] = df_csv['Close'] * df_csv['Quota']
df_csv.to_csv('sum_mv.csv', encoding='ansi')