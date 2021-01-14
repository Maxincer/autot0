import pandas as pd

fpath_tgt = r'D:\projects\autot0\data\input\tgt_secpools\20210114_target_secids.csv'

df_tgt = pd.read_csv(fpath_tgt, converters={'SecurityID': lambda x: str(x).zfill(6)})

list_secids = df_tgt['SecurityID'].to_list()

fpath_marginable = r'D:\projects\autot0\data\input\pretrddata\marginable_secpools\huat\每日券池-20210114.xlsx'
df_marginable = pd.read_excel(
    fpath_marginable,
    dtype={'委托数量': float, '委托期限': int, '委托利率': float},
    converters={'证券代码': lambda x: str(x).zfill(6)}
)
list_dicts_marginable = df_marginable.to_dict('records')
for dict_marginable in list_dicts_marginable:
    secid = dict_marginable['证券代码']
    if secid in list_secids:

