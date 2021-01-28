from dbfread import DBF
import pandas as pd

table1 = DBF(r'D:\projects\autot0\data\rzrq_dzls.DBF')
table2 = DBF(r'D:\projects\autot0\data\rzrq_jyls.DBF')

df1 = pd.DataFrame(table1)
df1.to_excel('df1.xlsx')
print(df1)

df2 = pd.DataFrame(table2)
df2.to_excel('df2.xlsx')

print(df2)



