# NOTE: This script makes a lot of assumptions that are based on when it was written (28/05/2024)
# Such as the order of the columns and number of delegations (35)
# Make sure that the website still follows the same structure before running it.
import requests
import re
import pandas as pd
from lxml import html


r = requests.get('https://zh.wikipedia.org/wiki/第十四届全国人民代表大会代表名单')
dfs = pd.read_html(r.text)
final_df = []
columns = ['姓名', '政党', '性别', '民族', '出生日期', '职务', '备注']
delegation_names = [li.text_content().strip() for li in 
                    html.fromstring(r.text).find('.//*[@id="toc-代表团-sublist"]').findall(".//li/a/div")]
delegation_names = [re.search(r'\d+\.\d+(.+?)（', item).group(1) for item in delegation_names]

for df, delegation_name in zip(dfs[1:36], delegation_names):
    df.columns = df.columns.get_level_values(0)
    df = df.iloc[:, :7]
    df.columns = columns
    df['代表团'] = delegation_name
    final_df.append(df)
    
final_df = pd.concat(final_df)
final_df.to_excel("china_national_people_congress.xlsx", index=False)