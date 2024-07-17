# filename: analyze_and_optimize_ads.py

import pandas as pd

# 读取数据
df = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv')

# 增加New_keywordBid和操作原因的列
df['New_keywordBid'] = ""
df['Operation'] = ""

# 定义一：最近7天的平均ACOS值大于0.24且小于等于0.5，并且最近30天的平均ACOS值大于0且小于等于0.5
cond1 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] <= 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.5)
df.loc[cond1, 'New_keywordBid'] = df['keywordBid'] / (((df['ACOS_7d']-0.24)/0.24)+1)
df.loc[cond1, 'Operation'] = "Adjust bid due to definition 1"

# 定义二：最近7天的平均ACOS值大于0.5，并且最近30天的平均ACOS值小于等于0.36
cond2 = (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] <= 0.36)
df.loc[cond2, 'New_keywordBid'] = df['keywordBid'] / (((df['ACOS_7d']-0.24)/0.24)+1)
df.loc[cond2, 'Operation'] = "Adjust bid due to definition 2"

# 定义三：最近7天点击数大于等于10，最近7天销售为0，最近30天的平均ACOS值小于等于0.36
cond3 = (df['total_clicks_7d'] >= 10) & (df['total_sales14d_7d'] == 0) & (df['ACOS_30d'] <= 0.36)
df.loc[cond3, 'New_keywordBid'] = df['keywordBid'] - 0.04
df.loc[cond3, 'Operation'] = "Reduce bid due to definition 3"

# 定义四：最近7天点击数大于10，最近7天销售为0，最近30天的平均ACOS值大于0.5
cond4 = (df['total_clicks_7d'] > 10) & (df['total_sales14d_7d'] == 0) & (df['ACOS_30d'] > 0.5)
df.loc[cond4, 'New_keywordBid'] = "关闭"
df.loc[cond4, 'Operation'] = "Close bid due to definition 4"

# 定义五：最近7天的平均ACOS值大于0.5，最近30天的平均ACOS值大于0.36
cond5 = (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] > 0.36)
df.loc[cond5, 'New_keywordBid'] = "关闭"
df.loc[cond5, 'Operation'] = "Close bid due to definition 5"

# 定义六：最近30天销售为0，最近30天的总花费大于等于5
cond6 = (df['total_sales14d_30d'] == 0) & (df['total_cost_30d'] >= 5)
df.loc[cond6, 'New_keywordBid'] = "关闭"
df.loc[cond6, 'Operation'] = "Close bid due to definition 6"

# 定义七：最近30天销售为0，近30天点击数大于等于15，最近7天的点击数大于0
cond7 = (df['total_sales14d_30d'] == 0) & (df['total_clicks_30d'] >= 15) & (df['total_clicks_7d'] > 0)
df.loc[cond7, 'New_keywordBid'] = "关闭"
df.loc[cond7, 'Operation'] = "Close bid due to definition 7"

# 只输出被识别并处理的商品投放的信息
result_df = df[df['Operation'] != ""]

# 选择需要的列
result_df = result_df[['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 
                       'New_keywordBid', 'targeting', 'total_cost_7d', 'total_sales14d_7d', 
                       'total_cost_30d', 'total_clicks_7d', 'ACOS_7d', 'ACOS_30d', 'total_clicks_30d', 
                       'Operation']]

# 将结果写入CSV文件
result_df.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_DELOMO_FR_2024-07-09.csv', index=False)

print("Finished processing and saved the result.")