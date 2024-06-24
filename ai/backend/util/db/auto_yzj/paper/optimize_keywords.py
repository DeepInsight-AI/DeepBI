# filename: optimize_keywords.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义降价和原因字典
results = {
    'campaignName': [],
    'adGroupName': [],
    'keyword': [],
    'keywordBid': [],
    'New_keywordBid': [],
    'ACOS_30d': [],
    'ACOS_7d': [],
    'clicks_7d': [],
    '降价': [],
    '原因': []
}

# 遍历每一行数据进行处理
for index, row in data.iterrows():
    campaignName = row['campaignName']
    adGroupName = row['adGroupName']
    keyword = row['keyword']
    keywordBid = row['keywordBid']
    ACOS_30d = row['ACOS_30d']
    ACOS_7d = row['ACOS_7d']
    clicks_7d = row['total_clicks_7d']
    total_sales_7d = row['total_sales14d_7d']
    total_sales_30d = row['total_sales14d_30d']
    ORDER_1m = row['ORDER_1m']
  
    new_bid = keywordBid
    reason = []

    if 0.24 < ACOS_7d < 0.5 and 0 < ACOS_30d < 0.24:
        new_bid -= 0.03
        reason.append('定义一')

    if 0.24 < ACOS_7d < 0.5 and 0.24 < ACOS_30d < 0.5:
        new_bid -= 0.04
        reason.append('定义二')

    if total_sales_7d == 0 and clicks_7d > 0 and 0.24 < ACOS_30d < 0.5:
        new_bid -= 0.04
        reason.append('定义三')

    if 0.24 < ACOS_7d < 0.5 and ACOS_30d > 0.5:
        new_bid -= 0.05
        reason.append('定义四')

    if ACOS_7d > 0.5 and 0 < ACOS_30d < 0.24:
        new_bid -= 0.05
        reason.append('定义五')

    if total_sales_7d == 0 and clicks_7d > 0 and ORDER_1m == 0 and row['total_clicks_30d'] > 10:
        new_bid = '关闭'
        reason.append('定义六')

    if total_sales_7d == 0 and clicks_7d > 0 and ACOS_30d > 0.5:
        new_bid = '关闭'
        reason.append('定义七')

    if ACOS_7d > 0.5 and ACOS_30d > 0.24:
        new_bid = '关闭'
        reason.append('定义八')

    if reason:
        results['campaignName'].append(campaignName)
        results['adGroupName'].append(adGroupName)
        results['keyword'].append(keyword)
        results['keywordBid'].append(keywordBid)
        results['New_keywordBid'].append(new_bid)
        results['ACOS_30d'].append(ACOS_30d)
        results['ACOS_7d'].append(ACOS_7d)
        results['clicks_7d'].append(clicks_7d)
        results['降价'].append(keywordBid - new_bid if new_bid != '关闭' else 0)
        results['原因'].append(';'.join(reason))

# 生成新的DataFrame并保存
result_df = pd.DataFrame(results)
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_v1_11_ES_2024-06-20.csv'
result_df.to_csv(output_path, index=False)

print("处理完成，结果已保存到文件：", output_path)