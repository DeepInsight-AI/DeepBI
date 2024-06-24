# filename: analyze_ads_data.py
import pandas as pd

# 1. Load data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 2. Define ACOS ranges
def categorize_acos(acos):
    if pd.isna(acos) or acos == 0:
        return "无穷大"
    elif acos < 20:
        return "极低"
    elif 20 <= acos < 30:
        return "较低"
    elif 30 <= acos < 50:
        return "较高"
    elif acos >= 50:
        return "极高"
    else:
        return "未知"

data['ACOS_category'] = data['ACOS_7d'].apply(categorize_acos)

# 3. Define criteria for reasons
def find_reason(row):
    if row['ACOS_category'] == "较高" and row['total_clicks_30d'] > 10 and row['total_sales14d_30d'] < 0.1 * row['total_cost_30d']:
        return "acos值较高，关键词点击次数较多，销售额占比相对极少"
    elif row['ACOS_category'] == "极高" and row['total_sales14d_30d'] < 0.1 * row['total_cost_30d']:
        return "acos值极高，销售额占比相对极少"
    elif row['total_clicks_30d'] > 10 and row['total_cost_30d'] > 0 and row['total_sales14d_30d'] == 0:
        return "近一个月的历史点击次数超过10次，有花费但是没销售额"
    else:
        return None

data['reason'] = data.apply(find_reason, axis=1)

# 4. Filter relevant data and output to new CSV
output_data = data[['campaignName', 'adGroupName', 'total_cost_7d', 'ACOS_7d', 'total_clicks_30d', 'searchTerm', 'matchType', 'reason']]
output_data = output_data.dropna(subset=['reason'])
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_劣质搜索词_ES_2024-06-07.csv'

output_data.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("分析完成，结果已保存到", output_file_path)