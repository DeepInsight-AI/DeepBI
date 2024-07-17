# filename: gutana_search_term_analysis.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义字段映射
field_mapping = {
    "keyword": "关键词",
    "searchTerm": "搜索词",
    "adGroupName": "广告组名称",
    "adGroupId": "广告组的id",
    "matchType": "匹配类型",
    "campaignName": "广告活动名称",
    "campaignId": "广告活动ID",
    "ORDER_1m": "近一个月的订单数",
    "ORDER_7d": "近7天的订单数",
    "ORDER_yesterday": "昨天的订单数",
    "total_clicks_30d": "搜索词最近30天的总点击数",
    "total_clicks_7d": "搜索词最近7天的总点击数",
    "total_clicks_yesterday": "搜索词昨天的点击数",
    "total_sales14d_30d": "搜索词最近30天的总销售额",
    "total_sales14d_7d": "搜索词最近7天的总销售额",
    "total_sales14d_yesterday": "搜索词昨天的销售额",
    "total_cost_30d": "搜索词最近30天的总花费",
    "total_cost_7d": "搜索词最近7天的总花费",
    "total_cost_yesterday": "搜索词昨天的花费",
    "ACOS_30d": "搜索词最近30天的平均ACOS值",
    "ACOS_7d": "搜索词最近7天的平均ACOS值",
    "ACOS_yesterday": "搜索词昨天的ACOS值"
}

# ACOS分类函数
def classify_acos(acos):
    if acos < 0.20:
        return '极低'
    elif 0.20 <= acos < 0.30:
        return '较低'
    elif 0.30 <= acos < 0.50:
        return '较高'
    elif acos >= 0.50:
        return '极高'
    else:
        return '无穷大'

# 将数据转换为适当的格式
data['ACOS_7d_class'] = data['ACOS_7d'].apply(classify_acos)
data['total_cost_7d'] = pd.to_numeric(data['total_cost_7d'], errors='coerce')
data['total_sales14d_7d'] = pd.to_numeric(data['total_sales14d_7d'], errors='coerce')
data['total_clicks_30d'] = pd.to_numeric(data['total_clicks_30d'], errors='coerce')

# 定义搜索词识别和原因
def identify_reason(row):
    reasons = []
    if row['ACOS_7d_class'] == '较高' and row['total_clicks_7d'] > 10 and row['total_sales14d_7d'] / row['total_sales14d_30d'] < 0.1:
        reasons.append('定义一：ACOS较高，点击次数较多，销售额占比少')
    if row['ACOS_7d_class'] == '极高' and row['total_sales14d_7d'] / row['total_sales14d_30d'] < 0.1:
        reasons.append('定义二：ACOS极高，销售额占比少')
    if row['total_clicks_30d'] > 10 and row['total_cost_30d'] > 0 and row['total_sales14d_30d'] == 0:
        reasons.append('定义三：近一个月点击超过10次，有花费但无销售额')
    return ', '.join(reasons)

# 应用判断条件
data['reason'] = data.apply(identify_reason, axis=1)

# 过滤出满足条件的数据
filtered_data = data[data['reason'] != '']

# 提取需要的列
result_data = filtered_data[[
    "campaignName",
    "adGroupName",
    "total_cost_7d",
    "ACOS_7d",
    "total_clicks_30d",
    "searchTerm",
    "matchType",
    "reason"
]]

# 重命名列以符合要求
result_data.columns = [
    "广告活动",
    "广告组",
    "近七天的花费",
    "近七天的acos值",
    "近一个月的总点击数",
    "搜索词",
    "匹配类型",
    "原因"
]

# 保存结果为CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_劣质搜索词_v1_0_LAPASA_IT_2024-07-09.csv'
result_data.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"结果已保存至 {output_file_path}")