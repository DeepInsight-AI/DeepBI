# filename: detect_anomalies_debug.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv'
try:
    data = pd.read_csv(file_path)
    print("CSV 文件加载成功。")
except Exception as e:
    print(f"加载 CSV 文件失败: {e}")
    exit(1)

# 新增广告商品SKU
data['advertisedSku'] = data['campaignName'] + "_" + data['adGroupName']

# 计算7天和30天每单平均点击次数
try:
    data['avg_clicks_per_order_7d'] = (data['total_clicks_7d'] / data['total_purchases7d_7d']).round(2)
    data['avg_clicks_per_order_30d'] = (data['total_clicks_30d'] / data['total_purchases7d_30d']).round(2)
    print("计算平均点击次数成功。")
except Exception as e:
    print(f"计算平均点击次数失败: {e}")
    exit(1)

# 定义异常判断函数
def detect_anomalies(row):
    anomalies = []
    
    # 检查点击量足够但销售额为0的异常
    if pd.isna(row['ACOS_yesterday']) and row['clicks_yesterday'] > 0:
        if row['clicks_yesterday'] > row['avg_clicks_per_order_7d'] and row['total_sales14d_yesterday'] == 0:
            anomalies.append('昨天点击量足够但无销售')
        if row['clicks_yesterday'] > row['avg_clicks_per_order_30d'] and row['total_sales14d_yesterday'] == 0:
            anomalies.append('昨天点击量足够但无销售')
    
    # 检查ACOS波动异常
    if not pd.isna(row['ACOS_yesterday']):
        if abs(row['ACOS_yesterday'] - row['ACOS_7d']) / row['ACOS_7d'] > 0.3 or abs(row['ACOS_yesterday'] - row['ACOS_30d']) / row['ACOS_30d'] > 0.3:
            anomalies.append('ACOS值波动异常')
    
    return ', '.join(anomalies)

# 应用异常判断函数
try:
    data['Anomaly Description'] = data.apply(detect_anomalies, axis=1)
    print("异常检测完成。")
except Exception as e:
    print(f"异常检测失败: {e}")
    exit(1)

# 筛选出存在异常的记录
anomalies_data = data[data['Anomaly Description'] != '']

# 选择所需的列
output_columns = [
    'campaignName', 'adGroupName', 'advertisedSku', 
    'Anomaly Description', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d', 
    'clicks_yesterday', 'avg_clicks_per_order_7d', 'avg_clicks_per_order_30d'
]
result = anomalies_data[output_columns]

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\异常检测_投放词_点击量足够但ACOS异常1_v1_0_LAPASA_US_2024-07-14.csv'
try:
    result.to_csv(output_file_path, index=False)
    print(f"异常检测完成，结果已保存到CSV文件: {output_file_path}")
except Exception as e:
    print(f"保存 CSV 文件失败: {e}")
    exit(1)