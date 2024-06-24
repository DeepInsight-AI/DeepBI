# filename: combined_script_with_date.py
import pandas as pd
from datetime import datetime

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv')

# 获取当前日期
current_date = datetime.now().strftime('%Y-%m-%d')

# 将当前日期添加到数据框中
data['date'] = current_date

# 打印所有列名
print(data.columns)

# 筛选条件
conditions = (
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24) &
    (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.24)
)

# 计算竞价调整
data['竞价操作'] = 0.05  # 初始竞价提高5%
data.loc[conditions, '竞价操作'] = 0.5  # 最高提高到50%

# 添加原因列
data['对广告位进行竞价操作的原因'] = '满足定义一的条件'

# 输出结果
output_data = data[['date', 'campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', '竞价操作', '对广告位进行竞价操作的原因']]

# 保存到CSV文件
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\优质广告位_FR_2024-5-27_deepseek.csv', index=False)

# 打印结果以验证
print(output_data)