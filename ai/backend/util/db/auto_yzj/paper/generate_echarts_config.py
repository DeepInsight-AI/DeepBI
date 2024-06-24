# filename: generate_echarts_config.py
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Bar

# 读取数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\test\预处理.csv')

# 筛选条件
conditions = (
    (data['avg_ACOS_7d'] > 0.24)
    & (data['ACOS'] > 0.24)
    & (data['clicks'] >= 10)
    & (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m'])
)

# 应用筛选条件
filtered_data = data[conditions]

# 更新预算
filtered_data['Budget'] = filtered_data['Budget'].apply(lambda x: max(8, x - 5))

# 输出结果到CSV文件
output_columns = [
    'date',
    'campaignName',
    'Budget',
    'clicks',
    'ACOS',
    'avg_ACOS_7d',
    'clicks_7d',
    'sales_1m',
    'avg_ACOS_1m',
    'clicks_1m',
    'sales_1m',
    'country_avg_ACOS_1m',
    'Budget',
    'reason'
]

output_data = filtered_data[output_columns]
output_data['reason'] = '符合定义一的条件，因此降低预算'
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\test\提问策略\测试_FR_2024-05-28_deepseek.csv', index=False)

# 生成ECharts配置代码
bar = (
    Bar()
    .add_xaxis(output_data['campaignName'].tolist())
    .add_yaxis("预算", output_data['Budget'].tolist())
    .set_global_opts(title_opts=opts.TitleOpts(title="广告活动预算调整"))
)

# 输出ECharts配置代码
echarts_config = bar.dump_options()
print(echarts_config)

# 确认执行成功
print("数据处理完成，结果已保存到CSV文件，并生成了ECharts配置代码。")