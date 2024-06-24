# filename: generate_echarts_code.py
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Bar

# 读取数据
data = pd.read_csv('/backend/util/db/auto_yzj/日常优化/手动sp广告/test/预处理1.csv')

# 筛选符合条件的广告活动
filtered_data = data[(data['avg_ACOS_7d'] > 0.24) & (data['ACOS'] > 0.24) & (data['clicks'] >= 10) & (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m'])]

# 准备数据
bar = (
    Bar()
    .add_xaxis(filtered_data['campaignName'].tolist())
    .add_yaxis("预算", filtered_data['Budget'].tolist())
    .set_global_opts(title_opts=opts.TitleOpts(title="劣质广告活动预算调整"))
)

# 生成Echarts代码
echarts_code = bar.dump_options()

# 输出Echarts代码
print(echarts_code)

# 确认执行成功
print("Echarts代码生成成功。")
