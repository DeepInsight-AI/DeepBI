# filename: echarts_chart.py
from pyecharts import options as opts
from pyecharts.charts import Bar
import pandas as pd

# 读取数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\test\预处理.csv')

# 假设我们需要展示昨天的销售额和花费
yesterday = pd.to_datetime('2024-05-28') - pd.Timedelta(days=1)
yesterday_data = data[data['date'] == yesterday.strftime('%Y-%m-%d')]

# 准备数据
sales_data = yesterday_data['sales'].tolist()
cost_data = yesterday_data['cost'].tolist()

# 创建图表
bar = (
    Bar()
    .add_xaxis(yesterday_data['campaignName'].tolist())
    .add_yaxis("销售额", sales_data)
    .add_yaxis("花费", cost_data)
    .set_global_opts(title_opts=opts.TitleOpts(title="昨天广告活动销售额与花费"))
)

# 生成图表配置信息
chart_config = bar.dump_options()

# 打印图表配置，以便手动在Echarts中使用
print(chart_config)