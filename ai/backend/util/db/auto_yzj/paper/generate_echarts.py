# filename: generate_echarts.py
from pyecharts.charts import Bar
from pyecharts import options as opts
import pandas as pd

# 读取数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\test\预处理.csv')

# 假设我们需要展示的是销售额，这里我们只取前5个广告活动的数据作为示例
top_campaigns = data['campaignName'].unique()[:5]
sales_data = data[data['campaignName'].isin(top_campaigns)]['sales'].values

# 创建柱状图
bar = Bar()
bar.add_xaxis(top_campaigns.tolist())
bar.add_yaxis("销售额", sales_data.tolist())
bar.set_global_opts(title_opts=opts.TitleOpts(title="广告活动销售额"))

# 生成HTML文件
bar.render("sales_chart.html")

# 确认执行成功
print("柱状图已生成并保存为sales_chart.html文件。")