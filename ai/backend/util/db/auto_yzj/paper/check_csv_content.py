# filename: check_csv_content.py
import pandas as pd

# 设置文件路径
input_filepath = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/特殊商品投放/预处理.csv"

# 读取CSV文件
try:
    df = pd.read_csv(input_filepath)
    # 输出全部内容或者前几行内容
    print("CSV文件内容（前5行）：")
    print(df.head())
except Exception as e:
    print(f"读取CSV文件出错: {e}")