# filename: deepbi_analysis.py
import pandas as pd
import os

def main(path, brand, cur_time, country):
    # 数据路径
    data_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\商品投放搜索词优化\预处理.csv"
    file_name = "手动_优质_ASIN_搜索词" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)

    # 读取数据
    df = pd.read_csv(data_path)

    # 定义筛选条件
    condition1 = (df['total_sales14d_7d'] > 0)
    condition2 = (df['ORDER_1m'] > 0) & (df['ACOS_30d'] < 0.35)

    # 过滤数据
    filtered_df1 = df[condition1].copy()
    filtered_df1['reason'] = '定义一'

    filtered_df2 = df[condition2 & ~condition1].copy()  # 确保没有重复选中
    filtered_df2['reason'] = '定义二'

    # 合并结果
    result_df = pd.concat([filtered_df1, filtered_df2], ignore_index=True)

    # 提取所需列
    columns = [
        'campaignName',
        'campaignId',
        'adGroupName',
        'adGroupId',
        'ACOS_7d',
        'total_sales14d_7d',
        'ORDER_1m',
        'ACOS_30d',
        'searchTerm',
        'reason'
    ]
    result_df = result_df[columns]

    # 保存结果
    result_df.to_csv(output_file_path, index=False)

    print(f"结果已保存至: {output_file_path}")
