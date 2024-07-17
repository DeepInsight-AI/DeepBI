# filename: 手动_特殊关键词_v1_1_ES_2024-06-20.py
import os
import pandas as pd


def main(path, brand, cur_time, country):
    # 加载数据
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv'
    file_name = "手动_特殊关键词" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # 过滤符合条件的广告组
    filtered_data = data[(data['total_sales_15d'] == 0) & (data['total_clicks_7d'] <= 12)]

    # 对关键词的竞价增加0.02
    filtered_data['new_keywordBid'] = filtered_data['keywordBid'] + 0.02

    # 添加操作竞价原因
    filtered_data['reason'] = '广告组最近15天的总销售额为0，且广告组里的所有关键词的最近7天的总点击次数小于等于12'

    # 选择需要输出的字段
    output_columns = [
        'campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d',
        'keyword', 'matchType', 'keywordBid', 'keywordId', 'new_keywordBid', 'reason'
    ]
    output_data = filtered_data[output_columns]

    # 指定输出文件路径
    output_data.to_csv(output_file_path, index=False)

    print(f"结果已保存到 {output_file_path}")
