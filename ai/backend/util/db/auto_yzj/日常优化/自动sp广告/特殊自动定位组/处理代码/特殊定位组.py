# filename: optimize_keywords.py
import os
import pandas as pd

def main(path, brand, cur_time, country):
    # 读取数据集
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\特殊自动定位组\预处理.csv'
    file_name = "自动_特殊定位组" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # 定义调整竞价的条件并执行提价操作
    conditions = [
        {
            'condition': (data['total_sales_15d'] == 0) & (data['total_clicks_7d'] <= 12),
            'increment': 0.02,
            'reason': '定义一：广告组的最近15天的总销售额为0，并且广告组里的所有自动定位词的最近7天的总点击次数小于等于12'
        }
    ]

    results = []

    for condition in conditions:
        filtered_data = data[condition['condition']]
        for _, row in filtered_data.iterrows():
            New_keywordBid = row['keywordBid'] + condition['increment']
            results.append([
                row['campaignName'],
                row['adGroupName'],
                row['total_sales_15d'],
                row['total_clicks_7d'],
                New_keywordBid,
                row['keyword'],
                row['keywordId'],
                row['keywordBid'],
                condition['increment'],
                condition['reason']
            ])

    # 创建 DataFrame 存储结果
    columns = [
        'campaignName',
        'adGroupName',
        'total_sales_15d',
        'total_clicks_7d',
        'New_keywordBid',
        'keyword',
        'keywordId',
        'keywordBid',
        'increment',
        'reason'
    ]

    results_df = pd.DataFrame(results, columns=columns)

    # 将结果保存到CSV文件
    results_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

    print(f"Results have been saved to {output_file_path}")
