# filename: sku_filtering.py

import pandas as pd

def main():
    # 读取数据
    csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
    df = pd.read_csv(csv_path)

    # 添加满足的定义列
    df['满足的定义'] = ''

    # 定义筛选逻辑
    conditions = [
        (
            (df['ORDER_1m'] < 8) &
            (df['ACOS_7d'] > 0.24) &
            (df['total_cost_7d'] > 5),
            '定义一'
        ),
        (
            (df['ORDER_1m'] < 8) &
            (df['ACOS_30d'] > 0.24) &
            (df['total_sales_7d'] == 0) &
            (df['total_cost_7d'] > 5),
            '定义二'
        ),
        (
            (df['ORDER_1m'] < 8) &
            (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) &
            (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) &
            (df['total_cost_7d'] > 5),
            '定义三'
        ),
        (
            (df['ORDER_1m'] < 8) &
            (df['ACOS_7d'] > 0.24) &
            (df['ACOS_30d'] > 0.24),
            '定义四'
        ),
        (
            (df['ACOS_7d'] > 0.5),
            '定义五'
        ),
        (
            (df['total_cost_30d'] > 5) &
            (df['total_sales_30d'] == 0),
            '定义六'
        ),
        (
            (df['ORDER_1m'] < 8) &
            (df['total_cost_7d'] >= 5) &
            (df['total_sales_7d'] == 0),
            '定义七'
        ),
        (
            (df['ORDER_1m'] >= 8) &
            (df['total_cost_7d'] >= 10) &
            (df['total_sales_7d'] == 0),
            '定义八'
        )
    ]

    # 应用条件
    for condition, label in conditions:
        df.loc[condition, '满足的定义'] = df.loc[condition, '满足的定义'] + ';' + label

    # 去掉没有匹配到任何定义的记录
    result_df = df[df['满足的定义'] != '']

    # 去掉前面的 ';'
    result_df['满足的定义'] = result_df['满足的定义'].str.lstrip(';')

    # 选择需要的列
    columns = [
        'campaignName',
        'adId',
        'adGroupName',
        'ACOS_30d',
        'ACOS_7d',
        'total_clicks_7d',
        'advertisedSku',
        'ORDER_1m',
        '满足的定义'
    ]

    result_df = result_df[columns]

    # 保存结果到新的CSV文件
    output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_IT_2024-07-12.csv'
    result_df.to_csv(output_path, index=False)

    print(f"Result saved to {output_path}")

if __name__ == '__main__':
    main()