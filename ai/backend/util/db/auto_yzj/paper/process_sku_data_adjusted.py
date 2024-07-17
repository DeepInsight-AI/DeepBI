# filename: process_sku_data_adjusted.py
import pandas as pd

def process_sku_data():
    try:
        # 文件路径
        input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\关闭SKU\预处理.csv'
        output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\关闭SKU\提问策略\自动_关闭SKU_v1_1_LAPASA_FR_2024-07-03.csv'

        print("读取CSV文件...")
        # 读取CSV文件到DataFrame
        df = pd.read_csv(input_file_path)

        # 打印数据集信息
        print("数据集加载成功。前5行数据如下：")
        print(df.head())

        # 添加新列用于标识满足的定义
        df['Definition'] = ''

        print("应用定义条件...")
        # 调整条件值进行测试
        new_acos_7d_threshold = 0.05
        
        # 定义一：
        condition1 = (
            (df['ORDER_1m'] < 5) & 
            (df['ACOS_7d'] > new_acos_7d_threshold) & 
            (df['total_clicks_7d'] > 13)
        )

        # 定义二：
        condition2 = (
            (df['ORDER_1m'] < 5) & 
            (df['ACOS_30d'] > new_acos_7d_threshold) & 
            (df['total_sales14d_7d'] == 0) & 
            (df['total_clicks_7d'] > 13)
        )

        # 定义三：
        condition3 = (
            (df['ORDER_1m'] < 5) & 
            (df['ACOS_7d'] > new_acos_7d_threshold) & 
            (df['ACOS_30d'] > new_acos_7d_threshold)
        )

        # 定义四：
        condition4 = (
            (df['total_clicks_30d'] > 50) & 
            (df['total_sales14d_30d'] == 0)
        )

        # 定义五：
        condition5 = (
            (df['ORDER_1m'] < 5) & 
            (df['total_clicks_7d'] >= 19) & 
            (df['total_sales14d_7d'] == 0)
        )

        # 定义六：
        condition6 = (
            (df['total_clicks_7d'] >= 30) & 
            (df['total_sales14d_7d'] == 0)
        )

        print("更新满足条件的定义字段...")
        # 更新满足条件的定义字段
        df.loc[condition1, 'Definition'] = '定义一'
        df.loc[condition2, 'Definition'] = '定义二'
        df.loc[condition3, 'Definition'] = '定义三'
        df.loc[condition4, 'Definition'] = '定义四'
        df.loc[condition5, 'Definition'] = '定义五'
        df.loc[condition6, 'Definition'] = '定义六'

        # 打印满足条件的数据示例
        print("满足条件的数据示例如下：")
        print(df[df['Definition'] != ''].head())

        # 过滤出符合条件的SKU
        result_df = df[df['Definition'] != '']

        # 保存到新的CSV文件
        result_df = result_df[['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', 'Definition']]
        result_df.to_csv(output_file_path, index=False)

        print(f"结果已保存到 {output_file_path}")

    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == "__main__":
    process_sku_data()