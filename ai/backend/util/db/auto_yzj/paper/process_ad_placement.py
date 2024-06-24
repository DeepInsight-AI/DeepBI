# filename: process_ad_placement.py

import pandas as pd
import os

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'

def safe_read_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        print("CSV文件读取成功")
        return df
    except Exception as e:
        print(f"无法读取CSV文件: {e}")
        exit(1)

def preprocess_data(df):
    try:
        df['total_clicks_7d'] = pd.to_numeric(df['total_clicks_7d'], errors='coerce')
        df['total_sales14d_7d'] = pd.to_numeric(df['total_sales14d_7d'], errors='coerce')
        df['ACOS_7d'] = pd.to_numeric(df['ACOS_7d'], errors='coerce')
        df['total_clicks_3d'] = pd.to_numeric(df['total_clicks_3d'], errors='coerce')
        df['ACOS_3d'] = pd.to_numeric(df['ACOS_3d'], errors='coerce')
        print("数据预处理成功")
    except Exception as e:
        print(f"数据预处理时出错: {e}")
        exit(1)

def analyze_definitions(df):
    try:
        condition1 = (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 0)
        result1 = df[condition1]
        result1['action'] = '定义一: 竞价变为0'
        print("定义一分析成功")
    except Exception as e:
        print(f"定义一分析时出错: {e}")
        exit(1)

    try:
        condition3 = (df['ACOS_7d'] >= 50)
        result3 = df[condition3]
        result3['action'] = '定义三: 竞价变为0'
        print("定义三分析成功")
    except Exception as e:
        print(f"定义三分析时出错: {e}")
        exit(1)

    try:
        result2 = pd.DataFrame(columns=df.columns)
        campaigns = df['campaignName'].unique()
        for campaign in campaigns:
            campaign_df = df[df['campaignName'] == campaign]
            valid_placements = campaign_df[(campaign_df['ACOS_7d'] > 24) & (campaign_df['ACOS_7d'] < 50)]
            if len(valid_placements) >= 3:
                max_acos = valid_placements['ACOS_7d'].max()
                min_acos = valid_placements['ACOS_7d'].min()
                if abs(max_acos - min_acos) >= 0.2:
                    max_acos_row = valid_placements[valid_placements['ACOS_7d'] == max_acos]
                    max_acos_row['action'] = '定义二: 降低竞价3%'
                    result2 = pd.concat([result2, max_acos_row])
        print("定义二分析成功")
    except Exception as e:
        print(f"定义二分析时出错: {e}")
        exit(1)

    try:
        final_result = pd.concat([result1, result2, result3])
        print("结果合并成功")
        return final_result
    except Exception as e:
        print(f"结果合并时出错: {e}")
        exit(1)

def select_columns(final_result):
    try:
        output_columns = [
            'campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 
            'total_clicks_7d', 'total_clicks_3d', 'action'
        ]
        final_result = final_result[output_columns]
        print("字段选择成功")
        return final_result
    except Exception as e:
        print(f"字段选择时出错: {e}")
        exit(1)

def save_to_csv(final_result, output_file):
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            print(f"目录 {output_dir} 已创建")
        except Exception as e:
            print(f"无法创建目录 {output_dir}: {e}")
            exit(1)
    try:
        final_result.to_csv(output_file, index=False)
        print(f"结果文件已保存到: {output_file}")
    except Exception as e:
        print(f"无法保存结果到CSV文件: {e}")
        exit(1)

def main():
    df = safe_read_csv(file_path)
    preprocess_data(df)
    final_result = analyze_definitions(df)
    final_result = select_columns(final_result)
    print("最终数据内容:")
    print(final_result.head())
    output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_ES_2024-06-121.csv'
    save_to_csv(final_result, output_file)

if __name__ == "__main__":
    main()