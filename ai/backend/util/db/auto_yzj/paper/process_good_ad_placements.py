# filename: process_good_ad_placements.py
import pandas as pd

# 定义函数用于查找表现较好的广告位
def find_good_ad_placements(df):
    good_ad_placements = []

    for campaign in df['campaignName'].unique():
        campaign_data = df[df['campaignName'] == campaign]

        # 最近7天ACOS最小，且在0到0.24之间，且点击次数不是最大的广告位
        min_acos_7d = campaign_data[(campaign_data['ACOS_7d'] > 0) & (campaign_data['ACOS_7d'] <= 0.24)]
        if not min_acos_7d.empty:
            min_acos_7d = min_acos_7d.loc[min_acos_7d['ACOS_7d'].idxmin()]
            if min_acos_7d['total_clicks_7d'] < campaign_data['total_clicks_7d'].max():
                # 最近3天ACOS最小，且在0到0.24之间，且点击次数不是最大的广告位
                min_acos_3d = campaign_data[(campaign_data['ACOS_3d'] > 0) & (campaign_data['ACOS_3d'] <= 0.24)]
                if not min_acos_3d.empty:
                    min_acos_3d = min_acos_3d.loc[min_acos_3d['ACOS_3d'].idxmin()]
                    if min_acos_3d['total_clicks_3d'] < campaign_data['total_clicks_3d'].max():
                        # 符合条件，记录该广告位
                        good_ad_placements.append({
                            'campaignName': campaign,
                            'placementClassification': min_acos_7d['placementClassification'],
                            'ACOS_7d': min_acos_7d['ACOS_7d'],
                            'ACOS_3d': min_acos_3d['ACOS_3d'],
                            'total_clicks_7d': min_acos_7d['total_clicks_7d'],
                            'total_clicks_3d': min_acos_3d['total_clicks_3d'],
                            'reason': 'ACOS值符合条件且点击次数不是最大的广告位'
                        })

    return good_ad_placements

# 主函数
def main():
    # 读取CSV文件
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
    df = pd.read_csv(file_path)

    # 查找表现较好的广告位
    good_ad_placements = find_good_ad_placements(df)

    # 输出结果到新的CSV文件
    output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_ES_2024-06-12.csv'
    result_df = pd.DataFrame(good_ad_placements)
    result_df.to_csv(output_path, index=False)

    print("Process completed successfully. Check the output file at:", output_path)

# 执行主函数
if __name__ == "__main__":
    main()