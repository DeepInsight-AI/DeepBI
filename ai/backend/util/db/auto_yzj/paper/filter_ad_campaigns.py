# filename: filter_ad_campaigns.py
import pandas as pd

def main():
    # 读取CSV文件路径
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'
    
    # 读取CSV文件
    data = pd.read_csv(file_path)
    
    # 过滤符合条件的数据：近七天的总销售额大于0且acos值小于0.2
    filtered_data = data[(data['total_sales14d_7d'] > 0) & (data['ACOS_7d'] < 0.2)]
    
    # 添加原因列
    filtered_data['reason'] = '近七天有销售额且acos值低于0.2'
    
    # 选择并重命名需要输出的列
    result = filtered_data[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'matchType', 'reason']]
    result.columns = ['Campaign Name', 'adGroupName', 'week_acos', 'searchTerm', 'matchtype', 'reason']
    
    # 输出文件路径
    output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_优质搜索词_IT_2024-06-06.csv'
    
    # 保存结果数据到新的CSV文件
    result.to_csv(output_file_path, index=False, encoding='utf-8-sig')
    
    print("任务已完成，过滤后的数据已保存到CSV文件中。")

if __name__ == "__main__":
    main()