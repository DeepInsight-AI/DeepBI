import os
from ai.backend.util.db.auto_process.automatic_status_quo_analysis.export.export_path import get_export_path
import pandas as pd
import json

def csv_to_json(csv_path):
    """将 CSV 文件转换为 JSON 文件"""
    # 读取 CSV 文件
    df = pd.read_csv(csv_path, encoding='utf-8-sig')

    # 将 DataFrame 转换为 JSON 格式
    # orient='records' 会将每行数据作为一个 JSON 对象，并以列表形式返回
    json_data = df.to_json(orient='records', lines=False, force_ascii=False)
    return json_data
