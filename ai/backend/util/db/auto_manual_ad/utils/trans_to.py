import json
import os

import pandas as pd


# 读取csv路径，将他保存到json文件中
def csv_to_json(csv_path):
    directory_path = os.path.dirname(csv_path)
    json_path = os.path.join(directory_path, "fields.json")
    # fields.json文件只会被写一次
    if os.path.exists(json_path) and json_equal_to_csv(json_path, csv_path):
        pass
    else:
        # 读取CSV文件
        df = pd.read_csv(csv_path, encoding='utf-8')

        # 创建字段描述列表
        field_desc = []
        for column in df.columns:
            # 假设每个字段的值是字段的描述
            field_desc.append({
                "name": column,
                "comment": column  # 取第一行作为字段的注释
            })

        # 构建JSON结构
        json_data = {
            "field_desc": field_desc
        }

        # 将字典转换为JSON字符串
        json_string = json.dumps(json_data, ensure_ascii=False, indent=4)
        # 保存到fields.json中

        with open(json_path, 'w', encoding='utf-8') as json_file:
            json_file.write(json_string)


# 检查json和csv的一致性
def json_equal_to_csv(json_path, csv_path):
    # 读取csv文件
    df = pd.read_csv(csv_path, encoding='utf-8')
    csv_fields = set(df.columns)
    # 读取JSON文件
    with open(json_path, 'r', encoding='utf-8') as json_file:
        json_data = json.load(json_file)
    json_fields = set(field['name'] for field in json_data['field_desc'])
    return csv_fields == json_fields


# 读取json文件，然后将它转成str
def json_to_str(input_path):
    # 打开并读取JSON文件
    with open(input_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    json_str = json.dumps(data, ensure_ascii=False, indent=4)
    return json_str


def csv_to_str(csv_file_path):
    # 步骤2: 读取 CSV 文件
    csv_data = pd.read_csv(csv_file_path)

    # 这里将 CSV 数据转换为适合传递的格式（例如，转换为字符串或字典）
    csv_data_str = csv_data.to_csv(index=False)

    return csv_data_str


def md_to_str(file_path):
    # 使用with语句打开文件，确保文件最终会被关闭
    with open(file_path, 'r', encoding='utf-8') as file:
        # 读取文件内容并将其转换为字符串
        content = file.read()
        return content


def save_csv(contents, file_path):
    # 打开文件，准备写入
    with open(file_path, 'w', encoding='utf-8') as file:
        # 写入字符串到文本文件
        file.write(contents)

    print(f"文本已保存到 '{file_path}'")
