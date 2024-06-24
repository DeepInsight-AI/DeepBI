import os


def find_files(directory, suffix):
    # 存储所有.suffix文件的列表
    sql_files = []

    # 遍历目录
    for root, dirs, files in os.walk(directory):
        for file in files:
            # 检查文件后缀是否为.sql
            if file.endswith(suffix):
                # 将完整路径添加到列表中
                sql_files.append(os.path.join(root, file))

    return sql_files

def find_file_by_name(directory, filename):
    """
    根据指定的文件名查询文件路径

    参数:
    directory (str): 要搜索的目录路径
    filename (str): 要查找的文件名

    返回:
    str: 文件的完整路径，如果文件不存在则返回 None
    """
    csv_files = []
    # 遍历目录
    for root, dirs, files in os.walk(directory):
        # 检查文件名是否在当前目录中
        if filename in files:
            # 构建文件的完整路径并返回
            csv_files.append(os.path.join(root, filename))
    # 文件未找到，返回 None
    return csv_files

# 示例用法
# file_path = find_file_by_name('./', 'example.txt')
# if file_path:
#     print("文件路径:", file_path)
# else:
#     print("文件未找到")
