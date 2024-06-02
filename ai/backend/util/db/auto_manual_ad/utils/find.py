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
