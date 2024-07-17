import os
import shutil
from ai.backend.util.db.auto_yzj.utils.find import find_files


def move_files_to_destination(cur_time: str, mds: list, country: str, brand: str, strategy: str):
    if strategy == 'daily':
        output_dir = "./日常优化/输出结果/"
    elif strategy == 'overstock':
        output_dir = "./滞销品优化/输出结果/"
    else:
        raise ValueError("Invalid strategy provided.")

    # 创建以 cur_time 为名称的文件夹
    destination_dir = os.path.join(output_dir, f"{brand}_{country}_{cur_time}")
    os.makedirs(destination_dir, exist_ok=True)

    # 移动所有找到的文件到目标文件夹
    for file_path in mds:
        file_name = os.path.basename(file_path)
        destination_file = os.path.join(destination_dir, file_name)
        shutil.move(file_path, destination_file)
        print(f"Moved '{file_name}' to '{destination_file}'")


def preprocess_csv(cur_time: str, country: str, brand: str, strategy: str):
    if strategy == 'daily':
        directory = "./日常优化/"
    elif strategy == 'overstock':
        directory = "./滞销品优化/"
    else:
        raise ValueError("Invalid strategy provided.")

    suffix = brand + '_' + country + '_' + cur_time + '.csv'
    mds = find_files(directory=directory, suffix=suffix)
    print(mds)
    if len(mds) > 0:
        move_files_to_destination(cur_time, mds, country, brand, strategy)

