import os
import shutil
from ai.backend.util.db.auto_yzj.utils.find import find_files


def move_files_to_destination(cur_time: str, mds: list, country: str):
    # 创建以 cur_time 为名称的文件夹
    destination_dir = os.path.join("./日常优化/输出结果/", f"{country}_{cur_time}")
    os.makedirs(destination_dir, exist_ok=True)

    # 移动所有找到的文件到目标文件夹
    for file_path in mds:
        file_name = os.path.basename(file_path)
        destination_file = os.path.join(destination_dir, file_name)
        shutil.move(file_path, destination_file)
        print(f"Moved '{file_name}' to '{destination_file}'")


def preprocess_csv(cur_time: str, country: str):
    suffix = country + '_' + cur_time + '.csv'
    mds = find_files(directory="./日常优化/", suffix=suffix)
    print(mds)
    if len(mds) > 0:
        move_files_to_destination(cur_time, mds, country)
