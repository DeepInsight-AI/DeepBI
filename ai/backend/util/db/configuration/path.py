import os


def get_config_path():
    script_dir = os.path.dirname(__file__)  # 获取当前脚本所在目录
    return script_dir
