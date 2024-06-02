import time
from datetime import datetime


def current_time():
    # 获取当前时间戳
    current_timestamp = time.time()

    # 格式化日期为年-月-日格式
    formatted_date = datetime.fromtimestamp(current_timestamp).strftime('%Y-%m-%d')

    return formatted_date
