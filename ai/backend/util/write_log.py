import logging

# 创建一个日志记录器
logger = logging.getLogger('DeepBI_logger')

# 配置日志记录级别，这里设置为INFO，可以根据需要选择不同级别
logger.setLevel(logging.INFO)

# 创建一个文件处理程序，将日志写入文件
file_handler = logging.FileHandler('DeepBI.log')

# 创建一个格式化器，定义日志记录的格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 将文件处理程序添加到日志记录器
logger.addHandler(file_handler)

