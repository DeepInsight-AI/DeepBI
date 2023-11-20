import logging
import time

# 创建一个日志记录器
logger = logging.getLogger('redash_logger')

# 配置日志记录级别，这里设置为INFO，可以根据需要选择不同级别
logger.setLevel(logging.INFO)

# 创建一个文件处理程序，将日志写入文件
file_handler = logging.FileHandler('holmes.log')

# 创建一个格式化器，定义日志记录的格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 将文件处理程序添加到日志记录器
logger.addHandler(file_handler)

# 示例日志记录
# logger.debug('这是一条调试信息')
# logger.info('这是一条信息')
# logger.warning('这是一条警告')
# logger.error('这是一条错误')
# logger.critical('这是一条严重错误')

# message = "12556"
# print(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + ' ---- ' + "send a message:{}".format(
#                 message))
#
#
