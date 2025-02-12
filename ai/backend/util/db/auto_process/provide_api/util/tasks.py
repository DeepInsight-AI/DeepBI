import random
import requests
import time
import pymysql
from datetime import datetime
import signal
from multiprocessing import Process
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


def insert_keyword(cursor, data):
    """防重复插入核心逻辑"""
    check_sql = """
    SELECT id FROM amazon_related_keywords
    WHERE source = %s
    AND search_term = %s
    AND expanding_word = %s
    AND date = %s
    """
    cursor.execute(check_sql, (
        data['source'],
        data['search_term'],
        data['expanding_word'],
        data['date']
    ))

    # 存在重复记录则跳过
    if cursor.fetchone():
        print(f"重复数据已跳过：{data}")
        return False

    # 执行插入新数据
    insert_sql = """
    INSERT INTO amazon_related_keywords
    (source, search_term, expanding_word, date, create_time)
    VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(insert_sql, (
        data['source'],
        data['search_term'],
        data['expanding_word'],
        data['date'],
        datetime.now()  # 自动记录创建时间
    ))
    return True


def save_to_database(data, db_config=None):
    """封装数据库保存操作"""
    if not db_config:
        db_config = {
            'host': '192.168.2.117',
            'user': 'amazon_seo',
            'password': 'test123!@#',
            "port": 3306,
            'db': 'amazon_seo',
            'charset': 'utf8mb4'
        }

    connection = None
    try:
        connection = pymysql.connect(**db_config)
        with connection.cursor() as cursor:
            if insert_keyword(cursor, data):
                connection.commit()
                print("数据插入成功")
                return True
            connection.rollback()
            return False
    except Exception as e:
        print(f"数据库操作失败: {str(e)}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            connection.close()


def get_baidu_suggestions(keyword):
    """获取正确编码的百度搜索建议"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.baidu.com/',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }
    params = {
        'prod': 'pc',
        'wd': keyword.encode('gbk')  # 百度使用gbk编码
    }
    retries = 3

    for attempt in range(retries):
        try:
            response = requests.get(
                url='https://www.baidu.com/sugrec',
                params=params,
                headers=headers,
                timeout=5
            )
            response.encoding = 'UTF-8'  # 确保响应正确解码

            if response.status_code == 200:
                data = response.json()
                # 提取建议词列表
                return [item['q'] for item in data.get('g', []) if 'q' in item]
            else:
                # 非200状态码触发重试
                raise Exception(f"HTTP状态码错误: {response.status_code}")
        except Exception as e:
            print(f"第{attempt + 1}次请求失败: {str(e)}")
            if attempt < retries - 1:
                time.sleep(random.uniform(3, 5))  # 等待后重试
            else:
                return []  # 重试耗尽返回空
    return []


def get_deep_suggestions(keyword, target_count=50):
    """持续获取建议直到达到目标数量（允许重复）"""
    all_suggestions = []  # 使用列表保留重复项
    queue = [keyword]  # 待处理关键词队列
    processed = set()  # 已处理关键词记录
    today = datetime.today()
    cur_time = today.strftime('%Y-%m-%d')

    while len(all_suggestions) < target_count and queue:
        current_keyword = queue.pop(0)

        # 获取当前关键词的建议
        suggestions = get_baidu_suggestions(current_keyword)

        # 直接扩展结果列表
        all_suggestions.extend(suggestions)

        # 将新建议加入处理队列（如果尚未处理）
        for suggestion in suggestions:
            data = {
                        'source': 'baidu',
                        'search_term': current_keyword,
                        'expanding_word': suggestion,
                        'date': cur_time
                    }
            save_to_database(data)
            if suggestion not in processed:
                processed.add(suggestion)
                queue.append(suggestion)

        # 避免频繁请求，添加随机延迟
        time.sleep(random.uniform(9.5, 10.5))

        # 提前终止条件
        if len(all_suggestions) >= target_count:
            break

    return all_suggestions[:target_count]


# ... existing code ...
current_process = None
is_running = False


def task_wrapper(keyword, target_count):
    """带执行控制的包装任务"""
    global current_process, is_running

    def run_task():
        global is_running
        try:
            is_running = True
            print(f"开始执行任务，目标数量: {target_count}")
            deep_suggestions = get_deep_suggestions(keyword, target_count=target_count)
            print(f"成功获取 {len(deep_suggestions)} 条建议")
            # 这里添加数据库存储逻辑
        finally:
            is_running = False

    # 终止正在运行的进程
    if current_process and current_process.is_alive():
        print("检测到未完成的任务，终止中...")
        current_process.terminate()
        current_process.join(timeout=5)

    # 启动新进程
    current_process = Process(target=run_task)
    current_process.start()


def setup_scheduler(keyword, target_count):
    """配置定时任务"""
    scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
    trigger = CronTrigger(hour=0, minute=1, timezone='Asia/Shanghai')

    scheduler.add_job(
        task_wrapper,
        trigger=trigger,
        args=(keyword, target_count),
        misfire_grace_time=300
    )

    # 添加优雅退出
    def shutdown(signum, frame):
        print("接收到终止信号，关闭调度器...")
        scheduler.shutdown()
        if current_process and current_process.is_alive():
            current_process.terminate()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    scheduler.start()
    print("定时任务已启动，每天 00:01 执行")
    return scheduler
if __name__ == '__main__':
    # 配置参数
    config = {
        'keyword': "amazon广告",
        'target_count': 50000
    }

    # 初始化调度器
    scheduler = setup_scheduler(config['keyword'], config['target_count'])

    # 保持主线程运行
    try:
        while True:
            time.sleep(3600)  # 每小时检查一次
    except KeyboardInterrupt:
        scheduler.shutdown()
