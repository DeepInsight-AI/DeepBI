import random
import time
from functools import wraps
from ai.backend.util.db.util.common import get_ad_my_credentials,get_proxies
from collections.abc import Iterable


def log_method_call(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        self.log(f"Calling method: {method.__name__}")
        result = method(self, *args, **kwargs)
        self.log(f"Method {method.__name__} completed.")
        return result
    return wrapper


class BaseApi:
    def __init__(self, db, brand, market):
        self.brand = brand
        self.market = market
        self.db = db
        self.credentials, self.access_token = get_ad_my_credentials(self.db, self.market, self.brand)
        self.attempts_time = 5

    def load_credentials(self):
        # 假设这个方法是通用的，可以直接在这里实现
        my_credentials, access_token = get_ad_my_credentials(self.db, self.market, self.brand)
        return my_credentials, access_token

    def log(self, message):
        # 一个简单的日志记录方法
        print(message)

    def wait_time(self):
        wait_time = random.randint(5, 10)
        print(f"Waiting for {wait_time} seconds before retrying...")
        time.sleep(wait_time)

    def to_iterable(self,obj):
        if isinstance(obj, Iterable) and not isinstance(obj, (str, bytes)):
            return obj  # 如果是可迭代的（非字符串或字节），返回原对象
        else:
            return [obj]

