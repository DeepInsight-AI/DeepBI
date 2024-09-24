from ai.backend.util.db.util.common import get_ad_my_credentials,get_proxies


class BaseApi:
    def __init__(self, db, brand, market):
        self.brand = brand
        self.market = market
        self.db = db
        self.credentials, self.access_token = get_ad_my_credentials(self.db, self.market, self.brand)

    def load_credentials(self):
        # 假设这个方法是通用的，可以直接在这里实现
        my_credentials, access_token = get_ad_my_credentials(self.db, self.market, self.brand)
        return my_credentials, access_token

    def log(self, message):
        # 一个简单的日志记录方法
        print(message)
