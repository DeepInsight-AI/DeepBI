import hashlib
import time
import requests



class ProcessShowData():
    # 环境配置
    config = {
        'test': {
            'INSERT_SECRATE': "10470c3b4b1fed12c3baac014be15fac67c6e815",
            'ONLINE_URL': "http://192.168.1.185:5009/api/data/"
        },
        'pre': {
            'INSERT_SECRATE': "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618",
            'ONLINE_URL': "http://192.168.5.165:5009/api/data/"
        }
    }

    # 默认使用生产环境
    environment = 'test'

    @classmethod
    def set_debug_mode(cls, debug):
        """根据debug状态设置环境"""
        cls.environment = 'test'

    @classmethod
    def sha1(cls, input_string):
        """对输入的字符串进行 SHA-256 哈希加密"""
        hash_object = hashlib.sha256()
        hash_object.update(input_string.encode('utf-8'))
        hashed_string = hash_object.hexdigest()
        return hashed_string

    @classmethod
    def post_data(cls, data, op_type):
        if not data or op_type is None or "" == op_type:
            return "error"
        print(cls.environment)
        timestamp = int(time.time())
        secrete = cls.config[cls.environment]['INSERT_SECRATE']
        token = cls.sha1(secrete + str(timestamp) + secrete)
        #
        headers = {
            'Content-Type': 'application/json',
            'token': str(token),
            'timestamp': str(timestamp)
        }
        url = cls.config[cls.environment]['ONLINE_URL'] + op_type
        # 发送POST请求
        response = requests.post(url,
                                 headers=headers, json=data)

        # 输出响应内容
        print(response.status_code)
        data = response.json()
        if response.status_code == 200:
            return True, data
        else:
            print("操作失败")
            print(response.text)
            return False

    @classmethod
    def create(cls, data):
        return cls.post_data(data, "create")

    @classmethod
    def update(cls, data):
        return cls.post_data(data, "update")

    @classmethod
    def automatically_add_targets(cls, data):
        return cls.post_data(data, "automatically")

    # @classmethod
    # def delete(cls, data):
    #     if "ID" not in data:
    #         return False
    #     if "UID" not in data:
    #         return False
    #     return cls.post_data(data, "delete")
    @classmethod
    def get_data(cls, file_name):
        """获取数据"""
        timestamp = int(time.time())
        secrete = cls.config[cls.environment]['INSERT_SECRATE']
        token = cls.sha1(secrete + str(timestamp) + secrete)
        headers = {
            'token': str(token),
            'timestamp': str(timestamp)
        }
        url = cls.config[cls.environment]['ONLINE_URL'] + "get_data"

        # 发送GET请求
        response = requests.get(url, headers=headers, json=file_name)

        # 输出响应内容
        print(response.status_code)
        if response.status_code == 200:
            return True, response.json()
        else:
            print("操作失败")
            print(response.text)
            return False

    @classmethod
    def get_report(cls, file_name):
        """获取报告"""
        timestamp = int(time.time())
        secrete = cls.config[cls.environment]['INSERT_SECRATE']
        token = cls.sha1(secrete + str(timestamp) + secrete)
        headers = {
            'token': str(token),
            'timestamp': str(timestamp)
        }
        url = cls.config[cls.environment]['ONLINE_URL'] + "get_report"

        # 发送GET请求
        response = requests.get(url, headers=headers, json=file_name)

        # 输出响应内容
        print(response.status_code)
        if response.status_code == 200:
            return True, response.json()
        else:
            print("操作失败")
            print(response.text)
            return False


if __name__ == "__main__":
    # 要发送的JSON数据
    # update_data = {
    #     "db":"amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "JP",
    #     "require": "bid",
    #     "position": "product_target",
    #     "type": "SP",
    #     "ID": "100003525913061",
    #     "text": "26",
    #     "user":"wanghequan",
    #     # "campaignId":"90124165629540",
    #     # "adGroupId":"118677141166299"
    # }
    #
    # res = ProcessShowData.update(update_data)
    # ## 批量添加关键词一次最多1000条
    # update_data = {
    #     "db": "amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "US",
    #     "require": "create_batch",
    #     "position": "keyword",
    #     "type": "SP",
    #     "ID": ["test word1", "test word2"],  #
    #     "text": ["0.5","0.4"],
    #     "campaignId": ["90124165629540", "90124165753222"],
    #     "adGroupId": ["118677141166299", "90124165753222"],
    #     "matchType": ["EXACT", "PHRASE"],  # NEGATIVE_PHRASE 如果添加的是asin则需要传"asin"
    #     "user": "wanghequan"
    # }
    # res = ProcessShowData.update(update_data)
    ## 批量添加投放一次最多1000条
    update_data = {
        "db": "amazon_ads",
        "brand": "LAPASA",
        "market": "US",
        "require": "create_batch",
        "position": "product_target",
        "type": "SP",
        "ID": ["B071JRS5GF", "B071JRS5GF"],  # 可以是词或者ASIN
        "text": ["0.5","0.4"],
        "campaignId": ["90124165629540", "90124165753222"],
        "adGroupId": ["118677141166299", "90124165753222"],
        "matchType": ["ASIN_SAME_AS", "ASIN_EXPANDED_FROM"],  # NEGATIVE_PHRASE 如果添加的是asin则需要传"asin"
        "user": "wanghequan"
    }
    res = ProcessShowData.update(update_data)
    # # 添加否定关键词和商品（自动判定）
    # update_data = {
    #     "db": "amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "UK",
    #     "require": "create",
    #     "position": "negative_target",
    #     "type": "SP",
    #     "ID": "tartan pyjamas 20",#可以是词或者ASIN
    #     "text": "",
    #     "campaignId": "459447767497303",
    #     "adGroupId": "464481432509078",
    #     "matchType": "NEGATIVE_EXACT",#NEGATIVE_PHRASE 如果添加的是asin则不需要传
    #     "user": "wanghequan"
    # }
    # res = ProcessShowData.update(update_data)
    # # 批量添加否定关键词和商品（自动判定）一次最多1000条
    # update_data = {
    #     "db": "amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "US",
    #     "require": "create_batch",
    #     "position": "negative_target",
    #     "type": "SP",
    #     "ID": ["test word1", "test word2"],  # 可以是词或者ASIN
    #     "text": "",
    #     "campaignId": ["90124165629540", "90124165753222"],
    #     "adGroupId": ["118677141166299", "90124165753222"],
    #     "matchType": ["NEGATIVE_EXACT", "asin"],  # NEGATIVE_PHRASE 如果添加的是asin则需要传"asin"
    #     "user": "wanghequan"
    # }
    # res = ProcessShowData.update(update_data)
    # ## 修改投放状态
    # update_data = {
    #     "db":"amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "US",
    #     "require": "state",
    #     "position": "product_target",
    #     "type": "SP",
    #     "ID": "targetId",#可以是词或者ASIN
    #     "text": "PAUSED",
    #     "user": "wanghequan"
    # }
    # res = ProcessShowData.update(update_data)
    ## 修改否定关键词状态
    # update_data = {
    #     "db": "amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "UK",
    #     "require": "state",
    #     "position": "negative_keyword",
    #     "type": "SP",
    #     "ID": "155010353421654",
    #     "text": "PAUSED",#ENABLED  PAUSED
    #     "user": "wanghequan"
    # }
    # res = ProcessShowData.update(update_data)
    # ## 修改否定投放状态
    # update_data = {
    #     "db": "amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "US",
    #     "require": "state",
    #     "position": "negative_target",
    #     "type": "SP",
    #     "ID": "targetId",
    #     "text": "PAUSED",#ENABLED
    #     "user": "wanghequan"
    # }
    # res = ProcessShowData.update(update_data)
    # print(res)
    # # 批量修改关键词状态 一次最多1000条
    # update_data = {
    #     "db": "amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "US",
    #     "require": "state_batch",
    #     "position": "keyword",
    #     "type": "SP",
    #     "ID": ["keywordId1", "keywordId2"],
    #     "text": ["PAUSED", "PAUSED"],  # ENABLED
    #     "user": "wanghequan"
    # }
    # res = ProcessShowData.update(update_data)
    # ## 批量修改投放状态 一次最多1000条
    # update_data = {
    #     "db": "amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "US",
    #     "require": "state_batch",
    #     "position": "product_target",
    #     "type": "SP",
    #     "ID": ["targetId1", "targetId2"],
    #     "text": ["PAUSED", "PAUSED"],  # ENABLED
    #     "user": "wanghequan"
    # }
    # res = ProcessShowData.update(update_data)
    # ## 删除否定关键词 可以单条也可以批量 一次最多1000条
    # update_data = {
    #     "db": "amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "US",
    #     "require": "delete",
    #     "position": "negative_keyword",
    #     "type": "SP",
    #     "ID": "keywordId",  # 批量为["keywordId1", "keywordId2"]
    #     "text": "",  # 不需要传
    #     "user": "wanghequan"
    # }
    # res = ProcessShowData.update(update_data)
    # ## 删除否定ASIN 可以单条也可以批量 一次最多1000条
    # update_data = {
    #     "db": "amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "US",
    #     "require": "delete",
    #     "position": "negative_target",
    #     "type": "SP",
    #     "ID": "targetId",  # 批量为["targetId1", "targetId2"]
    #     "text": "",  # 不需要传
    #     "user": "wanghequan"
    # }
    # res = ProcessShowData.update(update_data)
    # add_data = {
    #     "brand": "LAPASA",
    #     "market": "DE",
    #     "type": "SP",
    #     "strategy": "manual",
    #     "replication": "False",
    #     "text": {"parent_asin1": [{"keyword":"keyword1","matchType":"matchType1","bid":"bid1"},{},{}],"parent_asin2":[{},{},{}]},
    #     "budget": "10"
    # }
    # res = ProcessShowData.create(add_data)
    # print(res)
    # add_data = {
    #     "file": "execution_times"
    # }
    # res = ProcessShowData.get_data(add_data)
    # print(res)
    # automatically_data = {
    #     "db": "amazon_bdzx",
    #     "brand": "DELOMO",
    #     "market": "IT",
    #     "strategy": "automatically_add_targets",
    #     "user": "wanghequan"
    # }
    #
    # res = ProcessShowData.automatically_add_targets(automatically_data)
    # print(res)
    # 获取报告
    get_data = {
        "UID": "1",
        "market": "US"
    }
    res = ProcessShowData.get_report(get_data)
    print(res)
