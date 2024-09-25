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


if __name__ == "__main__":
    # 要发送的JSON数据
    # update_data = {
    #     "db":"amazon_ads",
    #     "brand": "LAPASA",
    #     "market": "IT",
    #     "require": "bid_batch",
    #     "position": "automatic_targeting",
    #     "type": "SP",
    #     "ID": ["211711817244392", "263489723525844", "12345"],
    #     "text": ["1", "1.0", "1"],
    #     "user":"wanghequan"
    # }
    #
    # res = ProcessShowData.update(update_data)
    # print(res)
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
    automatically_data = {
        "db": "amazon_bdzx",
        "brand": "DELOMO",
        "market": "IT",
        "strategy": "automatically_add_targets",
        "user": "wanghequan"
    }

    res = ProcessShowData.automatically_add_targets(automatically_data)
    print(res)
