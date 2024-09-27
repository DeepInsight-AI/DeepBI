import hashlib
import time
import requests
import os


# 开发环境
# INSERT_SECRATE = "123123123"
# ONLINE_URL = "http://127.0.0.1:5000/api/data/"
# 测试环境
# INSERT_SECRATE = "10470c3b4b1fed12c3baac014be15fac67c6e815"
# ONLINE_URL = "http://192.168.2.152:8000/api/data/"


# 线上环境
# INSERT_SECRATE = "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618"
# ONLINE_URL = "https://atlas.deepbi.com/api/data/"


class ProcessShowData():
    # 环境配置
    config = {
        'dev': {
            'INSERT_SECRATE': "123123123",
            'ONLINE_URL': "http://127.0.0.1:5000/api/data/"
        },
        'test': {
            'INSERT_SECRATE': "10470c3b4b1fed12c3baac014be15fac67c6e815",
            'ONLINE_URL': "http://192.168.2.152:8000/api/data/"
        },
        'pre': {
            'INSERT_SECRATE': "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618",
            'ONLINE_URL': "https://atlas.deepbi.cn/api/data/"
        }
    }

    # 默认使用生产环境
    environment = 'pre'

    @classmethod
    def set_debug_mode(cls, debug):
        """根据debug状态设置环境"""
        cls.environment = 'test' if debug else 'pre'

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
        if int(data['code']) == 200:
            return True, data
        else:
            print("操作失败")
            print(response.text)
            return False, data

    @classmethod
    def insert(cls, data):
        return cls.post_data(data, "insert")

    @classmethod
    def update(cls, data):
        if "ID" not in data:
            return False
        if "UID" not in data:
            return False
        return cls.post_data(data, "update")

    @classmethod
    def delete(cls, data):
        if "ID" not in data:
            return False
        if "UID" not in data:
            return False
        return cls.post_data(data, "delete")

    @classmethod
    def get_accesstoken(cls, data):
        if "UID" not in data:
            return False
        if "AreaCode" not in data:
            return False
        if "OuthType" not in data:
            return False
        OuthType = data['OuthType']
        return cls.post_data(data, str(OuthType.lower())+"_token")
        pass


# if __name__ == "__main__":
    #     # 要发送的JSON数据
    #     add_data = {
    #         "UID": "1",
    #         "ContinentCode": "NA",
    #         "CountryCode": "IT",
    #         "DataType": "AD7DAYS",
    #         "StartDate": "2025-01-02",
    #         "EndDate": "2025-01-02",
    #         "ShowData": "{thisi is other data}",
    #         "Other": "其他"
    #     }
    #     # 更新筛选条件为 ID 和UID
    #     update_data = {
    #         "ID": "5",
    #         "UID": "1",
    #         "ContinentCode": "BA",
    #         "CountryCode": "BT",
    #         "DataType": "BD7DAYS",
    #         "StartDate": "1025-01-02",
    #         "EndDate": "1025-01-02",
    #         "ShowData": "{Bthisi is other data}",
    #         "Other": "B其他"
    #     }
    # delete_data = {
    #     "ID": "532",
    #     "UID": "1"
    # }
    # # #     # ProcessShowData.insert(add_data)
    # # #     ProcessShowData.update(update_data)
    # ProcessShowData.delete(delete_data)
    # return
