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
            'ONLINE_URL': "https://192.168.5.165:8000/api/data/"
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

    # @classmethod
    # def delete(cls, data):
    #     if "ID" not in data:
    #         return False
    #     if "UID" not in data:
    #         return False
    #     return cls.post_data(data, "delete")

if __name__ == "__main__":
    # 要发送的JSON数据
    update_data = {
        "brand": "LAPASA",
        "market": "DE",
        "require": "bid",
        "position": "placement",
        "type": "SP",
        "ID": "438171503570439",
        "text": "10",
        "placement": "PLACEMENT_REST_OF_SEARCH"
    }

    res = ProcessShowData.update(update_data)
    print(res)

