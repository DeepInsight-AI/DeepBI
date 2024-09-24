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
        'test_pre': {
            'INSERT_SECRATE': '',
            "ONLINE_URL": "https://pre_atlas.deepbi.com/api/data/"
        },
        'pre': {
            'INSERT_SECRATE': "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618",
            'ONLINE_URL': "https://atlas.deepbi.com/api/data/"
        }
    }

    # 默认使用生产环境
    environment = 'pre'

    @classmethod
    def set_debug_mode(cls, debug):
        """根据debug状态设置环境"""
        cls.environment = 'pre'

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
    def post_file(cls, file_path, data=None, file_key='upfile',  op_type='upload_report'):
        import os
        timestamp = int(time.time())
        secrete = cls.config[cls.environment]['INSERT_SECRATE']
        token = cls.sha1(secrete + str(timestamp) + secrete)
        if os.path.exists(file_path) is False:
            print("文件不存在")
            return False, "文件不存在"

        headers = {
            'token': str(token),
            'timestamp': str(timestamp)
        }

        # 构造文件部分
        files = {file_key: open(file_path, 'rb')}
        url = cls.config[cls.environment]['ONLINE_URL'] + op_type
        # 发送带有文件的 POST 请求
        response = requests.post(url, headers=headers, files=files, data=data)
        # 处理响应
        response_data = response.json() if response.content else {}
        if response.status_code == 200:
            return True, response_data
        else:
            print("文件上传失败")
            print(response.text)
            return False, response_data

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
        return cls.post_data(data, str(OuthType.lower()) + "_token")
        pass

    @classmethod
    def get_user_outh(cls, data):
        if "UID" not in data:
            return False
        return cls.post_data(data, "get_user_outh")

    @classmethod
    def update_report_status(cls, data):
        """更新报告状态"""
        if "UID" not in data:
            return False
        if "CountryCode" not in data:
            return False
        if "state" not in data:
            return False
        return cls.post_data(data, "update_report_status")

    @classmethod
    def upload_report(cls, data, file_path):
        """上传报告"""
        if "UID" not in data:
            return False
        if "CountryCode" not in data:
            return False
        import os
        if os.path.exists(file_path) is False:
            print("文件不存在")
            return False
        return cls.post_file(cls, file_path, data=data)


if __name__ == "__main__":
    # 获取授权状态，包括报告状态
    data = {
       "UID": 1
    }
    result, msg = ProcessShowData.get_user_outh(data)
    print(result, msg)
    # #  上传 报告结果, 上传返回数据中的 data 为该报告的线上id ，可以用于独立发送 邮件
    # file = "./resource/uploads/1.pdf"
    # data = {
    #     "UID": 1,
    #     "CountryCode": "MX",
    #     "send_email": 1  # 是否发送邮件 1:是  0:否，默认否
    # }
    # result, msg = ProcessShowData.post_file(file, data)
    # print(result, msg)
    # # 更新报告状态
    # data = {
    #     "UID": 1,
    #     "CountryCode": "MX",
    #     "state": 1  # 0:授权后默认状态，表示开始获取数据 1:已经获取完毕，开始生成报告 2:报告已经生成
    # }
    # result, msg = ProcessShowData.update_report_status(data)
    # print(result, msg)
    # 下载报告地址 /user/download_report/<int:id>
    # pass
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
    #     delete_data = {
    #         "ID": "1843",
    #         "DataType":"W-FBA",
    #         "UID": "3"
    #     }
    # # #     # ProcessShowData.insert(add_data)
    # # #     ProcessShowData.update(update_data)
    #     ProcessShowData.delete(delete_data)
    # return
