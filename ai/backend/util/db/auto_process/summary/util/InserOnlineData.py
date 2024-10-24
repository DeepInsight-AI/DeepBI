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
            'ONLINE_URL': "http://test.deepbi.com/api/data/"
        },
        'test_pre': {
            'INSERT_SECRATE': '69c5fcebaa65b560eaf06c3fbeb481ae44b8dpre',
            "ONLINE_URL": "https://pre_atlas.deepbi.com/api/data/"
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
        """单个添加"""
        return cls.post_data(data, "insert")

    @classmethod
    def inserts(cls, data):
        """批量添加"""
        return cls.post_data(data, "inserts")

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

    @classmethod
    def add_or_update_asin(cls, post_data=None):
        """更新或者提交 某个用户某个国家 asin 关系"""
        if "UID" not in post_data:
            return False, "UID 不能为空"
        if "CountryCode" not in post_data:
            return False, "CountryCode 不能为空"
        if "AsinData" not in post_data:
            return False, "AsinData 不能为空"
        return cls.post_data(post_data, "add_or_update_asin")

    @classmethod
    def add_or_update_asin_country_data(cls, post_data=None):
        """更新或者提交 某个用户某个国家 asin 总体广告数据 每天 按照每天更新"""
        if "UID" not in post_data:
            return False, "UID 不能为空"
        if "CountryCode" not in post_data:
            return False, "CountryCode 不能为空"
        if "DataDate" not in post_data:
            return False, "DataDate 不能为空"
        if "Data" not in post_data:
            return False, "Data 不能为空"
        if "DataType" not in post_data or len(post_data.get('DataType', "")) != 8:
            return False, "DataType 不能为空且长度必须为8"
        return cls.post_data(post_data, "add_or_update_asin_country_data")

    @classmethod
    def add_or_update_asin_campaign_data(cls, post_data):
        """更新或者提交 某个用户某个国家 某个asin 多个计划,每个计划汇总数据"""
        if "UID" not in post_data:
            return False, "UID 不能为空"
        if "CountryCode" not in post_data:
            return False, "CountryCode 不能为空"
        if "DataDate" not in post_data:
            return False, "DataDate 不能为空"
        if "Data" not in post_data:
            return False, "Data 不能为空"
        if "DataType" not in post_data or len(post_data.get('DataType', "")) != 8:
            return False, "DataType 不能为空且长度必须为8"
        if "Asin" not in post_data:
            return False, "Asin 不能为空"
        return cls.post_data(post_data, "add_or_update_asin_campaign_data")

    @classmethod
    def add_or_update_asin_campaign_word_data(cls, post_data):
        """更新或者提交，某个计划的所有词1天的数据 """
        if "UID" not in post_data:
            return False, "UID 不能为空"
        if "Data" not in post_data:
            return False, "Data 不能为空"
        if "DataType" not in post_data or len(post_data.get('DataType', "")) != 8:
            return False, "DataType 不能为空且长度必须为8"
        if "CampaignID" not in post_data:
            return False, "CampaignID 不能为空"
        return cls.post_data(post_data, "add_or_update_asin_campaign_word_data")

    @classmethod
    def show_auth_asin(cls, post_data):
        """获取授权信息，按照国家返回"""
        if "UID" not in post_data:
            return False, "UID 不能为空"
        return cls.post_data(post_data, "show_auth_asin")

    @classmethod
    def user_account_info(cls, post_data):
        """获取用户列表"""
        return cls.post_data(post_data, "user_account_info")

    @classmethod
    def set_account_dbname(cls, post_data):
        """设置线上数据库名称"""
        return cls.post_data(post_data, "set_account_dbname")


if __name__ == "__main__":
    # 获取用户信息
    data = {
        "CloseFlag": 0  # 1 关闭的 0 没有关闭的
    }
    data, msg = ProcessShowData.user_account_info(post_data=data)
    print(data, msg)
    # # # 获取授权状态，包括报告状态
    # data = {
    #    "UID": 1
    # }
    # result, msg = ProcessShowData.get_user_outh(data)
    # print(result, msg)
    # data = {
    #     "UID": 1
    # }
    # result, msg = ProcessShowData.show_auth_asin(
    #     post_data=data)
    # print(result, msg)
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
