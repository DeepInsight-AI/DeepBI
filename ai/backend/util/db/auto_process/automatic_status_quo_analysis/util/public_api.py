import hashlib
import time
import requests


class ProcessShowData():
    # 环境配置
    config = {
        'dev': {
            'SECRATE': "localhost3949ba59abbe56e057f20f883e",
            'REQUEST_URL': "http://127.0.0.1:5000/api/public/"
        },
        'test': {
            'SECRATE': "",
            'REQUEST_URL': "http://192.168.2.152:5008/api/public/"
        },
        'online': {
            'SECRATE': "3849ba59abbe56e057f0f883eonline",
            'REQUEST_URL': "https://atlas.deepbi.cn/api/public/"
        }
    }

    # 默认使用生产环境
    environment = 'online'

    @classmethod
    def set_debug_mode(cls, debug):
        """根据debug状态设置环境"""
        cls.environment = "dev"

    @classmethod
    def sha1(cls, input_string):
        """对输入的字符串进行 SHA-256 哈希加密"""
        hash_object = hashlib.sha256()
        hash_object.update(input_string.encode('utf-8'))
        hashed_string = hash_object.hexdigest()
        return hashed_string

    @classmethod
    def post_data(cls, data, op_type):
        if op_type is None or "" == op_type:
            return "error"
        timestamp = int(time.time())
        secrete = cls.config[cls.environment]['SECRATE']
        token = cls.sha1(secrete + str(timestamp) + secrete)
        #
        headers = {
            'Content-Type': 'application/json',
            'token': str(token),
            'timestamp': str(timestamp)
        }
        url = cls.config[cls.environment]['REQUEST_URL'] + op_type
        # 发送POST请求
        response = requests.post(url,
                                 headers=headers, json=data)

        # 输出响应内容
        data = response.json()
        if int(data['code']) == 200:
            return True, data
        else:
            print("操作失败")
            print(response.text)
            return False, data

    @classmethod
    def post_file(cls, file_path, data=None, file_key='upfile',  op_type='upload'):
        import os
        timestamp = int(time.time())
        secrete = cls.config[cls.environment]['SECRATE']
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
        url = cls.config[cls.environment]['REQUEST_URL'] + op_type
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
    def get_accesstoken(cls, data):
        if "uid" not in data:
            return False
        if "shopid" not in data:
            return False
        if "outh_type" not in data:
            return False
        OuthType = data['outh_type']
        return cls.post_data(data, str(OuthType.lower()) + "_accesstoken")
        pass

    @classmethod
    def get_all_user(cls):
        """获取所有用户信息"""
        return cls.post_data([], "get_user")

    @classmethod
    def get_user_shop_outhinfo(cls, data):
        """获取某个用户店铺授权信息"""
        if not data.get('uid', ''):
            print("need uid")
            return False
        return cls.post_data(data, "get_user_shop")

    @classmethod
    def delete_report(cls, data):
        if not data.get('uid', ''):
            print("need uid")
            return False
        if not data.get('shopid', ''):
            print("need shopid")
            return False
        if not data.get('country_code', ''):
            print("need country_code")
            return False
        return cls.post_data(data, "delete_report")
        pass

    @classmethod
    def send_email(cls, data):
        if not data.get('report_id'):
            return False
        return cls.post_data(data, "send_email")

    @classmethod
    def update_report_state(cls, data):
        if not data.get('uid', ''):
            print("need uid")
            return False
        if not data.get('shopid', ''):
            print("need shopid")
            return False
        if not data.get('country_code', ''):
            print("need country_code")
            return False
        return cls.post_data(data, "update_report_state")

    @classmethod
    def get_report_info(cls, data):
        if not data.get('uid', ''):
            print("need uid")
            return False
        return cls.post_data(data, "get_report_info")


if __name__ == "__main__":
    # 获取报告信息
    data = {
        "uid": 2,
        "shopid": "",  # 可以不传递
        "country_code": ""  # 可以不传递
    }
    report_info = ProcessShowData.get_report_info(data)
    print(report_info)
    # 获取所有用户
    # users = ProcessShowData.get_all_user()
    # print(users)
    # 获取 用户店铺信息 包括授权
    # data = {
    #     "uid": 2
    # }
    # shops_and_shop_outh = ProcessShowData.get_user_shop_outhinfo(data)
    # print(shops_and_shop_outh)
    # 获取 AD accesstoken
    # data = {
    #     "uid": '2',
    #     "shopid": "1",
    #     "outh_type": "AD",
    #     "area_code": "NA"  # 地区 非国家
    # }
    # accesstoken_info = ProcessShowData.get_accesstoken(data)
    # print(accesstoken_info)
    # 获取 SP accesstoken
    # data = {
    #     "uid": '2',
    #     "shopid": "1",
    #     "outh_type": "SP",
    #     "area_code": "NA",  # 地区 非国家
    #     # "country_code": "US" 不再使用
    # }
    # accesstoken_info = ProcessShowData.get_accesstoken(data)
    # print(accesstoken_info)
    # 上传 报告结果, 上传返回数据中的 data 为该报告的线上id ，可以用于独立发送 邮件
    file = "./uploads/1.pdf"
    data = {
        "uid": 2,
        "shopid": "1",
        "country_code": "MX",
        "send_email": 0  # 是否通知发送邮件 默认0 不发，1 发送，可以不传递，默认不发送(上传与发送分开)
    }
    result, msg = ProcessShowData.post_file(file, data)
    print(result, msg)
    # 删除已经上传的报告, 如果有多个，会删除多个
    # data = {
    #     "uid": 2,
    #     "shopid": "1",
    #     "country_code": "US"
    # }
    # result, msg = ProcessShowData.delete_report(data)
    # print(result, msg)

    # 单独发送报告给 用户，只传递报告 id
    # data = {
    #     'report_id': '3'
    # }
    # result, msg = ProcessShowData.send_email(data)
    # print(result, msg)

    # 创建 报告状态
    # data = {
    #     "uid": 2,
    #     "shopid": "1",
    #     "country_code": "MX",
    #     'state': '1'  # 报告状态 0 开始抓去数据 1 数据完毕分析中 这里调用不可设置2 2 报告完毕可以下载
    # }
    # result, msg = ProcessShowData.update_report_state(data)
    # print(result, msg)
