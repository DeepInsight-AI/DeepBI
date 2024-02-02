import requests
import logging

# const
# 开放接口 URI
TENANT_ACCESS_TOKEN_URI = "/open-apis/auth/v3/tenant_access_token/internal"
JSAPI_TICKET_URI = "/open-apis/jssdk/ticket/get"


class Auth(object):
    def __init__(self, feishu_host, app_id, app_secret):
        self.feishu_host = feishu_host
        self.app_id = app_id
        self.app_secret = app_secret
        self.tenant_access_token = ""

    def get_ticket(self):
        # 获取jsapi_ticket，具体参考文档：https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/h5_js_sdk/authorization
        self.authorize_tenant_access_token()
        url = "{}{}".format(self.feishu_host, JSAPI_TICKET_URI)
        headers = {
            "Authorization": "Bearer " + self.tenant_access_token,
            "Content-Type": "application/json",
        }
        resp = requests.post(url=url, headers=headers)
        Auth._check_error_response(resp)
        return resp.json().get("data").get("ticket", "")

    def authorize_tenant_access_token(self):
        # 获取tenant_access_token，基于开放平台能力实现，具体参考文档：https://open.feishu.cn/document/ukTMukTMukTM/ukDNz4SO0MjL5QzM/auth-v3/auth/tenant_access_token_internal
        url = "{}{}".format(self.feishu_host, TENANT_ACCESS_TOKEN_URI)
        req_body = {"app_id": self.app_id, "app_secret": self.app_secret}
        response = requests.post(url, req_body)
        Auth._check_error_response(response)
        self.tenant_access_token = response.json().get("tenant_access_token")

    @staticmethod
    def _check_error_response(resp):
        # 检查响应体是否包含错误信息
        if resp.status_code != 200:
            raise resp.raise_for_status()
        response_dict = resp.json()
        code = response_dict.get("code", -1)
        if code != 0:
            logging.error(response_dict)
            raise FeishuException(code=code, msg=response_dict.get("msg"))


class FeishuException(Exception):
    # 处理并展示飞书侧返回的错误码和错误信息
    def __init__(self, code=0, msg=None):
        self.code = code
        self.msg = msg

    def __str__(self) -> str:
        return "{}:{}".format(self.code, self.msg)

    __repr__ = __str__
