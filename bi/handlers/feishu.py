# bi/handlers/feishu.py
import os
import time
import hashlib
import requests
from flask import Blueprint, request, jsonify
from bi.authentication import current_org
from bi.handlers.base import json_response
from bi.models import settings
from bi.utils.auth import Auth
from dotenv import load_dotenv, find_dotenv
import logging

# 加载环境变量
load_dotenv(find_dotenv())

# 创建一个Blueprint对象
feishu = Blueprint('feishu', __name__, url_prefix='/feishu')
logger = logging.getLogger(__name__)

# 应用出现错误时，使用Flask的errorhandler装饰器实现应用错误处理
@feishu.errorhandler(Exception)
def auth_error_handler(ex):
    logger.error('Feishu error: %s', ex)
    response = jsonify(message=str(ex))
    response.status_code = (
        ex.response.status_code if isinstance(ex, requests.HTTPError) else 500
    )
    return response

@feishu.route("/get_config_parameters", methods=["GET"])
def get_config_parameters():
    NONCE_STR = "13oEviLbrTo458A3NjrOwS70oTOXVOAm"
    APP_ID = os.getenv("APP_ID")
    APP_SECRET = os.getenv("APP_SECRET")
    FEISHU_HOST = os.getenv("FEISHU_HOST")
    auth = Auth(FEISHU_HOST, APP_ID, APP_SECRET)

    url = request.args.get("url")
    ticket = auth.get_ticket()
    timestamp = int(time.time()) * 1000
    verify_str = "jsapi_ticket={}&noncestr={}&timestamp={}&url={}".format(
        ticket, NONCE_STR, timestamp, url
    )
    signature = hashlib.sha1(verify_str.encode("utf-8")).hexdigest()

    return jsonify({
        "appid": APP_ID,
        "signature": signature,
        "noncestr": NONCE_STR,
        "timestamp": timestamp,
    })

# 你可以根据需要添加更多的路由和处理函数
# 例如，处理用户信息的路由
@feishu.route("/get_userinfo", methods=["POST"])
def get_userinfo():
    code = request.json.get("code")
    FEISHU_HOST = os.getenv("FEISHU_HOST")
    APP_ID = os.getenv("APP_ID")
    APP_SECRET = os.getenv("APP_SECRET")
    url = "{}{}".format(FEISHU_HOST, "/open-apis/authen/v1/access_token")
    req_body = {
        "app_id": APP_ID,
        "app_secret": APP_SECRET,
        "grant_type": "authorization_code",
        "code": code
    }
    response = requests.post(url, json=req_body)
    return jsonify(response.json())

# 在app.py中注册蓝图
# app.register_blueprint(feishu)