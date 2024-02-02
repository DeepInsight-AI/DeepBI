# bi/handlers/feishu.py
import os
import time
import hashlib
import requests
from flask import request, jsonify
from bi.handlers import routes  # 假设routes是全局可用的，根据你的项目结构导入
from bi.handlers.feishu_auth import Auth  # 确保这个路径正确
from dotenv import load_dotenv, find_dotenv
import logging
from bi.handlers.base import json_response, org_scoped_rule
# 加载环境变量
load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)

# @routes.route("/login", methods=["GET"])
@routes.route(org_scoped_rule("/login"), methods=["GET"])
def login():
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

# 根据需要添加更多的路由和处理函数