#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import time
import hashlib
import requests
from auth import Auth
from dotenv import load_dotenv, find_dotenv
from flask import Flask, request, jsonify, render_template

# const
# 随机字符串，用于签名生成加密使用
NONCE_STR = "13oEviLbrTo458A3NjrOwS70oTOXVOAm"

# 从 .env 文件加载环境变量参数
load_dotenv(find_dotenv())

# 获取环境变量
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
FEISHU_HOST = os.getenv("FEISHU_HOST")

# # 应用出现错误时，实用flask的errorhandler装饰器实现应用错误处理
# @app.errorhandler(Exception)
# def auth_error_handler(ex):
#     response = jsonify(message=str(ex))
#     response.status_code = (
#         ex.response.status_code if isinstance(ex, requests.HTTPError) else 500
#     )
#     return response


# 用获取的环境变量初始化Auth类，由APP ID和APP SECRET获取access token，进而获取jsapi_ticket
auth = Auth(FEISHU_HOST, APP_ID, APP_SECRET)

# 获取并返回接入方前端将要调用的config接口所需的参数
@app.route("/get_config_parameters", methods=["GET"])
def get_config_parameters():    
    # 接入方前端传来的需要鉴权的网页url
    url = request.args.get("url")
    # 初始化Auth类时获取的jsapi_ticket
    ticket = auth.get_ticket()
    # 当前时间戳，毫秒级
    timestamp = int(time.time()) * 1000
    # 拼接成字符串 
    verify_str = "jsapi_ticket={}&noncestr={}&timestamp={}&url={}".format(
        ticket, NONCE_STR, timestamp, url
    )
    # 对字符串做sha1加密，得到签名signature
    signature = hashlib.sha1(verify_str.encode("utf-8")).hexdigest()
    # 将鉴权所需参数返回给前端
    return jsonify(
        {
            "appid": APP_ID,
            "signature": signature,
            "noncestr": NONCE_STR,
            "timestamp": timestamp,
        }
    )


# @app.route("/get_userinfo", methods=["POST"])
# # 接受前端传来的用户信息
# def get_userinfo():
#     code = request.json.get("code")
#     # 根据code获取用户信息
#     url = "{}{}".format(FEISHU_HOST, "/open-apis/authen/v1/access_token")
#     req_body = {"app_id": APP_ID, "app_secret": APP_SECRET, "grant_type": "authorization_code", "code": code}
#     response = requests.post(url, req_body)
#     # 返回用户信息
#     return jsonify(response.json())