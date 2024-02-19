# # bi/handlers/feishu.py
# import os
# import time
# import hashlib
# import requests
# from flask import request, jsonify,redirect,render_template,session,Blueprint,url_for
# from bi.handlers import routes  # 假设routes是全局可用的，根据你的项目结构导入
# from bi.handlers.feishu_auth import Auth
# from dotenv import load_dotenv, find_dotenv
# import logging
# from bi.handlers.base import json_response, org_scoped_rule
# from bi import __version__, limiter, models, settings, __DeepBI_version__
# from bi.authentication import current_org, get_login_url, get_next_path
# # 加载环境变量
# load_dotenv(find_dotenv())

# logger = logging.getLogger(__name__)
# USER_INFO_KEY = "UserInfo"
# auth = ""
# # @routes.route("/login", methods=["GET"])
# # @routes.route(org_scoped_rule("/login"), methods=["GET"])
# # def login():
# #     NONCE_STR = "13oEviLbrTo458A3NjrOwS70oTOXVOAm"
# #     APP_ID = os.getenv("APP_ID")
# #     APP_SECRET = os.getenv("APP_SECRET")
# #     FEISHU_HOST = os.getenv("FEISHU_HOST")
# #     auth = Auth(FEISHU_HOST, APP_ID, APP_SECRET)

# #     url = request.args.get("url")
# #     ticket = auth.get_ticket()
# #     timestamp = int(time.time()) * 1000
# #     verify_str = "jsapi_ticket={}&noncestr={}&timestamp={}&url={}".format(
# #         ticket, NONCE_STR, timestamp, url
# #     )
# #     signature = hashlib.sha1(verify_str.encode("utf-8")).hexdigest()

# #     return jsonify({
# #         "appid": APP_ID,
# #         "signature": signature,
# #         "noncestr": NONCE_STR,
# #         "timestamp": timestamp,
# #     })

# # 业务逻辑类
# class Biz(object):
#     @staticmethod
#     def home_handler():
#         # 主页加载流程
#         return Biz._show_user_info()

#     @staticmethod
#     def login_handler():
#         # 需要走免登流程
#         return render_template("login.html", user_info={"name": "unknown"}, login_info="needLogin")

#     # @staticmethod
#     # def login_failed_handler(err_info):
#         # 出错后的页面加载流程
#         # return Biz._show_err_info(err_info)

#     # Session in Flask has a concept very similar to that of a cookie, 
#     # i.e. data containing identifier to recognize the computer on the network, 
#     # except the fact that session data is stored in a server.
#     @staticmethod
#     def _show_user_info():
#         # 直接展示session中存储的用户信息
#         return render_template("login.html", user_info=session[USER_INFO_KEY], login_info="alreadyLogin")

#     # @staticmethod
#     # def _show_err_info(err_info):
#         ## 将错误信息展示在页面上
#         # return render_template("err_info.html", err_info=err_info)

# # 出错时走错误页面加载流程Biz.login_failed_handler(err_info)
# # @app.errorhandler(Exception)
# # def auth_error_handler(ex):
# #     return Biz.login_failed_handler(ex)


# # 默认的主页路径
# # @app.route("/login", methods=["GET"])
# @routes.route(org_scoped_rule("/login"), methods=["GET"])
# def login():
#     # APP_ID = os.getenv("APP_ID")
#     # APP_SECRET = os.getenv("APP_SECRET")
#     # FEISHU_HOST = os.getenv("FEISHU_HOST")
#     # auth = Auth(FEISHU_HOST, APP_ID, APP_SECRET)
#     auth = Auth(os.getenv("FEISHU_HOST"), os.getenv("APP_ID"), os.getenv("APP_SECRET"))
#     print("zxctest=====================")
#     # 打开本网页应用会执行的第一个函数

#     # 如果session当中没有存储user info，则走免登业务流程Biz.login_handler()
#     if USER_INFO_KEY not in session:
#         logging.info("need to get user information")
#         return Biz.login_handler()
#     else:
#         # 如果session中已经有user info，则直接走主页加载流程Biz.home_handler()
#         logging.info("already have user information")
#         return Biz.home_handler()

# @app.route("/callback", methods=["GET"])
# def callback():
#     # 获取 user info

#     # 拿到前端传来的临时授权码 Code
#     code = request.args.get("code")
#     # 先获取 user_access_token
#     auth.authorize_user_access_token(code)
#     # 再获取 user info
#     user_info = auth.get_user_info()
#     # 将 user info 存入 session
#     session[USER_INFO_KEY] = user_info
#     return jsonify(user_info)

# @app.route("/get_appid", methods=["GET"])
# def get_appid():
#     # 获取 appid
#     # 为了安全，app_id不应对外泄露，尤其不应在前端明文书写，因此此处从服务端传递过去
#     return jsonify(
#         {
#             "appid": os.getenv("APP_ID")
#         }
#     )

# # 登录完成
# @app.route("/login_success", methods=["GET"])
# def login_success():
#     # 登录完成后，展示主页
#     print("login_success+++++++++++++++++++++") 
#     return jsonify({"msg": "login success-------------------"})