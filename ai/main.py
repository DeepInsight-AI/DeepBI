from flask import Flask, request, jsonify
import asyncio
from ai.backend.chat_task import ChatClass
from ai.backend.aidb.autopilot.autopilot_mysql_api import AutopilotMysql
from concurrent.futures import ThreadPoolExecutor
# from flask_cors import CORS,cross_origin
app = Flask(__name__)
# CORS(app)

# cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

@app.route("/api/chat", methods=["POST", "OPTIONS"])
def chat():
    if request.method == "OPTIONS":
        # 手动创建OPTIONS响应
        response = jsonify()  # 使用jsonify创建一个空的响应体
        response.status_code = 200  # 设置状态码为200
    else:
        # 实际的POST请求处理
        data = request.get_json()
        print("data: ", data)
        response = jsonify(message="This is a chat request")
    
    # 为所有响应添加CORS相关的头部信息
    response.headers["Access-Control-Allow-Origin"] = "http://192.168.2.123:8338"  # 允许特定来源
    response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"  # 允许的方法
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"  # 允许的头部
    # 如果你的请求需要凭证（如cookies或认证信息），还需要添加以下头部
    # response.headers["Access-Control-Allow-Credentials"] = "true"

    return response

if __name__ == '__main__':
    app.run(port=8339)



#     import asyncio
# from ai.backend.start_server import WSServer

# if __name__ == '__main__':
#     server_port = 8339
#     s = WSServer(server_port)
    # s.serve_forever()
