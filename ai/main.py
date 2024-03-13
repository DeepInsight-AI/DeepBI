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
    # 预检请求的处理
    if request.method == "OPTIONS":
        response = app.make_default_options_response()
    else:
        # 实际的POST请求处理
        data = request.get_json()
        print("data: ", data)
        response = jsonify(message="This is a chat request")
    
    # 添加CORS相关的头部信息
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")

    return response

if __name__ == '__main__':
    app.run(port=8339)



#     import asyncio
# from ai.backend.start_server import WSServer

# if __name__ == '__main__':
#     server_port = 8339
#     s = WSServer(server_port)
    # s.serve_forever()
