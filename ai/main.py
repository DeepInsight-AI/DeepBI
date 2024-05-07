from dotenv import load_dotenv

# 加载 .env 文件中的环境变量
load_dotenv()
from flask import Flask, request, jsonify, stream_with_context, Response
from flask_cors import CORS,cross_origin
from ai.backend.chat_task import ChatClass
import time  # 用于模拟延迟
import json
import asyncio
import threading
import os


app = Flask(__name__)
# CORS(app)

# cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

class MockWebSocket:
    def __init__(self):
        self.messages = []
        self.chat_id = 0
        pass

    def set_chat_id(self, chat_id):
        self.chat_id = chat_id

    async def send(self, message):
        message = json.loads(message)
        message['chat_id'] = self.chat_id
        message = json.dumps(message)
        message_with_delimiter = message + "---ENDOFMESSAGE---"
        self.messages.append(message_with_delimiter)

def generate_stream(mock_socket, user_name, user_id, message,chat_id):
    try:
        def background_task():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            master = ChatClass(mock_socket, user_name, user_id, message, chat_id)
            loop.run_until_complete(master.consume())
            loop.close()

        thread = threading.Thread(target=background_task)
        thread.start()

        while thread.is_alive() or mock_socket.messages:
            if mock_socket.messages:
                message = mock_socket.messages.pop(0)
                print('send message: ', message)
                yield f"{message}\n\n"
            else:
                time.sleep(1)

    except GeneratorExit:
        print("Client connection closed, stopping background task.")

        if thread and thread.is_alive():
            thread.join(timeout=1)


    except Exception as e:
        print("error: ", e)
        return "error"

@app.route("/api/chat", methods=["POST"])
@cross_origin()
def chat():
    data = request.get_json()
    print("data: ", data)
    user_id = data['user_id']
    user_name = data['user_name']
    message = data['message']
    chat_id = data['chat_id']
    print("user_id: ", user_id)
    print("user_name: ", user_name)
    print("message: ", message)
    print("chat_id: ", chat_id)
    mock_socket = MockWebSocket()
    mock_socket.set_chat_id(chat_id)

    # return Response(stream_with_context(asyncio.run(demo1(mock_socket))), mimetype='text/event-stream')
    return Response(stream_with_context(generate_stream(mock_socket, user_name, user_id, message,chat_id)), mimetype='text/event-stream')
    # s = ChatClass(mock_socket, user_name, user_id, message,chat_id)
    # return Response(stream(content), mimetype='text/plain')



@app.route("/api/readRag", methods=["GET"])
@cross_origin()
def readRag():
    try:
        # 获取 userid 和数据库 id 参数
        user_id = request.args.get('user_id')
        db_id = request.args.get('db_id')
        if not user_id or not db_id:
            return jsonify({"error": "Missing parameters."}), 400

        # 根据命名规范构建文件路径
        filename = f'.rag_{user_id}_db{db_id}.json'
        json_file = f'/opt/DeepBI/user_upload_files/{filename}'

        # 读取 JSON 文件
        with open(json_file, 'r',encoding='utf-8') as file:
            data = json.load(file)
        return jsonify(data)

    except FileNotFoundError:
        return jsonify({"error": "File not found."}), 404
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON format."}), 500


@app.route('/api/updateRag', methods=['POST'])
def updateRag():
    try:
        # 获取传递的数据参数
        user_id = request.args.get('user_id')
        db_id = request.args.get('db_id')
        data_key = request.args.get('data_key')
        data_value = request.args.get('data_value')

        # 检查参数是否存在
        if not user_id or not db_id or not data_key or not data_value:
            return jsonify({"error": "Missing parameters."}), 400

        # 根据命名规范构建文件路径
        filename = f'.rag_{user_id}_db{db_id}.json'
        json_file = f'/opt/DeepBI/user_upload_files/{filename}'

        # 如果文件不存在，则返回错误
        if not os.path.exists(json_file):
            return jsonify({"error": "File not found."}), 404

        # 如果文件已存在，则读取文件内容
        with open(json_file, 'r',encoding='utf-8') as file:
            file_data = json.load(file)

            # 检查 data 是否已存在于文件中
            if data_key in file_data and file_data[data_key] == data_value:
                return jsonify({"message": "Data already exists."}), 200
            else:
                file_data[data_key] = data_value

            # 更新文件内容
            with open(json_file, 'w',encoding='utf-8') as file:
                json.dump(file_data, file)

            return jsonify({"message": "File updated."}), 200
    except Exception as e:
            return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(port=8347, host='0.0.0.0', debug=True)
    # app.run(port=8341, host='0.0.0.0', debug=True, threaded=False) # 尝试设置 主线程中调用API服务

