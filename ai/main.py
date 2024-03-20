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
        print("Message to send:", message_with_delimiter)

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

def run_app():
    app.run(port=8341, host='0.0.0.0', debug=True)

if __name__ == '__main__':
    run_app()
    # app.run(port=8341, host='0.0.0.0', debug=True, threaded=False) # 尝试设置 主线程中调用API服务

