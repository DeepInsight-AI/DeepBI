from flask import Flask, request, jsonify, stream_with_context, Response
from flask_cors import CORS,cross_origin
from ai.backend.chat_task1 import ChatClass
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
        pass

    async def send(self, message):
        self.messages.append(message)
        print("Message to send:", message)       

# async def demo1(mock_socket):
#     result_message = {
#                 'state': 200,
#                 'receiver': 'bi',
#                 'data': {
#                     'data_type': 'mysql_code',
#                     'content': "123123123123",
#                     'name': "12312312312312"
#                 },
#                 'id': "123213"
#     }
#     result_message = json.dumps(result_message)
#     print("result_message111: ", result_message)
#     mock_socket.send(result_message)
#     await demo2(mock_socket)

# async def demo2(mock_socket):
#     await asyncio.sleep(5)
#     result_message = {
#                 'state': 200,
#                 'receiver': 'bi',
#                 'data': {
#                     'data_type': 'mysql_code',
#                     'content': "456456456456",
#                     'name': "456456456456"
#                 },
#                 'id': "456456"
#     }
#     result_message = json.dumps(result_message)
#     print("result_message222: ", result_message)
#     mock_socket.send(result_message)

# def generate_stream(mock_socket):
#     while True:
#         if mock_socket.messages:
#             message = mock_socket.messages.pop(0)
#             print("message: ", message)
#             yield f"{message}\n\n"
#         else:
#             time.sleep(1)


def generate_stream(mock_socket):
    def background_task():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(demo1(mock_socket))
    
    thread = threading.Thread(target=background_task)
    thread.start()
    
    while thread.is_alive() or mock_socket.messages:
        if mock_socket.messages:
            message = mock_socket.messages.pop(0)
            yield f"{message}\n\n"
        else:
            time.sleep(1)
    
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
    s = ChatClass(mock_socket, user_name, user_id, message,chat_id)
    # return Response(stream_with_context(asyncio.run(demo1(mock_socket))), mimetype='text/event-stream')
    return Response(stream_with_context(generate_stream(mock_socket)), mimetype='text/event-stream')
    # s = ChatClass(mock_socket, user_name, user_id, message,chat_id)
    # return Response(stream(content), mimetype='text/plain')

if __name__ == '__main__':
    app.run(port=8339)

