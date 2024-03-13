from flask import Flask, request
import asyncio
from ai.backend.chat_task import ChatClass
from ai.backend.aidb.autopilot.autopilot_mysql_api import AutopilotMysql
from concurrent.futures import ThreadPoolExecutor
from flask_cors import CORS
app = Flask(__name__)
CORS(app)

@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.get_json()
    print("data: ", data)
    return "This is a chat request"

if __name__ == '__main__':
    app.run(port=8339)



#     import asyncio
# from ai.backend.start_server import WSServer

# if __name__ == '__main__':
#     server_port = 8339
#     s = WSServer(server_port)
    # s.serve_forever()
