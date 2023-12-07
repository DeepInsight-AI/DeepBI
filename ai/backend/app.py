from flask import Flask, request
import asyncio
from ai.backend.chat_task import ChatClass
from ai.backend.aidb.autopilot.autopilot_mysql_api import AutopilotMysql

app = Flask(__name__)


# 异步函数包装器
def run_async(func):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(func(*args, **kwargs))
        loop.close()
        return result

    return wrapper


@app.route('/endpoint', methods=['GET'])
def get_request():
    return 'This is a GET request'


@app.route('/api/autopilot', methods=['POST'])
@run_async
async def post_request():
    print(request.data)
    data = request.get_json()
    print('data: ', data)

    user_name = data['user_name']
    report_id = data['report_id']
    file_name = data['file_name']

    chat_class = ChatClass(None, user_name)
    autopilotMysql = AutopilotMysql(chat_class)
    json_str = {
        "file_name": file_name,
        "report_id": report_id
    }
    await autopilotMysql.deal_question(json_str)

    return 'This is a POST request'


if __name__ == '__main__':
    app.run(port=8340)
