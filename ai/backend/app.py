from flask import Flask, request
import asyncio
from ai.backend.chat_task import ChatClass
from ai.backend.aidb.autopilot.autopilot_mysql import AutopilotMysql

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
    db_comment = data['db_comment']
    report_desc = data['report_desc']
    databases_id = data['databases_id']

    chat_class = ChatClass(None, user_name)
    autopilotMysql = AutopilotMysql(chat_class)
    first_json_str = {
        "state": 200,
        "sender": "bi",
        "database": "mysql",
        "data": {
            "data_type": "mysql_comment_second",
            "content": db_comment,
            "databases_id": databases_id
        }
    }
    await autopilotMysql.deal_question(first_json_str, None)

    qst_json_str = {
        "state": 200,
        "sender": "user",
        "database": "mysql",
        "data": {
            "data_type": "question",
            "content": report_desc
        }
    }
    await autopilotMysql.deal_question(qst_json_str, None)

    return 'This is a POST request'

if __name__ == '__main__':
    app.run(port=8340)
