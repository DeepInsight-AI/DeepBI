from flask import Flask, request
import asyncio
from ai.backend.chat_task import ChatClass
from ai.backend.aidb.autopilot.autopilot_mysql_api import AutopilotMysql
from concurrent.futures import ThreadPoolExecutor
import threading

app = Flask(__name__)
executor = ThreadPoolExecutor()


# Async function wrapper
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


@app.route('/api/autopilot1', methods=['POST'])
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




@app.route('/api/autopilot1', methods=['POST'])
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

    def create_chat():
        # print("create_chat: ")
        print("self.openai_proxy , ", self.openai_proxy)

        # TODO: #1143 handle token limit exceeded error
        if self.openai_proxy is None:
            response = oai.ChatCompletion.create(
                context=messages[-1].pop("context", None), use_cache=self.use_cache,
                messages=self._oai_system_message + messages,
                agent_name=self.name,
                **llm_config
            )
        else:
            response = oai.ChatCompletion.create(
                context=messages[-1].pop("context", None), use_cache=self.use_cache,
                messages=self._oai_system_message + messages,
                openai_proxy=self.openai_proxy,
                agent_name=self.name,
                **llm_config
            )

        # print("response: ", response)
        return response

    async def consume_async():
        # loop = asyncio.new_event_loop()
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, create_chat)
        # print("result :", result)
        return result

    response = await consume_async()

    # 在主线程中执行 process_data
    t = threading.Thread(target=process_data, args=(data,))
    t.start()

    return 'This is a POST request'





if __name__ == '__main__':
    app.run(port=8340)
