import tornado.ioloop
import tornado.web
import asyncio

from flask import Flask, request
from ai.backend.chat_task import ChatClass
from ai.backend.aidb.autopilot.autopilot_mysql_api import AutopilotMysql
from concurrent.futures import ThreadPoolExecutor
import threading
import json

class MainHandler(tornado.web.RequestHandler):
    async def post(self):
        data = json.loads(self.request.body.decode('utf-8'))

        # 异步处理接收到的数据
        await self.process_data(data)

        # 返回响应
        self.write("POST request handled asynchronously in main thread")

    async def process_data(self, data):
        # 在这里异步处理接收到的数据，例如打印或执行其他操作
        print("Received data:", data)
        # 模拟异步处理
        print("Data processed asynchronously")

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

def make_app():
    return tornado.web.Application([
        (r"/api/autopilot", MainHandler),
    ])

# if __name__ == "__main__":
#     app = make_app()
#     app.listen(8340)
#     tornado.ioloop.IOLoop.current().start()
