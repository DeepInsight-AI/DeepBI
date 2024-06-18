import tornado.ioloop
import tornado.web
import json
from ai.backend.chat_task import ChatClass
from ai.backend.aidb.autopilot.autopilot_mysql_api import AutopilotMysql
from ai.backend.aidb.autopilot.autopilot_starrocks_api import AutopilotStarrocks
from ai.backend.aidb.autopilot.autopilot_mongodb_api import AutopilotMongoDB
from ai.backend.aidb.autopilot.autopilot_csv_api import AutopilotCSV
from ai.backend.base_config import CONFIG
from ai.backend.aidb.dashboard.prettify_dashboard import PrettifyDashboard


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

        report_file_name = CONFIG.up_file_path + file_name
        with open(report_file_name, 'r') as file:
            data = json.load(file)

        databases_type = 'mysql'
        if data.get('databases_type') is not None:
            databases_type = data['databases_type']

        chat_class = ChatClass(None, user_name)
        json_str = {
            "file_name": file_name,
            "report_id": report_id
        }

        if databases_type == 'starrocks':
            autopilot_starrocks = AutopilotStarrocks(chat_class)
            await autopilot_starrocks.deal_question(json_str)
        elif "mongodb" == databases_type:
            autopilot_mongodb = AutopilotMongoDB(chat_class)
            # new db
            await autopilot_mongodb.deal_question(json_str)
        elif "csv" == databases_type:
            autopilot_csv = AutopilotCSV(chat_class)
            await autopilot_csv.deal_question(json_str)
        else:
            autopilotMysql = AutopilotMysql(chat_class)
            await autopilotMysql.deal_question(json_str)


class DashboardHandler(tornado.web.RequestHandler):
    def get(self, page_name):
        print("view html :", page_name)
        if str(page_name).endswith('.html'):
            self.render(f"{page_name}")

    async def post(self):
        data = json.loads(self.request.body.decode('utf-8'))
        print('/api/dashboard data : ', data)

        # 异步处理接收到的数据
        await self.process_data(data)

        # 返回响应
        self.write("POST request handled asynchronously in main thread")

    async def process_data(self, data):
        # 在这里异步处理接收到的数据，例如打印或执行其他操作
        print("Received data:", data)
        # 模拟异步处理
        print("Data processed asynchronously")

        user_name = data['user_name']
        task_id = data['task_id']
        file_name = data['file_name']

        chat_class = ChatClass(None, user_name)
        prettifyDashboard = PrettifyDashboard(chat_class)
        json_str = {
            "file_name": file_name,
            "task_id": task_id
        }
        await prettifyDashboard.deal_question(json_str)


def make_app():
    return tornado.web.Application([
        (r"/api/autopilot", MainHandler),
        (r"/api/dashboard", DashboardHandler),
    ])


class CustomApplication(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/api/autopilot", MainHandler),
            (r"/api/dashboard", DashboardHandler),
            (r"/api/dashboard/(.*)", DashboardHandler),
        ]

        print('template_path :', CONFIG.up_file_path)

        settings = {
            "template_path": CONFIG.up_file_path,  # 指定模板路径
        }

        super().__init__(handlers, **settings)

# if __name__ == "__main__":
#     app = make_app()
#     app.listen(8340)
#     tornado.ioloop.IOLoop.current().start()
