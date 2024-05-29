from dotenv import load_dotenv
import tornado.web

# 加载 .env 文件中的环境变量
load_dotenv()

from ai.backend.app2 import CustomApplication

# app = CustomApplication()
# app.listen(8340)
# tornado.ioloop.IOLoop.current().start()


from ai.backend.util.database_util import Main
db_id = str(1)
obj = Main(db_id)
obj.run()
# this is a test
obj.run_decode()
