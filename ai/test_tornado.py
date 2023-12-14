from dotenv import load_dotenv
import tornado.web

# 加载 .env 文件中的环境变量
load_dotenv()

# from ai.backend.app import app
# app.run(port=8340)

from ai.backend import app2

app = app2.make_app()
app.listen(8340)
tornado.ioloop.IOLoop.current().start()

from ai.backend.util.db.postgresql_report import PsgReport
# 更新数据
report_id = 36
# data_to_update = (2, report_id)
# PsgReport().update_data(data_to_update)
