from ai.backend.util import base_util


class Config():
    def __init__(self):
        self.load_conf()

    def load_conf(self):
        # self.database_model = 'test'
        self.database_model = 'online'
        self.request_timeout = 55
        self.max_retry_period = 90
        self.max_retry_times = 3
        self.up_file_path = base_util.get_upload_path()

        self.web_server_ip = base_util.get_web_server_ip()

        self.language_chinese = 'CN'
        self.language_english = 'EN'
        self.default_language_mode = self.language_chinese

        self.if_hide_sensitive = True

        self.python_base_dependency = """python installed dependency environment: pymysql, pandas, mysql-connector-python, pyecharts, sklearn, psycopg2, sqlalchemy"""

        self.max_token_num = 5000

        self.talker_bi = 'bi'
        self.talker_user = 'user'
        self.talker_log = 'log'
        self.talker_api = 'api'

        self.type_comment = 'mysql_comment'
        self.type_comment_first = 'mysql_comment_first'
        self.type_comment_second = 'mysql_comment_second'
        self.type_data_check = 'data_check'
        self.type_answer = 'answer'
        self.type_question = 'question'
        self.type_log_data = 'log_data'
        self.type_test = 'test'

        self.local_base_mysql_info = """
        """

        self.online_base_mysql_info = """
        """

        self.local_base_postgresql_info = """
        """

        self.local_base_csv_info = """
        """

        self.local_base_xls_info = """
        """

        self.default_base_message = """
                """
CONFIG = Config()


database_model = 'online'
request_timeout = 55
max_retry_period = 90
max_retry_times = 3
csv_file_path = base_util.get_upload_path()
print('csv_file_path :', csv_file_path)

language_chinese = 'CN'
language_english = 'EN'
default_language_mode = language_chinese

if_hide_sensitive = True

python_base_dependency = """python installed dependency environment: pymysql, pandas, mysql-connector-python, pyecharts, sklearn, psycopg2, sqlalchemy"""

receiver_BI = 'redash'
receiver_user = 'user'

local_base_mysql_info = """
"""

# 在线数据库信息
online_base_mysql_info = """
"""

# 在线数据库信息
local_base_postgresql_info = """
"""

local_base_csv_info = """
"""

local_base_xls_info = """
"""

default_base_message = """

        """
