from ai.backend.util import base_util


class Config:
    def __init__(self):
        self.load_conf()

    def load_conf(self):
        self.ApiHost = "https://apiserver.deep-thought.io/proxy"

        # self.database_model = 'test'
        self.database_model = 'online'
        self.request_timeout = 90
        self.max_retry_period = 90
        self.max_retry_times = 3
        self.up_file_path = base_util.get_upload_path()
        self.csv_file_path = base_util.get_upload_path()

        self.web_server_ip = base_util.get_web_server_ip()

        self.web_language = base_util.get_web_language()
        print('web_language : ', self.web_language)

        self.language_chinese = 'CN'
        self.language_english = 'EN'
        self.language_japanese = 'JP'
        self.default_language_mode = self.language_chinese

        self.if_hide_sensitive = False

        self.python_base_dependency = """python installed dependency environment: pymysql, pandas, mysql-connector-python, pyecharts, sklearn, psycopg2, sqlalchemy, pymongo"""

        self.max_token_num = 7500

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

        self.local_base_mysql_info = """ """
        self.apikey_openai = 'OpenAI'
        self.apikey_deepinsight = 'DeepInsight'
        self.apikey_azure = 'Azure'

        self.local_base_mysql_info = """
        """

        self.local_base_mongodb_info = """ """

        self.online_base_mysql_info = """ """

        self.local_base_postgresql_info = """ """

        self.local_base_csv_info = """ """

        self.local_base_xls_info = """ """

        self.default_base_message = """ """

        self.agents_functions = ['task_generate_echart', 'task_generate_report', 'task_base']
        self.default_agents_functions = 'task_base'


CONFIG = Config()
