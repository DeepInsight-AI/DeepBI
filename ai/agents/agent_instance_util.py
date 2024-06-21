from typing import Optional
import re
import json
from ai.backend.util.write_log import logger
import traceback
from ai.backend.util.token_util import num_tokens_from_messages
from ai.agents.prompt import EXCEL_ECHART_TIPS_MESS, \
    MYSQL_ECHART_TIPS_MESS, MYSQL_MATPLOTLIB_TIPS_MESS, POSTGRESQL_ECHART_TIPS_MESS, MONGODB_ECHART_TIPS_MESS, CSV_ECHART_TIPS_MESS
from ai.agents.agentchat import (UserProxyAgent, GroupChat, AssistantAgent, GroupChatManager,
                                 PythonProxyAgent, BIProxyAgent, TaskPlannerAgent, TaskSelectorAgent, CheckAgent,
                                 ChartPresenterAgent)
from ai.backend.base_config import CONFIG

max_retry_times = CONFIG.max_retry_times
language_chinese = CONFIG.language_chinese
language_english = CONFIG.language_english
language_japanese = CONFIG.language_japanese
default_language_mode = CONFIG.default_language_mode
local_base_postgresql_info = CONFIG.local_base_postgresql_info
local_base_xls_info = CONFIG.local_base_xls_info
local_base_mysql_info = CONFIG.local_base_mysql_info
local_base_mongodb_info = CONFIG.local_base_mongodb_info
python_base_dependency = CONFIG.python_base_dependency
request_timeout = CONFIG.request_timeout


class AgentInstanceUtil:
    def __init__(
        self,
        websocket: Optional = None,
        # base_message: Optional[str] = default_base_message,
        base_message: Optional[str] = None,
        user_name: Optional[str] = "default_user",
        delay_messages: Optional = None,
        outgoing: Optional = None,
        incoming: Optional = None,
        db_id: Optional = None,
    ):
        self.base_message = base_message
        self.websocket = websocket
        self.user_name = user_name
        self.delay_messages = delay_messages
        self.outgoing = outgoing
        self.incoming = incoming
        # add by lu, Set the model name in use
        self.llm_in_use_name = None

        # self.base_mysql_info = online_base_mysql_info
        self.base_mysql_info = local_base_mysql_info
        # self.base_csv_info = local_base_csv_info
        self.base_csv_info = local_base_xls_info

        self.base_postgresql_info = local_base_postgresql_info
        self.base_mongodb_info = local_base_mongodb_info
        self.base_starrocks_info = None
        self.is_log_out = True
        self.language_mode = default_language_mode
        self.set_language_mode(self.language_mode)
        self.api_key_use = False
        self.openai_proxy = None
        self.db_id = db_id

    def set_api_key(self, api_key, ApiType="openai", api_host=None, ApiModel=None, LlmSetting=None):
        self.api_key = api_key
        if api_host is not None:
            # api_base = "https://api.openai.com/"
            print('api_host: ', api_host)
            self.config_list_gpt4 = [
                {
                    'model': ApiModel,
                    'api_key': api_key,
                    'api_base': api_host,
                    'api_type': ApiType,
                    'llm_setting': LlmSetting
                },
            ]

            self.config_list_gpt4_turbo = [
                {
                    'model': ApiModel,
                    'api_key': self.api_key,
                    'api_base': api_host,
                    'api_type': ApiType,
                    'llm_setting': LlmSetting
                },
            ]

            self.config_list_gpt35_turbo = [
                {
                    'model': 'gpt-3.5-turbo-1106',
                    'api_key': self.api_key,
                    'api_base': api_host,
                    'api_type': ApiType,
                    'llm_setting': LlmSetting
                },
            ]
        else:

            self.config_list_gpt4 = [
                {
                    'model': ApiModel,
                    'api_key': api_key,
                    'api_type': ApiType,
                    'llm_setting': LlmSetting
                },
            ]

            self.config_list_gpt4_turbo = [
                {
                    'model': ApiModel,
                    'api_key': self.api_key,

                    'api_type': ApiType,
                    'llm_setting': LlmSetting
                },
            ]

            self.config_list_gpt35_turbo = [
                {
                    'model': 'gpt-3.5-turbo-1106',
                    'api_key': self.api_key,
                    'api_type': ApiType,
                    'llm_setting': LlmSetting
                },
            ]

            if api_host is not None:
                # api_base = "https://api.openai.com/"
                print('api_host: ', api_host)
                self.config_list_gpt4[0]['api_base'] = api_host
                self.config_list_gpt4_turbo[0]['api_base'] = api_host
                self.config_list_gpt35_turbo[0]['api_base'] = api_host

        self.gpt4_turbo_config = {
            "seed": 42,  # change the seed for different trials
            "temperature": 0,
            "config_list": self.config_list_gpt4_turbo,
            "request_timeout": request_timeout,
        }

        self.gpt4_config = {
            "seed": 42,  # change the seed for different trials
            "temperature": 0,
            "config_list": self.config_list_gpt4,
            "request_timeout": request_timeout,
        }

        self.gpt35_turbo_config = {
            "seed": 42,  # change the seed for different trials
            "temperature": 0,
            "config_list": self.config_list_gpt35_turbo,
            "request_timeout": request_timeout,
        }

    def set_base_message(self, message):
        print('run function set_base_message ... ')
        for table in message['table_desc']:
            for field in table['field_desc']:
                field_keys = list(field.keys())
                for key in field_keys:
                    if key not in ['name', 'comment']:
                        field.pop(key)

        self.base_message = str(message)
        print('base_message : ', message)

    def get_agent_mysql_engineer(self):
        """mysql engineer"""
        mysql_llm_config = {
            "functions": [
                {
                    "name": "run_mysql_code",
                    "description": "run mysql code",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "mysql_code_str": {
                                "type": "string",
                                "description": "mysql code",
                            },
                            "data_name": {
                                "type": "string",
                                "description": "Annotations for MySQL code generated data. Generally, it is the name of the chart or report, and supports Chinese. If a name is specified in the question, use the given name, otherwise generate a suitable name. ",
                            }
                        },
                        "required": ["mysql_code_str", "data_name"],
                    },
                },

            ],
            "config_list": self.config_list_gpt4_turbo,
            "request_timeout": request_timeout,
        }

        mysql_engineer = AssistantAgent(
            name="mysql_engineer",
            llm_config=mysql_llm_config,
            system_message='''You are a mysql report engineer, a world-class engineer that can complete any report by executing mysql code.
                You write mysql code to solve tasks. Wrap the code in a code block that specifies the script type. The user can't modify your code. So do not suggest incomplete code which requires others to modify. Don't use a code block if it's not intended to be executed by the executor.
                Don't include multiple code blocks in one response. Do not ask others to copy and paste the result. Check the execution result returned by the executor.
                If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, collect additional info you need, and think of a different approach to try.
                Hand over your code to the Executor for execution.
                Don’t query too much data, Try to merge query data as simply as possible.
                Be careful to avoid using mysql special keywords in mysql code.
                If function call is needed, the function name mast be 'run_mysql_code', be sure contains no other characters.
                Reply "TERMINATE" in the end when everything is done.
                ''',
            function_map={"bi_run_chart_code": BIProxyAgent.run_chart_code},
            websocket=self.websocket,
            is_log_out=self.is_log_out,
            user_name=self.user_name,
            openai_proxy=self.openai_proxy,
        )
        return mysql_engineer

    def get_agent_postgresql_engineer(self):
        """ postgresql engineer"""
        postgresql_llm_config = {
            "functions": [
                {
                    "name": "run_mysql_code",
                    "description": "run sql code",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "mysql_code_str": {
                                "type": "string",
                                "description": "sql code",
                            },
                            "data_name": {
                                "type": "string",
                                "description": "Annotations for SQL code generated data. Generally, it is the name of the chart or report, and supports Chinese. If a name is specified in the question, the given name is used.",
                            }
                        },
                        "required": ["mysql_code_str", "data_name"],
                    },
                },

            ],
            "config_list": self.config_list_gpt4_turbo,
            "request_timeout": request_timeout,
        }

        postgresql_engineer = AssistantAgent(
            name="postgresql_engineer",
            llm_config=postgresql_llm_config,
            system_message='''You are a postgresql report engineer, a world-class engineer that can complete any report by executing postgresql code.
                 You write postgresql code to solve tasks. Wrap the code in a code block that specifies the script type. The user can't modify your code. So do not suggest incomplete code which requires others to modify. Don't use a code block if it's not intended to be executed by the executor.
                 Don't include multiple code blocks in one response. Do not ask others to copy and paste the result. Check the execution result returned by the executor.
                 If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, collect additional info you need, and think of a different approach to try.
                 Hand over your code to the Executor for execution.
                 Don’t query too much data, Try to merge query data as simply as possible.
                 Be careful to avoid using postgresql special keywords in postgresql code.
                 Reply "TERMINATE" in the end when everything is done.
                 ''',
            websocket=self.websocket,
            is_log_out=self.is_log_out,
            user_name=self.user_name,
            openai_proxy=self.openai_proxy,
        )
        return postgresql_engineer

    def get_agent_chart_presenter(self):
        """chart designer"""
        function_call_name = "bi_run_chart_code"

        chart_llm_config = {
            "functions": [
                {
                    "name": function_call_name,
                    "description": "Convert data into chart",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "chart_code_str": {
                                "type": "string",
                                "description": "chart json code ",
                            }
                        },
                        "required": ["chart_code_str"],
                    },
                }
            ],
            "config_list": self.config_list_gpt4_turbo,
            "request_timeout": request_timeout,
        }

        # chart_presenter = AssistantAgent(
        chart_presenter = ChartPresenterAgent(
            name="chart_presenter",
            llm_config=chart_llm_config,
            system_message='''You are a chart data presenter, and your task is to choose the appropriate presentation method for the data.
                     There are currently several types of charts that can be used, including line, column, area, pie, scanner, bubble, heatmap, box, and table.
                     For example, selecting a set of data to display in a column chart format and specifying the x and y axis data.
                     Usually, there can only be one set of x-axis data, while there can be multiple sets of y-axis data.

                     Please check the SQL statement in context. If a gourp by is included in the SQL statement, the chart must use fields that are not designated as x-axis or y-axis as gourp by values.
                     There can only be one mapping on the x-axis, such as {"mon": "x"}.
                     There can be one or more mappings on the y-axis, such as {"prao": "y", "prbo": "y"}.
                     gourp by can only have one mapping, such as
                         {"fix":"series"}

                     The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
                      [
                         {"globalSeriesType":"box","columnMapping":{"mon":"x","prao":"y","prbo":"y","prco":"y","fix":"series"}}
                      ].

                     If there is no suitable chart, or if the user requests a table, use the table to display, and the returned results are as follows:
                      [
                         {"globalSeriesType": "table", "columnMapping": ""}
                      ]

                     ''',
            user_name=self.user_name,
            openai_proxy=self.openai_proxy,
            function_call_name=function_call_name,

        )
        return chart_presenter

    def get_agent_mongodb_engineer(self):
        """ mongodb engineer, for web, name mast be 'run_mysql_code'"""
        mongodb_llm_config = {
            "functions": [
                {
                    "name": "run_mysql_code",
                    "description": "run sql code",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "mysql_code_str": {
                                "type": "string",
                                "description": """
                                Example of mongodb output as fellows:
                 The user table users is used as an example，
                 Example Query information about all users: {"collection": "users"};
                 Example Query the '_id' and 'name' fields of all users: {"collection": "users", "fields": {"_id": 1, "name": 2}};
                 Example Query the user named "deep":  {"collection": "users", "query": {"name": "deep"}};
                 Example Query all users whose names start with 'deep': {"collection": "users","query":{"name": {"$regex": "^deep"}}}
                 Example Query the number of users starting with 'deep' among all users :{"collection": "users","query":{"name": {"$regex": "^deep"}},"count":"1"}
                 Query the top 10 users in descending order of "name":{"collection": "users","sort":[{"name":"name", "direction": -1}],"limit":10}
                 This is a mongodb database, not mysql, do not use mysql statements,Must be returned using the sample json format.
                                """,
                            },
                            "data_name": {
                                "type": "string",
                                "description": "Annotations for SQL code generated data. Generally, it is the name of "
                                               "the chart or report, and supports Chinese. If a name is specified in the question, "
                                "the given name is used.",
                            }
                        },
                        "required": ["mongodb_code_str", "data_name"],
                    },
                },

            ],
            "config_list": self.config_list_gpt4_turbo,
            "request_timeout": request_timeout,
        }

        mongodb_engineer = AssistantAgent(
            name="mongodb_engineer",
            llm_config=mongodb_llm_config,
            system_message='''You are a mongodb report engineer, a world-class engineer that can complete any report by executing mongodb code.
                 You write mongodb code to solve tasks. Wrap the code in a code block that specifies the script type. The user can't modify your code. So do not suggest incomplete code which requires others to modify. Don't use a code block if it's not intended to be executed by the executor.
                 Don't include multiple code blocks in one response. Do not ask others to copy and paste the result. Check the execution result returned by the executor.
                 If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, collect additional info you need, and think of a different approach to try.
                 Hand over your code to the Executor for execution.
                 Don’t query too much data, Try to merge query data as simply as possible.
                 Be careful to avoid using mongodb special keywords in mongodb code.
                 The output mast be formatted as a JSON instance that conforms to the json schema below, the JSON is a list of dict.

                 Reply "TERMINATE" in the end when everything is done.
                 ''',
            websocket=self.websocket,
            is_log_out=self.is_log_out,
            user_name=self.user_name,
            openai_proxy=self.openai_proxy,
        )
        """
            define mongodb operate
        """
        return mongodb_engineer

    def get_agent_chart_presenter_old(self):
        """chart designer"""
        chart_llm_config = {
            "functions": [
                {
                    "name": "bi_run_chart_code",
                    "description": "Convert data into chart",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "chart_code_str": {
                                "type": "string",
                                "description": "chart json code ",
                            }
                        },
                        "required": ["chart_code_str"],
                    },
                }
            ],
            "config_list": self.config_list_gpt4_turbo,
            "request_timeout": request_timeout,
        }

        chart_presenter = AssistantAgent(
            name="chart_presenter",
            llm_config=chart_llm_config,
            system_message='''You are a chart data presenter, and your task is to choose the appropriate presentation method for the data.
                      There are currently several types of charts that can be used, including line, column, area, pie, scanner, bubble, heatmap, box, and table.
                      For example, selecting a set of data to display in a column chart format and specifying the x and y axis data.
                      Usually, there can only be one set of x-axis data, while there can be multiple sets of y-axis data.
                      Hand over your code to the Executor for execution.
                      There can only be x-axis and y-axis mappings in columnMapping
                      In columnMapping, some or all data can be mapped.
                      There can only be one mapping on the x-axis, such as {"mon": "x"}.
                      There can be one or more mappings on the y-axis, such as {"prao": "y", "prbo": "y"}.
                      The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
                       [
                           {"globalSeriesType":"box","columnMapping":{"mon":"x","prao":"y","prbo":"y","prco":"y"}}
                       ].

                       If there is no suitable chart, or if the user requests a table, use the table to display, and the returned results are as follows:
                       [
                          {"globalSeriesType": "table", "columnMapping": ""}
                       ]

                       Reply "TERMINATE" in the end when everything is done.
                      ''',
            user_name=self.user_name,
            openai_proxy=self.openai_proxy,
        )
        return chart_presenter

    def get_agent_base_mysql_assistant(self):
        """ Basic Agent, processing mysql data source """
        base_mysql_assistant = TaskSelectorAgent(
            name="base_mysql_assistant",
            system_message="""You are a helpful AI assistant.
                Solve tasks using your coding and language skills.
                In the following cases, suggest python code (in a python coding block) for the user to execute.
                    1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                    2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                Reply "TERMINATE" in the end when everything is done.
                When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                Be careful to avoid using mysql special keywords in mysql code.
                """ + '\n' + self.base_mysql_info + '\n' + python_base_dependency + '\n' + self.quesion_answer_language,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "config_list": self.config_list_gpt4_turbo,
                "request_timeout": request_timeout,
            },
            openai_proxy=self.openai_proxy,
        )
        return base_mysql_assistant

    def get_agent_base_mongodb_assistant(self):
        """ Basic Agent, processing mongodb data source """
        base_mongodb_assistant = TaskSelectorAgent(
            name="base_mysql_assistant",
            system_message="""You are a helpful AI assistant.
                Solve tasks using your coding and language skills.
                In the following cases, suggest python code (in a python coding block) for the user to execute.
                    1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                    2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                Reply "TERMINATE" in the end when everything is done.
                When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                Be careful to avoid using mysql special keywords in mysql code.
                """ + '\n' + self.base_mongodb_info + '\n' + python_base_dependency + '\n' + self.quesion_answer_language,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "config_list": self.config_list_gpt4_turbo,
                "request_timeout": request_timeout,
            },
            openai_proxy=self.openai_proxy,
        )
        return base_mongodb_assistant

    def get_agent_base_assistant(self, use_cache=True):
        """ Basic Agent """
        base_assistant = TaskSelectorAgent(
            name="base_assistant",
            system_message="""You are a helpful AI assistant.
                Divide the questions raised by users into corresponding task types.
                Different tasks have different processing methods.
                Task types are generally divided into the following categories:
                - Report generation task: query data, and finally display the data in the form of charts.
                - base tasks: analyze existing data and draw conclusions about the given problem.

            Reply "TERMINATE" in the end when everything is done.
                 """ + '\n' + self.base_mysql_info,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "functions": [
                    {
                        "name": "task_mysql_echart_code",
                        "description": """chart generation task, The user ask that the data be finally displayed in the form of a chart.If the question does not clearly state that a  chart is to be generated, it does not belong to this task.""",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "qustion_message": {
                                    "type": "string",
                                    "description": "Task content",
                                }
                            },
                            "required": ["qustion_message"],
                        },
                    },
                    {
                        "name": "task_mysql_base",
                        "description": "Processing a task ",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "qustion_message": {
                                    "type": "string",
                                    "description": "Task content",
                                }
                            },
                            "required": ["qustion_message"],
                        },
                    },
                ],
                "config_list": self.config_list_gpt4_turbo,
                "request_timeout": request_timeout,
            },
            openai_proxy=self.openai_proxy,
            use_cache=use_cache,
        )
        return base_assistant

    def get_agent_base_csv_assistant(self):
        """ Basic Agent, processing csv data source """
        base_csv_assistant = TaskSelectorAgent(
            name="base_csv_assistant",
            system_message="""You are a helpful AI assistant.
                      Solve tasks using your coding and language skills.
                      In the following cases, suggest python code (in a python coding block) for the user to execute.
                          1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                          2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                      Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                      When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                      If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                      If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                      When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                      Reply "TERMINATE" in the end when everything is done.
                      When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.

                      The only source data you need to process is csv files.
                IMPORTANT:
                First, determine whether there is a need to display data in the form of charts in the user's question, such as "Please give me a histogram of the top 10 sales of sub-category products." If such a need exists, it is recommended that the function call <task_csv_echart_code>.If it does not exist, directly answer the questions.
                      """ + '\n' + self.base_csv_info + '\n' + python_base_dependency + '\n' + self.quesion_answer_language,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "functions": [
                    {
                        "name": "task_csv_echart_code",
                        "description": "chart generation task, The user requires that the data be finally displayed in the form of a  chart.If the question does not clearly state that a  chart is to be generated, it does not belong to this task.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "qustion_message": {
                                    "type": "string",
                                    "description": "Task content",
                                }
                            },
                            "required": ["qustion_message"],
                        },
                    },
                ],
                "config_list": self.config_list_gpt4_turbo,
                "request_timeout": request_timeout,
            },
            openai_proxy=self.openai_proxy,
        )
        return base_csv_assistant

    def get_agent_base_postgresql_assistant(self):
        """ Basic Agent, processing postgresql data source"""
        base_postgresql_assistant = TaskSelectorAgent(
            name="base_postgresql_assistant",
            system_message="""You are a helpful AI assistant.
                         Solve tasks using your coding and language skills.
                         In the following cases, suggest python code (in a python coding block) for the user to execute.
                             1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                             2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                         Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                         When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                         If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                         If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                         When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                         Reply "TERMINATE" in the end when everything is done.
                         When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.

                         The only source data you need to process is csv files.
                   IMPORTANT:
                   First, determine whether there is a need to display data in the form of charts in the user's question, such as "Please give me a histogram of the top 10 sales of sub-category products." If such a need exists, it is recommended that the function call <task_postgresql_echart_code>.If it does not exist, directly answer the questions.
                         """ + '\n' + self.base_postgresql_info + '\n' + python_base_dependency + '\n' + self.quesion_answer_language,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "functions": [
                    {
                        "name": "task_postgresql_echart_code",
                        "description": "chart generation task, The user requires that the data be finally displayed in the form of a  chart.If the question does not clearly state that a  chart is to be generated, it does not belong to this task.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "qustion_message": {
                                    "type": "string",
                                    "description": "Task content",
                                }
                            },
                            "required": ["qustion_message"],
                        },
                    },
                ],
                "config_list": self.config_list_gpt4_turbo,
                "request_timeout": request_timeout,
            },
            openai_proxy=self.openai_proxy,
        )
        return base_postgresql_assistant

    def get_agent_analyst(self):
        analyst = AssistantAgent(
            name="Analyst",
            system_message='''Analyst. You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.
                              Reply "TERMINATE" in the end when everything is done.
                  ''',
            llm_config=self.gpt4_turbo_config,
            websocket=self.websocket,
            user_name=self.user_name,
            openai_proxy=self.openai_proxy,
        )
        return analyst

    def get_agent_chart_deleter(self):
        chart_deleter = AssistantAgent(
            name="chart_deleter",
            system_message="""Analyze the list of report chart names that the user wants to delete from the questions raised by the user.
                            If you are unsure which report chart the user wants to delete, please let user know.
                            If you can determine which report charts the user wants to delete,
                              the output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
                                [
                                {"report_name": "report_1"},
                                {},
                                {},
                                ].

                            """,
            llm_config=self.gpt4_turbo_config,
            user_name=self.user_name,
            openai_proxy=self.openai_proxy,
        )
        return chart_deleter

    def get_agent_database_describer(self):
        database_describer = AssistantAgent(
            name="database_describer",
            system_message="""data_describer.You are a data describer, describing in one sentence your understanding of the data selected by the user. For example, the data selected by the user includes X tables, and what data is in each table.""",
            llm_config=self.gpt4_turbo_config,
            user_name=self.user_name,
            websocket=self.websocket,
            openai_proxy=self.openai_proxy,
        )
        return database_describer

    def get_agent_bi_proxy(self):
        """ BI proxy """
        bi_proxy = BIProxyAgent(
            name="Executor",
            system_message="""Executor. Executes code written by chart engineers or mysql_engineer through functions and reports the results.
                            Reply "TERMINATE" in the end when everything is done.
            """,
            human_input_mode="NEVER",
            websocket=self.websocket,
            code_execution_config={"last_n_messages": 1, "work_dir": "paper", "use_docker": False},
            default_auto_reply="TERMINATE",
            user_name=self.user_name,
            function_map={"run_mysql_code": BIProxyAgent.run_mysql_code,
                          "bi_run_chart_code": BIProxyAgent.run_chart_code,
                          "delete_chart": BIProxyAgent.delete_chart,
                          "run_img_code": BIProxyAgent.run_img_code},
            outgoing=self.outgoing,
            delay_messages=self.delay_messages,
            incoming=self.incoming,
            openai_proxy=self.openai_proxy,
        )
        return bi_proxy

    def get_agent_planner_user(self, is_log_out=True, report_file_name=None):
        """Disposable conversation initiator, no reply"""
        planner_user = UserProxyAgent(
            name="planner_user",
            max_consecutive_auto_reply=0,  # terminate without auto-reply
            human_input_mode="NEVER",
            websocket=self.websocket,
            is_log_out=is_log_out,
            openai_proxy=self.openai_proxy,
            report_file_name=report_file_name,
        )
        return planner_user

    def get_agent_api_check(self):
        """Disposable conversation initiator, no reply"""
        api_check = CheckAgent(
            name="api_check",
            llm_config=self.gpt4_turbo_config,
            websocket=self.websocket,
            is_log_out=False,
            user_name=self.user_name,
            openai_proxy=self.openai_proxy,
        )
        return api_check

    def get_agent_task_selector(self):
        function_llm_config = {
            "functions": [
                {
                    "name": "task_generate_report",
                    "description": " Process Report Generation Task ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "qustion_message": {
                                "type": "string",
                                "description": "Task content",
                            }
                        },
                        "required": ["qustion_message"],
                    },
                },
                {
                    "name": "task_analysis_data",
                    "description": " Processing a task of analyzing data",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "qustion_message": {
                                "type": "string",
                                "description": "Task content",
                            }
                        },
                        "required": ["qustion_message"],
                    },
                },
                {
                    "name": "task_delete_chart",
                    "description": " Processing a task of delete chart",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "qustion_message": {
                                "type": "string",
                                "description": "Task content",
                            }
                        },
                        "required": ["qustion_message"],
                    },
                },
                {
                    "name": "task_other",
                    "description": "task other",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "qustion_message": {
                                "type": "string",
                                "description": "Task content",
                            }
                        },
                        "required": ["qustion_message"],
                    },
                },
            ],
            "config_list": self.config_list_gpt4_turbo,
            "request_timeout": request_timeout,
        }

        task_selector = TaskSelectorAgent(
            name="task_selector",
            system_message='''Task selector. Divide the questions raised by users into corresponding task types.
                    Different tasks have differentask_selectort processing methods.
                    Task types are generally divided into the following categories:
                      - Report or chart generation task: The user requires that the data be finally displayed in the form of a table or chart.If the question does not clearly state that a report or chart is to be generated, it does not belong to this task.
                      - Data analysis tasks: not report or chart generation tasks, but issues related to data analysis.
                      - Chart deletion task: Delete unnecessary report charts according to user needs.
                      - Other tasks: Not any of the tasks listed above, but other types of tasks.

                Reply "TERMINATE" in the end when everything is done.
                ''',
            user_name=self.user_name,
            llm_config=function_llm_config,
            openai_proxy=self.openai_proxy,

        )
        return task_selector

    def get_agent_task_planner(self):
        """ Make plans for tasks and assign tasks to other agents step by step """
        task_planner = TaskPlannerAgent(
            name="task_planner",
            system_message=''' Planner. Suggest a plan based on the administrator's needs.
            This plan may involve one or more of these individuals.
            - mysql_engineer: Can write MYSQL code but cannot execute the code.
            - chart_presenter: can choose the appropriate chart structure for the data.
            - Executor: can execute the code of MYSQL engineer and chart engineer.
            - Analyst: can analyze the given data and give the user conclusions.

            Before making a plan, clarify which individuals are in this chat and do not assign work to individuals who are not in this chat.
            First, explain the plan. Make it clear which parts are completed by which individuals.
            Reply "TERMINATE" in the end when everything is done.

            ''',
            llm_config=self.gpt4_turbo_config,
            human_input_mode="NEVER",
            websocket=self.websocket,
            is_log_out=False,
            default_auto_reply="TERMINATE",
            user_name=self.user_name,
            # max_consecutive_auto_reply=0,  # terminate without auto-reply
            function_map={"task_generate_report": self.task_generate_report,
                          "task_analysis_data": self.task_analysis_data,
                          "task_delete_chart": self.task_delete_chart,
                          "task_other": self.task_other
                          },
        )
        return task_planner

    def get_agent_data_planner(self):
        data_planner = AssistantAgent(
            name="data_planner",
            system_message="""You are a data planner, and your job is to list what data is needed for the task.
                                    In general, you can obtain the database table structure of the raw data, and the data list that needs to be generated can be formulated.

                                  Give a description of the purpose of each report.
                                  The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
                                  [
                                  {"report_name": "report_1", "description":"description of the report"},
                                  {},
                                  {},
                                  ].
                                  Reply "TERMINATE" in the end when everything is done.
                          """,
            llm_config=self.gpt4_turbo_config,
            websocket=self.websocket,
            is_log_out=self.is_log_out,
            user_name=self.user_name,
            openai_proxy=self.openai_proxy,

        )
        return data_planner

    def get_agent_chart_planner(self):
        chart_planner = AssistantAgent(
            name="chart_planner",
            system_message="""You are the report planner and your task is to prepare data analysis reports based on user questions.
            In general, you can obtain the database table structure of the raw data, and you can formulate the list of report tasks that need to be generated.
            Reports need to be represented from multiple dimensions. To keep them compact, merge them properly.
                    Give a description of the purpose of each report.
                    The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
                    [
                    {"report_name": "report_1", "description":"description of the report"},
                    {},
                    {},
                    ].
                    report_name is generally the name of the chart or report, and supports Chinese. If a name is specified in the question, the given name is used.
                    Reply "TERMINATE" in the end when everything is done.
            """,
            llm_config=self.gpt4_turbo_config,
            websocket=self.websocket,
            is_log_out=self.is_log_out,
            user_name=self.user_name,
            openai_proxy=self.openai_proxy,
        )
        return chart_planner

    def get_agent_python_executor(self, report_file_name=None, is_auto_pilot=False):
        python_executor = PythonProxyAgent(
            name="python_executor",
            system_message="python executor. Execute the python code and report the result.",
            code_execution_config={"last_n_messages": 1, "work_dir": "paper", "use_docker": False},
            human_input_mode="NEVER",
            websocket=self.websocket,
            user_name=self.user_name,
            default_auto_reply="TERMINATE",
            # outgoing=self.outgoing,
            # incoming=self.incoming,
            db_id=self.db_id,
            report_file_name=report_file_name,
            is_auto_pilot=is_auto_pilot
        )
        return python_executor

    def get_agent_csv_echart_assistant(self, use_cache=True):
        """ csv_echart_assistant """
        csv_echart_assistant = AssistantAgent(
            name="csv_echart_assistant",
            system_message="""You are a helpful AI assistant.
                    Solve tasks using your coding and language skills.
                    In the following cases, suggest python code (in a python coding block) for the user to execute.
                        1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                        2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                    Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                    When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                    If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                    If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                    When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                    Reply "TERMINATE" in the end when everything is done.
                    When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                    It involves data queries that truncate the data if it exceeds 1000 rows, or reduce the number of rows by summing and other means.
                    It involves data queries that truncate the data if it exceeds 1000 rows, or reduce the number of rows by summing and other means.
                    It involves data queries that truncate the data if it exceeds 1000 rows, or reduce the number of rows by summing and other means.
                    It involves data queries that truncate the data if it exceeds 1000 rows, or reduce the number of rows by summing and other means.
                    """ + '\n' + self.base_csv_info + '\n' + python_base_dependency + '\n' + CSV_ECHART_TIPS_MESS,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config=self.gpt4_turbo_config,
            openai_proxy=self.openai_proxy,
            use_cache=use_cache,

        )
        return csv_echart_assistant

    def get_agent_mysql_echart_assistant(self, use_cache=True, report_file_name=None):
        """mysql_echart_assistant"""
        mysql_echart_assistant = AssistantAgent(
            name="mysql_echart_assistant",
            system_message="""You are a helpful AI assistant.
                                          Solve tasks using your coding and language skills.
                                          In the following cases, suggest python code (in a python coding block) for the user to execute.
                                              1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                                              2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                                          Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                                          When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                                          If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                                          If you need to use %Y-%M to query the date or timestamp, please use %Y-%M. You cannot use %%Y-%%M.(For example you should use SELECT * FROM your_table WHERE DATE_FORMAT(your_date_column, '%Y-%M') = '2024-February'; instead of SELECT * FROM your_table WHERE DATE_FORMAT(your_date_column, '%%Y-%%M') = '2024-%%M';)
                                          If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                                          When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                                          Reply "TERMINATE" in the end when everything is done.
                                          When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                                          Be careful to avoid using mysql special keywords in mysql code.
                                          One SQL query result is limited to 20 items.
                                          """ + '\n' + self.base_mysql_info + '\n' + python_base_dependency + '\n' + MYSQL_ECHART_TIPS_MESS,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config=self.gpt4_turbo_config,
            openai_proxy=self.openai_proxy,
            use_cache=use_cache,
            report_file_name=report_file_name,

        )
        return mysql_echart_assistant

    def get_agent_mysql_echart_assistant35(self, use_cache=True, report_file_name=None):
        """mysql_echart_assistant"""
        mysql_echart_assistant = AssistantAgent(
            name="mysql_echart_assistant",
            system_message="""You are a helpful AI assistant.
                                            Solve tasks using your coding and language skills.
                                            In the following cases, suggest python code (in a python coding block) for the user to execute.
                                                1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                                                2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                                            Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                                            When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                                            If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                                            If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                                            When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                                            Reply "TERMINATE" in the end when everything is done.
                                            When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                                            Be careful to avoid using mysql special keywords in mysql code.
                                            One SQL query result is limited to 20 items.
                                            """ + '\n' + self.base_mysql_info + '\n' + python_base_dependency + '\n' + MYSQL_ECHART_TIPS_MESS,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config=self.gpt35_turbo_config,
            openai_proxy=self.openai_proxy,
            use_cache=use_cache,
            report_file_name=report_file_name,

        )
        return mysql_echart_assistant

    def get_agent_postgresql_echart_assistant(self, use_cache=True):
        """mysql_echart_assistant"""
        postgresql_echart_assistant = AssistantAgent(
            name="postgresql_echart_assistant",
            system_message="""You are a helpful AI assistant.
                                            Solve tasks using your coding and language skills.
                                            In the following cases, suggest python code (in a python coding block) for the user to execute.
                                                1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                                                2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                                            Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                                            When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                                            If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                                            If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                                            When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                                            Reply "TERMINATE" in the end when everything is done.
                                            When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                                            """ + '\n' + self.base_postgresql_info + '\n' + python_base_dependency + '\n' + POSTGRESQL_ECHART_TIPS_MESS,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config=self.gpt4_turbo_config,
            openai_proxy=self.openai_proxy,
            use_cache=use_cache,

        )
        return postgresql_echart_assistant

    def get_agent_starrocks_echart_assistant(self, use_cache=True, report_file_name=None):
        """starrocks_echart_assistant"""
        starrocks_echart_assistant = AssistantAgent(
            name="starrocks_echart_assistant",
            system_message="""You are a helpful AI assistant.
                                             Solve tasks using your coding and language skills.
                                             In the following cases, suggest python code (in a python coding block) for the user to execute.
                                                 1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                                                 2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                                             Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                                             When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                                             If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                                             If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                                             When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                                             Reply "TERMINATE" in the end when everything is done.
                                             When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                                             Don't generate html files.
                                             """ + '\n' + self.base_starrocks_info + '\n' + python_base_dependency + '\n' + MYSQL_ECHART_TIPS_MESS,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config=self.gpt4_turbo_config,
            openai_proxy=self.openai_proxy,
            use_cache=use_cache,
            report_file_name=report_file_name,

        )
        return starrocks_echart_assistant

    def get_agent_mongodb_echart_assistant(self, use_cache=True, report_file_name=None):
        """mongodb_echart_assistant # new db """
        mongodb_echart_assistant = AssistantAgent(
            name="mongodb_echart_assistant",
            system_message="""You are a helpful AI assistant.
                                             Solve tasks using your coding and language skills.
                                             In the following cases, suggest python code (in a python coding block) for the user to execute.
                                                 1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                                                 2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                                             Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                                             When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                                             If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                                             If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                                             When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                                             The database name needs to be replaced in the database connection string
                                             Reply "TERMINATE" in the end when everything is done.
                                             When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                                             """ + '\n' + self.base_mongodb_info + '\n' + python_base_dependency + '\n' + MONGODB_ECHART_TIPS_MESS,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config=self.gpt4_turbo_config,
            openai_proxy=self.openai_proxy,
            use_cache=use_cache,
            report_file_name=report_file_name,

        )
        return mongodb_echart_assistant

    def get_agent_mysql_matplotlib_assistant(self):
        mysql_matplotlib_assistant = AssistantAgent(
            name="mysql_matplotlib_assistant",
            system_message="""You are a helpful AI assistant.
                                    Solve tasks using your coding and language skills.
                                    In the following cases, suggest python code (in a python coding block) for the user to execute.
                                        1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                                        2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                                    Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                                    When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                                    If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                                    If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                                    When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                                    The database name needs to be replaced with the actual database name in the database connection string, "your_dbname" in the database connection string  "mongodb://your_host:your_port/your_dbname"  must be replaced with the actual database name: q
                                    Reply "TERMINATE" in the end when everything is done.
                                    When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                                    """ + '\n' + self.base_mysql_info + '\n' + python_base_dependency + '\n' + MYSQL_MATPLOTLIB_TIPS_MESS,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config=self.gpt4_turbo_config,
            openai_proxy=self.openai_proxy,

        )
        return mysql_matplotlib_assistant

    def get_agent_mongodb_matplotlib_assistant(self):
        mysql_matplotlib_assistant = AssistantAgent(
            name="mongodb_matplotlib_assistant",
            system_message="""You are a helpful AI assistant.
                                    Solve tasks using your coding and language skills.
                                    In the following cases, suggest python code (in a python coding block) for the user to execute.
                                        1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                                        2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                                    Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                                    When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                                    If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                                    If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                                    When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                                    Reply "TERMINATE" in the end when everything is done.
                                    When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                                    """ + '\n' + self.base_mysql_info + '\n' + python_base_dependency + '\n' + MYSQL_MATPLOTLIB_TIPS_MESS,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config=self.gpt4_turbo_config,
            openai_proxy=self.openai_proxy,

        )
        return mysql_matplotlib_assistant

    def get_agent_data_checker_assistant(self):
        """ data_checker """
        data_checker_assistant = CheckAgent(
            name="data_checker_assistant",
            system_message="""
            You are a data annotation checker, and your task is to understand the meanings of various tables and fields in the database. If the annotation is easy to understand, it means it has passed the inspection, otherwise it does not pass.
            You must provide scores for the meaning of each table and field.
            -  is_pass (meaning passed or not): A score of 0 indicates that the check was not passed, while a score of 1 indicates that the check was passed.

            Do not modify other attributes in the input json, just add or modify the is_pass attribute.

             The following is an input case:
             ```
            {"table_name":"order_details2","table_comment":"order details 1","field_desc":[{"name":"id","comment":"Order ID"},{"name":"amount","comment":"sales amount"}]}

             ```

             The output must contain a JSON instance.
                  JSON instances are distinguished by the  ``` symbol at the beginning and end.
                  The output should be formatted as a JSON instance that conforms to the JSON schema below,

               ```
            {"table_name":"order_details2","table_comment":"order details 1","is_pass":1,"field_desc":[{"name":"id","comment":"Order ID","is_pass":1},{"name":"amount","comment":"sales amount","is_pass":1}]}
               ```

            """,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "config_list": self.config_list_gpt4_turbo,
                "request_timeout": request_timeout,
            },
            openai_proxy=self.openai_proxy,

        )
        return data_checker_assistant

    def get_agent_data_checker_assistant1108(self):
        """ data_checker """
        data_checker_assistant = AssistantAgent(
            name="data_checker_assistant",
            system_message="""
             You are a data annotation checker, and your task is to understand the meanings of various tables and fields in the database. If the annotation is easy to understand, it means it has passed the inspection, otherwise it does not pass.
             You must provide scores for the meaning of each table and field.
             -  is_pass (meaning passed or not): A score of 0 indicates that the check was not passed, while a score of 1 indicates that the check was passed.

             Do not modify other attributes in the input json, just add or modify the is_pass attribute.

              The following is an input case:
              ```
              {"databases_desc":"this is a mysql order database.","table_desc":[{"table_name":"order_details2","table_comment":"order details 2","field_desc":[{"name":"id","comment":"Order ID"},{"name":"amount","comment":"sales amount"}]}]}
              ```

              The output must contain a JSON instance.
                   JSON instances are distinguished by the  ``` symbol at the beginning and end.
                   The output should be formatted as a JSON instance that conforms to the JSON schema below,

                ```
                {"databases_desc":"this is a mysql order database.","is_pass":1,"table_desc":[{"table_name":"order_details2","table_comment":"orderdetails 2","is_pass":1,"field_desc":[{"name":"id","comment":"Order ID","is_pass":1},{"name":"amount","comment":"sales amount","is_pass":1}]}]}
                ```

             """,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config=self.gpt4_turbo_config,
            openai_proxy=self.openai_proxy,

        )
        return data_checker_assistant

    def get_agent_GroupChatManager(self, agents):
        groupchat = GroupChat(
            agents=agents,
            messages=[],
            max_round=10,
        )
        manager = GroupChatManager(groupchat=groupchat, llm_config=self.gpt4_turbo_config,
                                   websocket=self.websocket)
        return manager

    def set_socket(self, websocket):
        self.websocket = websocket

    def set_language_mode(self, language_mode):
        self.language_mode = language_mode

        if self.language_mode == language_english:
            self.error_message_timeout = 'Sorry, this AI-GPT interface call timed out, please try again.'
            self.question_ask = ' This is my question: '
            self.quesion_answer_language = 'Answer questions in English.'
            self.data_analysis_error = 'Failed to analyze data, please check whether the relevant data is sufficient.'

        elif self.language_mode == language_chinese:
            self.error_message_timeout = "十分抱歉，本次AI-GPT接口调用超时，请再次重试"
            self.question_ask = ' 以下是我的问题，请用中文回答: '
            self.quesion_answer_language = '用中文回答问题.'
            self.data_analysis_error = '分析数据失败，请检查相关数据是否充分'

        elif self.language_mode == language_japanese:
            self.error_message_timeout = "申し訳ありませんが、今回のAI-GPTインターフェース呼び出しがタイムアウトしました。もう一度お試しください。"
            self.question_ask = ' これが私の質問です。: '
            self.quesion_answer_language = '日本語で質問に答える。'
            self.data_analysis_error = 'データの分析に失敗しました。関連データが十分かどうかを確認してください。'

    def set_base_csv_info(self, db_info):
        csv_content = []
        if db_info.get('table_desc'):
            for tb in db_info.get('table_desc'):
                table_name = tb.get('table_name')
                table_comment = tb.get('table_comment')
                print("table_name: ", table_name)
                print("table_comment: ", table_comment)

                tb_desc = {
                    "file_comment": table_comment,
                    "file_path": CONFIG.csv_file_path + table_name,
                }
                print('tb_desc : ', tb_desc)
                csv_content.append(tb_desc)

            self.base_csv_info = """This is a csv data path list:""" + '\n' + str(csv_content)
            print("base_csv_info: ", self.base_csv_info)
        else:
            return False

    async def task_generate_report(self, qustion_message):
        """ Task type 1: Call BI and generate reports """
        try:
            error_times = 0
            report_demand_list = []
            for i in range(max_retry_times):
                try:
                    answer_contents = []
                    planner_user = self.get_agent_planner_user()
                    chart_planner = self.get_agent_chart_planner()

                    await planner_user.initiate_chat(
                        chart_planner,
                        message=self.base_message + '\n' + " This is my question: " + '\n' + str(qustion_message),
                    )

                    answer_message = planner_user.last_message()["content"]
                    # print("answer_message: ", answer_message)

                    match = re.search(
                        r"\[.*\]", answer_message.strip(), re.MULTILINE | re.IGNORECASE | re.DOTALL
                    )
                    json_str = ""
                    if match:
                        json_str = match.group()
                    print("json_str : ", json_str)
                    report_demand_list = json.loads(json_str)
                    print("report_demand_list : ", report_demand_list)

                    logger.info(
                        "from user:[{}".format(self.user_name) + "] , " + "，report_demand_list" + str(
                            report_demand_list))
                    break
                except Exception as e:
                    traceback.print_exc()
                    logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))
                    error_times = error_times + 1

            if error_times >= max_retry_times:
                return self.error_message_timeout

            for report_task in report_demand_list:

                error_times = 0
                for i in range(max_retry_times):
                    try:
                        mysql_engineer = self.get_agent_mysql_engineer()
                        bi_proxy = self.get_agent_bi_proxy()
                        chart_presenter = self.get_agent_chart_presenter()

                        groupchat = GroupChat(
                            agents=[mysql_engineer, bi_proxy, chart_presenter],
                            messages=[],
                            max_round=10,
                        )
                        manager = GroupChatManager(groupchat=groupchat, llm_config=self.gpt4_turbo_config,
                                                   websocket=self.websocket)

                        q_str = "我需要一个命名为 " + '\n' + report_task["report_name"] + '\n' + "的图表，" + report_task[
                            "description"]
                        await planner_user.initiate_chat(
                            manager,
                            message='This is database related information: ' + '\n' + self.base_message + '\n' + " This is my question: " + '\n' + str(
                                q_str),
                        )

                        answer_message = manager._oai_messages[bi_proxy]
                        is_done = False
                        for answer_mess in answer_message:
                            if answer_mess['role']:
                                if answer_mess['role'] == 'function':
                                    answer_contents.append(answer_mess)
                                    print("answer_mess: ", answer_mess)
                                    is_done = True
                        if is_done:
                            break
                    except Exception as e:
                        # 使用 traceback 打印详细信息
                        traceback.print_exc()
                        logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))
                        error_times = error_times + 1

                if error_times == max_retry_times:
                    return self.error_message_timeout

            if len(answer_contents) > 0:
                answer_contents.append(answer_message)

            max_num = len(answer_contents)
            for i in range(max_num):

                message = [
                    {
                        "role": "system",
                        "content": str(answer_contents),
                    }
                ]

                num_tokens = num_tokens_from_messages(message, model='gpt-4')
                print("num_tokens : ", num_tokens)
                if num_tokens < 20000:
                    error_times = 0  # 失败次数
                    for i in range(max_retry_times):
                        try:
                            planner_user = self.get_agent_planner_user()
                            analyst = self.get_agent_analyst()

                            # 分析哪些报表成功生成了
                            await planner_user.initiate_chat(
                                analyst,
                                message=str(
                                    answer_contents) + '\n' + " 以下是我的问题，请用中文回答: " + '\n' + " 1,本次生成哪些报表？简单描述一下各报表 "
                                + '\n' + " 以下是一个回答案例: " + '\n' +
                                """总结:
                                        -- Monthly Sales Summary Q1 2019: 同名图表历史已生成过，此次不再生成,若要重新生成图表，请先删除已有同名报表。2019年第一季度的月度销售总结。它包括了每个月的总销售额、利润和订单数量的详细信息.
                                        -- Summary Q1 2018: 生成成功。2018年第一季度的月度销售总结。它包括了每个月的总销售额、利润和订单数量的详细信息.
                                    """,
                            )

                            answer_message = planner_user.last_message()["content"]
                            # answer_message = planner_user.chat_messages[-1]["content"]
                            print("answer_message: ", answer_message)
                            answer_message = answer_message.replace("TERMINATE", "")
                            return answer_message

                        except Exception as e:
                            traceback.print_exc()
                            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                            error_times = error_times + 1

                    if error_times == max_retry_times:
                        return self.error_message_timeout

                else:
                    answer_contents.pop(0)
        except Exception as e:
            # print(e)
            # 使用 traceback 打印详细信息
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))
        return "报表生成失败，请检查相关数据是否充分。"

    async def task_generate_report1108(self, qustion_message):
        """ 任务类型1:  调用 bi, 生成报表 """
        try:
            print('运行 【task_generate_report】函数')
            error_times = 0  # 失败次数
            report_demand_list = []
            for i in range(max_retry_times):
                try:
                    answer_contents = []
                    logger.info("from user:[{}".format(self.user_name) + "] , " + "运行 【task_generate_report】函数")

                    planner_user = self.get_agent_planner_user()
                    chart_planner = self.get_agent_chart_planner()

                    # 1,根据任务生成报表
                    await planner_user.initiate_chat(
                        chart_planner,
                        message=self.base_message + '\n' + " This is my question: " + '\n' + str(qustion_message),
                    )

                    answer_message = planner_user.last_message()["content"]
                    # print("answer_message: ", answer_message)

                    match = re.search(
                        r"\[.*\]", answer_message.strip(), re.MULTILINE | re.IGNORECASE | re.DOTALL
                    )
                    json_str = ""
                    if match:
                        json_str = match.group()
                    print("json_str : ", json_str)
                    report_demand_list = json.loads(json_str)
                    print("report_demand_list : ", report_demand_list)

                    logger.info(
                        "from user:[{}".format(self.user_name) + "] , " + "，report_demand_list" + str(
                            report_demand_list))
                    break
                except Exception as e:
                    traceback.print_exc()
                    logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))
                    error_times = error_times + 1

            if error_times >= max_retry_times:
                return self.error_message_timeout
                # return "十分抱歉，本次AI-GPT接口调用超时，请再次重试"

            for report_task in report_demand_list:
                print("【开始处理 report_task 】: ", report_task)
                logger.info(
                    "from user:[{}".format(self.user_name) + "] , " + "，【开始处理 report_task 】:" + str(report_task))

                # 判断同名历史图表是否存在
                # if report_task["report_name"] in table_names:
                #     print("图表已生成，跳过")
                #     answer_content = report_task["report_name"] + " 同名图表历史已生成过，此次不再生成。"
                #     answer_contents.append(answer_content)
                #     continue

                error_times = 0  # 失败次数
                for i in range(max_retry_times):
                    try:
                        mysql_engineer = self.get_agent_mysql_engineer()
                        bi_proxy = self.get_agent_bi_proxy()
                        chart_presenter = self.get_agent_chart_presenter()
                        """ 创建对话，解决问题"""
                        # 2,根据报表任务，遍历创建
                        groupchat = GroupChat(
                            agents=[mysql_engineer, bi_proxy, chart_presenter],
                            messages=[],
                            max_round=10,
                        )
                        manager = GroupChatManager(groupchat=groupchat, llm_config=self.gpt4_turbo_config,
                                                   websocket=self.websocket)

                        q_str = "我需要一个命名为 " + '\n' + report_task["report_name"] + '\n' + "的图表，" + report_task[
                            "description"]
                        await planner_user.initiate_chat(
                            manager,
                            message='This is database related information: ' + '\n' + self.base_message + '\n' + " This is my question: " + '\n' + str(
                                q_str),
                        )

                        answer_message = manager._oai_messages[bi_proxy]
                        is_done = False
                        for answer_mess in answer_message:
                            if answer_mess['role']:
                                if answer_mess['role'] == 'function':
                                    answer_contents.append(answer_mess)
                                    print("answer_mess: ", answer_mess)
                                    is_done = True
                        if is_done:
                            break
                    except Exception as e:
                        # 使用 traceback 打印详细信息
                        traceback.print_exc()
                        logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))
                        error_times = error_times + 1

                if error_times == max_retry_times:
                    return self.error_message_timeout
                    # return "十分抱歉，本次AI-GPT接口调用超时，请再次重试"

            if len(answer_contents) > 0:
                answer_contents.append(answer_message)

            max_num = len(answer_contents)
            for i in range(max_num):

                message = [
                    {
                        "role": "system",
                        "content": str(answer_contents),
                    }
                ]

                num_tokens = num_tokens_from_messages(message, model='gpt-4')
                print("num_tokens : ", num_tokens)
                if num_tokens < 20000:
                    error_times = 0  # 失败次数
                    for i in range(max_retry_times):
                        try:
                            planner_user = self.get_agent_planner_user()
                            analyst = self.get_agent_analyst()

                            # 分析哪些报表成功生成了
                            await planner_user.initiate_chat(
                                analyst,
                                message=str(
                                    answer_contents) + '\n' + " 以下是我的问题，请用中文回答: " + '\n' + " 1,本次生成哪些报表？简单描述一下各报表 "
                                + '\n' + " 以下是一个回答案例: " + '\n' +
                                """总结:
                                        -- Monthly Sales Summary Q1 2019: 同名图表历史已生成过，此次不再生成,若要重新生成图表，请先删除已有同名报表。2019年第一季度的月度销售总结。它包括了每个月的总销售额、利润和订单数量的详细信息.
                                        -- Summary Q1 2018: 生成成功。2018年第一季度的月度销售总结。它包括了每个月的总销售额、利润和订单数量的详细信息.
                                    """,
                            )

                            answer_message = planner_user.last_message()["content"]
                            # answer_message = planner_user.chat_messages[-1]["content"]
                            print("answer_message: ", answer_message)
                            answer_message = answer_message.replace("TERMINATE", "")
                            return answer_message

                        except Exception as e:
                            traceback.print_exc()
                            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                            error_times = error_times + 1

                    if error_times == max_retry_times:
                        return self.error_message_timeout
                        # return "十分抱歉，本次AI-GPT接口调用超时，请再次重试"

                else:
                    # 去除最上层数据
                    answer_contents.pop(0)
        except Exception as e:
            # print(e)
            # 使用 traceback 打印详细信息
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))
        return "报表生成失败，请检查相关数据是否充分。"

    async def task_analysis_data(self, qustion_message):
        """ 任务类型2:  数据分析 """
        try:
            base_content = []
            report_demand_list = []
            json_str = ""
            error_times = 0  # 失败次数
            # max_retry_times = 3 # 最大重试次数
            for i in range(max_retry_times):
                try:
                    planner_user = self.get_agent_planner_user()
                    data_planner = self.get_agent_data_planner()

                    # 1,根据任务生成报表
                    await planner_user.initiate_chat(
                        data_planner,
                        message=self.base_message + '\n' + " This is my question: " + '\n' + str(qustion_message),
                    )

                    answer_message = planner_user.last_message()["content"]
                    # print("answer_message: ", answer_message)

                    match = re.search(
                        r"\[.*\]", answer_message.strip(), re.MULTILINE | re.IGNORECASE | re.DOTALL
                    )

                    if match:
                        json_str = match.group()
                    print("json_str : ", json_str)
                    report_demand_list = json.loads(json_str)
                    print("report_demand_list : ", report_demand_list)
                    break
                except Exception as e:
                    traceback.print_exc()
                    logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                    error_times = error_times + 1

            if error_times >= max_retry_times:
                return self.error_message_timeout
                # return "十分抱歉，本次AI-GPT接口调用超时，请再次重试"

            logger.info(
                "from user:[{}".format(self.user_name) + "] , " + "，report_demand_list" + str(report_demand_list))

            # 2,根据报表任务，遍历创建

            error_times = 0  # 失败次数
            for i in range(max_retry_times):
                try:
                    # 基础信息
                    # base_content = []
                    for report_task in report_demand_list:
                        print("【开始处理 report_task 】: ", report_task)
                        planner_user = self.get_agent_planner_user()
                        mysql_engineer = self.get_agent_mysql_engineer()
                        bi_proxy = self.get_agent_bi_proxy()

                        groupchat = GroupChat(
                            agents=[mysql_engineer, bi_proxy, planner_user],
                            messages=[],
                            max_round=10,
                        )
                        manager = GroupChatManager(groupchat=groupchat, llm_config=self.gpt4_turbo_config,
                                                   websocket=self.websocket)

                        logger.info(
                            "from user:[{}".format(self.user_name) + "] , " + "，【开始处理 report_task 】:" + str(
                                report_task))

                        # if report_task["report_name"] in table_names:
                        #     print("图表已生成，跳过")
                        #     continue
                        """ 创建对话，解决问题"""
                        q_str = "i want a report, " + report_task["report_name"] + ":" + report_task["description"]
                        await planner_user.initiate_chat(
                            manager,
                            message=self.base_message + '\n' + " 我需要获得以下数据: " + '\n' + str(q_str),
                        )

                        # answer_message = planner_user.last_message()["content"]
                        answer_message = manager._oai_messages[self.bi_proxy]

                        for answer_mess in answer_message:
                            if answer_mess['role']:
                                if answer_mess['role'] == 'function':
                                    base_content.append(answer_mess)
                                    print("answer_mess: ", answer_mess)

                    if len(base_content) > 0:
                        base_content.append(report_demand_list)
                        break
                except Exception as e:
                    traceback.print_exc()
                    logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                    error_times = error_times + 1

            if error_times == max_retry_times:
                return self.error_message_timeout
                # return "十分抱歉，本次AI-GPT接口调用超时，请再次重试"

            print("数据获取完毕")
            # 判断token是否超长

            max_num = len(base_content)
            print("max_num: ", max_num)

            for i in range(max_num):
                message = [
                    {
                        "role": "system",
                        "content": str(base_content),
                    }
                ]
                num_tokens = num_tokens_from_messages(message, model='gpt-4')
                print("num_tokens : ", num_tokens)
                if num_tokens < 7000:
                    error_times = 0  # 失败次数
                    for i in range(max_retry_times):
                        try:
                            planner_user = self.get_agent_planner_user()
                            analyst = self.get_agent_analyst()

                            # 1,开始分析
                            await planner_user.initiate_chat(
                                analyst,
                                message=str(base_content) + '\n' + " 以下是我的问题，请用中文回答: " + '\n' + str(qustion_message),
                            )

                            answer_message = planner_user.last_message()["content"]
                            # answer_message = planner_user.chat_messages[-1]["content"]
                            print("answer_message: ", answer_message)
                            return answer_message

                        except Exception as e:
                            traceback.print_exc()
                            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                            error_times = error_times + 1

                    if error_times == max_retry_times:
                        return self.error_message_timeout
                        # return "十分抱歉，本次AI-GPT接口调用超时，请再次重试"
                else:
                    # 去除最上层数据
                    base_content.pop(0)

            return "十分抱歉，无法回答您的问题，请检查相关数据是否充分。"
        except Exception as e:
            print(e)
            # 使用 traceback 打印详细信息
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))

        return "分析数据失败，请检查相关数据是否充分。"

    async def task_delete_chart(self, qustion_message):
        """ 任务类型3:  删除图表 """
        print('运行 【task_delete_chart】函数')
        logger.info("from user:[{}".format(self.user_name) + "] , " + "运行 【task_delete_chart】函数")

        planner_user = self.get_agent_planner_user()
        chart_deleter = self.get_agent_chart_deleter()
        bi_proxy = self.get_agent_bi_proxy()

        # 1，拿取分析数据
        re_bool, data_str = await bi_proxy.ask_data_code(qustion_message)

        if not re_bool:
            return "获取数据失败，请检查相关数据是否充分。"

        table_names = []
        for table_str in data_str:
            # print("table_str: ", table_str)
            table_name = table_str.get("table_name")
            # print("table_name: ", table_name)
            table_names.append(table_name)

        content = "This is a list of existing report chart names: " + str(table_names)
        print(content)
        logger.info(
            "from user:[{}".format(self.user_name) + "] , " + "This is a list of existing report chart names: " + str(
                table_names))
        try:
            # 1,根据任务生成报表
            await planner_user.initiate_chat(
                chart_deleter,
                message=content + '\n' + " This is my question: " + '\n' + str(qustion_message),
            )
        except Exception as e:
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
            return self.error_message_timeout
            # return "十分抱歉，本次AI-GPT接口调用超时，请再次重试"

        try:
            answer_message = planner_user.last_message()["content"]
            print("answer_message: ", answer_message)
            logger.info("from user:[{}".format(self.user_name) + "] , " + "answer_message: " + str(answer_message))

            match = re.search(
                r"\[.*\]", answer_message.strip(), re.MULTILINE | re.IGNORECASE | re.DOTALL
            )
            json_str = ""
            if match:
                json_str = match.group()
            print("json_str : ", json_str)
            report_demand_list = json.loads(json_str)
            print("delete_tables_list : ", report_demand_list)

            delete_table_names = []
            for table_str in report_demand_list:
                table_name = table_str.get("report_name")
                delete_table_names.append(table_name)

            if len(delete_table_names) > 0:
                answer_message = await bi_proxy.delete_chart(delete_table_names)
                return answer_message

        except Exception as e:
            # print(e)
            # 使用 traceback 打印详细信息
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
        # return "Failed to delete chart. Please check if the provided chart list format is correct and if the chart name exists."
        return "删除图表失败。 请检查提供的图表列表格式是否正确以及图表名称是否存在。"

    async def task_chart_img(self, qustion_message):
        """ 任务类型:  使用 matplotlib 生成图表图片，暂时停用 """
        try:

            base_content = []
            base_mess = []
            report_demand_list = []
            json_str = ""
            error_times = 0  # 失败次数
            for i in range(max_retry_times):
                try:
                    mysql_matplotlib_assistant = self.get_agent_mysql_matplotlib_assistant()
                    python_executor = self.get_agent_python_executor()

                    """ 创建对话，解决问题"""
                    await python_executor.initiate_chat(
                        mysql_matplotlib_assistant,
                        message=self.base_message + '\n' + self.question_ask + '\n' + str(
                            qustion_message),
                    )

                    # answer_message = planner_user.last_message()["content"]
                    # answer_message = manager._oai_messages[user_proxy_1030_img]
                    # print("answer_message_base_assistant_1030_img : ",
                    #       base_assistant_1030_img.chat_messages[user_proxy_1030_img])

                    answer_message = mysql_matplotlib_assistant.chat_messages[python_executor]

                    for answer_mess in answer_message:
                        # print("answer_mess :", answer_mess)
                        if answer_mess['content']:
                            if str(answer_mess['content']).__contains__('execution succeeded'):

                                print("answer_mess: ", answer_mess)
                                match = re.search(
                                    r"\[.*\]", str(answer_mess).strip(), re.MULTILINE | re.IGNORECASE | re.DOTALL
                                )
                                if match:
                                    json_str = match.group()
                                print("json_str : ", json_str)
                                report_demand_list = json.loads(json_str)
                                for jstr in report_demand_list:
                                    if str(jstr).__contains__('img_name') and str(jstr).__contains__('description'):
                                        base_content.append(jstr)

                    print("base_content: ", base_content)

                    if len(base_content) > 0:
                        base_mess.append(answer_mess)
                        break
                except Exception as e:
                    traceback.print_exc()
                    logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                    error_times = error_times + 1

            if error_times >= max_retry_times:
                return self.error_message_timeout
                # return "十分抱歉，本次AI-GPT接口调用超时，请再次重试"

            logger.info(
                "from user:[{}".format(self.user_name) + "] , " + "，report_demand_list" + str(report_demand_list))

            # 调用接口生成图片
            for img_str in base_content:
                img_name = img_str.get('description')
                img_url = 'http://cn.deep-thought.io/show_img/' + img_str.get('img_name')
                print("img_url : ", img_url)
                # img_url = "http://cn.deep-thought.io/show_img/top10_subcategory.jpg"
                re_str = await self.bi_proxy.run_img_code(img_url, img_name)
                base_mess.append(re_str)

            error_times = 0  # 失败次数
            for i in range(max_retry_times):
                try:
                    planner_user = self.get_agent_planner_user()
                    analyst = self.get_agent_analyst()
                    question_supplement = 'Please make an analysis and summary in English, including which charts were generated, and briefly introduce the contents of these charts.'
                    if self.language_mode == language_chinese:
                        question_supplement = " 请用中文做一下分析总结，内容包括哪些图表生成了，简单介绍一下这些图表的内容。"

                    # 1,开始分析
                    await planner_user.initiate_chat(
                        analyst,
                        message=str(base_mess) + '\n' + str(
                            qustion_message) + '\n' + self.question_ask + '\n' + question_supplement,

                    )

                    answer_message = planner_user.last_message()["content"]
                    # answer_message = planner_user.chat_messages[-1]["content"]
                    print("answer_message: ", answer_message)
                    return answer_message

                except Exception as e:
                    traceback.print_exc()
                    logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                    error_times = error_times + 1

            if error_times == max_retry_times:
                # return "十分抱歉，本次AI-GPT接口调用超时，请再次重试"
                return self.error_message_timeout

            if self.language_mode == language_chinese:
                return "十分抱歉，无法回答您的问题，请检查相关数据是否充分。"
            else:
                return 'Sorry, we cannot answer your question. Please check whether the relevant data is sufficient.'
        except Exception as e:
            print(e)
            # 使用 traceback 打印详细信息
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))

        if self.language_mode == language_chinese:
            return "分析数据失败，请检查相关数据是否充分。"
        else:
            return 'Failed to analyze data, please check whether the relevant data is sufficient.'
