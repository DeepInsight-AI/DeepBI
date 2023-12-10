import traceback
from ai.backend.util.write_log import logger
from ai.backend.base_config import CONFIG
import re
import ast
from ai.backend.aidb import AIDB
from ai.agents.agentchat import HumanProxyAgent, TaskSelectorAgent, Questioner, AssistantAgent
from jinja2 import Template
from pathlib import Path
import time


class Autopilot(AIDB):
    def get_agent_user_proxy(self):
        """ Human Proxy  """
        user_proxy = HumanProxyAgent(
            name="Admin",
            system_message="A human admin. Interact with the planner to discuss the plan. ",
            code_execution_config={"last_n_messages": 1, "work_dir": "paper"},
            human_input_mode="NEVER",
            websocket=self.websocket,
            user_name=self.user_name,
            function_map={
                "task_generate_echart": self.task_generate_echart,
                "task_base": self.task_base,
            },
            outgoing=self.outgoing,
        )
        return user_proxy

    def get_agent_select_analysis_assistant(self):
        """select_analysis_assistant"""
        select_analysis_assistant = TaskSelectorAgent(
            name="select_analysis_assistant",
            system_message="""You are a helpful AI assistant.
                       Divide the questions raised by users into corresponding task types.
                       Different tasks have different processing methods.
                       Task types are generally divided into the following categories:
                       - Report generation task: query data, and finally display the data in the form of charts.
                       - base tasks: analyze existing data and draw conclusions about the given problem.

                   Reply "TERMINATE" in the end when everything is done.
                        """ + '\n' + self.agent_instance_util.base_mysql_info,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "functions": [
                    {
                        "name": "task_generate_echart",
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
                        "name": "task_base",
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
                "config_list": self.agent_instance_util.config_list_gpt4_turbo,
                "request_timeout": CONFIG.request_timeout,
            },
            openai_proxy=self.agent_instance_util.openai_proxy,
        )
        return select_analysis_assistant

    async def start_chatgroup(self, q_str):
        user_proxy = self.get_agent_user_proxy()
        base_mysql_assistant = self.get_agent_select_analysis_assistant()

        await user_proxy.initiate_chat(
            base_mysql_assistant,
            message=self.agent_instance_util.base_message + '\n' + self.question_ask + '\n' + str(q_str),
        )

    async def task_base(self, qustion_message):
        """ Task type: base question """
        return self.error_no_report_question

    async def task_generate_echart(self, qustion_message):
        return self.qustion_message

    def get_agent_questioner(self):
        """ Questioner  """
        questioner = Questioner(
            name="questioner",
            human_input_mode="NEVER",
            max_consecutive_auto_reply=2,
            llm_config=self.agent_instance_util.gpt4_turbo_config,
            default_auto_reply="请继续补充分析维度，不要重复.",
            websocket=self.websocket,
            openai_proxy=self.agent_instance_util.openai_proxy,
            log_list=self.log_list,
        )

        return questioner

    def get_agent_ai_analyst(self):
        """ ai_analyst """
        ai_analyst = AssistantAgent(
            name="ai_data_analyst",
            system_message="""You are a helpful AI data analysis.
            Please tell me from which dimensions you need to analyze and help me make a report plan.
            Reports need to be represented from multiple dimensions. To keep them compact, merge them properly.
            Give a description of the purpose of each report.
        The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
        [
        {“report_name”: “report_1”, “description”:”description of the report”;},
        {},
        {},
        ].
        Reply "TERMINATE" in the end when everything is done.
            """ + '\n' + '请用中文回答',
            llm_config=self.agent_instance_util.gpt4_turbo_config,
            websocket=self.websocket,
            openai_proxy=self.agent_instance_util.openai_proxy,
            log_list=self.log_list,
        )
        return ai_analyst

    def get_agent_analyst(self, report_file_name=None):
        analyst = AssistantAgent(
            name="Analyst",
            system_message='''Analyst. You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.
                                 Reply "TERMINATE" in the end when everything is done.
        The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
        [
        {“analysis_item”: “analysis”, “description”:”description of the analysis”;},
        {},
        {},
        ].
                     ''',
            llm_config=self.agent_instance_util.gpt4_turbo_config,
            websocket=self.agent_instance_util.websocket,
            user_name=self.agent_instance_util.user_name,
            openai_proxy=self.agent_instance_util.openai_proxy,
            report_file_name=report_file_name,
        )
        return analyst

    def generate_report_template(self, data):
        # 给定的数据
        # data = last_answer

        # 获取当前工作目录的路径
        current_directory = Path.cwd()

        if str(current_directory).endswith('/ai'):
            html_template_path = str(current_directory) + '/backend/aidb/autopilot/html_template'
        else:
            html_template_path = str(current_directory) + '/ai/backend/aidb/autopilot/html_template'

        print('html_template_path:', html_template_path)

        # 读取模板文件
        with open(html_template_path + '/report_2.html', 'r') as file:
            template_str = file.read()
            print('template_str :', template_str)

        # 使用Jinja2渲染模板
        timestamp = int(time.time() * 1000)
        template = Template(template_str)
        print('template : ', template)
        rendered_html = template.render(data, timestamp=timestamp)
        # rendered_html = template.render(str(data), timestamp=timestamp)

        # 将渲染后的HTML写入文件
        with open(CONFIG.up_file_path + 'output_' + str(timestamp) + '.html', 'w') as output_file:
            output_file.write(rendered_html)

        print("HTML文件已生成：output.html")
        return str(rendered_html)
