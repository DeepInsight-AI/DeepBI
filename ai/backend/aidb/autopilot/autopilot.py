from ai.backend.base_config import CONFIG
from ai.backend.aidb import AIDB
from ai.agents.agentchat import HumanProxyAgent, TaskSelectorAgent, Questioner, AssistantAgent
from jinja2 import Template
from pathlib import Path
import time
import os


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

    def get_agent_ai_analyst(self, report_file_name=None):
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
             """,
            llm_config=self.agent_instance_util.gpt4_turbo_config,
            websocket=self.websocket,
            openai_proxy=self.agent_instance_util.openai_proxy,
            report_file_name=report_file_name,
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

        for item in data['report_question']:
            if item['answer'] is None or item['echart_code'] is None:
                data['report_question'].remove(item)

        for item in data['report_analyst']:
            if item['analysis_item'] is None or item['description'] is None:
                data['report_analyst'].remove(item)

        for item in data['report_thought']:
            if item['report_name'] is None or item['description'] is None:
                data['report_thought'].remove(item)

        # 获取当前工作目录的路径
        current_directory = CONFIG.up_file_path

        # 构建路径时使用 os.path.join，并使用 os.path.normpath 进行规范化
        html_template_path = os.path.join(os.path.normpath(current_directory.replace('user_upload_files', '')), 'ai',
                                          'backend', 'aidb', 'autopilot')
        html_template_path = html_template_path.replace('\\', '/')
        # html_file_path = os.path.normpath(html_file_path)

        if CONFIG.web_language == 'CN':
            html_template_path = os.path.join(html_template_path, 'html_template')
        else:
            html_template_path = os.path.join(html_template_path, 'html_template_en')

        # 读取模板文件
        template_file_path = os.path.join(html_template_path, 'report_2.html')

        with open(template_file_path, 'r', encoding='utf-8') as file:
            template_str = file.read()

        print('html_template_path:', html_template_path)
        print('template_str:', template_str)
        # print('template_str :', template_str)

        # 使用Jinja2渲染模板
        timestamp = int(time.time() * 1000)
        template = Template(template_str)
        rendered_html = template.render(data, timestamp=timestamp)
        # print('rendered_html : ', rendered_html)

        # 将渲染后的HTML写入文件
        with open(CONFIG.up_file_path + 'output_' + str(timestamp) + '.html', 'w', encoding='utf-8') as output_file:
            output_file.write(rendered_html)

        print("HTML文件已生成: output.html")
        return str(rendered_html)

    def get_agent_questioner(self, report_file_name=None):
        """ Questioner  """
        questioner = Questioner(
            name="questioner",
            human_input_mode="NEVER",
            max_consecutive_auto_reply=2,
            llm_config=self.agent_instance_util.gpt4_turbo_config,
            default_auto_reply="Please continue to add analysis dimensions and do not repeat them.",
            websocket=self.websocket,
            openai_proxy=self.agent_instance_util.openai_proxy,
            report_file_name=report_file_name,
        )

        return questioner
