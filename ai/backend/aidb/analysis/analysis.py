import traceback
from ai.backend.util.write_log import logger
from ai.backend.base_config import CONFIG
from ai.backend.aidb import AIDB
from ai.agents.agentchat import HumanProxyAgent, TaskSelectorAgent, TableSelectorAgent
from requests.exceptions import HTTPError
from ai.agents.agentchat.dwx_base_question_selector_agent import BaseQuestionSelectorAgent
import re, json


class Analysis(AIDB):

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
        function_names = ['task_generate_echart', 'task_base']
        function_select = f"Read the conversation above. Then select the type of task from {function_names}. Only the task type is returned.",

        task_message = {
            'task_generate_echart': 'chart generation task, The user ask that the data be finally displayed in the form of a chart.If the question does not clearly state that a  chart is to be generated, it does not belong to this task.',
            'task_base': 'base task'
        }

        select_analysis_assistant = TaskSelectorAgent(
            name="select_task_assistant",
            system_message="""You are a helpful AI assistant.
                       Divide the questions raised by users into corresponding task types.
                       Different tasks have different processing methods.
                       Task types are generally divided into the following categories:

                        """ + str(task_message) + '\n' + str(function_select),
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "config_list": self.agent_instance_util.config_list_gpt4_turbo,
                "request_timeout": CONFIG.request_timeout,
            },
            openai_proxy=self.agent_instance_util.openai_proxy,
        )
        return select_analysis_assistant

    async def start_chatgroup(self, q_str):
        try:
            user_proxy = self.get_agent_user_proxy()
            base_mysql_assistant = self.get_agent_select_analysis_assistant()

            await user_proxy.initiate_chat(
                base_mysql_assistant,
                message=str(q_str),
            )
        except HTTPError as http_err:
            traceback.print_exc()
            error_message = self.generate_error_message(http_err)
            await self.put_message(200, CONFIG.talker_user, CONFIG.type_answer, error_message)
        except Exception as e:
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))
            await self.put_message(200, CONFIG.talker_user, CONFIG.type_answer, self.error_message_timeout)

    async def task_base(self, qustion_message):
        """ Task type: base question """
        return self.error_no_report_question

    async def task_generate_echart(self, qustion_message):
        return self.qustion_message

    ### add new function
    def get_agent_user_proxy_dwx(self):
        """ Human Proxy  """
        user_proxy = HumanProxyAgent(
            name="Admin",
            system_message="A human admin. Interact with the planner to discuss the plan. ",
            code_execution_config={"last_n_messages": 1, "work_dir": "paper"},
            human_input_mode="NEVER",
            websocket=self.websocket,
            user_name=self.user_name,
            function_map={
                "total_question": self.total_question,
                "base_question": self.base_question,
            },
            outgoing=self.outgoing,
        )
        return user_proxy

    def get_agent_base_question_select_assistant_dwx(self):
        """select_analysis_assistant"""
        function_names = ['total_question', 'base_question']
        function_select = f"Read the conversation above. Then select the type of task from {function_names}. Only the task type is returned.",

        task_message = {
            'total_question': '涉及到全店 日成本，日成本率，日营收，日利润率，月成本，月成本率，月营收，月利润率 的问题',
            'base_question': '非 total_question 里面涉及到的 其他问题'
        }

        select_analysis_assistant = BaseQuestionSelectorAgent(
            name="base_question_select_assistant",
            system_message="""You are a helpful AI assistant.
                       Divide the questions raised by users into corresponding task types.
                       Different tasks have different processing methods.
                       Task types are generally divided into the following categories:

                        """ + str(task_message) + '\n' + str(function_select),
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "config_list": self.agent_instance_util.config_list_gpt4_turbo,
                "request_timeout": CONFIG.request_timeout,
            },
            openai_proxy=self.agent_instance_util.openai_proxy,
        )
        return select_analysis_assistant

    async def total_question(self, qustion_message):
        return qustion_message

    async def base_question(self, qustion_message):
        return qustion_message

    async def select_question_type(self, qustion_message, use_cache):

        select_table_assistant = self.get_agent_select_question_assistant(use_cache=use_cache)
        planner_user = self.agent_instance_util.get_agent_planner_user()

        await planner_user.initiate_chat(
            select_table_assistant,
            message=qustion_message,
        )
        select_table_message = planner_user.last_message()["content"]

        match = re.search(
            r"\[.*\]", select_table_message.strip(), re.MULTILINE | re.IGNORECASE | re.DOTALL
        )
        json_str = ""
        if match:
            json_str = match.group()
        print("json_str : ", json_str)
        select_table_list = json.loads(json_str)
        print("select_table_list : ", select_table_list)

        if "total_question" in str(json_str):
            return "total_question"
        elif "base_question"  in str(json_str):
            return "base_question"
        else:
            return None

    def get_agent_select_question_assistant(self, use_cache=True):
        """select_table_assistant"""

        function_names = ['total_question', 'base_question']
        function_select = f"Read the conversation above. Then select the type of task from {function_names}. Only the task type is returned.",

        task_message = {
            'total_question': '涉及到关于全店的日成本，日成本率，日消费，日利润率，月成本，月成本率，月营收，月收入，月利润率  或  当日成本，当日成本率，当日消费，当日利润率，当月成本，当月成本率，当月营收，当月收入，当月利润率 的问题',
            'base_question': '非 total_question 里面涉及到的 其他问题'
        }

        select_analysis_assistant = TableSelectorAgent(
            name="select_table_assistant",
            system_message="""You are a helpful AI assistant.
                        Divide the questions raised by users into corresponding task types.
                        Different tasks have different processing methods.
                        The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
         [
         "task_name":"task_name"
         ].
         Reply "TERMINATE" in the end when everything is done.

                        Task types are generally divided into the following categories:

                         """ + str(task_message) + '\n' + str(function_select),
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "config_list": self.agent_instance_util.config_list_gpt4_turbo,
                "request_timeout": CONFIG.request_timeout,
            },
            openai_proxy=self.agent_instance_util.openai_proxy,
        )
        return select_analysis_assistant
