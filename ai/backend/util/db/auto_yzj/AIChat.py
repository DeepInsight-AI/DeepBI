import asyncio
import os
import traceback

from ai.agents.agent_instance_util import max_retry_times
from ai.agents.agentchat import PythonProxyAgent, AssistantAgent
from ai.backend.base_config import CONFIG
from ai.backend.util.db.auto_yzj.utils.trans_to import csv_to_str, json_to_str, md_to_str


# 配置信息和函数定义


def load_base_info(input_path):
    # 获取他的文件名
    dir_path = os.path.dirname(input_path)
    # 获取绝对路径
    absolute_path = os.path.abspath(input_path)
    base_csv_info = "我的数据集在" + R"" + absolute_path + "这个位置。"
    base_info = base_csv_info + '\n' + '下面的json字符串是对该csv中的字段的解释'
    base_info += json_to_str(R"" + (dir_path + '/fields.json'))
    return base_info


class AIChat:
    def __init__(self, input_path='', u_name='', db_id=0, use_deepseek_config=False):
        api_key = 'H131_SGfA_6ebc65009decbf74934d19a5e03417'
        api_host = "https://apiserver.deep-thought.io/proxy"
        model = 'gpt-4o-2024-05-13'

        config_list_gpt4_turbo = [
            {
                'model': model,
                'api_key': api_key,
                'api_base': api_host,
            },
        ]
        if use_deepseek_config:
            api_key = 'sk-6fa69b7626304d94a688a81b5494e713'
            api_host = "https://api.deepseek.com"
            model = 'deepseek-chat'
            config_list_gpt4_turbo = [
                {
                    'model': model,
                    'api_key': api_key,
                    'api_base': api_host,
                },
            ]
        self.user_name = u_name
        self.db_id = db_id
        self.base_info = load_base_info(input_path)
        self.python_base_dependency = """python installed dependency environment: pymysql, pandas,
        mysql-connector-python, pyecharts, sklearn, psycopg2, pymongo, snapshot_selenium"""
        self.config_list_gpt4_turbo = config_list_gpt4_turbo

    # 输入的csv文件路径

    def get_agent_base_csv_assistant(self):
        base_csv_assistant = AssistantAgent(
            name="base_csv_assistant",
            system_message="""You are a helpful AI assistant. Solve tasks using your coding and language skills. In
            the following cases, suggest python code (in a python coding block) for the user to execute. 1. When you
            need to collect info, use the code to output the info you need, for example, browse or search the web,
            download/read a file, print the content of a webpage or a file, get the current date/time,
            check the operating system. After sufficient info is printed and the task is ready to be solved based on
            your language skill, you can solve the task by yourself. 2. When you need to perform some task with code,
            use the code to perform the task and output the result. Finish the task smartly. Solve the task step by
            step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code,
            and which step uses your language skill. When using code, you must indicate the script type in the code
            block. The user cannot provide any other feedback or perform any other action beyond executing the code
            you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to
            modify. Don't use a code block if it's not intended to be executed by the user. If you want the user to
            save the code in a file before executing it, put # filename: <filename> inside the code block as the
            first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the
            result. Instead, use 'print' function for the output when relevant. Check the execution result returned
            by the user. If the result indicates there is an error, fix the error and output the code again. Suggest
            the full code instead of partial code or code changes. If the error can't be fixed or if the task is not
            solved even after the code is executed successfully, analyze the problem, revisit your assumption,
            collect additional info you need, and think of a different approach to try. When you find an answer,
            verify the answer carefully. Include verifiable evidence in your response if possible. Reply "TERMINATE"
            in the end when everything is done. When you find an answer,  You are a report analysis, you have the
            knowledge and skills to turn raw data into information and insight, which can be used to make business
            decisions.include your analysis in your reply.

                  The only source data you need to process is csv files.
            IMPORTANT:
            First, determine whether there is a need to display data in the form of charts in the user's question, such as "Please give me a histogram of the top 10 sales of sub-category products." If such a need exists, it is recommended that the function call <task_csv_echart_code>.If it does not exist, directly answer the questions.
                  """ + '\n' + self.base_info + '\n' + self.python_base_dependency + '\n',
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=None,
            llm_config={"config_list": self.config_list_gpt4_turbo,
                        "request_timeout": CONFIG.request_timeout
            },
        )
        return base_csv_assistant

    def get_agent_python_executor(self, report_file_name=None):
        python_executor = PythonProxyAgent(
            name="python_executor",
            system_message="python executor. Execute the python code and report the result.",
            code_execution_config={"last_n_messages": 1, "work_dir": "paper"},
            human_input_mode="NEVER",
            websocket=None,
            user_name=self.user_name,
            default_auto_reply="TERMINATE",
            db_id=self.db_id,
            report_file_name=report_file_name,
        )
        return python_executor

    async def ask_question(self, question):
        base_mysql_assistant = self.get_agent_base_csv_assistant()
        python_executor = self.get_agent_python_executor()
        await python_executor.initiate_chat(
            base_mysql_assistant,
            clear_history=True,
            message=question
        )

    async def ask_question1(self, question):
        try:
            error_times = 0
            for i in range(max_retry_times):
                try:
                    base_mysql_assistant = self.get_agent_base_csv_assistant()
                    python_executor = self.get_agent_python_executor()

                    await python_executor.initiate_chat(
                        base_mysql_assistant,
                        message=question
                    )

                    answer_message = python_executor.chat_messages[base_mysql_assistant]
                    print("answer_message: ", answer_message)

                    for i in range(len(answer_message)):
                        answer_mess = answer_message[len(answer_message) - 1 - i]
                        # print("answer_mess :", answer_mess)
                        if answer_mess['content'] and answer_mess['content'] != 'TERMINATE':
                            print("answer_mess['content'] ", answer_mess['content'])
                            return answer_mess['content']

                except Exception as e:
                    traceback.print_exc()
                    error_times = error_times + 1



        except Exception as e:
            traceback.print_exc()


# 使用示例
if __name__ == '__main__':
    # ai_assistant = AIChat(u_name="fasfdsaf", db_id=56,
    #                       input_path=R'C:\Users\33259\Desktop\DeepBI\ai\backend\util\db\auto_yzj'
    #                                  R'\日常优化\张梦圆\手动sp广告关键词优化\预处理1.csv')
    # q_str = md_to_str(
    #     R'C:\Users\33259\Desktop\DeepBI\ai\backend\util\db\auto_yzj\日常优化\张梦圆\手动sp广告关键词优化\提问策略\手动_劣质关键词_v1_0.md')
    # answer = asyncio.get_event_loop().run_until_complete(ai_assistant.ask_question(q_str))
    # print("Agent 回答: ", answer)
    txt = R'C:\Users\33259\Desktop\DeepBI\ai\backend\util\db\auto_yzj\todo.md'
    print(txt[0:-2])
