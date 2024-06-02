from ai.agents.agentchat import PythonProxyAgent, AssistantAgent
import asyncio

from ai.backend.util.db.auto_yzj.utils.trans_to import csv_to_str, json_to_str, md_to_str

python_base_dependency = """python installed dependency environment: pymysql, pandas, mysql-connector-python, pyecharts, sklearn, psycopg2, pymongo, snapshot_selenium"""

# 这两部分提交GitHub时需要注意隐私
api_key = 'H167_egFj_97561e8b594b013be702fae59b3a1c'
api_host = "https://apiserver.deep-thought.io/proxy"
config_list_gpt4_turbo = [
    {
        'model': 'gpt-4o-2024-05-13',
        'api_key': api_key,
        'api_base': api_host,
    },
]
user_name = '2_bob'
db_id = 0

"""
base_csv_info = 1. pd读出整表后的字符串
                2. 将json转化成字符串 (在预处理时自动生成一个{filed_name,field_name},然后需要人工修改)
"""
base_info = csv_to_str(
    R'C:\Users\33259\Desktop\DeepBI\ai\backend\util\db\auto_yzj\日常优化\张梦圆\手动sp广告关键词优化\预处理.csv')
base_info += '\n' + '下面的json字符串是对该csv中的字段的解释'
base_info += json_to_str(
    R'C:\Users\33259\Desktop\DeepBI\ai\backend\util\db\auto_yzj\日常优化\张梦圆\手动sp广告关键词优化\fields.json')


def get_agent_base_csv_assistant(base_csv_info=base_info, quesion_answer_language=''):
    """ Basic Agent, processing csv data source """
    base_csv_assistant = AssistantAgent(
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
                  """ + '\n' + base_csv_info + '\n' + python_base_dependency + '\n' + quesion_answer_language,
        human_input_mode="NEVER",
        user_name=user_name,
        websocket=None,
        llm_config={
            "config_list": config_list_gpt4_turbo,
        },
    )
    return base_csv_assistant


def get_agent_python_executor(report_file_name=None):
    python_executor = PythonProxyAgent(
        name="python_executor",
        system_message="python executor. Execute the python code and report the result.",
        code_execution_config={"last_n_messages": 1, "work_dir": "paper"},
        human_input_mode="NEVER",
        websocket=None,
        user_name=user_name,
        default_auto_reply="TERMINATE",
        # outgoing=self.outgoing,
        # incoming=self.incoming,
        db_id=db_id,
        report_file_name=report_file_name,
    )
    return python_executor


async def ask_question(question, language=''):
    base_mysql_assistant = get_agent_base_csv_assistant()
    python_executor = get_agent_python_executor()

    await python_executor.initiate_chat(
        base_mysql_assistant,
        message=str(question)
    )

    answer_message = python_executor.chat_messages[base_mysql_assistant]
    print("Agent 回答: ", answer_message)


if __name__ == '__main__':
    #     # print(str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
    q_str = md_to_str(
        R'C:\Users\33259\Desktop\DeepBI\ai\backend\util\db\auto_yzj\日常优化\张梦圆\手动sp广告关键词优化\提问策略\优质关键词.md')
    asyncio.get_event_loop().run_until_complete(ask_question(q_str))
