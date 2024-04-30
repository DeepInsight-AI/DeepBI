import ast
import json
import math
import re

from ai.agents.agentchat import TaskSelectorAgent, HumanProxyAgent ,ConversableAgent
from dwx_prompt import GENERATE_ECHART_PROMPT
import asyncio
import time
import openai
import diskcache
from ai.agents.code_utils import (
    DEFAULT_MODEL,
    UNKNOWN,
    execute_code,
    extract_code,
    infer_lang,
    append_report_logger,
)

api_key = '***'
api_host = "***"

config_list_gpt4_turbo = [
    {
        'model': 'gpt-4-1106-preview',
        'api_key': api_key,
        'api_base': api_host,
    },
]

user_name = '2_bob'

echart_coder = ConversableAgent(
    name="echart_coder",
    system_message="""You are a professional echart code expert. The user will provide you with data, and you will generate corresponding echart code based on the following template.

When generating, please note:

1. Do not modify the data content, including the language.
2. Generate echart graphics that you think meet the requirements, such as bar charts, line charts, etc., according to the needs.
3. Ensure that the echart meets the requirements.
4.Only generate the generated Echart code and output it in standard JSON format as the result, without including any other irrelevant characters or content.
5.If there is Chinese content, please escape it into Unicode character encoding to facilitate display in HTML.
6.注意生成的echartcode代码中标题在图中间展示，可选按钮在图右上角展示，注意元素不要重叠，注意美观性

The following is an echart format:\n
                        """ + GENERATE_ECHART_PROMPT,

    human_input_mode="NEVER",
    user_name=user_name,
    websocket=None,
    use_cache=False,
    llm_config={
        "config_list": config_list_gpt4_turbo,
    },
)


user_proxy = HumanProxyAgent(
    name="Admin",
    system_message="A human admin. Interact with the planner to discuss the plan. ",
    code_execution_config=False,
    human_input_mode="NEVER",
    websocket=None,
    user_name=user_name,
    outgoing=None,
)


async def ask_question(data):

    # 这是之前的
    await user_proxy.initiate_chat(
        echart_coder,
        message='\n' + str("请将以下数据转化为对应的echart代码：{}".format(data)),
    )

    answer_message = user_proxy.last_message()["content"]
    print()
    return answer_message



def generate_ecode(data):
    try:
        res = asyncio.get_event_loop().run_until_complete(ask_question(data))
        # 增加判断看是否可以正确转化为json格式
        res_echart = str(res).replace("```json\n","").replace("\n```","").replace("\\\\","\\")
        try:
            json.loads(res_echart)
            return res_echart
        except Exception as e:
            return ""
        # return str(res).replace("```json\n","").replace("\n```","")

    except Exception as e:
        print("根据数据生成echartcode失败，报错如下：",e)

def fun_Wenxin():
    pass
