import ast
import math

from ai.agents.agentchat import TaskSelectorAgent, HumanProxyAgent ,ConversableAgent
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

api_key = 'H131_FfEW_1a45a0dcfc03539cdc084e2bfa00e7'
api_host = "https://apiserver.deep-thought.io/proxy"

config_list_gpt4_turbo = [
    {
        'model': 'gpt-4-1106-preview',
        'api_key': api_key,
        'api_base': api_host,
    },
]

user_name = '2_bob'

question_ask = f"Read the conversation above. Then select the type of task from . Only the task type is returned.",

# 默认的 summarizer 配置
default_system_message = [
"""You are a professional AI Summarize assistant.
It is generally believed that a natural sales ratio between 40-50% is healthy, an advertising expenditure ratio below 10% is healthy,
and an Acos value of advertising data below 24% is healthy. If the above data are all healthy, our main goal is to increase sales.
NOTE:Just return one paragraph
""",
"""You are a professional AI Summarize assistant.
We believe that a SD ad sales share below 35% is unhealthy, 55%-65% of SP advertising sales is healthy,
and below 90% of SP manual advertising sales share is unhealthy.
NOTE:Just return one paragraph.We only analyze sales.""",
"""You are a professional AI Summarize assistant.
For each list and the whole, we believe that Acos and SPAcos are both healthy if they are below 24%, and SP advertising sales are healthy if they are above 55%.
NOTE:Just return one paragraph""",
"""You are a professional AI Summarize assistant.
For each list and the whole, we believe that SPAcos and SP手动Acos and SP自动Acos are both healthy if they are below 24%, and SP manual advertising sales account for more than 90% is healthy.
NOTE:Just return one paragraph""",
"""You are a professional AI Summarize assistant.
For each list and overall, we believe that SD advertising sales account for 35%-40% is healthy, and SD_ACOS is healthy when it is around 8%.
NOTE:Just return one paragraph,Focus on the proportion of SD advertising sales""",
"""You are a professional AI Summarize assistant.
Summarize the given paragraphs
NOTE:Just return one paragraph""",
"""You are a professional AI Summarize assistant.
Given a year's worth of monthly data, determine whether sales have obvious off-seasons and peak seasons, and analyze current sales trends and acos value trends
NOTE:Just return one paragraph"""
]


user_proxy = HumanProxyAgent(
    name="Admin",
    system_message="A human admin. Interact with the planner to discuss the plan. ",
    code_execution_config={"last_n_messages": 1, "work_dir": "paper"},
    human_input_mode="NEVER",
    max_consecutive_auto_reply=0,
    websocket=None,
    user_name=user_name,
    outgoing=None,
)


async def ask_question(data, custom_message=0):

    # 创建新的 summarizer 实例，设置 system_message
    summarizer = ConversableAgent(
        name="translater",
        system_message= default_system_message[custom_message],
        human_input_mode="NEVER",
        user_name=user_name,
        websocket=None,
        use_cache=False,
        llm_config={
            "config_list": config_list_gpt4_turbo,
        }
    )

    # 发起聊天
    await user_proxy.initiate_chat(
        summarizer,
        message='\n' + str("根据给定的数据，进行总结，给出现状分析。") + '\n' + str(data),
    )

    # 获取回答
    answer_message = summarizer.last_message()["content"]
    print("answer_message:", answer_message)
    return answer_message




if __name__ == '__main__':
    print(str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
    res = asyncio.get_event_loop().run_until_complete(ask_question("熊猫","English"))

    print("============\n",res)


