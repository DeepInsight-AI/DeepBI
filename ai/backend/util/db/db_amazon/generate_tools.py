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

api_key = '***'
api_host = "****"

config_list_gpt4_turbo = [
    {
        'model': 'gpt-4-1106-preview',
        'api_key': api_key,
        'api_base': api_host,
    },
]

user_name = '2_bob'

question_ask = f"Read the conversation above. Then select the type of task from . Only the task type is returned.",

translater = ConversableAgent(
    name="translater",
    system_message="""You are a professional AI translation assistant.
    Translate the user's input, which is in the format of <class 'pandas.core.series.Series'>, into the language specified by the user. Then, output the translated result as a list.
Output For example:
["a", "b", "c", ...]
Note: Please translate the above content into English one by one, without skipping or omitting any.
                        """ ,
                   # + str(question_ask),
    human_input_mode="NEVER",
    user_name=user_name,
    websocket=None,
    use_cache=False,
    llm_config={
        "config_list": config_list_gpt4_turbo,
    }
)

user_proxy = HumanProxyAgent(
    name="Admin",
    system_message="A human admin. Interact with the planner to discuss the plan. ",
    code_execution_config={"last_n_messages": 1, "work_dir": "paper"},
    human_input_mode="NEVER",
    websocket=None,
    user_name=user_name,
    outgoing=None,
)


async def ask_question(question,targetlanguage):

    # 这是之前的
    await user_proxy.initiate_chat(
        translater,
        message='\n' + str("请将以下内容:{}  翻译成{}语言，按照原格式输出，不要改变格式，仅仅翻译".format(question,targetlanguage)),
    )

    answer_message = user_proxy.last_message()["content"]
    print()
    return answer_message

async def ask_question1(question,targetlanguage):

    async def generate(question,targetlanguage):
        await user_proxy.initiate_chat(
            translater,
            message='\n' + str("请将以下内容:{}  翻译成{}语言，按照原格式输出，不要改变格式，仅仅翻译".format(question, targetlanguage)),
        )

        answer_message = user_proxy.last_message()["content"]
        return answer_message

    res = []
    truntimes = math.ceil(len(question)/50)
    for i in range(truntimes):
        if i == truntimes-1:
            tempres=await generate(question[50*i:len(question)],targetlanguage)
        else:
            tempres=await generate(question[50*i:50*(i+1)],targetlanguage)
        try:
            res.extend(eval(str(tempres)))
            # res.extend(ast.literal_eval(tempres))
        except:
            print("翻译结果格式转换失败")
    return res




if __name__ == '__main__':
    print(str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
    res = asyncio.get_event_loop().run_until_complete(ask_question("熊猫","English"))

    print("============\n",res)


