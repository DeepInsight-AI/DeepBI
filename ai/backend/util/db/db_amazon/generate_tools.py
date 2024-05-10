import math

from ai.agents.agentchat import TaskSelectorAgent, HumanProxyAgent ,ConversableAgent ,UserProxyAgent

api_key = '****'
api_host = "****"
config_list_gpt4_turbo = [
    {
        'model': 'gpt-4-1106-preview',
        'api_key': api_key,
        'api_base': api_host,
    },
]

translater = ConversableAgent(
    name="translater",
    system_message="""You are a professional AI translation assistant.
    Translate the user's input, which is in the format of <class 'pandas.core.series.Series'>, into the language specified by the user. Then, output the translated result as a list.
Output For example:
["a=\"Test\"", "b", "c", ...]
Note: Please translate the above content into English one by one, without skipping or omitting any.If the original string contains special characters, please make sure to escape them.
                        """ ,
                   # + str(question_ask),
    human_input_mode="NEVER",
    user_name="translater",
    websocket=None,
    use_cache=False,
    llm_config={
        "config_list": config_list_gpt4_turbo,
    }
)

user_proxy = UserProxyAgent(
    name="Admin",
    system_message="A human admin. Interact with the planner to discuss the plan. ",
    code_execution_config={"last_n_messages": 1, "work_dir": "paper"},
    human_input_mode="NEVER",
    websocket=None,
)


async def ask_question(question,targetlanguage):

    await user_proxy.initiate_chat(
        translater,
        message='\n' + str("Please translate the following content: {} into {} language, maintain the original format without changes, and translate only. Do not return any other content.".format(question,targetlanguage)),
    )

    answer_message = user_proxy.last_message()["content"]
    print()
    return answer_message

async def ask_question1(question,targetlanguage):

    async def generate(question,targetlanguage):
        await user_proxy.initiate_chat(
            translater,
            message='\n' + str("Please translate the following content: {} into {} language, maintain the original format without changes, and translate only.".format(question, targetlanguage)),
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
            print(len(eval(str(tempres))))
            res.extend(eval(str(tempres)))
            # res.extend(ast.literal_eval(tempres))
        except:
            print("翻译结果格式转换失败")
    return res



# if __name__ == '__main__':
#     print(str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
#     res = asyncio.get_event_loop().run_until_complete(ask_question("merino wool base layer","US"))
#
#     print("============\n",res)


