from ai.agents.agentchat.contrib.retrieve_assistant_agent import RetrieveAssistantAgent
from ai.agents.agentchat.contrib.retrieve_user_proxy_agent import RetrieveUserProxyAgent
import asyncio
import time

api_key = 'H7_APpJNc_23fbb3be5f987500ff1f8bc8e9292a'
api_host = "https://apiserver.deep-thought.io/proxy"
config_list_gpt4_turbo = [
    {
        'model': 'gpt-4-1106-preview',
        'api_key': api_key,
        'api_base': api_host,
    },
]


llm_config = {
    "config_list": config_list_gpt4_turbo,
}

assistant = RetrieveAssistantAgent(
    name="assistant",
    system_message="You are a helpful assistant.",
    llm_config=llm_config,
)


# 指定JSON文件路径
# json_file_path = "table_structure.json"
#
# # 读取JSON文件
# with open(json_file_path, "r") as json_file:
#     # 使用json.load()加载JSON数据
#     data = json.load(json_file)

ragproxyagent = RetrieveUserProxyAgent(
    name="ragproxyagent",
    retrieve_config={
        "task": "qa",
        "docs_path": "https://raw.githubusercontent.com/microsoft/autogen/main/README.md",
        # "docs_path": "./table_structure.json",
        # "docs_path": data,
    },
)



async def ask_question():
    assistant.reset()
    await ragproxyagent.initiate_chat(assistant, problem="这些是什么数据")
    # await ragproxyagent.initiate_chat(assistant, problem="100-3=?")


async def ask_question1():
    retrieve_planner_user = RetrieveUserProxyAgent(
        name="retrieve_planner_user",
        max_consecutive_auto_reply=0,  # terminate without auto-reply
        human_input_mode="NEVER",
        websocket=None,
        is_log_out=None,
        openai_proxy=None,
        report_file_name=None,
        retrieve_config={
            "task": "qa",
            # "docs_path": "./table_structure.json",
            "docs_path": "https://raw.githubusercontent.com/microsoft/autogen/main/README.md",

        },
    )

    database_describer = RetrieveAssistantAgent(
        name="retrieve_database_describer",
        system_message="""data_describer.You are a data describer, describing in one sentence your understanding of the data selected by the user. For example, the data selected by the user includes X tables, and what data is in each table.""",
        llm_config=llm_config,
        user_name='tom',
        websocket=None,
        openai_proxy=None,
    )

    await retrieve_planner_user.initiate_chat(database_describer, problem="这些是什么数据")
    # await ragproxyagent.initiate_chat(assistant, problem="100-3=?")


"""
wget https://www.sqlite.org/2023/sqlite-autoconf-3360000.tar.gz
   tar xvf sqlite-autoconf-3360000.tar.gz
   cd sqlite-autoconf-3360000
 ./configure
   make
   sudo make install
 sqlite3 --version


 pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pypdf==4.0.0
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple psycopg2-binary

"""

if __name__ == '__main__':
    print(str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
    # asyncio.get_event_loop().run_until_complete(ask_question())
    asyncio.get_event_loop().run_until_complete(ask_question1())
