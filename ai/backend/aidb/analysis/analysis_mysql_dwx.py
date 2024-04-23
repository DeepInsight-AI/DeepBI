import traceback
import json
from ai.backend.util.write_log import logger
from ai.backend.base_config import CONFIG
from ai.backend.util import database_util
from .analysis import Analysis
import re
import ast
from ai.backend.util import base_util
from ai.agents.agentchat.contrib import RetrieveAssistantAgent, RetrievePythonProxyAgent
import os
from ai.agents.prompt import MYSQL_ECHART_TIPS_MESS
from ai.agents.agentchat import PythonProxyAgent, AssistantAgent

max_retry_times = CONFIG.max_retry_times


class AnalysisMysql(Analysis):

    async def deal_question(self, json_str, message):
        """
        Process mysql data source and select the corresponding workflow
        """
        result = {'state': 200, 'data': {}, 'receiver': ''}
        q_sender = json_str['sender']
        q_data_type = json_str['data']['data_type']
        print('q_data_type : ', q_data_type)
        q_str = json_str['data']['content']

        print("self.agent_instance_util.api_key_use :", self.agent_instance_util.api_key_use)
        if not self.agent_instance_util.api_key_use:
            re_check = await self.check_api_key()
            print('re_check : ', re_check)
            if not re_check:
                return

        if q_sender == 'user':
            if q_data_type == 'question':
                # print("agent_instance_util.base_message :", self.agent_instance_util.base_message)
                if self.agent_instance_util.base_message is not None:
                    await self.start_chatgroup(q_str)
                else:
                    await self.put_message(500, receiver=CONFIG.talker_user, data_type=CONFIG.type_answer,
                                           content=self.error_miss_data)
        elif q_sender == 'bi':
            if q_data_type == CONFIG.type_comment:
                await self.check_data_base(q_str)
            elif q_data_type == CONFIG.type_comment_first:
                self.db_info_json = q_str

                if json_str.get('data').get('language_mode'):
                    q_language_mode = json_str['data']['language_mode']
                    if q_language_mode == CONFIG.language_chinese or q_language_mode == CONFIG.language_english or q_language_mode == CONFIG.language_japanese:
                        self.set_language_mode(q_language_mode)
                        self.agent_instance_util.set_language_mode(q_language_mode)

                if CONFIG.database_model == 'online':
                    databases_id = json_str['data']['databases_id']
                    db_id = str(databases_id)
                    obj = database_util.Main(db_id)
                    if_suss, db_info = obj.run()
                    if if_suss:
                        self.agent_instance_util.base_mysql_info = ' When connecting to the database, be sure to bring the port. This is mysql database info :' + '\n' + str(
                            db_info)
                        self.agent_instance_util.db_id = db_id
                        self.agent_instance_util.set_base_message(q_str, databases_id=db_id)


                else:
                    self.agent_instance_util.set_base_message(q_str)

                await self.get_data_desc(q_str)
            elif q_data_type == CONFIG.type_comment_second:
                self.db_info_json = q_str

                if json_str.get('data').get('language_mode'):
                    q_language_mode = json_str['data']['language_mode']
                    if q_language_mode == CONFIG.language_chinese or q_language_mode == CONFIG.language_english or q_language_mode == CONFIG.language_japanese:
                        self.set_language_mode(q_language_mode)
                        self.agent_instance_util.set_language_mode(q_language_mode)

                if CONFIG.database_model == 'online':
                    databases_id = json_str['data']['databases_id']
                    db_id = str(databases_id)
                    print("db_id:", db_id)
                    obj = database_util.Main(db_id)
                    if_suss, db_info = obj.run()
                    if if_suss:
                        self.agent_instance_util.base_mysql_info = '  When connecting to the database, be sure to bring the port. This is mysql database info :' + '\n' + str(
                            db_info)
                        self.agent_instance_util.set_base_message(q_str, databases_id=db_id)
                        self.agent_instance_util.db_id = db_id
                else:
                    self.agent_instance_util.set_base_message(q_str)

                await self.put_message(200, receiver=CONFIG.talker_bi, data_type=CONFIG.type_comment_second,
                                       content='')
            elif q_data_type == 'mysql_code' or q_data_type == 'chart_code' or q_data_type == 'delete_chart' or q_data_type == 'ask_data':
                self.delay_messages['bi'][q_data_type].append(message)
                print("delay_messages : ", self.delay_messages)
                return
        else:
            print('error : q_sender is not user or bi')
            await self.put_message(500, receiver=CONFIG.talker_bi, data_type=CONFIG.type_comment_second,
                                   content='error : q_sender is not user or bi')

    async def task_base(self, qustion_message):
        """ Task type: mysql data analysis"""
        try:
            error_times = 0
            use_cache = True
            for i in range(max_retry_times):
                try:
                    if self.agent_instance_util.is_rag:
                        table_comment = await self.select_table_comment(qustion_message, use_cache)
                        table_desc_length = len(table_comment['table_desc'])

                        # retrieve_rag_doc = await self.select_rag_doc(qustion_message, use_cache)
                        # if table_desc_length > 0 or len(retrieve_rag_doc) > 20:
                        #     answer_message = await self.task_base_rag(qustion_message, table_comment, use_cache,
                        #                                               retrieve_rag_doc)

                        if table_desc_length > 0:
                            answer_message = await self.task_base_rag(qustion_message=qustion_message,
                                                                      table_comment=table_comment, use_cache=use_cache)
                        else:
                            use_cache = False
                            continue
                    else:
                        base_mysql_assistant = self.agent_instance_util.get_agent_base_mysql_assistant(
                            use_cache=use_cache)
                        python_executor = self.agent_instance_util.get_agent_python_executor()

                        await python_executor.initiate_chat(
                            base_mysql_assistant,
                            message=self.agent_instance_util.base_message + '\n' + self.question_ask + '\n' + str(
                                qustion_message),
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
                    logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                    error_times = error_times + 1
                use_cache = False
            if error_times >= max_retry_times:
                return self.error_message_timeout

        except Exception as e:
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))

        return self.agent_instance_util.data_analysis_error

    async def task_generate_echart(self, qustion_message):
        try:
            base_content = []
            base_mess = []
            report_demand_list = []
            json_str = ""
            error_times = 0
            use_cache = True
            for i in range(max_retry_times):
                try:
                    if self.agent_instance_util.is_rag:
                        table_comment = await self.select_table_comment(qustion_message, use_cache)
                        table_desc_length = len(table_comment['table_desc'])
                        if table_desc_length > 0:
                            answer_message = await self.task_generate_echart_rag(qustion_message, table_comment,
                                                                                 use_cache)
                        else:
                            use_cache = False
                            continue
                    else:
                        mysql_echart_assistant = self.agent_instance_util.get_agent_mysql_echart_assistant(
                            use_cache=use_cache)
                        python_executor = self.agent_instance_util.get_agent_python_executor()

                        await python_executor.initiate_chat(
                            mysql_echart_assistant,
                            message=self.agent_instance_util.base_message + '\n' + self.question_ask + '\n' + str(
                                qustion_message),
                        )

                        answer_message = mysql_echart_assistant.chat_messages[python_executor]

                    for answer_mess in answer_message:
                        # print("answer_mess :", answer_mess)
                        if answer_mess['content']:
                            if str(answer_mess['content']).__contains__('execution succeeded'):

                                answer_mess_content = str(answer_mess['content']).replace('\n', '')

                                print("answer_mess: ", answer_mess)
                                match = re.search(
                                    r"\[.*\]", answer_mess_content.strip(), re.MULTILINE | re.IGNORECASE | re.DOTALL
                                )

                                if match:
                                    json_str = match.group()
                                print("json_str : ", json_str)
                                # report_demand_list = json.loads(json_str)

                                chart_code_str = str(json_str).replace("\n", "")
                                if len(chart_code_str) > 0:
                                    print("chart_code_str: ", chart_code_str)
                                    if base_util.is_json(chart_code_str):
                                        report_demand_list = json.loads(chart_code_str)

                                        print("report_demand_list: ", report_demand_list)

                                        for jstr in report_demand_list:
                                            if str(jstr).__contains__('echart_name') and str(jstr).__contains__(
                                                'echart_code'):
                                                base_content.append(jstr)
                                    else:
                                        # String instantiated as object
                                        report_demand_list = ast.literal_eval(chart_code_str)
                                        print("report_demand_list: ", report_demand_list)
                                        for jstr in report_demand_list:
                                            if str(jstr).__contains__('echart_name') and str(jstr).__contains__(
                                                'echart_code'):
                                                base_content.append(jstr)

                    print("base_content: ", base_content)
                    base_mess = []
                    base_mess.append(answer_message)
                    # Reduce debugging processes in the message.
                    message_return = []
                    for step_res in answer_message:
                        if step_res["content"] and "execution failed" in str(step_res["content"]):
                            message_return.pop()
                        else:
                            message_return.append(step_res)
                    if len(message_return) == 1:
                        message_return.extend(answer_message[-2:])
                    break


                except Exception as e:
                    traceback.print_exc()
                    logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                    error_times = error_times + 1

                use_cache = False

            if error_times >= max_retry_times:
                return self.error_message_timeout

            logger.info(
                "from user:[{}".format(self.user_name) + "] , " + "，report_demand_list" + str(report_demand_list))

            # bi_proxy = self.agent_instance_util.get_agent_bi_proxy()
            is_chart = False
            # Call the interface to generate pictures
            for img_str in base_content:
                echart_name = img_str.get('echart_name')
                echart_code = img_str.get('echart_code')

                if len(echart_code) > 0 and str(echart_code).__contains__('x'):
                    is_chart = True
                    print("echart_name : ", echart_name)
                    # 格式化echart_code
                    # if base_util.is_json(str(echart_code)):
                    #     json_obj = json.loads(str(echart_code))
                    #     echart_code = json.dumps(json_obj)
            #
            #         re_str = await bi_proxy.run_echart_code(str(echart_code), echart_name)
            #         base_mess.append(re_str)

            error_times = 0
            for i in range(max_retry_times):
                try:
                    planner_user = self.agent_instance_util.get_agent_planner_user()
                    analyst = self.agent_instance_util.get_agent_analyst()

                    question_supplement = 'Please make an analysis and summary in English, including which charts were generated, and briefly introduce the contents of these charts.'
                    if self.language_mode == CONFIG.language_chinese:

                        if is_chart:
                            question_supplement = " 请用中文，简单介绍一下已生成图表中的数据内容."
                        else:
                            question_supplement = " 请用中文，从上诉对话中分析总结出问题的答案."
                    elif self.language_mode == CONFIG.language_japanese:
                        if is_chart:
                            question_supplement = " 生成されたグラフのデータ内容について、簡単に日本語で説明してください。"
                        else:
                            question_supplement = " 上記の対話から問題の答えを分析し、日本語で要約してください。"

                    await planner_user.initiate_chat(
                        analyst,
                        message=str(
                            message_return) + '\n' + self.question_ask + '\n' + question_supplement,
                    )

                    answer_message = planner_user.last_message()["content"]
                    return answer_message

                except Exception as e:
                    traceback.print_exc()
                    logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                    error_times = error_times + 1

            if error_times == max_retry_times:
                return self.error_message_timeout

        except Exception as e:
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
        return self.agent_instance_util.data_analysis_error

    async def task_base_rag(self, qustion_message, table_comment, use_cache, retrieve_rag_doc=None):
        """ Task type: mysql data analysis"""
        if os.path.exists(self.agent_instance_util.get_rag_doc()):

            # add function select_rag_doc
            # retrieve_rag_doc = await self.select_rag_doc(qustion_message, use_cache)
            from ai.backend.util import database_util
            obj = database_util.Main(self.agent_instance_util.db_id)
            if_suss, db_info = obj.run_decode()
            # print(if_suss)
            # print(db_info)
            if str(db_info['db']).__contains__('dwx'):

                ##################### 3, only  funciton_call
                # self.table_comment = table_comment
                # self.use_cache = use_cache
                #
                # wxMysqlRagUitl = self.set_function_call_dwx(db_info=db_info)
                #
                # base_mysql_assistant = self.get_agent_base_mysql_assistant_dwx(use_cache=use_cache,
                #                                                                add_message='')
                # python_executor = self.get_agent_python_executor_dwx()
                #
                # await python_executor.initiate_chat(
                #     base_mysql_assistant,
                #     message=
                #     # 'this is databases info: ' + '\n'
                #     # + str(table_comment)+ '\n'
                #     # + str(retrieve_rag_doc)+ '\n' +
                #     self.question_ask + '\n' + str(
                #         qustion_message),
                # )
                #
                # # 关闭数据库链接
                # wxMysqlRagUitl.connect_close()
                ##################### only  funciton_call

                ##################### 2, retrieve agent + funciton_call
                # wxMysqlRagUitl = self.set_function_call_dwx(db_info=db_info)
                # base_mysql_assistant = self.get_agent_retrieve_base_mysql_assistant_dwx(use_cache=use_cache)
                # python_executor = self.get_agent_retrieve_python_executor_dwx(
                #     docs_path=self.agent_instance_util.get_rag_doc())
                # await python_executor.initiate_chat(
                #     base_mysql_assistant,
                #     problem=self.question_ask + '\n' + str(
                #         qustion_message),
                #     db_info='this is databases info: ' + '\n' + str(table_comment),
                # )
                # # 关闭数据库链接
                # wxMysqlRagUitl.connect_close()
                ##################### retrieve agent + funciton_call

                ##################### 1, only retrieve agent
                # base_mysql_assistant = self.get_agent_retrieve_base_mysql_assistant(use_cache=use_cache)
                # python_executor = self.get_agent_retrieve_python_executor(
                #     docs_path=self.agent_instance_util.get_rag_doc())
                # await python_executor.initiate_chat(
                #     base_mysql_assistant,
                #     problem=self.question_ask + '\n' + str(
                #         qustion_message),
                #     db_info='this is databases info: ' + '\n' + str(table_comment),
                # )
                ##################### only retrieve agent

                ##### 4, two fucntion
                self.table_comment = table_comment
                self.use_cache = use_cache
                question_type = await self.select_question_type(qustion_message=qustion_message, use_cache=use_cache)
                if question_type == 'total_question':
                    answer_message = await self.total_question(qustion_message, db_info)
                    return answer_message
                else:
                    # answer_message = await self.base_question(qustion_message)
                    answer_message = await self.base_question_llm(qustion_message)
                    return answer_message
            else:
                # 非 电玩猩 数据库
                base_mysql_assistant = self.get_agent_retrieve_base_mysql_assistant(use_cache=use_cache)
                python_executor = self.get_agent_retrieve_python_executor(
                    docs_path=self.agent_instance_util.get_rag_doc())
                await python_executor.initiate_chat(
                    base_mysql_assistant,
                    problem=self.question_ask + '\n' + str(
                        qustion_message),
                    db_info='this is databases info: ' + '\n' + str(table_comment),
                )

        else:
            base_mysql_assistant = self.agent_instance_util.get_agent_base_mysql_assistant(use_cache=use_cache)
            python_executor = self.agent_instance_util.get_agent_python_executor()

            await python_executor.initiate_chat(
                base_mysql_assistant,
                message='this is databases info: ' + '\n' + str(table_comment) + '\n' + self.question_ask + '\n' + str(
                    qustion_message),
            )

        answer_message = python_executor.chat_messages[base_mysql_assistant]
        print("answer_message: ", answer_message)

        return answer_message

    async def task_generate_echart_rag(self, qustion_message, table_comment, use_cache):
        if os.path.exists(self.agent_instance_util.get_rag_doc()):
            #####################  1，old RAG (Search using vector database)

            # mysql_echart_assistant = self.get_agent_retrieve_mysql_echart_assistant(use_cache=use_cache)
            # python_executor = self.get_agent_retrieve_python_executor(docs_path=self.agent_instance_util.get_rag_doc())
            #
            # await python_executor.initiate_chat(
            #     mysql_echart_assistant,
            #     problem=self.question_ask + '\n' + str(
            #         qustion_message),
            #     db_info='this is databases info: ' + '\n' + str(table_comment),
            # )
            #####################  old RAG

            #####################  2，LLM RAG (Search using LLM gpt3.5)

            # table_comment = self.table_comment
            # use_cache = self.use_cache
            retrieve_rag_doc = await self.select_rag_doc(qustion_message, use_cache)

            mysql_echart_assistant = self.agent_instance_util.get_agent_mysql_echart_assistant(
                use_cache=use_cache)
            python_executor = self.agent_instance_util.get_agent_python_executor()

            await python_executor.initiate_chat(
                mysql_echart_assistant,
                message=
                'this is databases info: ' + '\n'
                + str(table_comment) + '\n'
                + str(retrieve_rag_doc) + '\n' +
                self.question_ask + '\n' + str(
                    qustion_message),
            )
            #####################  LLM RAG


        else:
            mysql_echart_assistant = self.agent_instance_util.get_agent_mysql_echart_assistant(
                use_cache=use_cache)
            python_executor = self.agent_instance_util.get_agent_python_executor()

            await python_executor.initiate_chat(
                mysql_echart_assistant,
                message='this is databases info: ' + '\n' + str(table_comment) + '\n' + self.question_ask + '\n' + str(
                    qustion_message),
            )

        answer_message = mysql_echart_assistant.chat_messages[python_executor]
        print("answer_message: ", answer_message)

        return answer_message

    def get_agent_retrieve_base_mysql_assistant(self, use_cache=True):
        """ Basic Agent, processing mysql data source """
        retrieve_base_mysql_assistant = RetrieveAssistantAgent(
            name="retrieve_base_mysql_assistant",
            system_message="""You are a helpful AI assistant.
                Solve tasks using your coding and language skills.
                In the following cases, suggest python code (in a python coding block) for the user to execute.
                    Do not provide executable code other than python code.

                    1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                    2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.Please refrain from generating any graphics based on the data or producing any code related to generating graphics.
                If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                Do not merely substitute the statistical characteristics of the overall data with those derived from sample data. In any case, even if the sample data is insufficient, do not fabricate data for the sake of visualization. Instead, you should reassess whether there is a flaw in the execution logic and attempt again, or plainly acknowledge the limitation.If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                Reply "TERMINATE" in the end when everything is done.
                When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                Be careful to avoid using mysql special keywords in mysql code.If creating a database connection using PyMySQL, please note that in the connect function of PyMySQL, the cursorclass parameter should be set to the default value: cursorclass=pymysql.cursors.Cursor or left unset. Do not set it to cursorclass=pymysql.cursors.DictCursor.
                If you choose to use a connection method similar to db_connection_str = f"mysql+pymysql://{db_config['user']}:\"{db_config['passwd']}\"@{db_config['host']}:{db_config['port']}/{db_config['db']}?charset=utf8mb4", please be mindful of special characters in your password. Ensure that you correctly handle escaping and quotation marks in the connection string to guarantee the correctness of db_connection.
                """ + '\n' + self.agent_instance_util.base_mysql_info + '\n' + CONFIG.python_base_dependency + '\n' + self.agent_instance_util.quesion_answer_language,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config=self.agent_instance_util.gpt4_turbo_config,
            openai_proxy=self.agent_instance_util.openai_proxy,
            use_cache=use_cache,
        )
        return retrieve_base_mysql_assistant

    def get_agent_retrieve_base_mysql_assistant_dwx(self, use_cache=True):
        """ Basic Agent, processing mysql data source """
        retrieve_base_mysql_assistant = RetrieveAssistantAgent(
            name="retrieve_base_mysql_assistant_dwx",
            system_message="""You are a helpful AI assistant.
                Solve tasks using your coding and language skills.
                In the following cases, suggest python code (in a python coding block) for the user to execute.
                    Do not provide executable code other than python code.

                    1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                    2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.Please refrain from generating any graphics based on the data or producing any code related to generating graphics.
                If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                Do not merely substitute the statistical characteristics of the overall data with those derived from sample data. In any case, even if the sample data is insufficient, do not fabricate data for the sake of visualization. Instead, you should reassess whether there is a flaw in the execution logic and attempt again, or plainly acknowledge the limitation.If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                Reply "TERMINATE" in the end when everything is done.
                When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                Be careful to avoid using mysql special keywords in mysql code.If creating a database connection using PyMySQL, please note that in the connect function of PyMySQL, the cursorclass parameter should be set to the default value: cursorclass=pymysql.cursors.Cursor or left unset. Do not set it to cursorclass=pymysql.cursors.DictCursor.
                If you choose to use a connection method similar to db_connection_str = f"mysql+pymysql://{db_config['user']}:\"{db_config['passwd']}\"@{db_config['host']}:{db_config['port']}/{db_config['db']}?charset=utf8mb4", please be mindful of special characters in your password. Ensure that you correctly handle escaping and quotation marks in the connection string to guarantee the correctness of db_connection.
                """ + '\n' + self.agent_instance_util.base_mysql_info + '\n' + CONFIG.python_base_dependency + '\n' + self.agent_instance_util.quesion_answer_language,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "config_list": self.agent_instance_util.config_list_gpt4_turbo,
                "seed": 42,  # change the seed for different trials
                "temperature": 0,
                "functions": self.function_config,
            },
            openai_proxy=self.agent_instance_util.openai_proxy,
            use_cache=use_cache,
        )
        return retrieve_base_mysql_assistant

    def get_agent_retrieve_python_executor(self, report_file_name=None, docs_path=None):
        retrieve_python_executor = RetrievePythonProxyAgent(
            name="retrieve_python_executor",
            system_message="python executor. Execute the python code and report the result.",
            code_execution_config={"last_n_messages": 1, "work_dir": "paper"},
            human_input_mode="NEVER",
            websocket=self.websocket,
            user_name=self.user_name,
            default_auto_reply="TERMINATE",
            # outgoing=self.outgoing,
            # incoming=self.incoming,
            db_id=self.agent_instance_util.db_id,
            report_file_name=report_file_name,
            retrieve_config={
                "task": "qa",
                "docs_path": docs_path,
                "custom_text_types": "json",
                "extra_docs": False,
                "collection_name": "autogen_docs_" + str(self.agent_instance_util.uid) + '_db' + str(
                    self.agent_instance_util.db_id),
                "chunk_mode": "multi_lines",
                # "chunk_mode": "one_line",
            },
        )
        return retrieve_python_executor

    def get_agent_retrieve_python_executor_dwx(self, report_file_name=None, docs_path=None):
        retrieve_python_executor = RetrievePythonProxyAgent(
            name="retrieve_python_executor_dwx",
            system_message="python executor. Execute the python code and report the result.",
            code_execution_config={"last_n_messages": 1, "work_dir": "paper"},
            human_input_mode="NEVER",
            websocket=self.websocket,
            user_name=self.user_name,
            default_auto_reply="TERMINATE",
            # outgoing=self.outgoing,
            # incoming=self.incoming,
            db_id=self.agent_instance_util.db_id,
            report_file_name=report_file_name,
            retrieve_config={
                "task": "qa",
                "docs_path": docs_path,
                "custom_text_types": "json",
                "extra_docs": False,
                "collection_name": "autogen_docs_" + str(self.agent_instance_util.uid) + '_db' + str(
                    self.agent_instance_util.db_id),
                "chunk_mode": "multi_lines",
                # "chunk_mode": "one_line",
            },
            function_map=self.function_map,

        )
        return retrieve_python_executor

    def get_agent_retrieve_mysql_echart_assistant(self, use_cache=True, report_file_name=None):
        """mysql_echart_assistant"""
        retrieve_mysql_echart_assistant = RetrieveAssistantAgent(
            name="retrieve_mysql_echart_assistant",
            system_message="""You are a helpful AI assistant.
                                            Solve tasks using your coding and language skills.
                                            In the following cases, suggest python code (in a python coding block) for the user to execute.
                                                1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                                                2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                                            Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                                            When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                                            If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                                            If you need to use %Y-%M to query the date or timestamp, please use %Y-%M. You cannot use %%Y-%%M.(For example you should use SELECT * FROM your_table WHERE DATE_FORMAT(your_date_column, '%Y-%M') = '2024-February'; instead of SELECT * FROM your_table WHERE DATE_FORMAT(your_date_column, '%%Y-%%M') = '2024-%%M';)
                                            Do not merely substitute the statistical characteristics of the overall data with those derived from sample data. In any case, even if the sample data is insufficient, do not fabricate data for the sake of visualization. Instead, you should reassess whether there is a flaw in the execution logic and attempt again, or plainly acknowledge the limitation.If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                                            When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                                            Reply "TERMINATE" in the end when everything is done.
                                            When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                                            Be careful to avoid using mysql special keywords in mysql code.If creating a database connection using PyMySQL, please note that in the connect function of PyMySQL, the cursorclass parameter should be set to the default value: cursorclass=pymysql.cursors.Cursor or left unset. Do not set it to cursorclass=pymysql.cursors.DictCursor.
                                            If you choose to use a connection method similar to db_connection_str = f"mysql+pymysql://{db_config['user']}:\"{db_config['passwd']}\"@{db_config['host']}:{db_config['port']}/{db_config['db']}?charset=utf8mb4", please be mindful of special characters in your password. Ensure that you correctly handle escaping and quotation marks in the connection string to guarantee the correctness of db_connection.
                                            One SQL query result is limited to 20 items.
                                            Don't generate html files.
                                            """ + '\n' + self.agent_instance_util.base_mysql_info + '\n' + CONFIG.python_base_dependency + '\n' + MYSQL_ECHART_TIPS_MESS,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config=self.agent_instance_util.gpt4_turbo_config,
            openai_proxy=self.agent_instance_util.openai_proxy,
            use_cache=use_cache,
            report_file_name=report_file_name,

        )
        return retrieve_mysql_echart_assistant

    def get_agent_base_mysql_assistant_dwx(self, use_cache=True, add_message=''):
        """ Basic Agent, processing mysql data source """

        base_mysql_assistant = AssistantAgent(
            name="base_mysql_assistant",
            system_message="""You are a helpful AI assistant.
                Solve tasks using your coding and language skills.
                In the following cases, suggest python code (in a python coding block) for the user to execute.
                    1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
                    2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
                Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
                When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
                If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
                Do not merely substitute the statistical characteristics of the overall data with those derived from sample data. In any case, even if the sample data is insufficient, do not fabricate data for the sake of visualization. Instead, you should reassess whether there is a flaw in the execution logic and attempt again, or plainly acknowledge the limitation.If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                Reply "TERMINATE" in the end when everything is done.
                When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                Be careful to avoid using mysql special keywords in mysql code.
                """ + '\n' + self.agent_instance_util.base_mysql_info + '\n' + CONFIG.python_base_dependency + '\n' + self.agent_instance_util.quesion_answer_language + '\n' + add_message,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "config_list": self.agent_instance_util.config_list_gpt4_turbo,
                "seed": 42,  # change the seed for different trials
                "temperature": 0,
                "functions": self.function_config,
            },
            openai_proxy=self.agent_instance_util.openai_proxy,
            use_cache=use_cache,
        )
        return base_mysql_assistant

    def get_agent_python_executor_dwx(self, report_file_name=None):

        python_executor = PythonProxyAgent(
            name="python_executor",
            system_message="python executor. Execute the python code and report the result.",
            code_execution_config={"last_n_messages": 1, "work_dir": "paper"},
            human_input_mode="NEVER",
            websocket=self.websocket,
            user_name=self.user_name,
            default_auto_reply="TERMINATE",
            # outgoing=self.outgoing,
            # incoming=self.incoming,
            db_id=self.agent_instance_util.db_id,
            report_file_name=report_file_name,
            function_map=self.function_map,
        )
        return python_executor

    def set_function_call_dwx(self, db_info):

        if str(db_info['db']).__contains__('dwx_all'):
            # 电玩猩多店
            from ai.backend.util.db.db_dwx.dwx_all_mysql_rag_util import DwxMysqlRagUitl

            wxMysqlRagUitl = DwxMysqlRagUitl(db_info=db_info)
            self.function_map = {
                "get_total_daily_cost": wxMysqlRagUitl.get_total_daily_cost,
                "get_total_daily_cost_rate": wxMysqlRagUitl.get_total_daily_cost_rate,
                "get_total_daily_profit_rate": wxMysqlRagUitl.get_total_daily_profit_rate,
                "get_total_daily_income": wxMysqlRagUitl.get_total_daily_income,
                "get_total_monthly_cost": wxMysqlRagUitl.get_total_monthly_cost,
                "get_total_monthly_cost_rate": wxMysqlRagUitl.get_total_monthly_cost_rate,
                "get_total_monthly_income": wxMysqlRagUitl.get_total_monthly_income,
                "get_total_monthly_profit_rate": wxMysqlRagUitl.get_total_monthly_profit_rate,
                "base_question": self.base_question,
            }

            self.function_config = [
                {
                    "name": "get_total_daily_cost",
                    "description": "按日计算某个门店日成本",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one day，like '1999-01-01'",
                            },
                            "BusinessName": {
                                "type": "string",
                                "description": "单个门店完整名称，问题中会带上， 比如 'xxx店'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_daily_cost_rate",
                    "description": " 按日计算某个门店日成本率 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one day，like '1999-01-01'",
                            },
                            "BusinessName": {
                                "type": "string",
                                "description": "单个门店完整名称，问题中会带上， 比如 'xxx店'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_daily_profit_rate",
                    "description": " 按日计算某个门店日利润率 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one day，like '1999-01-01'",
                            },
                            "BusinessName": {
                                "type": "string",
                                "description": "单个门店完整名称，问题中会带上， 比如 'xxx店'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_daily_income",
                    "description": " 按日计算某个门店日消费 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one day，like '1999-01-01'",
                            },
                            "BusinessName": {
                                "type": "string",
                                "description": "单个门店完整名称，问题中会带上， 比如 'xxx店'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_monthly_cost",
                    "description": " 按月计算某个门店月成本 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one month，like '1999-01'",
                            },
                            "BusinessName": {
                                "type": "string",
                                "description": "单个门店完整名称，问题中会带上， 比如 'xxx店'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_monthly_cost_rate",
                    "description": " 按月计算某个门店月成本率 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one month，like '1999-01'",
                            },
                            "BusinessName": {
                                "type": "string",
                                "description": "单个门店完整名称，问题中会带上， 比如 'xxx店'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_monthly_income",
                    "description": " 按月计算某个门店月营收或月收入 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one month，like '1999-01'",
                            },
                            "BusinessName": {
                                "type": "string",
                                "description": "单个门店完整名称，问题中会带上， 比如 'xxx店'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_monthly_profit_rate",
                    "description": " 按月计算某个门店月利润率 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one month，like '1999-01'",
                            },
                            "BusinessName": {
                                "type": "string",
                                "description": "单个门店完整名称，问题中会带上， 比如 'xxx店'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "base_question",
                    "description": "获取其他问题的答案",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "qustion_message": {
                                "type": "string",
                                "description": "qustion message",
                            }
                        },
                        "required": ["qustion_message"],
                    },
                },

            ]

        else:
            # 电玩猩单店
            from ai.backend.util.db.db_dwx.dwx_mysql_rag_util import DwxMysqlRagUitl

            wxMysqlRagUitl = DwxMysqlRagUitl(db_info=db_info)
            self.function_map = {
                "get_total_daily_cost": wxMysqlRagUitl.get_total_daily_cost,
                "get_total_daily_cost_rate": wxMysqlRagUitl.get_total_daily_cost_rate,
                "get_total_daily_profit_rate": wxMysqlRagUitl.get_total_daily_profit_rate,
                "get_total_daily_income": wxMysqlRagUitl.get_total_daily_income,
                "get_total_monthly_cost": wxMysqlRagUitl.get_total_monthly_cost,
                "get_total_monthly_cost_rate": wxMysqlRagUitl.get_total_monthly_cost_rate,
                "get_total_monthly_income": wxMysqlRagUitl.get_total_monthly_income,
                "get_total_monthly_profit_rate": wxMysqlRagUitl.get_total_monthly_profit_rate,
                "base_question": self.base_question
            }

            self.function_config = [
                {
                    "name": "get_total_daily_cost",
                    "description": " 按日计算全店总成本 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one day，like '1999-01-01'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_daily_cost_rate",
                    "description": " 按日计算全店日成本率 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one day，like '1999-01-01'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_daily_profit_rate",
                    "description": " 按日计算全店日利润率 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one day，like '1999-01-01'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_daily_income",
                    "description": " 按日计算全店日营收或日收入",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one day，like '1999-01-01'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_monthly_cost",
                    "description": " 按月计算全店月成本 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one month，like '1999-01'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_monthly_cost_rate",
                    "description": " 按月计算全店月成本率 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one month，like '1999-01'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_monthly_income",
                    "description": " 按月计算全店月营收或月收入 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one month，like '1999-01'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "get_total_monthly_profit_rate",
                    "description": " 按月计算全店月利润率 ",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "date of one month，like '1999-01'",
                            }
                        },
                        "required": ["date"],
                    },
                },
                {
                    "name": "base_question",
                    "description": "获取其他问题的答案",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "qustion_message": {
                                "type": "string",
                                "description": "qustion message",
                            }
                        },
                        "required": ["qustion_message"],
                    },
                },

            ]

        return wxMysqlRagUitl

    async def base_question(self, qustion_message):
        table_comment = self.table_comment
        use_cache = self.use_cache

        base_mysql_assistant = self.get_agent_retrieve_base_mysql_assistant(use_cache=use_cache)
        python_executor = self.get_agent_retrieve_python_executor(
            docs_path=self.agent_instance_util.get_rag_doc())
        await python_executor.initiate_chat(
            base_mysql_assistant,
            problem=self.question_ask + '\n' + str(
                qustion_message),
            db_info='this is databases info: ' + '\n' + str(table_comment),
        )
        answer_message = python_executor.chat_messages[base_mysql_assistant]
        # python_executor_answer_message = base_mysql_assistant.chat_messages[python_executor]

        # answer_message = [base_mysql_assistant_answer_message, python_executor_answer_message]

        return answer_message

    async def total_question(self, qustion_message, db_info):

        wxMysqlRagUitl = self.set_function_call_dwx(db_info=db_info)

        base_mysql_assistant = self.get_agent_base_mysql_assistant_dwx(use_cache=self.use_cache,
                                                                       add_message='')
        python_executor = self.get_agent_python_executor_dwx()

        await python_executor.initiate_chat(
            base_mysql_assistant,
            message=
            # 'this is databases info: ' + '\n'
            # + str(table_comment)+ '\n'
            # + str(retrieve_rag_doc)+ '\n' +
            self.question_ask + '\n' + str(
                qustion_message),
        )

        # 关闭数据库链接
        wxMysqlRagUitl.connect_close()

        answer_message = python_executor.chat_messages[base_mysql_assistant]
        print("answer_message: ", answer_message)
        return answer_message

    async def base_question_llm(self, qustion_message):

        table_comment = self.table_comment
        use_cache = self.use_cache
        retrieve_rag_doc = await self.select_rag_doc(qustion_message, use_cache)

        base_mysql_assistant = self.agent_instance_util.get_agent_base_mysql_assistant(use_cache=use_cache,
                                                                   add_message='')
        python_executor = self.agent_instance_util.get_agent_python_executor()

        await python_executor.initiate_chat(
            base_mysql_assistant,
            message=
            'this is databases info: ' + '\n'
            + str(table_comment) + '\n'
            + str(retrieve_rag_doc) + '\n' +
            self.question_ask + '\n' + str(
                qustion_message),
        )
        answer_message = python_executor.chat_messages[base_mysql_assistant]

        return answer_message
