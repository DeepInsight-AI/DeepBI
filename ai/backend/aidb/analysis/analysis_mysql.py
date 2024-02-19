import traceback
import json
from ai.backend.util.write_log import logger
from ai.backend.base_config import CONFIG
from ai.backend.util import database_util
from .analysis import Analysis
import re
import ast
from ai.agents.agentchat import AssistantAgent
from ai.backend.util import base_util

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
            for i in range(max_retry_times):
                try:
                    if self.agent_instance_util.is_rag:
                        table_comment = await self.select_table_comment(qustion_message)
                        answer_message = await self.task_base_rag(qustion_message, table_comment)
                    else:
                        base_mysql_assistant = self.agent_instance_util.get_agent_base_mysql_assistant()
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
                        table_comment = await self.select_table_comment(qustion_message)
                        answer_message = await self.task_generate_echart_rag(qustion_message, table_comment, use_cache)
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

            bi_proxy = self.agent_instance_util.get_agent_bi_proxy()
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

                    re_str = await bi_proxy.run_echart_code(str(echart_code), echart_name)
                    base_mess.append(re_str)

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
                            base_mess) + '\n' + self.question_ask + '\n' + question_supplement,
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

    async def task_base_rag(self, qustion_message, table_comment):
        """ Task type: mysql data analysis"""
        base_mysql_assistant = self.agent_instance_util.get_agent_base_mysql_assistant()
        python_executor = self.agent_instance_util.get_agent_python_executor()

        await python_executor.initiate_chat(
            base_mysql_assistant,
            message='this is databases info: ' + '\n' + str(table_comment) + '\n' + self.question_ask + '\n' + str(
                qustion_message),
        )
        ############################################
        # base_mysql_assistant = self.get_agent_retrieve_base_mysql_assistant_rag()
        # docs_path = CONFIG.up_file_path + '.rag_' + str(self.user_name) + '_' + str(
        #     self.agent_instance_util.db_id) + '.json'
        # python_executor = self.get_agent_retrieve_python_executor(docs_path=self.agent_instance_util.rag_doc)
        #
        # await python_executor.initiate_chat(
        #     base_mysql_assistant,
        #     problem='this is table info: ' + '\n' + str(table_comment) + '\n' + self.question_ask + '\n' + str(
        #         qustion_message),
        # )
        ############################################

        answer_message = python_executor.chat_messages[base_mysql_assistant]
        print("answer_message: ", answer_message)

        return answer_message

    async def task_generate_echart_rag(self, qustion_message, table_comment, use_cache):
        mysql_echart_assistant = self.agent_instance_util.get_agent_mysql_echart_assistant(
            use_cache=use_cache)
        python_executor = self.agent_instance_util.get_agent_python_executor()

        await python_executor.initiate_chat(
            mysql_echart_assistant,
            message='this is table info: ' + '\n' + str(table_comment) + '\n' + self.question_ask + '\n' + str(
                qustion_message),
        )

        # base_mysql_assistant = self.get_agent_retrieve_base_mysql_assistant_rag()

                  # docs_path = CONFIG.up_file_path + '.rag_' + str(self.user_name) + '_' + str(
        #     self.agent_instance_util.db_id) + '.json'
        # python_executor = self.get_agent_retrieve_python_executor(docs_path=docs_path)
        #
        # await python_executor.initiate_chat(
        #     base_mysql_assistant,
        #     problem='this is table info: ' + '\n' + str(table_comment) + '\n' + self.question_ask + '\n' + str(
        #         qustion_message),
        # )

        answer_message = mysql_echart_assistant.chat_messages[python_executor]
        print("answer_message: ", answer_message)

        return answer_message
