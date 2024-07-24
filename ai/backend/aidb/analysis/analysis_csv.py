import traceback
import json
from ai.backend.util.write_log import logger
from ai.backend.base_config import CONFIG
from .analysis import Analysis
import pandas as pd
from ai.backend.util import base_util
import re
import ast
from ai.agents.agentchat import AssistantAgent
import chardet

language_chinese = CONFIG.language_chinese
max_retry_times = CONFIG.max_retry_times


class AnalysisCsv(Analysis):
    async def deal_question(self, json_str, message):
        """  Process csv data source and select the corresponding workflow """
        result = {'state': 200, 'data': {}, 'receiver': ''}
        q_sender = json_str['sender']
        q_data_type = json_str['data']['data_type']
        print('q_data_type : ', q_data_type)
        q_str = json_str['data']['content']

        if not self.agent_instance_util.api_key_use:
            re_check = await self.check_api_key()
            if not re_check:
                return

        if q_sender == CONFIG.talker_user:
            if q_data_type == CONFIG.type_question:
                if self.agent_instance_util.base_message is not None:
                    await self.start_chatgroup(q_str)

                    # try:
                    #     await self.start_chatgroup_csv(q_str)
                    # except Exception as e:
                    #     traceback.print_exc()
                    #     logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))
                    #     await self.put_message(200, receiver=CONFIG.talker_user, data_type=CONFIG.type_answer,
                    #                            content=self.error_message_timeout)
                else:
                    await self.put_message(500, receiver=CONFIG.talker_user, data_type=CONFIG.type_answer,
                                           content=self.error_miss_data)
        elif q_sender == CONFIG.talker_bi:
            if q_data_type == CONFIG.type_comment:
                result['receiver'] = 'bi'
                result['data']['data_type'] = 'mysql_comment'

                await self.check_data_csv(q_str)
            elif q_data_type == CONFIG.type_comment_first:
                if json_str.get('data').get('language_mode'):
                    q_language_mode = json_str['data']['language_mode']
                    if q_language_mode == CONFIG.language_chinese or q_language_mode == CONFIG.language_english or q_language_mode == CONFIG.language_japanese:
                        self.set_language_mode(q_language_mode)
                        self.agent_instance_util.set_language_mode(q_language_mode)

                if CONFIG.database_model == 'online':
                    # Set csv basic information
                    self.agent_instance_util.set_base_csv_info(q_str)
                    self.agent_instance_util.base_message = str(q_str)
                else:
                    self.agent_instance_util.set_base_csv_info(q_str)
                    self.agent_instance_util.base_message = str(q_str)

                await self.get_data_desc(q_str)
            elif q_data_type == CONFIG.type_comment_second:
                if json_str.get('data').get('language_mode'):
                    q_language_mode = json_str['data']['language_mode']
                    if q_language_mode == CONFIG.language_chinese or q_language_mode == CONFIG.language_english or q_language_mode == CONFIG.language_japanese:
                        self.set_language_mode(q_language_mode)
                        self.agent_instance_util.set_language_mode(q_language_mode)

                if CONFIG.database_model == 'online':
                    self.agent_instance_util.set_base_csv_info(q_str)
                    self.agent_instance_util.base_message = str(q_str)
                else:
                    self.agent_instance_util.set_base_csv_info(q_str)
                    self.agent_instance_util.base_message = str(q_str)

                await self.put_message(200, receiver=CONFIG.talker_bi, data_type=CONFIG.type_comment_second,
                                       content='')

            elif q_data_type == 'mysql_code' or q_data_type == 'chart_code' or q_data_type == 'delete_chart' or q_data_type == 'ask_data':
                self.delay_messages['bi'][q_data_type].append(message)
                print("delay_messages : ", self.delay_messages)
                return

    async def start_chatgroup_csv(self, q_str):

        user_proxy = self.get_agent_user_proxy()
        base_csv_assistant = self.get_agent_select_analysis_assistant()

        await user_proxy.initiate_chat(
            base_csv_assistant,
            message=self.agent_instance_util.base_csv_info + '\n' + """ The following is an introduction to the data in the csv file:""" + '\n' + self.agent_instance_util.base_message + '\n' + self.question_ask + '\n' + str(
                q_str),
        )

    async def check_data_csv(self, q_str):
        """Check the data description to see if it meets the requirements"""
        print("CONFIG.up_file_path  : " + CONFIG.up_file_path)
        if q_str.get('table_desc'):
            for tb in q_str.get('table_desc'):
                if len(tb.get('field_desc')) == 0:
                    table_name = tb.get('table_name')

                    # Read file and detect encoding
                    csv_file = CONFIG.up_file_path + table_name
                    f = open(csv_file, 'rb')
                    # Read the file using the detected encoding
                    encoding = chardet.detect(f.read())['encoding']
                    f.close()
                    if str(table_name).endswith('.csv'):
                        data = pd.read_csv(open(csv_file, encoding=encoding, errors='ignore'))
                    else:
                        data = pd.read_excel(csv_file)

                    # Get column headers (first row of data)
                    column_titles = list(data.columns)
                    # print("column_titles : ", column_titles)

                    for i in range(len(column_titles)):
                        tb['field_desc'].append({
                            "name": column_titles[i],
                            "comment": '',
                            "in_use": 1
                        })

        await self.check_data_base(q_str)

    async def task_generate_echart(self, qustion_message):
        """ Task type: csv echart code block"""
        try:
            base_content = []
            base_mess = []
            report_demand_list = []
            json_str = ""
            error_times = 0
            use_cache = True
            for i in range(max_retry_times):
                try:
                    csv_echart_assistant = self.agent_instance_util.get_agent_csv_echart_assistant(use_cache)
                    python_executor = self.agent_instance_util.get_agent_python_executor()

                    await python_executor.initiate_chat(
                        csv_echart_assistant,
                        message=self.agent_instance_util.base_message + '\n' + self.question_ask + '\n' + str(
                            qustion_message),
                    )

                    answer_message = csv_echart_assistant.chat_messages[python_executor]

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

                    # if len(base_content) > 0:
                    # base_mess.append(answer_mess)
                    # break
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
            # 调用接口生成图片
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

            error_times = 0  # 失败次数
            for i in range(max_retry_times):
                try:
                    planner_user = self.agent_instance_util.get_agent_planner_user()
                    analyst = self.agent_instance_util.get_agent_analyst()

                    question_supplement = 'Please make an analysis and summary in English, including which charts were generated, and briefly introduce the contents of these charts.'
                    if self.language_mode == language_chinese:
                        # question_supplement = " 请用中文做一下分析总结，内容包括哪些图表生成了，简单介绍一下这些图表的内容。"
                        if is_chart:
                            question_supplement = " 请用中文，简单介绍一下已生成图表中的数据内容."
                        else:
                            question_supplement = " 请用中文，从上诉对话中分析总结出问题的答案."
                    elif self.language_mode == CONFIG.language_japanese:
                        if is_chart:
                            question_supplement = " 生成されたグラフのデータ内容について、簡単に日本語で説明してください。"
                        else:
                            question_supplement = " 上記の対話から問題の答えを分析し、日本語で要約してください。"

                    # 1,开始分析
                    await planner_user.initiate_chat(
                        analyst,
                        message=str(base_mess) + '\n' + str(
                            qustion_message) + '\n' + self.question_ask + '\n' + question_supplement,
                    )

                    answer_message = planner_user.last_message()["content"]
                    print("answer_message: ", answer_message)
                    return answer_message

                except Exception as e:
                    traceback.print_exc()
                    logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                    error_times = error_times + 1

            if error_times == max_retry_times:
                # return "十分抱歉，本次AI-GPT接口调用超时，请再次重试"
                return self.error_message_timeout

            if self.language_mode == language_chinese:
                return "十分抱歉，无法回答您的问题，请检查相关数据是否充分。"
            elif self.language_mode == CONFIG.language_japanese:
                return "申し訳ありませんが、質問にお答えできません。関連データが十分かどうかを確認してください。"
            else:
                return 'Sorry, we cannot answer your question. Please check whether the relevant data is sufficient.'
        except Exception as e:
            print(e)
            # 使用 traceback 打印详细信息
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))

        if self.language_mode == language_chinese:
            return "分析数据失败，请检查相关数据是否充分。"
        elif self.language_mode == CONFIG.language_japanese:
            return "データの分析に失敗しました。関連データが十分かどうかを確認してください。"
        else:
            return 'Failed to analyze data, please check whether the relevant data is sufficient.'

    async def task_base(self, qustion_message):
        """ Task type:  data analysis"""
        try:
            error_times = 0
            for i in range(max_retry_times):
                try:
                    base_mysql_assistant = self.get_agent_base_csv_assistant()
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

    def get_agent_base_csv_assistant(self):
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
                       """ + '\n' + self.agent_instance_util.base_csv_info + '\n' + CONFIG.python_base_dependency + '\n' + self.agent_instance_util.quesion_answer_language,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "config_list": self.agent_instance_util.config_list_gpt4_turbo,
                "request_timeout": CONFIG.request_timeout,
            },
            openai_proxy=self.agent_instance_util.openai_proxy,
        )
        return base_csv_assistant
