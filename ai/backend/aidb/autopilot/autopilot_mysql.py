# coding:utf-8
import traceback
import json
from ai.backend.util.write_log import logger
from ai.backend.base_config import CONFIG
from ai.backend.util import database_util
from .autopilot import Autopilot
import re
import ast
from ai.agents.agentchat import AssistantAgent
from ai.backend.util import base_util

max_retry_times = CONFIG.max_retry_times


class AutopilotMysql(Autopilot):

    async def deal_question(self, json_str, message):
        """
        Process mysql data source and select the corresponding workflow
        """
        result = {'state': 200, 'data': {}, 'receiver': ''}
        q_sender = json_str['sender']
        q_data_type = json_str['data']['data_type']
        print('q_data_type : ', q_data_type)
        q_str = json_str['data']['content']
        print('q_str: ', q_str)

        print("self.agent_instance_util.api_key_use :", self.agent_instance_util.api_key_use)

        if not self.agent_instance_util.api_key_use:
            re_check = await self.check_api_key()
            if not re_check:
                return

        if q_sender == 'user':
            if q_data_type == 'question':
                # print("agent_instance_util.base_message :", self.agent_instance_util.base_message)
                if self.agent_instance_util.base_message is not None:
                    try:
                        await self.start_chatgroup(q_str)
                    except Exception as e:
                        traceback.print_exc()
                        logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))

                        result['receiver'] = 'user'
                        result['data']['data_type'] = 'answer'
                        result['data']['content'] = self.error_message_timeout
                        consume_output = json.dumps(result)
                        await self.outgoing.put(consume_output)
                else:
                    await self.put_message(500, receiver=CONFIG.talker_user, data_type=CONFIG.type_answer,
                                           content=self.error_miss_data)
        elif q_sender == CONFIG.talker_bi:
            if q_data_type == CONFIG.type_comment:
                await self.check_data_base(q_str)
            elif q_data_type == CONFIG.type_comment_first:
                if json_str.get('data').get('language_mode'):
                    q_language_mode = json_str['data']['language_mode']
                    if q_language_mode == CONFIG.language_chinese or q_language_mode == CONFIG.language_english:
                        self.set_language_mode(q_language_mode)
                        self.agent_instance_util.set_language_mode(q_language_mode)

                if CONFIG.database_model == 'online':
                    print('online')
                    databases_id = json_str['data']['databases_id']
                    db_id = str(databases_id)
                    obj = database_util.Main(db_id)
                    if_suss, db_info = obj.run()
                    if if_suss:
                        self.agent_instance_util.base_mysql_info = ' When connecting to the database, be sure to bring the port. This is mysql database info :' + '\n' + str(
                            db_info)
                        self.agent_instance_util.base_message = str(q_str)
                        self.agent_instance_util.db_id = db_id

                else:
                    self.agent_instance_util.base_message = str(q_str)

                await self.get_data_desc(q_str)
            elif q_data_type == CONFIG.type_comment_second:
                print(CONFIG.type_comment_second)
                if json_str.get('data').get('language_mode'):
                    q_language_mode = json_str['data']['language_mode']
                    if q_language_mode == CONFIG.language_chinese or q_language_mode == CONFIG.language_english:
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
                        self.agent_instance_util.base_message = str(q_str)
                        self.agent_instance_util.db_id = db_id
                else:
                    self.agent_instance_util.base_message = str(q_str)

                await self.put_message(200, receiver=CONFIG.talker_bi, data_type=CONFIG.type_comment_second,
                                       content='')
            elif q_data_type == 'mysql_code' or q_data_type == 'chart_code' or q_data_type == 'delete_chart' or q_data_type == 'ask_data':
                self.delay_messages['bi'][q_data_type].append(message)
                print("delay_messages : ", self.delay_messages)
                return
        else:
            print("q_sender is not right")
    async def task_base(self, qustion_message):
        """ Task type: mysql data analysis"""
        try:
            error_times = 0
            for i in range(max_retry_times):
                try:
                    base_mysql_assistant = self.get_agent_base_mysql_assistant()
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

    def get_agent_base_mysql_assistant(self):
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
                  If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
                  When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
                  Reply "TERMINATE" in the end when everything is done.
                  When you find an answer,  You are a report analysis, you have the knowledge and skills to turn raw data into information and insight, which can be used to make business decisions.include your analysis in your reply.
                  Be careful to avoid using mysql special keywords in mysql code.
                  """ + '\n' + self.agent_instance_util.base_mysql_info + '\n' + CONFIG.python_base_dependency + '\n' + self.agent_instance_util.quesion_answer_language,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "config_list": self.agent_instance_util.config_list_gpt4_turbo,
                "request_timeout": CONFIG.request_timeout,
            },
            openai_proxy=self.agent_instance_util.openai_proxy,
        )
        return base_mysql_assistant

    async def start_chatgroup(self, q_str):

        report_html_code = {}
        report_html_code['report_name'] = '电商销售报告'

        report_html_code['report_question'] = []

        question_message = await self.generate_quesiton(q_str)
        print('question_message :', question_message)

        report_html_code['report_thought'] = question_message

        question_list = []
        que_num = 1
        for ques in question_message:
            print('ques :', ques)
            report_demand = 'i need a echart report , ' + ques['report_name'] + ':' + ques['description']
            print("report_demand: ", report_demand)

            question = {}
            question['question'] = ques
            que_num = que_num + 1
            if que_num > 5:
                break

            answer_message, echart_code = await self.task_generate_echart(str(report_demand))
            question['answer'] = answer_message
            question['echart_code'] = echart_code
            report_html_code['report_question'].append(question)

            question_obj = {'question': report_demand, 'answer': answer_message, 'echart_code': ""}
            question_list.append(question_obj)

        print('question_list:   ', question_list)

        planner_user = self.agent_instance_util.get_agent_planner_user()
        analyst = self.get_agent_analyst()

        question_supplement = 'Please make an analysis and summary in English, including which charts were generated, and briefly introduce the contents of these charts.'
        if self.language_mode == CONFIG.language_chinese:
            question_supplement = " 请用中文帮我对报告做最终总结，给我有价值的结论"

        await planner_user.initiate_chat(
            analyst,
            message=str(
                question_list) + '\n' + "这是本次报告的目标: " + '\n' + q_str + '\n' + self.question_ask + '\n' + question_supplement,
        )

        last_analyst = planner_user.last_message()["content"]

        print('last_analyst : ', last_analyst)

        match = re.search(
            r"\[.*\]", last_analyst.strip(), re.MULTILINE | re.IGNORECASE | re.DOTALL
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
                report_html_code['report_analyst'] = report_demand_list
            else:
                report_demand_list = ast.literal_eval(chart_code_str)
                report_html_code['report_analyst'] = report_demand_list

        print('report_html_code +++++++++++++++++ :', report_html_code)

        rendered_html = self.generate_report_template(report_html_code)

        result_message = {
            'state': 200,
            'receiver': 'autopilot',
            'data': {
                'data_type': 'autopilot_code',
                'content': rendered_html
            }
        }

        send_json_str = json.dumps(result_message)
        await self.websocket.send(send_json_str)

    async def generate_quesiton(self, q_str):
        questioner = self.get_agent_questioner()
        ai_analyst = self.get_agent_ai_analyst()

        message = self.agent_instance_util.base_message + '\n' + self.question_ask + '\n\n' + q_str
        print(' generate_quesiton message:  ', message)

        await questioner.initiate_chat(
            ai_analyst,
            message=self.agent_instance_util.base_message + '\n' + self.question_ask + '\n\n' + q_str,
        )

        base_content = []
        question_message = ai_analyst.chat_messages[questioner]
        print('question_message : ', question_message)
        for answer_mess in question_message:
            # print("answer_mess :", answer_mess)
            if answer_mess['content']:
                if str(answer_mess['role']) == 'assistant':

                    answer_mess_content = str(answer_mess['content']).replace('\n', '')

                    print("answer_mess: ", answer_mess)
                    match = re.search(
                        r"\[.*\]", answer_mess_content.strip(), re.MULTILINE | re.IGNORECASE | re.DOTALL
                    )
                    json_str = ''
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

                                # 检查列表中是否存在相同名字的对象
                                name_exists = any(item['report_name'] == jstr['report_name'] for item in base_content)

                                if not name_exists:
                                    base_content.append(jstr)
                                    print("插入成功")
                                else:
                                    print("对象已存在，不重复插入")


                        else:
                            # String instantiated as object
                            report_demand_list = ast.literal_eval(chart_code_str)
                            print("report_demand_list: ", report_demand_list)
                            for jstr in report_demand_list:

                                # 检查列表中是否存在相同名字的对象
                                name_exists = any(item['report_name'] == jstr['report_name'] for item in base_content)

                                if not name_exists:
                                    base_content.append(jstr)
                                    print("插入成功")
                                else:
                                    print("对象已存在，不重复插入")

        return base_content

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

            # bi_proxy = self.agent_instance_util.get_agent_bi_proxy()
            is_chart = False
            # Call the interface to generate pictures
            last_echart_code = None
            for img_str in base_content:
                echart_name = img_str.get('echart_name')
                echart_code = img_str.get('echart_code')

                if len(echart_code) > 0 and str(echart_code).__contains__('x'):
                    is_chart = True
                    print("echart_name : ", echart_name)
                    last_echart_code = json.dumps(echart_code)
                    # re_str = await bi_proxy.run_echart_code(str(echart_code), echart_name)
                    # base_mess.append(re_str)
                    base_mess = []
                    base_mess.append(img_str)

            error_times = 0
            for i in range(max_retry_times):
                try:
                    planner_user = self.agent_instance_util.get_agent_planner_user()
                    analyst = self.get_agent_analyst()

                    question_supplement = 'Please make an analysis and summary in English, including which charts were generated, and briefly introduce the contents of these charts.'
                    if self.language_mode == CONFIG.language_chinese:
                        question_supplement = qustion_message + ".  请用中文帮我分析以上的报表数据，给我有价值的结论"
                        print("question_supplement : ", question_supplement)

                    await planner_user.initiate_chat(
                        analyst,
                        message=str(
                            base_mess) + '\n' + self.question_ask + '\n' + question_supplement,
                    )

                    answer_message = planner_user.last_message()["content"]

                    match = re.search(
                        r"\[.*\]", answer_message.strip(), re.MULTILINE | re.IGNORECASE | re.DOTALL
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
                            return report_demand_list, last_echart_code
                        else:
                            report_demand_list = ast.literal_eval(chart_code_str)
                            return report_demand_list, last_echart_code

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
