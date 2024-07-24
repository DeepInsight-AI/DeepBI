# coding:utf-8
import traceback
import json
from ai.backend.util.write_log import logger
from ai.backend.base_config import CONFIG
from ai.backend.util import database_util
from .autopilot import Autopilot
import re
import ast
from ai.backend.util import base_util
from ai.backend.util.db.postgresql_report import PsgReport
from ai.agents.agentchat import Questioner, AssistantAgent
from ai.backend.language_info import LanguageInfo

max_retry_times = CONFIG.max_retry_times
max_report_question = 5


class AutopilotMongoDB(Autopilot):
    # new db
    async def deal_question(self, json_str):
        """
        Process mongodb data source and select the corresponding workflow
        """

        report_file_name = CONFIG.up_file_path + json_str['file_name']
        report_id = json_str['report_id']

        with open(report_file_name, 'r') as file:
            data = json.load(file)
        db_comment = data['db_comment']
        db_id = str(data['databases_id'])
        q_str = data['report_desc']
        q_name = data['report_name']

        print("self.agent_instance_util.api_key_use :", self.agent_instance_util.api_key_use)

        if not self.agent_instance_util.api_key_use:
            re_check = await self.check_api_key(is_auto_pilot=True)
            if not re_check:
                return

        print("db_id:", db_id)
        obj = database_util.Main(db_id)
        if_suss, db_info = obj.run()
        if if_suss:
            self.agent_instance_util.base_mongodb_info = '  When connecting to the database, be sure to bring the port. This is mongodb database info :' + '\n' + str(
                db_info)
            # self.agent_instance_util.base_message = str(db_comment)
            self.agent_instance_util.set_base_message(db_comment)

            self.agent_instance_util.db_id = db_id
            # start chat
            try:
                psg = PsgReport()
                re = psg.select_data(report_id)
                if re is not None and len(re) > 0:
                    print('need deal task')
                    data_to_update = (1, report_id)
                    update_state = psg.update_data(data_to_update)
                    if update_state:
                        # new db
                        await self.start_chatgroup(q_str, report_file_name, report_id, q_name)
                else:
                    print('no task')

            except Exception as e:
                traceback.print_exc()
                # update report status
                data_to_update = (-1, report_id)
                PsgReport().update_data(data_to_update)

    async def task_base(self, qustion_message):
        return qustion_message

    def get_agent_base_mongodb_assistant(self):
        """ Basic Agent, processing mongodb data source """
        base_mongodb_assistant = AssistantAgent(
            name="base_mongodb_assistant",
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
                  Be careful to avoid using mongodb special keywords in mongodb code.
                  """ + '\n' + self.agent_instance_util.base_mongodb_info + '\n' + CONFIG.python_base_dependency + '\n' + self.agent_instance_util.quesion_answer_language,
            human_input_mode="NEVER",
            user_name=self.user_name,
            websocket=self.websocket,
            llm_config={
                "config_list": self.agent_instance_util.config_list_gpt4_turbo,
                "request_timeout": CONFIG.request_timeout,
            },
            openai_proxy=self.agent_instance_util.openai_proxy,
        )
        return base_mongodb_assistant

    async def start_chatgroup(self, q_str, report_file_name, report_id, q_name):

        report_html_code = {}
        try:
            report_html_code['report_name'] = q_name
            report_html_code['report_author'] = 'DeepBI'

            report_html_code['report_question'] = []
            report_html_code['report_thought'] = []
            report_html_code['report_analyst'] = []

            question_message = await self.generate_quesiton(q_str, report_file_name)

            print('question_message :', question_message)

            report_html_code['report_thought'] = question_message

            question_list = []
            que_num = 0
            for ques in question_message:
                print('ques :', ques)
                report_demand = 'i need a echart report , ' + ques['report_name'] + ' : ' + ques['description']
                # report_demand = ' 10-1= ?? '
                print("report_demand: ", report_demand)

                question = {}
                question['question'] = ques
                que_num = que_num + 1
                if que_num > max_report_question:
                    break
                # new db
                answer_message, echart_code = await self.task_generate_echart(str(report_demand), report_file_name)
                if answer_message is not None and echart_code is not None:
                    question['answer'] = answer_message
                    question['echart_code'] = echart_code
                    report_html_code['report_question'].append(question)

                question_obj = {'question': report_demand, 'answer': answer_message, 'echart_code': ""}
                question_list.append(question_obj)

            print('question_list:   ', question_list)

            planner_user = self.agent_instance_util.get_agent_planner_user(report_file_name=report_file_name)
            analyst = self.get_agent_analyst(report_file_name=report_file_name)

            question_supplement = " Make a final summary of the report and give me valuable conclusions. "

            await planner_user.initiate_chat(
                analyst,
                message=str(
                    question_list) + '\n' + " This is the goal of this report: " + '\n' + q_str + '\n' + LanguageInfo.question_ask + '\n' + question_supplement,
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

            last_analyst_str = str(json_str).replace("\n", "")
            if len(last_analyst_str) > 0:
                print("chart_code_str: ", last_analyst_str)
                if base_util.is_json(last_analyst_str):
                    report_demand_list = json.loads(last_analyst_str)
                    report_html_code['report_analyst'] = report_demand_list
                else:
                    report_demand_list = ast.literal_eval(last_analyst_str)
                    report_html_code['report_analyst'] = report_demand_list

        except Exception as e:
            traceback.print_exc()
            data_to_update = (-1, report_id)
            PsgReport().update_data(data_to_update)
        else:
            # 更新数据
            data_to_update = (2, report_id)
            PsgReport().update_data(data_to_update)

        print('report_html_code +++++++++++++++++ :', report_html_code)
        if len(report_html_code['report_thought']) > 0:
            rendered_html = self.generate_report_template(report_html_code)
            with open(report_file_name, 'r') as file:
                data = json.load(file)

            # 修改其中的值
            data['html_code'] = rendered_html
            # if self.log_list is not None:
            #     data['chat_log'] = self.log_list

            # 将更改后的内容写回文件
            with open(report_file_name, 'w') as file:
                json.dump(data, file, indent=4)

    async def generate_quesiton(self, q_str, report_file_name):
        questioner = self.get_agent_questioner(report_file_name)
        ai_analyst = self.get_agent_ai_analyst(report_file_name)

        message = self.agent_instance_util.base_message + '\n' + LanguageInfo.question_ask + '\n\n' + q_str
        print(' generate_quesiton message:  ', message)

        await questioner.initiate_chat(
            ai_analyst,
            message=message,
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
                        report_demand_list = None
                        if base_util.is_json(chart_code_str):
                            report_demand_list = json.loads(chart_code_str)
                        else:
                            # String instantiated as object
                            report_demand_list = ast.literal_eval(chart_code_str)
                        print("report_demand_list: ", report_demand_list)
                        if report_demand_list is not None:
                            for jstr in report_demand_list:
                                # 检查列表中是否存在相同名字的对象
                                name_exists = any(item['report_name'] == jstr['report_name'] for item in base_content)

                                if not name_exists:
                                    if len(base_content) > max_report_question:
                                        break
                                    base_content.append(jstr)
                                    # print("插入成功")
                                else:
                                    print("对象已存在，不重复插入")
        return base_content

    async def task_generate_echart(self, qustion_message, report_file_name):
        # new db
        try:
            base_content = []
            base_mess = []
            report_demand_list = []
            json_str = ""
            error_times = 0
            use_cache = True
            for i in range(max_retry_times):
                try:
                    # new db
                    mongodb_echart_assistant = self.agent_instance_util.get_agent_mongodb_echart_assistant(
                        use_cache=use_cache, report_file_name=report_file_name)

                    python_executor = self.agent_instance_util.get_agent_python_executor(
                        report_file_name=report_file_name, is_auto_pilot=True)
                    # new db
                    await python_executor.initiate_chat(
                        mongodb_echart_assistant,
                        message=self.agent_instance_util.base_message + '\n' + LanguageInfo.question_ask + '\n' + str(
                            qustion_message),
                    )

                    answer_message = mongodb_echart_assistant.chat_messages[python_executor]

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
                    # 格式化echart_code
                    try:
                        if base_util.is_json(str(echart_code)):
                            json_str = json.loads(str(echart_code))
                            json_str = json.dumps(json_str)
                            last_echart_code = json_str
                        else:
                            str_obj = ast.literal_eval(str(echart_code))
                            json_str = json.dumps(str_obj)
                            last_echart_code = json_str
                    except Exception as e:
                        traceback.print_exc()
                        logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                        last_echart_code = json.dumps(echart_code)

                    # re_str = await bi_proxy.run_echart_code(str(echart_code), echart_name)
                    # base_mess.append(re_str)
                    base_mess = []
                    base_mess.append(img_str)

            error_times = 0
            for i in range(max_retry_times):
                try:
                    planner_user = self.agent_instance_util.get_agent_planner_user(report_file_name=report_file_name)
                    analyst = self.get_agent_analyst(report_file_name=report_file_name)

                    question_supplement = qustion_message + '\n' + "Analyze the above report data and give me valuable conclusions"
                    print("question_supplement : ", question_supplement)

                    await planner_user.initiate_chat(
                        analyst,
                        message=str(base_mess) + '\n' + LanguageInfo.question_ask + '\n' + question_supplement,
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
                print(self.error_message_timeout)
                return None, None

        except Exception as e:
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
        print(self.agent_instance_util.data_analysis_error)
        return None, None
