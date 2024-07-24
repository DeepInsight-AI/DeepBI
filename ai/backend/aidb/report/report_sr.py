from .report import Report
import json
from ai.backend.util.write_log import logger
import traceback
from ai.backend.base_config import CONFIG
from ai.backend.util import database_util
from ai.backend.util.token_util import num_tokens_from_messages

max_retry_times = CONFIG.max_retry_times

class ReportStarrocks(Report):
    async def deal_report(self, json_str, message):
        """ Process mysql data sources, generate reports, and select corresponding workflows """
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
                else:
                    await self.put_message(500, receiver=CONFIG.talker_user, data_type=CONFIG.type_answer,
                                           content=self.error_miss_data)

        elif q_sender == CONFIG.talker_bi:
            if q_data_type == 'mysql_comment':
                await self.check_data_base(q_str)
            elif q_data_type == CONFIG.type_comment_first:
                if json_str.get('data').get('language_mode'):
                    q_language_mode = json_str['data']['language_mode']
                    if q_language_mode == CONFIG.language_chinese or q_language_mode == CONFIG.language_english or q_language_mode == CONFIG.language_japanese:
                        self.set_language_mode(q_language_mode)
                        self.agent_instance_util.set_language_mode(q_language_mode)

                if CONFIG.database_model == 'online':
                    # Set database account information
                    databases_id = json_str['data']['databases_id']
                    db_id = str(databases_id)
                    obj = database_util.Main(db_id)
                    if_suss, db_info = obj.run()
                    if if_suss:
                        self.agent_instance_util.base_mysql_info = '  When connecting to the database, be sure to bring the port. This is database info :' + '\n' + str(
                            db_info)
                        # self.agent_instance_util.base_message = str(q_str)
                        self.agent_instance_util.set_base_message(q_str)

                else:
                    # self.agent_instance_util.base_message = str(q_str)
                    self.agent_instance_util.set_base_message(q_str)

                # result['data']['content'] = json_str['data']['content']

                await self.get_data_desc(q_str)
            elif q_data_type == 'mysql_comment_second':
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
                        self.agent_instance_util.base_mysql_info = '  When connecting to the database, be sure to bring the port. This is database info :' + '\n' + str(
                            db_info)
                        # self.agent_instance_util.base_message = str(q_str)
                        self.agent_instance_util.set_base_message(q_str)

                else:
                    # self.agent_instance_util.base_message = str(q_str)
                    self.agent_instance_util.set_base_message(q_str)

                # result = ask_commentengineer(q_str, result)
                # result['data']['content'] = await get_data_desc(agent_instance_util, q_str)

                result['receiver'] = 'bi'
                result['data']['data_type'] = 'mysql_comment_second'
                # result['data']['content'] = json_str['data']['content']
                consume_output = json.dumps(result)
                await self.outgoing.put(consume_output)
            elif q_data_type == 'mysql_code' or q_data_type == 'chart_code' or q_data_type == 'delete_chart' or q_data_type == 'ask_data':
                self.delay_messages['bi'][q_data_type].append(message)
                print("delay_messages : ", self.delay_messages)
                return

    async def task_generate_report(self, qustion_message):
        """ Task type 1: Call BI and generate reports """
        try:
            answer_contents = []
            error_times = 0
            for i in range(max_retry_times):
                try:
                    planner_user = self.agent_instance_util.get_agent_planner_user()
                    mysql_engineer = self.agent_instance_util.get_agent_mysql_engineer()
                    bi_proxy = self.agent_instance_util.get_agent_bi_proxy()
                    chart_presenter = self.agent_instance_util.get_agent_chart_presenter()

                    agents = [mysql_engineer, bi_proxy, chart_presenter]

                    manager = self.agent_instance_util.get_agent_GroupChatManager(agents)

                    await planner_user.initiate_chat(
                        manager,
                        message='This is database related information: ' + '\n' + self.agent_instance_util.base_message + '\n' + " This is my question: " + '\n' + str(
                            qustion_message),
                    )

                    answer_message = manager._oai_messages[bi_proxy]
                    is_done = False
                    for answer_mess in answer_message:
                        if answer_mess['role']:
                            if answer_mess['role'] == 'function':
                                answer_contents.append(answer_mess)
                                print("answer_mess: ", answer_mess)
                                is_done = True
                    if is_done:
                        break
                except Exception as e:
                    traceback.print_exc()
                    logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))
                    error_times = error_times + 1

            if error_times == max_retry_times:
                return self.error_message_timeout

            if len(answer_contents) > 0:
                answer_contents.append(answer_message)

            max_num = len(answer_contents)
            for i in range(max_num):

                message = [
                    {
                        "role": "system",
                        "content": str(answer_contents),
                    }
                ]

                num_tokens = num_tokens_from_messages(message, model='gpt-4')
                print("num_tokens : ", num_tokens)
                if num_tokens < 20000:
                    error_times = 0
                    for i in range(max_retry_times):
                        try:
                            planner_user = self.agent_instance_util.get_agent_planner_user()
                            analyst = self.agent_instance_util.get_agent_analyst()

                            if self.language_mode == CONFIG.language_chinese:
                                que_str = " 以下是我的问题，请用中文回答: " + '\n' + " 简单介绍一下已生成图表中的数据内容 "
                            elif self.language_mode == CONFIG.language_japanese:
                                que_str = " 以下が私の質問です。日本語で回答してください: " + '\n' + " 生成されたグラフのデータ内容を簡単に説明してください。"
                            else:
                                que_str = " The following is my question, please answer it in English: " + '\n' + " Briefly introduce the data content in the generated chart. "

                            await planner_user.initiate_chat(
                                analyst,
                                message=str(
                                    answer_contents) + '\n' + que_str,
                            )

                            answer_message = planner_user.last_message()["content"]
                            # answer_message = planner_user.chat_messages[-1]["content"]
                            print("answer_message: ", answer_message)
                            answer_message = answer_message.replace("TERMINATE", "")
                            return answer_message
                        except Exception as e:
                            traceback.print_exc()
                            logger.error("from user:[{}".format(self.user_name) + "] , " + "error: " + str(e))
                            error_times = error_times + 1

                    if error_times == max_retry_times:
                        return self.error_message_timeout

                else:
                    answer_contents.pop(0)
        except Exception as e:
            traceback.print_exc()
            logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))
        return self.error_message_timeout

    async def task_base(self, qustion_message):
        """ Task type: base question """
        return self.error_no_report_question
