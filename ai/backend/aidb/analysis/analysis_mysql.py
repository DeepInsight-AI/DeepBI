import traceback
import json
from backend.util.write_log import logger
from backend.base_config import CONFIG
from backend.util import database_util
from .analysis import Analysis


class AnalysisMysql(Analysis):

    async def deal_question(self, json_str, message):
        """
        Process mysql data source and select the corresponding workflow
        """
        print(" mysql  deal_question  ++++++++++++++++++++ ")
        result = {'state': 200, 'data': {}, 'receiver': ''}
        q_sender = json_str['sender']
        q_data_type = json_str['data']['data_type']
        print('q_data_type : ', q_data_type)
        q_str = json_str['data']['content']

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
        elif q_sender == 'bi':
            if q_data_type == CONFIG.type_comment:
                await self.check_data_base(q_str)
            elif q_data_type == CONFIG.type_comment_first:
                if json_str.get('data').get('language_mode'):
                    q_language_mode = json_str['data']['language_mode']
                    if q_language_mode == CONFIG.language_chinese or q_language_mode == CONFIG.language_english:
                        self.set_language_mode(q_language_mode)
                        self.agent_instance_util.set_language_mode(q_language_mode)

                if CONFIG.database_model == 'online':
                    databases_id = json_str['data']['databases_id']
                    db_id = str(databases_id)
                    obj = database_util.Main(db_id)
                    if_suss, db_info = obj.run()
                    if if_suss:
                        self.agent_instance_util.base_mysql_info = ' When connecting to the database, be sure to bring the port. This is database info :' + '\n' + str(
                            db_info)
                        self.agent_instance_util.base_message = str(q_str)
                else:
                    self.agent_instance_util.base_message = str(q_str)

                await self.get_data_desc(q_str)
            elif q_data_type == 'mysql_comment_second':
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
                        self.agent_instance_util.base_mysql_info = '  When connecting to the database, be sure to bring the port. This is database info :' + '\n' + str(
                            db_info)
                        self.agent_instance_util.base_message = str(q_str)
                else:
                    self.agent_instance_util.base_message = str(q_str)

                await self.put_message(200, receiver=CONFIG.talker_bi, data_type=CONFIG.type_comment_second,
                                       content='')
            elif q_data_type == 'mysql_code' or q_data_type == 'chart_code' or q_data_type == 'delete_chart' or q_data_type == 'ask_data':
                self.delay_messages['bi'][q_data_type].append(message)
                print("delay_messages : ", self.delay_messages)
                return

    async def start_chatgroup(self, q_str):
        # print("agent_instance_util.base_message : ", self.agent_instance_util.base_message)
        user_proxy = self.agent_instance_util.get_agent_user_proxy()
        # base_mysql_assistant = self.agent_instance_util.get_agent_base_mysql_assistant()
        base_mysql_assistant = self.agent_instance_util.get_agent_base_assistant()

        await user_proxy.initiate_chat(
            base_mysql_assistant,
            message=self.agent_instance_util.base_message + '\n' + self.question_ask + '\n' + str(q_str),
        )
