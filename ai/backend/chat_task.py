import asyncio
import json
import time
import traceback
from ai.backend.util.write_log import logger
from ai.agents import AgentInstanceUtil
from ai.backend.memory import ChatMemoryManager
from ai.backend.base_config import CONFIG
from ai.backend.aidb.report import ReportMysql, ReportPostgresql, ReportStarrocks, ReportMongoDB
from ai.backend.aidb.analysis import AnalysisMysql, AnalysisCsv, AnalysisPostgresql, AnalysisStarrocks, AnalysisMongoDB
from ai.backend.aidb import AIDB
from ai.backend.aidb.autopilot import AutopilotMysql, AutopilotMongoDB, AutopilotCSV

message_pool: ChatMemoryManager = ChatMemoryManager(name="message_pool")


class ChatClass:
    def __init__(self, websocket, path):
        self.ws = websocket
        self.incoming = asyncio.Queue()
        self.outgoing = asyncio.Queue()
        self.path = path
        # Messages that cannot be processed currently are temporarily stored.
        self.delay_messages = {'user': [], 'bi': {'mysql_code': [], 'chart_code': [],
                                                  'delete_chart': [], 'ask_data': []}, 'close': []}

        # Generate unique session ID
        session_id = str(id(websocket))
        self.user_name = path.split('/')[-1] + '_' + session_id
        self.uid = self.user_name.split('_')[0]

        print(str(time.strftime("%Y-%m-%d %H:%M:%S",
                                time.localtime())) + ' ---- ' + " New user connected successfully:{}".format(
            self.user_name))

        self.agent_instance_util = AgentInstanceUtil(user_name=str(self.user_name),
                                                     delay_messages=self.delay_messages,
                                                     outgoing=self.outgoing,
                                                     incoming=self.incoming,
                                                     )
        self.agent_instance_util.set_socket(websocket)
        self.agent_instance_util.set_language_mode(CONFIG.default_language_mode)

        self.language_mode = CONFIG.default_language_mode
        # self.set_language_mode(self.language_mode)

        self.recent_messages = []

        self.analysisMysql = AnalysisMysql(self)
        self.analysisCsv = AnalysisCsv(self)
        self.analysisPostgresql = AnalysisPostgresql(self)
        self.analysisStarrocks = AnalysisStarrocks(self)
        self.analysisMongoDB = AnalysisMongoDB(self)

        self.reportMysql = ReportMysql(self)
        self.reportPostgresql = ReportPostgresql(self)
        self.reportStarrocks = ReportStarrocks(self)
        self.reportMongoDB = ReportMongoDB(self)

        self.autopilotMysql = AutopilotMysql(self)
        self.autopilotMongoDB = AutopilotMongoDB(self)
        self.autopilotCSV = AutopilotCSV(self)

    async def get_message(self):
        """ Receive messages and put them into the [pending] message queue """
        msg_in = await self.ws.recv()
        await self.incoming.put(msg_in)
        got_mess = str(time.strftime("%Y-%m-%d %H:%M:%S",
                                     time.localtime())) + ' ---- ' + "from user:[{}".format(
            self.user_name) + "], got a message:{}".format(msg_in)
        print(got_mess)
        logger.info(got_mess)

    async def send_message(self, message):
        send_mess = str(time.strftime("%Y-%m-%d %H:%M:%S",
                                      time.localtime())) + ' ---- ' + "from user:[{}".format(
            self.user_name) + "], reply a message:{}".format(message)
        print(send_mess)
        await self.ws.send(message)
        logger.info(send_mess)

    async def produce(self):
        """Get a message to be sent"""
        try:
            msg_out = await self.outgoing.get()
            return msg_out
        except asyncio.QueueEmpty:
            return ""  # Return an empty string if the queue is empty

    async def consume(self):
        """ Process received messages """
        try:
            message = await self.incoming.get()

            # do something 'consuming' :)
            result = {'state': 200, 'data': {}, 'receiver': ''}

            json_str = json.loads(message)
            print(json_str)

            if json_str.get('sender'):
                if json_str.get('sender') == 'heartCheck':
                    result['receiver'] = 'heartCheck'
                    consume_output = json.dumps(result)
                    return await self.outgoing.put(consume_output)

            q_state = json_str['state']

            if q_state == 200 or q_state == 500:
                q_data_type = json_str['data']['data_type']
                print('q_data_type : ', q_data_type)

                q_database = 'mysql'  # default value
                if json_str.get('database'):
                    q_database = json_str.get('database')

                q_chat_type = 'chat'  # default value
                if json_str.get('chat_type'):
                    q_chat_type = json_str.get('chat_type')

                if q_chat_type == 'test':
                    await AIDB(self).test_api_key()

                elif q_chat_type == 'chat':
                    if q_database == 'mysql':
                        print(" q_database ==  mysql ")
                        await self.analysisMysql.deal_question(json_str, message)
                    elif q_database == 'csv':
                        await self.analysisCsv.deal_question(json_str, message)
                    elif q_database == 'pg':
                        # postgresql
                        await self.analysisPostgresql.deal_question(json_str, message)
                    elif q_database == 'starrocks':
                        print(" q_database ==  starrocks ")
                        await self.analysisStarrocks.deal_question(json_str, message)
                    elif q_database == 'mongodb':
                        print(" q_database ==  mongodb ")
                        await self.analysisMongoDB.deal_question(json_str, message)

                elif q_chat_type == 'report':
                    if q_database == 'mysql':
                        await self.reportMysql.deal_report(json_str, message)
                    elif q_database == 'pg':
                        await self.reportPostgresql.deal_report(json_str, message)
                    elif q_database == 'starrocks':
                        await self.reportStarrocks.deal_report(json_str, message)
                    elif q_database == 'mongodb':
                        await self.reportMongoDB.deal_report(json_str, message)
                elif q_chat_type == 'autopilot':
                    if q_database == 'mysql':
                        await self.autopilotMysql.deal_question(json_str, message)
                    elif q_database == 'starrocks':
                        await self.autopilotMysql.deal_question(json_str, message)
                    elif q_database == 'mongodb':
                        await self.autopilotMongoDB.deal_question(json_str, message)
                    elif q_database == 'csv':
                        await self.autopilotCSV.deal_question(json_str, message)

            else:
                result['state'] = 500
                if self.language_mode == CONFIG.language_chinese:
                    result['data']['content'] = '状态码错误'
                else:
                    result['data']['content'] = 'Status code error'
                consume_output = json.dumps(result)
                await self.outgoing.put(consume_output)

        except Exception as e:
            traceback.print_exc()
            # logger.error("from user:[{}".format(self.user_name) + "] , " + str(e))

            result['state'] = 500
            if self.language_mode == CONFIG.language_chinese:
                result['data']['content'] = '数据格式异常'
            else:
                result['data']['content'] = 'Abnormal data format'
            consume_output = json.dumps(result)
            await self.outgoing.put(consume_output)

            # 捕获异常并记录日志
            logger.error("from user:[{}".format(self.user_name))
            logger.error("An error occurred: %s", str(e))
            logger.error(traceback.format_exc())
