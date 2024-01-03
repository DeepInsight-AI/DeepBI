from pathlib import Path
import traceback
from jinja2 import Template
import json
from llm import openai_response
from chartSetting import acquiesce_echarts_code
import ast
from ai.backend.base_config import CONFIG
from ai.backend.aidb import AIDB
from ai.backend.util.db.postgresql_dashboard import PsgReport
from ai.agents.agentchat import AssistantAgent
from ai.backend.aidb.dashboard.prompts import ECHARTS_BAR_PROMPT, ECHARTS_PIE_PROMPT, ECHARTS_LINE_PROMPT


class PrettifyDashboard(AIDB):

    async def deal_question(self, json_str):
        """
            deal question
        """
        file_name = json_str['file_name']
        html_file = file_name.replace('.json', '.html')
        task_file_name = CONFIG.up_file_path + file_name

        task_id = json_str['task_id']

        with open(task_file_name, 'r') as file:
            data = json.load(file)

        print("self.agent_instance_util.api_key_use :", self.agent_instance_util.api_key_use)

        if not self.agent_instance_util.api_key_use:
            re_check = await self.check_api_key()
            if not re_check:
                return

        try:
            psg = PsgReport()
            re = psg.select_data(task_id)
            if re is not None and len(re) > 0:
                print('need deal task')
                data_to_update = (1, task_id)
                update_state = psg.update_data(data_to_update)
                if update_state:
                    await self.generate_echart_code(data, task_file_name, task_id, html_file)
            else:
                print('no task')

        except Exception as e:
            traceback.print_exc()
            # update report status
            data_to_update = (-1, task_id)
            PsgReport().update_data(data_to_update)

    # 生成echart代码
    async def generate_echart_code(self, echart_json, task_file_name, task_id, html_file_name):

        # echart_json = generate_json()
        echart_code = {}
        for query_result in echart_json['query_result']:
            if query_result['chart_type'] == 'table':
                continue
            try:
                # ai
                print("开始调用openai,第" + str(query_result['id']) + "个图表,图表类型为：" + query_result['chart_type'] + "+++++")
                # echart_code = openai_response(query_result)

                planner_user = self.agent_instance_util.get_agent_planner_user()
                pretty_dashboard = self.get_agent_pretty_dashboard(echart_json['chart_type'])

                await planner_user.initiate_chat(
                    pretty_dashboard,
                    message=str(echart_json))

                echart_code = planner_user.last_message()["content"]
                print('echart_code : ', echart_code)

            except Exception as e:
                # print("Exception: ", e)
                # 默认配置
                print("调用openai失败，使用默认配置,第" + str(query_result['id']) + "个图表,图表类型为：" + query_result[
                    'chart_type'] + "-----")
                echart_code = acquiesce_echarts_code(query_result)
            query_result['echart_code'] = echart_code
            # write_echart_code(echart_json)
            # return
        print("生成完毕======")
        self.generate_html(echart_json, CONFIG.up_file_path + html_file_name)

        # 更新数据
        data_to_update = (2, task_id)
        PsgReport().update_data(data_to_update)
        html_to_update = (html_file_name, task_id)

        PsgReport().update_html_name(html_to_update)

    # 模版生成
    def generate_html(self, data, html_file_name):

        # 获取当前工作目录的路径
        current_directory = Path.cwd()

        if str(current_directory).endswith('/ai'):
            html_template_path = str(current_directory) + '/backend/aidb/dashboard/html_template'
        else:
            html_template_path = str(current_directory) + '/ai/backend/aidb/dashboard/html_template'

        print('html_template_path:', html_template_path)

        # 读取模板文件
        with open(html_template_path + '/dashboard_1.html', 'r') as file:
            template_str = file.read()
            # print('template_str :', template_str)

        # 使用Jinja2渲染模板
        template = Template(template_str)
        rendered_html = template.render(data)

        # 将渲染后的HTML写入文件
        with open(html_file_name, 'w') as output_file:
            output_file.write(rendered_html)

        print("HTML文件已生成：", html_file_name)

    def get_agent_pretty_dashboard(self, chart_type, report_file_name=None):
        system_content = ECHARTS_BAR_PROMPT
        if chart_type == "bar":
            system_content = ECHARTS_BAR_PROMPT
        elif chart_type == "pie":
            system_content = ECHARTS_PIE_PROMPT
        elif chart_type == "line":
            system_content = ECHARTS_LINE_PROMPT

        pretty_dashboard = AssistantAgent(
            name="pretty_dashboard",
            system_message=system_content,
            llm_config=self.agent_instance_util.gpt4_turbo_config,
            websocket=self.agent_instance_util.websocket,
            user_name=self.agent_instance_util.user_name,
            openai_proxy=self.agent_instance_util.openai_proxy,
            report_file_name=report_file_name,
        )
        return pretty_dashboard
