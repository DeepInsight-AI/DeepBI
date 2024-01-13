from ai.backend.base_config import CONFIG


class LanguageInfo():
    def __init__(self):
        select_language = ['EN', 'CN']
        if CONFIG.web_language in select_language:
            if CONFIG.web_language == 'CN':
                self.load_info_cn()
            else:
                self.load_info_en()
        else:
            self.load_info_en()

    def load_info_cn(self):
        self.question_ask = ' 以下是我的问题，请用中文回答: '

        """ error message """
        self.error_message_timeout = "十分抱歉，本次AI-GPT接口调用超时，请再次重试"
        self.error_miss_data = '缺少数据库注释'
        self.error_miss_key = "ApiKey设置有误,请修改!"
        self.error_no_report_question = "非常抱歉，本对话只处理报表生成类问题，这个问题请您到数据分析对话中提问"

        self.no_api_key = '未检测到apikey,请核查'
        self.api_key_success = 'API Key 检测通过~'
        self.api_key_fail = 'API Key 检测未通过~'

    def load_info_en(self):
        self.question_ask = ' This is my question，Answer user questions in English: '

        self.error_message_timeout = 'Sorry, this AI-GPT interface call timed out, please try again.'
        self.error_miss_data = 'Missing database annotation'
        self.error_miss_key = 'The ApiKey setting is incorrect, please modify it!'
        self.error_no_report_question = 'Sorry, this conversation only deals with report generation issues. Please ask this question in the data analysis conversation.'

        self.no_api_key = 'apikey not detected, please check'
        self.api_key_success = 'API Key Test Success~'
        self.api_key_fail = 'API Key Test Fail~'

LanguageInfo = LanguageInfo()
