from ai.backend.base_config import CONFIG


class LanguageInfo():
    def __init__(self):
        select_language = ['EN', 'CN', 'JP']
        if CONFIG.web_language in select_language:
            if CONFIG.web_language == 'CN':
                self.load_info_cn()
            elif CONFIG.web_language == 'JP':
                self.load_info_jp()
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
        self.qustion_message = '请为我解释一下这些数据'

    def load_info_en(self):
        self.question_ask = ' This is my question，Answer user questions in English: '

        self.error_message_timeout = 'Sorry, this AI-GPT interface call timed out, please try again.'
        self.error_miss_data = 'Missing database annotation'
        self.error_miss_key = 'The ApiKey setting is incorrect, please modify it!'
        self.error_no_report_question = 'Sorry, this conversation only deals with report generation issues. Please ask this question in the data analysis conversation.'

        self.no_api_key = 'apikey not detected, please check'
        self.api_key_success = 'API Key Test Success~'
        self.api_key_fail = 'API Key Test Fail~'
        self.qustion_message = 'Please explain this data to me.'

    def load_info_jp(self):
        self.question_ask = '以下が私の質問です。日本語で答えてください: '

        self.error_message_timeout = "申し訳ありませんが、このAI-GPTインターフェースの呼び出しがタイムアウトしました。もう一度お試しください。"
        self.error_miss_data = 'データベースアノテーションが不足しています'
        self.error_miss_key = "ApiKeyの設定が正しくありません。修正してください！"
        self.error_no_report_question = "申し訳ありませんが、この会話はレポート生成の問題のみを扱います。この質問はデータ分析の会話でお願いします。"

        self.no_api_key = 'apikeyが検出されませんでした。確認してください'
        self.api_key_success = 'APIキーの検査に成功しました〜'
        self.api_key_fail = 'APIキーの検査に失敗しました〜'
        self.qustion_message = 'このデータを説明してください。'



LanguageInfo = LanguageInfo()
