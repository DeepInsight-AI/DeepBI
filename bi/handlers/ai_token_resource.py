import os
from flask import request
from flask_restful import abort
import json
from bi.handlers.base import BaseResource, json_response
from bi import settings


class AiTokenResource(BaseResource):  # BaseResource
    def post(self):
        if not settings.DATA_SOURCE_FILE_DIR:
            abort(400, message="Need set DATA_SOURCE_FILE_DIR")
        try:
            data = request.json
            # result = {
            #     "HttpProxyHost": data['HttpProxyHost'],
            #     "HttpProxyPort": data['HttpProxyPort'],
            #     "OpenaiApiKey": data['OpenaiApiKey']
            # }
            user_id = self.current_user.id
            token_file = os.path.join(settings.DATA_SOURCE_FILE_DIR, ".token_" + str(user_id) + ".json")
            with open(token_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            return json_response({'code': 200, 'data': []})
        except Exception as e:
            return json_response({'code': 400, 'message': str(e)})

    def get(self):
        if not settings.DATA_SOURCE_FILE_DIR:
            abort(400, message="Need set DATA_SOURCE_FILE_DIR")
        # get file from request
        user_id = self.current_user.id
        token_file = os.path.join(settings.DATA_SOURCE_FILE_DIR, ".token_" + str(user_id) + ".json")
        if not os.path.exists(token_file):
            return json_response({'code': 200, "data": []})

        try:
            with open(token_file, 'r', encoding='utf-8') as file:
                # 加载JSON数据
                data = json.load(file)
            # token = data['OpenaiApiKey']
            # return json_response(
            #     {
            #         'code': 200,
            #         'data': {
            #             'HttpProxyHost': data['HttpProxyHost'],
            #             'HttpProxyPort': data['HttpProxyPort'],
            #             'OpenaiApiKey': token
            #         }
            #     }
            # )
            return json_response({'code': 200, 'data': data})

        except Exception as e:
            return json_response({'code': 400, 'message': str(e)})
