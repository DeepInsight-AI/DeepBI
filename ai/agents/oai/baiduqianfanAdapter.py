# -*- coding: utf-8 -*-
"""
百度千帆 适配器
"""
import json
import requests
import copy
import re

BAIDUQIANFAN_MODEL = ""


class BaiduqianfanClient:

    @classmethod
    def run(cls, api_key, secrat_key, data, model):
        if api_key is None or "" == api_key:
            raise Exception("Error, api key is empty")
        if secrat_key is None or "" == secrat_key:
            raise Exception("Error, secrat key is empty")
        # 获取access_token
        access_token = cls.get_access_token(api_key, secrat_key)
        # copy data
        messages_copy = copy.deepcopy(data['messages'])
        functon_call = data['functions'] if "functions" in data else None
        call_message = cls.input_to_openai(messages_copy, functon_call)
        response = cls.call_baiduqianfan(call_message, model, access_token)
        if "error_code" in response:
            raise Exception("Error, call baiduqianfan api error" + str(response))
        result = cls.output_to_openai(response, model)
        return result
        pass

    @classmethod
    def call_baiduqianfan(cls, messages, model, access_token):
        try:
            url = f"https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/{model}?access_token={access_token}"
            payload = json.dumps({
                "messages": messages
            })
            headers = {
                'Content-Type': 'application/json'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            return json.loads(response.text)
        except Exception as e:
            print(e)
            raise Exception("Error, call baiduqianfan api error")
        pass

    @classmethod
    def get_access_token(cls, api_key, secrat_key):
        try:
            url = f"https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id={api_key}&client_secret={secrat_key}"

            payload = json.dumps("")
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            return response.json().get("access_token")
        except Exception as e:
            print(e)
            raise Exception("Error, get access token error")
        pass

    @classmethod
    def input_to_openai(cls, messages, functions):
        """
            将输入的openai message转换为现在模型的message格式,这里主要处理的是function call 和 role 等
            百度的请求格式只有 role 和 content 两个字段
        """
        result = ""
        function_result = ""
        # process function
        if functions is not None:
            function_start = "接下来的所有对话中，你可以使用外部的工具来回答问题。你必须按照规定的格式来使用工具，当你使用工具时，我会在下一轮对话给你工具调用结果，然后你应该根据实际结果判断是否需要进一步使用工具，或给出你的回答。工具可能有多个，每个工具由名称、描述、参数组成，参数符合标准的json schema。"
            function_end = "如果你需要使用外部工具，那么你的输出必须按照如下格式，只包含2行，不需要输出任何解释或其他无关内容:\nAction: 使用的工具名称\nAction Input: 使用工具的参数，json格式。\n如果你不需要使用外部工具，不需要输出Action和Action Input，请输出你的回答。\n 你的问题："
            # 添加function start
            function_message = "\n\n"
            for item in functions:
                item_str = ""
                item_str += "名称：" + str(item['name']) + "\n"
                item_str += "描述：" + str(item['description']) + "\n"
                item_str += "参数：" + str(item['parameters']) + "\n"
                function_message += item_str + "\n\n"
                pass
            function_result = function_start + function_message + function_end
        # f function over
        # change role and function name
        transformed_message = []
        for message in messages:
            # update role: system or function to user
            if message['role'] == "system" or message['role'] == "function":
                message['role'] = "user"
            # update function_call to tool message
            if "function_call" in message:
                # trans function_call to baidu format. aim: Action ....
                function_call = message.pop("function_call")
                message['content'] = cls.function_call_to_content(function_call)
            transformed_message.append(message)
        result = []
        # marge message
        now_role = ""
        now_content = ""
        result = []
        for i, item in enumerate(transformed_message):
            # now content
            content = item.get('content')
            role = item.get("role")
            if now_role != "":
                if role == now_role:
                    now_content = now_content + "\n" + content
                else:
                    now_item = {}
                    now_item['content'] = now_content
                    now_item['role'] = now_role
                    # add old item
                    result.append(now_item)
                    # process new item
                    now_role = role
                    now_content = content
            else:
                now_role = role
                now_content = content
            # last item
            if i == len(transformed_message) - 1:
                now_item = {}
                now_item['content'] = now_content if "" == function_result else function_result + now_content
                now_item['role'] = now_role
                result.append(now_item)
        return result
        pass

    @classmethod
    def output_to_openai(cls, data, model):
        """
            data 返回数据 总内容 json 格式 

            将输出的message转换为openai的message格式,这里主要处理的是function call 和 role
            解析返回数据 到openai 格式
            百度 function 格式
            {
                "id": "as-da3bnrr8tv",
                "object": "chat.completion",
                "created": 1712652144,
                "result": "Action: get_current_weather\nAction Input: {\"location\": \"上海市\", \"unit\": \"摄氏度\"}",
                "is_truncated": false,
                "need_clear_history": false,
                "usage": {
                    "prompt_tokens": 306,
                    "completion_tokens": 23,
                    "total_tokens": 329
                }
            }
            百度 message 格式
            {
                "id": "as-fg4g836x8n",
                "object": "chat.completion",
                "created": 1709716601,
                "result": "消息内容",
                "is_truncated": false,
                "need_clear_history": false,
                "finish_reason": "normal",
                "usage": {
                    "prompt_tokens": 2,
                    "completion_tokens": 221,
                    "total_tokens": 223
                }
            } 
        """
        result = {}
        result['id'] = data["id"]
        result['model'] = model
        result['created'] = data["created"]
        result['object'] = data['object']
        # 如果返回是 function call
        if "Action" in data["result"] and "" in data['result'] and data['result'].startswith("Action"):
            # parse function call
            function_name, function_input_json = cls.extract_action_and_input(data['result'])
            if function_name is None or not isinstance(function_input_json, dict):
                raise Exception("parse ai return to function call error")
            # function call
            choices = {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": None,
                    "function_call": {
                        "name": function_name,
                        "arguments": json.dumps(function_input_json)
                    }
                },
                "finish_reason": "function_call"  # 原来是tools_call
            }
        else:
            # message call
            choices = {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": data['result'],
                },
                "finish_reason": "stop"  # 原来是tools_call
            }
            pass
        result['choices'] = [choices]
        result['usage'] = data['usage']
        return result
        pass

    @classmethod
    def extract_action_and_input(cls, s):
        # 使用正则表达式提取 "Action" 和 "Action Input"
        action_match = re.search(r"Action:\s*(.+)", s)
        input_match = re.search(r"Action Input:\s*(\{.*\})", s)

        if action_match and input_match:
            action = action_match.group(1).strip()
            action_input = input_match.group(1).strip()

            # 将字符串形式的 JSON 解析为字典
            try:
                action_input_dict = json.loads(action_input)
            except json.JSONDecodeError:
                action_input_dict = None

            return action, action_input_dict
        else:
            return None, None

    @classmethod
    def function_call_to_content(cls, function):
        content = ""
        if "name" in function:
            content = content + "Action: " + function["name"] + "\n"
        if "arguments" in function:
            content = content + "Action Input: " + function["arguments"]
        return content
        pass
