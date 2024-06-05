# -*- coding: utf-8 -*-
"""
    阿里百炼平台适配，可以自行调整模型
    适配Openai 接口
    pip install dashscope

"""
try:
    import dashscope
except Exception as e:
    raise Exception("dashscope not install, please install dashscope,use commnd : pip install dashscope")
import copy
import random
import time
from dashscope import Generation
from http import HTTPStatus

Ali_bailian_AI_MODEL = "qwen-max"

"""
 阿里role支持 
    system 系统设定，必须在第一个消息，不能有多个，可以没有
    user 用户输入信息角色
    function 用户调用function_call(tools)结果内容
    assistant AI返回助手信息
    tool 这里是 ai返回需要调用的tool 
"""


class AlibailianClient:

    @classmethod
    def run(cls, apiKey, data, model):
        if apiKey is None or apiKey == "":
            raise Exception("LLM DeepSeek apikey empty,use_model: ", model, " need apikey")
        dashscope.api_key = apiKey
        # call ai
        model = model if model is not None or "" == model else Ali_bailian_AI_MODEL
        result = cls.call_alibailian(data, model)
        return result
        pass

    @classmethod
    def call_alibailian(cls, data, model):
        new_message = {}
        messages_copy = copy.deepcopy(data['messages'])
        new_message['messages'] = cls.input_to_openai(messages_copy)

        try:
            if "functions" in data:
                response = Generation.call(model=model,
                                           tools=data['functions'],
                                           messages=new_message['messages'],
                                           seed=random.randint(1, 10000),
                                           result_format='message')
            else:
                response = Generation.call(model=model,
                                           messages=new_message['messages'],
                                           seed=random.randint(1, 10000),
                                           result_format='message')
            if response.status_code == HTTPStatus.OK:
                return cls.output_to_openai(response, model)
            else:
                raise Exception('Request id: %s, Status code: %s, error code: %s, error message: %s' % (
                    response.request_id, response.status_code,
                    response.code, response.message
                ))
        except Exception as e:
            raise Exception(str(e))
        pass

    @classmethod
    def input_to_openai(cls, messages):
        """
            将输入的openai message转换为现在模型的message格式,这里主要处理的是function call 和 role
        """
        # change role and function name
        transformed_message = []
        for message in messages:
            # update role system to user
            if message['role'] == "system":
                message['role'] = "user"
            elif message['role'] == "function":
                # update function_call result role
                message['role'] = "tool"
            # update function_call to tool message
            if "function_call" in message:
                message.pop("name")
                tool_calls = [
                    {
                        "function": message.pop('function_call'),
                        "id": "",
                        "type": "function"
                    }
                ]
                message['tool_calls'] = tool_calls
            transformed_message.append(message)
        result = []
        # marge message
        now_role = ""
        now_content = ""
        result = []
        for i, item in enumerate(transformed_message):
            content = item.get('content')
            role = item.get("role")

            # other not first system
            if now_role != "":
                if role == now_role and role != "tool":
                    now_content = now_content + "\n" + content
                else:
                    now_item = item
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
            if i == len(transformed_message) - 1:
                now_item = item
                now_item['content'] = now_content
                now_item['role'] = now_role
                result.append(now_item)
        return result
        pass

    @classmethod
    def output_to_openai(cls, data, model):
        """
            将输出的message转换为openai的message格式,这里主要处理的是function call 和 role
        """
        result = {}
        result['status_code'] = "200"
        result['id'] = data["request_id"]
        result['model'] = model
        result['created'] = int(time.time())
        result['object'] = "chat.completion"
        output = data["output"]
        if output['choices'][0]['finish_reason'] == "tool_calls":
            # function call
            choices = {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": None,
                    "function_call": {
                        "name": output['tool_calls'][0]['function']['name'],
                        "arguments": output['tool_calls'][0]['function']['arguments']
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
                    "content": output['choices'][0]['message']['content'],
                },
                "finish_reason": "stop"  # 原来是tools_call
            }
            pass
        result['choices'] = [choices]
        result['usage'] = {
            "prompt_tokens": data['usage']['input_tokens'],
            "completion_tokens": data['usage']['output_tokens'],
            "total_tokens": data['usage']['input_tokens'] + data['usage']['output_tokens']
        }
        return result
    pass
