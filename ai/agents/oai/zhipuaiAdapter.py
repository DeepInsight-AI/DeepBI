# -*- coding: utf-8 -*-
"""
info:  Define ZhiPuAI
"""
import json
import copy
# import base
try:
    from zhipuai import ZhipuAI
except:
    raise Exception("Error, need: pip install zhipuai")
# define default model
ZHIPU_AI_MODEL = "glm-4"
# define default temperature
ZHIPU_AI_temperature = 0.1


def object_to_dict(obj):
    if isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    elif isinstance(obj, list):
        return [object_to_dict(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: object_to_dict(value) for key, value in obj.items()}
    elif hasattr(obj, "__dict__"):
        return {key: object_to_dict(value) for key, value in obj.__dict__.items() if not key.startswith("__")}
    else:
        return str(obj)


class ZhiPuAIClient:

    @classmethod
    def run(cls, apiKey, data, model_name=None, temperature=None, use_url=None):
        if "" == apiKey or apiKey is None:
            raise Exception("LLM ZhiPuAI api key empty use_model: ", model_name, " need apikey")
        if model_name is None or "" == model_name:
            model_name = ZHIPU_AI_MODEL
        if temperature is None:
            temperature = ZHIPU_AI_temperature
        messages_copy = copy.deepcopy(data)
        zhipu_data = cls.input_to_openai(messages_copy)
        client = ZhipuAI(api_key=apiKey)
        if "functions" in data:
            tools = zhipu_data["tools"]
            response = client.chat.completions.create(
                model=model_name,
                messages=zhipu_data["messages"],
                tools=tools,
                tool_choice="auto",
                temperature=temperature
            )
        else:
            response = client.chat.completions.create(
                model=model_name,
                messages=zhipu_data["messages"],
                temperature=temperature
            )
        result = cls.output_to_openai(response)
        return result
        pass

    @classmethod
    def input_to_openai(cls, data):
        tools = []
        messages = []
        if "functions" in data:
            for item in data["functions"]:
                new_item = {
                    "type": "function",
                    "function": item
                }
                tools.append(new_item)
        if "messages" in data and data["messages"] is not None and len(data["messages"]) > 0:
            for item in data["messages"]:
                if item.get("content") is None and "function_call" in item:
                    new_item = {
                        "content": None,
                        "tool_calls": {
                            "id": 0,
                            "type": "function",
                            "function": item.get("function_call")
                        }
                    }
                else:
                    new_item = item
                messages.append(new_item)
        return object_to_dict({"tools": tools, "messages": messages})
        pass

    @classmethod
    def output_to_openai(cls, data):
        return cls.parse_function_call(object_to_dict(data))
        pass

    @classmethod
    def parse_function_call(cls, model_response):
        if "tool_calls" in model_response['choices'][0]['message'] and model_response['choices'][0]['message']['tool_calls'] is not None:
            tool_call = model_response['choices'][0]['message'].pop("tool_calls")
            args = tool_call[0]['function']['arguments']
            open_ai_choices = {
                "function_call": {
                    "name": tool_call[0]["function"]['name'],
                    "arguments": args
                }
            }
            model_response['choices'][0]['index'] = tool_call[0]['index'] if "index" in tool_call[0] else 0
            model_response['choices'][0]['logprobs'] = None
            model_response['choices'][0]['message']["content"] = None
            model_response['choices'][0]['message']["function_call"] = open_ai_choices
            model_response['choices'][0]['finish_reason'] = "function_call"
            return model_response
        else:
            return model_response
        pass
        pass
