# -*- coding: utf-8 -*-
import requests
import json
import time
import xml.etree.ElementTree as E_T
import re
import copy


# 强制 检查函数
STRICT_MODE_CHECK_FUNCTION = False
# 定义默认参数
DEEPSEEK_DEFAULT_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"
DEEPSEEK_TEMPERTURE = 0.7
DEEPSEEK_MAX_TOKENS = 10240

Deepseek_role_map = {
    # "open ai name: Deepseek name"
    "user": "user",
    "assistant": "assistant",
    "function": "user",
    "system": "assistant"
}

Deepseek_stop_reason_map = {
    "stop": "stop",
    "max_tokens": "length",
    "end_turn": "stop",
}


class DeepSeekClient:

    @classmethod
    def run(cls, apiKey, data, model=None, use_url=None):
        """
        define run function
        """

        if apiKey is None or apiKey == "":
            raise Exception("LLM DeepSeek apikey empty,use_model: ", model, " need apikey")
        messages_copy = copy.deepcopy(data['messages'])
        messages = cls.transform_message_role(messages_copy)
        model = model if model else DEEPSEEK_MODEL
        use_url = use_url if use_url else DEEPSEEK_DEFAULT_URL
        ai_result = cls.call_deepSeek(apiKey, messages, model, use_url)
        if ai_result:
            return cls.output_to_openai(ai_result)
        else:
            return False
        pass

    @classmethod
    def call_deepSeek(cls, apikey, message, model=DEEPSEEK_MODEL, use_url=DEEPSEEK_DEFAULT_URL, temperature=DEEPSEEK_TEMPERTURE):
        """
        call deepSeek
        """
        try:
            payload = json.dumps({
                "messages": message,
                "model": model,
                "frequency_penalty": 0,
                "max_tokens": 2048,
                "presence_penalty": 0,
                "stop": None,
                "stream": False,
                "temperature": temperature,
                "top_p": 1,
                "logprobs": False,
                "top_logprobs": None
            })
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Authorization': 'Bearer ' + str(apikey)
            }

            response = requests.request("POST", use_url, headers=headers, data=payload)
            result = json.loads(response.text)
        except Exception as e:
            print("error" * 10)
            print(response.text)
            print(e)
            result = False
        return result
        pass

    @classmethod
    def output_to_openai(cls, data):
        completion = str(data["choices"][0]['message']['content'])
        in_token = data['usage']['prompt_tokens']
        out_token = data['usage']['completion_tokens']
        sum_token = data['usage']['total_tokens']

        # check function call
        if STRICT_MODE_CHECK_FUNCTION:
            function_call_flag = completion.strip().startswith("<function_calls>")
        else:
            function_call_flag = completion.strip().startswith("<function_calls>") or\
                all(substring in completion for substring in ("<function_calls>", "<invoke>", "<tool_name>", "</invoke>"))
        # return openai result
        if function_call_flag:
            """
            have function call
            """
            choices = cls.return_to_open_function_call(completion)
        else:
            """ just process as message """
            # replace code
            completion = cls.replace_code_to_python(completion)
            choices = {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": completion,
                },
                "finish_reason": Deepseek_stop_reason_map[
                    data.get("finish_reason")] if 'finish_reason' in data else None
                if data.get("finish_reason")
                else None,
            }

        return {
            "id": f"chatcmpl-{str(time.time())}",
            "object": data['object'],
            "created": int(time.time()),
            "model": DEEPSEEK_MODEL,
            "usage": {
                "prompt_tokens": in_token,
                "completion_tokens": out_token,
                "total_tokens": sum_token,
            },
            "choices": [choices],
        }
        pass

    @classmethod
    def functions_to_tools_string(cls, functions):
        """
        trans openai functions to Deepseek2 tool
        """
        return_str = "Here are the tools available:\n<tools>\n"
        for item in functions:
            return_str = return_str + "<tool_description>\n"
            if "name" in item:
                return_str = return_str + "<tool_name>" + item['name'] + "</tool_name>\n"
            if "description" in item:
                return_str = return_str + "<description>" + item['name'] + "</description>\n"
            # parameters
            if "parameters" in item:
                return_str = return_str + "<parameters>\n"
                parameter_obj = item['parameters']
                if "type" in parameter_obj:
                    return_str = return_str + "<parameter>\n <name>" + parameter_obj['type'] + "</name>\n"
                    return_str = return_str + " <type>" + parameter_obj['type'] + "</type>\n"
                    if "properties" in parameter_obj:
                        return_str = return_str + " <parameters>\n"
                        for k, v in parameter_obj['properties'].items():
                            return_str = return_str + "  <parameter>\n   <name>" + k + "</name>\n"
                            for kk, vv in v.items():
                                return_str = return_str + "   <" + kk + ">" + vv + "</" + kk + ">\n"
                            return_str = return_str + "  </parameter>\n"
                        return_str = return_str + " </parameters>\n"
                if "required" in item['parameters']:
                    return_str = return_str + "<required>" + str(
                        ",".join(item['parameters']['required'])) + "</required>\n"
                return_str = return_str + "</parameter>\n</parameters>\n"
            return_str = return_str + "</tool_description>\n"
        return_str = return_str + "</tools>\n"
        return return_str
        pass

    @classmethod
    def functions_to_function_call_string(cls):
        """
        tell Deepseek2, function call return
        """
        return_str = """In this environment you have access to a set of tools you can use to answer the user's question.
                        Your answer must be english.You may call them like this.
                        Only invoke one function at a time and wait for the results before invoking another function:\n
                        <function_calls>\n<invoke>\n"""
        return_str = return_str + "<tool_name>$TOOL_NAME</tool_name>\n"
        return_str = return_str + "<parameters>\n"
        return_str = return_str + "<$PARAMETER_NAME>$PARAMETER_VALUE</$PARAMETER_NAME>\n ... \n"
        return_str = return_str + "</parameters>\n</invoke>\n"
        return_str = return_str + "</function_calls>\n"
        return return_str
        pass

    @classmethod
    def function_call_to_xml(cls, data):
        """
        openai message function_call to Deepseek2 message
        """
        return_str = "<function_calls>\n<invoke>"
        return_str = return_str + "<tool_name>" + data['name'] + "</tool_name>\n"
        return_str = return_str + "<arguments>" + data['arguments'] + "</arguments>\n"
        return_str = return_str + "</invoke>\n</function_calls>\n"
        return return_str
        pass

    @classmethod
    def return_to_open_function_call(cls, data):
        """
        trans Deepseek2 to openai function call return
        """
        xml_string = data
        match = re.search(r'<invoke>(.*?)</invoke>', xml_string, re.DOTALL)
        result = match.group(1)
        function_xml_str = "<function_calls>\n<invoke>\n" + result.strip() + "</invoke>\n</function_calls>"
        root = E_T.fromstring(function_xml_str)
        invokes = root.findall("invoke")
        function_call = {}

        tool_name = invokes[0].find("tool_name").text
        function_call['name'] = tool_name
        args = invokes[0].find("parameters")
        args_obj = {}
        for item in args:
            if "object" == item.tag:
                args_son = item.findall("./")
                if len(args_son) > 0:
                    for son_item in args_son:
                        arg_name = son_item.tag
                        arg_value = son_item.text
                        args_obj[arg_name] = arg_value
                else:
                    arg_item_obj = json.loads(item.text)
                    for k, v in arg_item_obj.items():
                        args_obj[k] = json.dumps(v)
            else:
                arg_name = item.tag
                arg_value = item.text
                args_obj[arg_name] = arg_value

        return {
            "index": 0,
            "message": {
                "role": "assistant",
                "content": None,
                "function_call": {
                        "name": tool_name,
                        "arguments": json.dumps(args_obj)
                },
                "logprobs": None,
                "finish_reason": "function_call"
            }
        }
        pass

    @classmethod
    def replace_code_to_python(cls, response_str):
        response_str = response_str.replace("\n<code>\n", "\n```python\n")
        response_str = response_str.replace("\n</code>\n", "\n```\n")
        return response_str

    @classmethod
    def replace_python_to_code(cls, response_str):
        response_str = response_str.replace("\n```python\n", "\n<code>\n")
        response_str = response_str.replace("\n```\n", "\n</code>\n")
        return response_str

    @classmethod
    def transform_message_role(cls, message):
        transformed_message = []
        now_content = ""
        now_role = ""
        system_flag = False
        # change role
        for i, item in enumerate(message):
            content = item.get('content')
            role = item.get("role")
            now_item = {}
            now_item['content'] = content
            if role == "system":
                if not system_flag:
                    now_item['role'] = "system"
                    system_flag = True
                    transformed_message.append(now_item)
                    continue
                else:
                    now_item['role'] = "user"
            else:
                # 如果不是系统
                if role == 'assistant':
                    now_item['role'] = "assistant"
                else:
                    now_item['role'] = "user"
            # 将所有的转换添加到结果
            transformed_message.append(now_item)
        now_role = ""
        now_content = ""
        result = []
        # update message role
        for i, item in enumerate(transformed_message):
            content = item.get('content')
            role = item.get("role")
            # other not first system
            if now_role != "":
                if role == now_role:
                    now_content = now_content + "\n" + content
                else:
                    now_item = {}
                    now_item['content'] = now_content
                    now_item['role'] = now_role
                    result.append(now_item)
                    now_role = role
                    now_content = content
            else:
                now_role = role
                now_content = content
            if i == len(transformed_message) - 1:
                now_item = {}
                now_item['content'] = now_content
                now_item['role'] = now_role
                result.append(now_item)

        return result
