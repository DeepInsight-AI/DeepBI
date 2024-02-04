# -*- coding: utf-8 -*-
"""
@ author:
@ info: claude to chatgpt
@ date: 2024/2/1 21:22

pip install boto3
pip install tiktoken
config  .aws/config
        .aws/credentials
"""

import json
import time
import xml.etree.ElementTree as E_T
try:
    import tiktoken
    import boto3
except:
    raise Exception("Error, need: pip install boto3 tiktoken")
# define default model
Claude_AI_MODEL = 'anthropic.claude-v2:1'
# define default temperature
Claude_AI_temperature = 0.1
Claude_role_map = {
    "system": "Human",
    "user": "Human",
    "assistant": "Assistant",
}

Claude_stop_reason_map = {
    "stop_sequence": "stop",
    "max_tokens": "length",
}


class AWSClaudeClient:

    @classmethod
    def run(cls, apiKey, data, model_name=None, temperature=None):
        if "ApiKey" not in apiKey or "ApkSecret" not in apiKey:
            raise Exception("agent_llm llm api key empty use_model: ", model_name, " need apikey and ApkSecret")

        if model_name is None:
            model_name = Claude_AI_MODEL
        if temperature is None:
            temperature = Claude_AI_temperature

        client_obj = boto3.client(
            service_name='bedrock-runtime',
            region_name="us-east-1",
            aws_access_key_id=apiKey['ApiKey'],
            aws_secret_access_key=apiKey['ApkSecret']
        )
        prompt = cls.input_to_openai(data['messages'])
        if "functions" in data:
            prompt_begin = cls.functions_to_function_call_string()
            prompt_begin = prompt_begin + cls.functions_to_tools_string(data['functions'])
            body = json.dumps({
                "prompt": prompt_begin + "\n" + prompt,
                "temperature": temperature,
                "max_tokens_to_sample": 100000,
                "top_p": 0.9,
                "stop_sequences": ["\n\nHuman:", "</function_calls>"]
            })
        else:
            prompt = cls.input_to_openai(data['messages'])
            body = json.dumps({
                "prompt": prompt,
                "temperature": temperature,
                "max_tokens_to_sample": 100000,
                "top_p": 0.9
            })
        modelId = model_name
        accept = 'application/json'
        contentType = 'application/json'
        response = client_obj.invoke_model(body=body, modelId=modelId, accept=accept, contentType=contentType)
        response_body = json.loads(response.get('body').read())
        return cls.output_to_openai(response_body)
        pass

    @classmethod
    def output_to_openai(cls, data):
        completion = data['completion']
        completion_tokens = cls.num_tokens_from_string(completion)
        # return openai result
        if completion.strip().startswith("<function_calls>"):
            """
            have function call
            """
            return cls.return_to_open_function_call(data, completion_tokens)
        else:
            """ just process as message """
            return {
                "id": f"chatcmpl-{str(time.time())}",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": Claude_AI_MODEL,
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": completion_tokens,
                    "total_tokens": completion_tokens,
                },
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": completion,
                        },
                        "index": 0,
                        "finish_reason": Claude_stop_reason_map[
                            data.get("stop_reason")] if 'stop_reason' in data else None
                        if data.get("stop_reason")
                        else None,
                    }
                ],
            }
        pass

    @classmethod
    def input_to_openai(cls, messages):
        prompt = ""
        for message in messages:
            role = message["role"]
            content = message["content"]
            if content is None and 'function_call' in message and message['function_call'] is not None:
                content = cls.function_call_to_xml(message['function_call'])
            transformed_role = Claude_role_map[role]
            prompt += f"\n\n{transformed_role.capitalize()}: {content}"
        prompt += "\n\nAssistant: "
        return prompt

    @classmethod
    def num_tokens_from_string(cls, string: str, encoding_name: str = "cl100k_base") -> int:
        """Returns the number of tokens in a text string."""
        encoding = tiktoken.get_encoding(encoding_name)
        num_tokens = len(encoding.encode(string))
        return num_tokens

    @classmethod
    def functions_to_tools_string(cls, functions):
        """
        trans openai functions to Claude2 tool
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
                    return_str = return_str + "<parameter>\n <name>name" + parameter_obj['type'] + "</name>\n"
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
        tell Claude2, function call return
        """
        return_str = """In this environment you have access to a set of tools you can use to answer the user's question.
                        Your answer must be english.You may call them like this.
                        Only invoke one function at a time and wait for the results before invoking another function:\n
                        <function_calls>\n"""
        return_str = return_str + "<tool_name>$TOOL_NAME</tool_name>\n"
        return_str = return_str + "<parameters>\n"
        return_str = return_str + "<$PARAMETER_NAME>$PARAMETER_VALUE</$PARAMETER_NAME>\n ... \n"
        return_str = return_str + "</parameters>\n"
        return_str = return_str + "</function_calls>\n"
        return return_str
        pass

    @classmethod
    def function_call_to_xml(cls, data):
        """
        openai message function_call to Claude2 message
        """
        return_str = "<function_calls>\n<invoke>"
        return_str = return_str + "<tool_name>" + data['name'] + "</tool_name>\n"
        return_str = return_str + "<arguments>" + data['arguments'] + "</arguments>\n"
        return_str = return_str + "</invoke>\n</function_calls>\n"
        return return_str
        pass

    @classmethod
    def return_to_open_function_call(cls, data, completion_tokens):
        """
        trans Claude2 to openai function call return
        """
        root = E_T.fromstring(data['completion'])
        invokes = root.findall("invoke")
        if len(invokes) < 1:
            raise Exception("Get Claude2 function call error")
        else:
            function_call = {}
            tool_name = invokes[0].find("tool_name").text
            function_call['name'] = tool_name
            args = invokes[0].find("parameters")
            args_obj = {}
            for item in args:
                arg_name = item.tag
                arg_value = item.text
                args_obj[arg_name] = arg_value

        return {
            "id": f"chatcmpl-{str(time.time())}",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": Claude_AI_MODEL,
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": completion_tokens,
                "total_tokens": completion_tokens,
            },
            "choices": [
                {
                    "role": "assistant",
                    "content": None,
                    "function_call": {
                        "name": tool_name,
                        "arguments": json.dumps(args_obj)
                    },
                    "index": 0,
                    "finish_reason": "function_call",
                    "logprobs": None
                }
            ]
        }
        pass
