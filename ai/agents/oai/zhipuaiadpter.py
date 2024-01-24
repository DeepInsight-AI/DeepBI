# -*- coding: utf-8 -*-
"""
info:  Define ZhiPuAI
"""
from zhipuai import ZhipuAI

# define default model
ZHIPU_AI_MODEL = "glm-4"


class ZhiPuAIClient:

    @classmethod
    def run(cls, apiKey, data):
        zhipu_data = cls.input_to_openai(data)
        client = ZhipuAI(api_key=apiKey)
        if "functions" in data:
            response = client.chat.completions.create(
                model=ZHIPU_AI_MODEL,  # 填写需要调用的模型名称
                messages=zhipu_data['messages'],
                functions=zhipu_data["functions"]
            )
        else:
            response = client.chat.completions.create(
                model=ZHIPU_AI_MODEL,
                messages=zhipu_data['messages']
            )
        return cls.output_to_openai(response)
        pass

    @classmethod
    def input_to_openai(cls, data):
        return data
        pass

    @classmethod
    def output_to_openai(cls, data):
        return data
        pass
