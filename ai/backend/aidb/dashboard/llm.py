# openai_response.py
import json
import requests
from prompts import assemble_prompt

def openai_response(data):
    config = {
    "token": "",
    "module": "gpt-4-1106-preview",
    "api_base": "https://apiserver.deep-thought.io/proxy",
    }
    data_json = json.dumps(data)
    data = {
            "messages": assemble_prompt(data['chart_type'], data_json),
        }
    # 设置自定义的请求头
    headers = {
            "token": config['token'],
            "ai_name": "openai",
            "module": config['module']
        }
    # 发送带请求头的 POST 请求到服务器
    url = config['api_base']  # 根据你的实际地址修改
    res = requests.post(url, json=data, headers=headers)
    response = res.json()
    response_content = response['choices'][0]['message']['content']
    return response_content
