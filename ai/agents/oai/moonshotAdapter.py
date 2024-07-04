# moonshotAdapter.py
import requests
import json
import time
Moonshot_DEFAULT_URL = "https://api.moonshot.cn/v1/chat/completions"
Moonshot_MODEL = 'moonshot-v1-128k'
Moonshot_TEMPERATURE = 0.3


class MoonshotClient:

    @classmethod
    def run(cls, apiKey, data, model = None, use_url=None):
        if not apiKey:
            raise Exception("Moonshot Api key is required")
        messages_copy = cls.transform_message_role(data['messages'])
        model = model if model else Moonshot_MODEL
        use_url = use_url if use_url else Moonshot_DEFAULT_URL
        ai_result = cls.call_moonshot(apiKey,messages_copy,model)

        if ai_result:
            return ai_result
        else:
            return False

    @classmethod
    def call_moonshot(cls, apiKey, messages, model=Moonshot_MODEL, use_url=Moonshot_DEFAULT_URL ,temperature= Moonshot_TEMPERATURE):
        try:
            payload = json.dumps({
                "model": model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": 1024
            })
            headers = {
                "Content-Type": "application/json",
                "Authorization": f'Bearer {apiKey}'
            }
            response = requests.post(use_url, headers=headers, data=payload)
            result = response.json()
        except Exception as e:
            print(f"Error: {e}")
            result = False
        return result

    @classmethod
    def transform_message_role(cls, messages):
        transformed_messages = []
        for msg in messages:
            transformed_messages.append({
                "role": msg["role"],
                "content": msg["content"]
        })
        return transformed_messages
