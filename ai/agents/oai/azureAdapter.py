# -*- coding: utf-8 -*-
try:
    import openai
    OPENAI_VERSION = openai.__version__
except ImportError:
    raise Exception("Error, need: pip install openai")

AZURE_OPENAI_API_DEFAULT_MODEL = "gpt-4"
OPEN_AI_TIMEOUT = 120
AZURE_OPENAI_API_VERSION = "2023-12-01-preview"


class AzureClient:
    @classmethod
    def run(cls, apiKey, data, model, use_api_host=None):
        """
        :param data: requrest
        :param model:  default is azure setting gpt-4
        :return: ai response
        """

        if apiKey is None or "" == apiKey:
            raise Exception("Error, need: apiKey")
        if use_api_host is None or "" == use_api_host:
            raise Exception("Error, need: apiHost")

        model = model if model else AZURE_OPENAI_API_DEFAULT_MODEL
        if OPENAI_VERSION.startswith("1."):
            from openai import AzureOpenAI
            # version 1.x.x
            client = AzureOpenAI(
                api_key=apiKey,
                api_version=AZURE_OPENAI_API_VERSION,
                azure_endpoint=use_api_host,
                timeout=OPEN_AI_TIMEOUT
            )
            response = client.chat.completions.create(
                engine=model,
                messages=data['messages'],
                functions=data["functions"] if "functions" in data else None,
                timeout=OPEN_AI_TIMEOUT,
            )
            pass
        elif OPENAI_VERSION.startswith("0."):
            # version 0.x.x
            openai.api_key = apiKey
            openai.api_base = use_api_host
            openai.api_type = 'azure'
            openai.api_version = AZURE_OPENAI_API_VERSION  # this might change in the future
            if "functions" in data:
                response = openai.ChatCompletion.create(
                    engine=model,
                    messages=data['messages'],
                    functions=data["functions"],
                    timeout=OPEN_AI_TIMEOUT,
                )
            else:
                response = openai.ChatCompletion.create(
                    engine=model,
                    messages=data['messages'],
                    timeout=OPEN_AI_TIMEOUT,
                )
        else:
            raise Exception("Python extension: openai version error")
        return response
