import tiktoken


# 定义函数 num_tokens_from_messages，该函数返回由一组消息所使用的token数。
def num_tokens_from_messages(messages, model="gpt-3.5-turbo"):
    """Return the number of tokens used by a list of messages."""

    num_tokens = 0
    # 计算每条消息的token数
    tokens_per_message = 4
    tokens_per_name = 1
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(str(value)) / 4
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += 3  # 每条回复都以助手为首
    return num_tokens


def num_tokens_from_messages_old(messages, model="gpt-3.5-turbo"):
    """Return the number of tokens used by a list of messages."""
    # 尝试获取模型的编码
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        # 如果模型没有找到，使用 cl100k_base 编码并给出警告
        print("Warning: model not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")
    # 针对不同的模型设置token数量
    if model in {
        "gpt-3.5-turbo-0613",
        "gpt-3.5-turbo-16k-0613",
        "gpt-4-0314",
        "gpt-4-32k-0314",
        "gpt-4-0613",
        "gpt-4-32k-0613",
    }:
        tokens_per_message = 3
        tokens_per_name = 1
    elif model == "gpt-3.5-turbo-0301":
        tokens_per_message = 4  # 每条消息遵循 {role/name}\n{content}\n 格式
        tokens_per_name = -1  # 如果有名字，角色会被省略
    elif "gpt-3.5-turbo" in model:
        # 对于 gpt-3.5-turbo 模型可能会有更新，此处返回假设为 gpt-3.5-turbo-0613 的token数量，并给出警告
        print("Warning: gpt-3.5-turbo may update over time. Returning num tokens assuming gpt-3.5-turbo-0613.")
        return num_tokens_from_messages(messages, model="gpt-3.5-turbo-0613")
    elif "gpt-4" in model:
        # 对于 gpt-4 模型可能会有更新，此处返回假设为 gpt-4-0613 的token数量，并给出警告
        print("Warning: gpt-4 may update over time. Returning num tokens assuming gpt-4-0613.")
        return num_tokens_from_messages(messages, model="gpt-4-0613")
    elif model in {
        "davinci",
        "curie",
        "babbage",
        "ada"
    }:
        print("Warning: gpt-3 related model is used. Returning num tokens assuming gpt2.")
        encoding = tiktoken.get_encoding("gpt2")
        num_tokens = 0
        # only calc the content
        for message in messages:
            for key, value in message.items():
                if key == "content":
                    num_tokens += len(encoding.encode(value))
        return num_tokens
    else:
        # 对于没有实现的模型，抛出未实现错误
        raise NotImplementedError(
            f"""num_tokens_from_messages() is not implemented for model {model}. See https://github.com/openai/openai-python/blob/main/chatml.md for information on how messages are converted to tokens."""
        )
    num_tokens = 0
    # 计算每条消息的token数
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += 3  # 每条回复都以助手为首
    return num_tokens



if __name__ == '__main__':
    message = [
        {
            "role": "system",
            "content": """12345678901234""",
        }
    ]

    num_tokens = num_tokens_from_messages(message)
    print("num_tokens : ", num_tokens)
