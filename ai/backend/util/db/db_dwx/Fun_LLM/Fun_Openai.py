# import openai
#
# openai.api_key = "sk-hgsnTlFvOTQVOnHa4pBtLuJec8TjnfaNuBdqeLGp1S1lYZL8"
#
#
# # 提问代码
# def chat_gpt(prompt):
#     # 调用 ChatGPT 接口
#     try:
#         model_engine = "text-davinci-003"
#         completion = openai.Completion.create(
#             engine=model_engine,
#             prompt=prompt,
#             max_tokens=1024,
#             n=1,
#             stop=None,
#             temperature=0.5,
#         )
#         response = completion.choices[0].text
#         print(response)
#     except Exception as error:
#         print("Error while connecting to dwx_mysql:", error)
#         return None
#
#
# if __name__ == '__main__':
#     (chat_gpt("cesium如何将3Dtile模型放在对应的位置上"))
#      # 你需要提出的问题)
