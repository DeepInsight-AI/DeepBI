# from dotenv import load_dotenv
#
# # 加载 .env 文件中的环境变量
# load_dotenv()
#
# # import asyncio
# from ai.backend.start_server import WSServer
#
# if __name__ == '__main__':
#     server_port = 8339
#     s = WSServer(server_port)
#     # t = threading.Thread(target=s.serve_forever)
#     # t.daemon = True
#     # t.start()
#     s.serve_forever()


import ai_chat_api
if __name__ == '__main__':
    app = ai_chat_api.app
    app.run(port=8341, host='0.0.0.0')
