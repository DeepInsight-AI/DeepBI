import asyncio
import websockets
import time
from ai.backend.chat_task import ChatClass


class WSServer:

    def __init__(self, server_port):
        self.server_port = server_port

    def serve_forever(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        server_ip = "0.0.0.0"
        # server_port = 5001
        server_port = self.server_port
        start_server = websockets.serve(self.handler, server_ip, server_port, ping_interval=None)

        # start_server = websockets.serve(lambda x, y: router(x, y), server_ip, server_port, ping_interval=None)
        print(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        print("=== start WebSocket server ===")
        print("start listen " + server_ip + ":" + str(server_port))

        # start_server = websockets.serve(self.handler, '0.0.0.0', 5678)
        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()

    async def handler(self, websocket, path):
        master = ChatClass(websocket, path)

        while True:
            listener_task = asyncio.ensure_future(master.get_message())
            producer_task = asyncio.ensure_future(master.produce())
            done, pending = await asyncio.wait(
                [listener_task, producer_task],
                return_when=asyncio.FIRST_COMPLETED)

            if listener_task in done:
                await master.consume()
            else:
                listener_task.cancel()

            if producer_task in done:
                msg_to_send = producer_task.result()
                await master.send_message(msg_to_send)
            else:
                producer_task.cancel()
