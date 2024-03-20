import toast from "react-hot-toast";
import { currentUser } from "@/services/auth";

export function generateChart(
  wsRef,
  params,
  onChange,
  onCancel
) {
  let wsProtocol = 'ws';
  if (window.location.protocol === 'https:') {
    wsProtocol = 'wss';
  }
  const wsUrl = `${wsProtocol}://${process.env.SOCKET}${currentUser.id}_${currentUser.name}`;

  const onError = (error) => {
    console.error("WebSocket error", error);
    toast.error(window.W_L.ERROR_MESSAGE);
  };

  const setupWebSocket = (ws) => {
    ws.addEventListener("message", async (event) => {
      const response = JSON.parse(event.data);
      onChange(response);
    });

    ws.addEventListener("close", (event) => {
      console.log("Connection closed", event.code, event.reason);
      onCancel();
    });

    ws.addEventListener("error", onError);
  };

  // 清理旧的WebSocket事件监听器
  if (wsRef.current) {
    wsRef.current.removeEventListener("message", onChange);
    wsRef.current.removeEventListener("close", onCancel);
    wsRef.current.removeEventListener("error", onError);
    if (wsRef.current.readyState === 1) {
      console.log("old ws connection is open");
      wsRef.current.send(JSON.stringify(params));
      setupWebSocket(wsRef.current);
      return;
    }
  }

  console.log("new ws connection");
  const ws = new WebSocket(wsUrl);
  wsRef.current = ws;

  ws.addEventListener("open", () => {
    console.log("new params", params);
    ws.send(JSON.stringify(params));
    setupWebSocket(ws);
  });
}