import toast from "react-hot-toast";
import { currentUser } from "@/services/auth";

const ERROR_MESSAGE =
  "Error generating chart. Check the Developer Console AND the backend logs for details. Feel free to open a Github issue.";

export function generateCode(
  wsRef,
  params,
  onChange,
  onCancel
) {
  const wsProtocol = 'ws';
  if (window.location.protocol === 'https:') {
    wsProtocol = 'wss';
  }
  const wsUrl = `${wsProtocol}://${process.env.SOCKET}${currentUser.id}_${currentUser.name}`;
  console.log("Connecting to backend @ ", wsUrl);

  const ws = new WebSocket(wsUrl);
  wsRef.current = ws;

  ws.addEventListener("open", () => {
    ws.send(JSON.stringify(params));
  });

  ws.addEventListener("message", async (event) => {
    const response = JSON.parse(event.data);
    onChange(response);
  });

  ws.addEventListener("close", (event) => {
    console.log("Connection closed", event.code, event.reason);
    onCancel();
  });

  ws.addEventListener("error", (error) => {
    console.error("WebSocket error", error);
    toast.error(ERROR_MESSAGE);
  });
}