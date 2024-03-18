
console.log(process.env);
console.log(process.env.SOCKET,"SOCKET")
console.log(process.env.REACT_APP_SOCKET_URL, "REACT_APP_SOCKET_URL")
console.log(process.env.REACT_APP_SSE_URL, "REACT_APP_SSE_URL")

export const DEEPBI_API_HOST = process.env.REACT_APP_SSE_URL

export const API_LOCAL = window.location.protocol + '//'

export const API_CHAT = `${API_LOCAL}${DEEPBI_API_HOST}/api/chat` as const;