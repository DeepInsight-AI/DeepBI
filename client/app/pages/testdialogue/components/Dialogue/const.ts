
export const DEEPBI_API_HOST = process.env.REACT_APP_SSE_URL

export const API_LOCAL = window.location.protocol + '//'

export const API_CHAT = `${API_LOCAL}${DEEPBI_API_HOST}/api/chat` as const;