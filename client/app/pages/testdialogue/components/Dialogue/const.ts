
export const DEEPBI_API_HOST = process.env.SSE_URL

export const API_LOCAL = window.location.protocol + '//'

export const API_CHAT = `${API_LOCAL}${DEEPBI_API_HOST}/api/chat` as const;