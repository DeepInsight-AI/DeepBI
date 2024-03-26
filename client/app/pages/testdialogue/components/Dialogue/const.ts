
// export const DEEPBI_API_HOST = process.env.SSE_URL

export const API_LOCAL = window.location.protocol + '//' + window.location.hostname + ':8341';

// export const API_CLOAL_HOST = `${API_LOCAL}${DEEPBI_API_HOST}` as const;

export const API_CHAT = `${API_LOCAL}/api/chat` as const;