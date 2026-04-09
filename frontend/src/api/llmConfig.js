// 作用：前端 API：LLM 配置相关后端接口调用封装。

import { getJSON } from './http'

async function sendJSON(path, method, body, options = {}) {
  const API_BASE = import.meta.env.VITE_API_BASE || ''
  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: body == null ? null : JSON.stringify(body),
    signal: options.signal,
  })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    try {
      const j = JSON.parse(text)
      if (j?.detail) {
        throw new Error(`HTTP ${res.status}: ${typeof j.detail === 'string' ? j.detail : JSON.stringify(j.detail)}`)
      }
    } catch {
      // ignore JSON parse errors
    }
    throw new Error(`HTTP ${res.status}: ${text || res.statusText}`)
  }
  return await res.json().catch(() => ({}))
}

export async function fetchLLMConfig(options = {}) {
  return await getJSON('/api/llm/config', { retries: 1, retryDelayMs: 200, ...options })
}

export async function fetchLLMModels(options = {}) {
  return await getJSON('/api/llm/models', { retries: 1, retryDelayMs: 200, ...options })
}

export async function putLLMConfig(items, options = {}) {
  return await sendJSON('/api/llm/config', 'PUT', { items }, options)
}
