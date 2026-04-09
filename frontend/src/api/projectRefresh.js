// 作用：前端 API：项目刷新相关后端接口调用封装。

async function sendJSON(path, method, body, options = {}) {
  const API_BASE = import.meta.env.VITE_API_BASE || ''
  let res
  try {
    res = await fetch(`${API_BASE}${path}`, {
      method,
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: body == null ? null : JSON.stringify(body),
      signal: options.signal,
    })
  } catch (e) {
    // Most commonly: server is down / connection reset / CORS / proxy issues.
    throw new Error(`请求失败：无法连接到服务器（${e?.message || String(e)}）`)
  }
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    // Try to extract FastAPI {detail: ...} for readable errors.
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

export async function manualRefreshProject(projectId, payload = {}, options = {}) {
  // Special-case 409: it is an expected "conflict/skip" scenario (refresh already running).
  // We return a structured payload instead of throwing so the UI won't print an error stack.
  const API_BASE = import.meta.env.VITE_API_BASE || ''
  let res
  try {
    res = await fetch(`${API_BASE}/api/projects/${projectId}/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: payload == null ? null : JSON.stringify(payload),
      signal: options.signal,
    })
  } catch (e) {
    throw new Error(`请求失败：无法连接到服务器（${e?.message || String(e)}）`)
  }

  if (res.ok) return await res.json().catch(() => ({}))

  const text = await res.text().catch(() => '')
  let detail = text || res.statusText
  try {
    const j = JSON.parse(text)
    if (j?.detail) detail = typeof j.detail === 'string' ? j.detail : JSON.stringify(j.detail)
  } catch {
    // ignore
  }

  if (res.status === 409) {
    return { ok: false, skipped: true, status: 409, detail }
  }
  throw new Error(`HTTP ${res.status}: ${detail}`)
}

export async function fetchProjectRefreshStatus(projectId, options = {}) {
  const API_BASE = import.meta.env.VITE_API_BASE || ''
  const res = await fetch(`${API_BASE}/api/projects/${projectId}/refresh/status`, {
    method: 'GET',
    headers: { Accept: 'application/json' },
    signal: options.signal,
  })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`HTTP ${res.status}: ${text || res.statusText}`)
  }
  return await res.json().catch(() => ({}))
}
