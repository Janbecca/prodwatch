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
    throw new Error(`HTTP ${res.status}: ${text || res.statusText}`)
  }
  return await res.json().catch(() => ({}))
}

export async function manualRefreshProject(projectId, payload = {}, options = {}) {
  return await sendJSON(`/api/projects/${projectId}/refresh`, 'POST', payload, options)
}

