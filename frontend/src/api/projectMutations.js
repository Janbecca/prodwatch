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

export async function createProject(payload, options = {}) {
  return await sendJSON('/api/projects', 'POST', payload, options)
}

export async function updateProject(projectId, payload, options = {}) {
  return await sendJSON(`/api/projects/${projectId}`, 'PUT', payload, options)
}

export async function setProjectActivation(projectId, isActive, status, options = {}) {
  return await sendJSON(
    `/api/projects/${projectId}/activation`,
    'POST',
    { is_active: isActive, status },
    options,
  )
}

export async function deleteProject(projectId, options = {}) {
  return await sendJSON(`/api/projects/${projectId}`, 'DELETE', null, options)
}
