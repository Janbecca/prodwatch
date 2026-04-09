// 作用：前端 API：元信息相关后端接口调用封装。

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
    let detail = null
    try {
      const j = JSON.parse(text)
      if (j?.detail != null) detail = j.detail
      else if (j?.message != null) detail = j.message
      else if (j?.error != null) detail = j.error
    } catch {
      // ignore
    }
    const msg = detail != null ? (typeof detail === 'string' ? detail : JSON.stringify(detail)) : (text || res.statusText)
    const err = new Error(`HTTP ${res.status}: ${msg}`)
    err.status = res.status
    throw err
  }
  return await res.json().catch(() => ({}))
}

export async function fetchBrands(options = {}) {
  const data = await getJSON('/api/meta/brands', options)
  return Array.isArray(data?.brands) ? data.brands : []
}

export async function createBrand(payload, options = {}) {
  return await sendJSON('/api/meta/brands', 'POST', payload, options)
}

export async function deleteBrand(brandId, options = {}) {
  return await sendJSON(`/api/meta/brands/${brandId}`, 'DELETE', null, options)
}

export async function fetchPlatforms(options = {}) {
  const data = await getJSON('/api/meta/platforms', options)
  return Array.isArray(data?.platforms) ? data.platforms : []
}
