// 作用：前端 API：HTTP 客户端与拦截器封装相关后端接口调用封装。

const API_BASE = import.meta.env.VITE_API_BASE || ''

export async function getJSON(path, options = {}) {
  const retries = Number.isFinite(Number(options.retries)) ? Number(options.retries) : 0
  const retryDelayMs = Number.isFinite(Number(options.retryDelayMs)) ? Number(options.retryDelayMs) : 150

  let lastErr = null
  for (let attempt = 0; attempt <= retries; attempt++) {
    if (options.signal?.aborted) throw new DOMException('Aborted', 'AbortError')
    try {
      const res = await fetch(`${API_BASE}${path}`, {
        method: 'GET',
        headers: { Accept: 'application/json' },
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
          // ignore JSON parse errors
        }
        const msg = detail != null ? (typeof detail === 'string' ? detail : JSON.stringify(detail)) : (text || res.statusText)
        const err = new Error(`HTTP ${res.status}: ${msg}`)
        err.status = res.status
        // Retry only for transient backend busy (503).
        if (res.status === 503 && attempt < retries) {
          await new Promise((r) => setTimeout(r, retryDelayMs * (attempt + 1)))
          continue
        }
        throw err
      }
      return await res.json()
    } catch (e) {
      if (e?.name === 'AbortError') throw e
      lastErr = e
      // Network/proxy hiccup: retry when configured.
      if (attempt < retries) {
        await new Promise((r) => setTimeout(r, retryDelayMs * (attempt + 1)))
        continue
      }
      throw e
    }
  }
  throw lastErr
}
