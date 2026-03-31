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
        const err = new Error(`HTTP ${res.status}: ${text || res.statusText}`)
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
