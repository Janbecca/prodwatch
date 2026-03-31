import { getJSON } from './http'

function appendList(params, key, values) {
  for (const v of values || []) {
    if (v == null) continue
    const s = String(v).trim()
    if (!s) continue
    params.append(key, s)
  }
}

function buildQuery(filters) {
  const params = new URLSearchParams()
  if (filters.projectId != null) params.set('project_id', String(filters.projectId))
  if (filters.reportType) params.set('report_type', String(filters.reportType))

  const [dataStart, dataEnd] = filters.dataRange || []
  if (dataStart && dataEnd) {
    params.set('data_start_date', String(dataStart))
    params.set('data_end_date', String(dataEnd))
  }

  const [createdStart, createdEnd] = filters.createdRange || []
  if (createdStart && createdEnd) {
    params.set('start_date', String(createdStart))
    params.set('end_date', String(createdEnd))
  }

  if (filters.search && String(filters.search).trim() !== '') params.set('search', String(filters.search).trim())
  return params.toString()
}

export async function fetchReportsList(filters, { page = 1, pageSize = 20 } = {}, options = {}) {
  const qs = buildQuery(filters)
  const paging = new URLSearchParams()
  paging.set('page', String(page))
  paging.set('page_size', String(pageSize))
  const join = qs ? `${qs}&${paging.toString()}` : paging.toString()
  return await getJSON(`/api/reports/list?${join}`, { retries: 2, ...options })
}

export async function fetchReportDetail(reportId, options = {}) {
  const qs = new URLSearchParams()
  qs.set('report_id', String(reportId))
  return await getJSON(`/api/reports/detail?${qs.toString()}`, { retries: 1, ...options })
}

export async function createReport(payload, options = {}) {
  const res = await fetch(`/api/reports/create`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(payload || {}),
    signal: options.signal,
  })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    // Try to extract FastAPI validation detail if response is JSON.
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
  return await res.json()
}

export async function generateReport(reportId, { force = false } = {}, options = {}) {
  const res = await fetch(`/api/reports/generate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({ report_id: Number(reportId), force: Boolean(force) }),
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
      // ignore
    }
    throw new Error(`HTTP ${res.status}: ${text || res.statusText}`)
  }
  return await res.json()
}

export async function deleteReport(reportId, options = {}) {
  const qs = new URLSearchParams()
  qs.set('report_id', String(reportId))
  // getJSON is GET-only; use fetch directly here
  const res = await fetch(`/api/reports/delete?${qs.toString()}`, { method: 'DELETE', headers: { Accept: 'application/json' }, signal: options.signal })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`HTTP ${res.status}: ${text || res.statusText}`)
  }
  return await res.json()
}

export async function fetchReportEvidenceList(reportId, { page = 1, pageSize = 20 } = {}, options = {}) {
  const qs = new URLSearchParams()
  qs.set('report_id', String(reportId))
  qs.set('page', String(page))
  qs.set('page_size', String(pageSize))
  return await getJSON(`/api/reports/evidence/list?${qs.toString()}`, { retries: 2, ...options })
}
