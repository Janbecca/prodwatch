// 作用：前端工具：date通用工具函数。

function pad2(n) {
  return String(n).padStart(2, '0')
}

export function formatDateYYYYMMDD(d) {
  const dt = d instanceof Date ? d : new Date(d)
  return `${dt.getFullYear()}-${pad2(dt.getMonth() + 1)}-${pad2(dt.getDate())}`
}

export function shiftDays(date, deltaDays) {
  const d = new Date(date.getTime())
  d.setDate(d.getDate() + Number(deltaDays || 0))
  return d
}

// timeQuery from dashboard store:
// - { days: 7|14|30 }
// - { start_date: 'YYYY-MM-DD', end_date: 'YYYY-MM-DD' }
export function resolveDateRange(timeQuery) {
  if (!timeQuery) return null

  if (Number.isFinite(Number(timeQuery.days))) {
    const days = Math.max(1, Number(timeQuery.days))
    const end = new Date()
    const start = shiftDays(end, -(days - 1))
    return { startDate: formatDateYYYYMMDD(start), endDate: formatDateYYYYMMDD(end) }
  }

  const startDate = timeQuery.start_date
  const endDate = timeQuery.end_date
  if (!startDate || !endDate) return null
  return { startDate: String(startDate), endDate: String(endDate) }
}

