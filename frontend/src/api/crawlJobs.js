// 用途：前端 API：crawl_job 状态/进度查询
import { getJSON } from './http'

function normalizeId(v) {
  const id = Number(v)
  if (!Number.isFinite(id) || id <= 0) throw new Error('无效的任务编号')
  return Math.trunc(id)
}

export async function fetchCrawlJobStatus(crawlJobId, options = {}) {
  const id = normalizeId(crawlJobId)
  const qs = new URLSearchParams()
  qs.set('crawl_job_id', String(id))
  return await getJSON(`/api/crawl_jobs/status?${qs.toString()}`, { retries: 1, retryDelayMs: 200, ...options })
}

export async function fetchCrawlJobProgress(crawlJobId, options = {}) {
  const id = normalizeId(crawlJobId)
  const qs = new URLSearchParams()
  qs.set('crawl_job_id', String(id))
  return await getJSON(`/api/crawl_jobs/progress?${qs.toString()}`, { retries: 0, ...options })
}

