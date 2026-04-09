// 用途：前端 API：crawl_job 状态查询
import { getJSON } from './http'

export async function fetchCrawlJobStatus(crawlJobId, options = {}) {
  const id = Number(crawlJobId)
  if (!Number.isFinite(id) || id <= 0) throw new Error('无效的任务编号')
  const qs = new URLSearchParams()
  qs.set('crawl_job_id', String(id))
  return await getJSON(`/api/crawl_jobs/status?${qs.toString()}`, { retries: 1, retryDelayMs: 200, ...options })
}
