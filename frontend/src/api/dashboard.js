// 作用：前端 API：仪表盘相关后端接口调用封装。

import { getJSON } from './http'

function appendList(params, key, values) {
  for (const v of values || []) {
    const n = Number(v)
    if (!Number.isFinite(n)) continue
    params.append(key, String(n))
  }
}

function buildQuery({
  projectId,
  startDate,
  endDate,
  platformIds,
  brandIds,
  extra = {},
}) {
  const params = new URLSearchParams()
  params.set('project_id', String(projectId))
  params.set('start_date', String(startDate))
  params.set('end_date', String(endDate))
  if (Array.isArray(platformIds) && platformIds.length > 0) appendList(params, 'platform_ids', platformIds)
  if (Array.isArray(brandIds) && brandIds.length > 0) appendList(params, 'brand_ids', brandIds)
  for (const [k, v] of Object.entries(extra || {})) {
    if (v == null) continue
    params.set(k, String(v))
  }
  return params.toString()
}

export async function fetchDashboardOverviewByBrand(
  { projectId, startDate, endDate, platformIds, brandIds },
  options = {}
) {
  const qs = buildQuery({ projectId, startDate, endDate, platformIds, brandIds })
  return await getJSON(`/api/dashboard/overview_by_brand?${qs}`, { retries: 2, ...options })
}

export async function fetchDashboardSentimentTrendDailyByBrand(
  { projectId, startDate, endDate, platformIds, brandIds, topN = 4 },
  options = {}
) {
  const qs = buildQuery({
    projectId,
    startDate,
    endDate,
    platformIds,
    brandIds,
    extra: { top_n: topN },
  })
  return await getJSON(`/api/dashboard/sentiment_trend_daily_by_brand?${qs}`, { retries: 2, ...options })
}

export async function fetchDashboardKeywordMonitorStacked(
  { projectId, startDate, endDate, platformIds, brandIds, topN = 15 },
  options = {}
) {
  const qs = buildQuery({
    projectId,
    startDate,
    endDate,
    platformIds,
    brandIds,
    extra: { top_n: topN },
  })
  return await getJSON(`/api/dashboard/keyword_monitor_stacked?${qs}`, { retries: 2, ...options })
}

export async function fetchDashboardTopicMonitorStacked(
  { projectId, startDate, endDate, platformIds, brandIds, topN = 15 },
  options = {}
) {
  const qs = buildQuery({
    projectId,
    startDate,
    endDate,
    platformIds,
    brandIds,
    extra: { top_n: topN },
  })
  const res = await getJSON(`/api/dashboard/topic_monitor_stacked?${qs}`, { retries: 2, ...options })
  // Backend returns [{topic,data}], but chart expects [{keyword,data}].
  const dates = Array.isArray(res?.dates) ? res.dates : []
  const seriesRaw = Array.isArray(res?.series) ? res.series : []
  return {
    ...res,
    dates,
    series: seriesRaw
      .map((it) => ({
        keyword: String(it?.topic ?? '').trim(),
        data: Array.isArray(it?.data) ? it.data : [],
      }))
      .filter((it) => it.keyword),
  }
}

export async function fetchDashboardFeatureMonitorStacked(
  { projectId, startDate, endDate, brandIds, topN = 15 },
  options = {}
) {
  const qs = buildQuery({
    projectId,
    startDate,
    endDate,
    platformIds: null,
    brandIds,
    extra: { top_n: topN },
  })
  return await getJSON(`/api/dashboard/feature_monitor_stacked?${qs}`, { retries: 2, ...options })
}
