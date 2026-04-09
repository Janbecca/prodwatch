// 作用：前端 API：帖子相关后端接口调用封装。

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
  params.set('project_id', String(filters.projectId))
  params.set('start_date', String(filters.startDate))
  params.set('end_date', String(filters.endDate))

  if (Array.isArray(filters.platformIds) && filters.platformIds.length) appendList(params, 'platform_ids', filters.platformIds)
  if (Array.isArray(filters.brandIds) && filters.brandIds.length) appendList(params, 'brand_ids', filters.brandIds)
  if (Array.isArray(filters.keywords) && filters.keywords.length) appendList(params, 'keywords', filters.keywords)
  if (Array.isArray(filters.sentiments) && filters.sentiments.length) appendList(params, 'sentiments', filters.sentiments)
  if (Array.isArray(filters.spamLabels) && filters.spamLabels.length) appendList(params, 'spam_labels', filters.spamLabels)

  if (filters.isValid !== null && filters.isValid !== undefined) params.set('is_valid', String(filters.isValid))

  if (filters.sentimentScoreMin !== null && filters.sentimentScoreMin !== undefined) {
    params.set('sentiment_score_min', String(filters.sentimentScoreMin))
  }
  if (filters.sentimentScoreMax !== null && filters.sentimentScoreMax !== undefined) {
    params.set('sentiment_score_max', String(filters.sentimentScoreMax))
  }

  if (filters.likeMin !== null && filters.likeMin !== undefined) params.set('like_min', String(filters.likeMin))
  if (filters.likeMax !== null && filters.likeMax !== undefined) params.set('like_max', String(filters.likeMax))
  if (filters.commentMin !== null && filters.commentMin !== undefined) params.set('comment_min', String(filters.commentMin))
  if (filters.commentMax !== null && filters.commentMax !== undefined) params.set('comment_max', String(filters.commentMax))
  if (filters.shareMin !== null && filters.shareMin !== undefined) params.set('share_min', String(filters.shareMin))
  if (filters.shareMax !== null && filters.shareMax !== undefined) params.set('share_max', String(filters.shareMax))

  if (filters.search && String(filters.search).trim() !== '') params.set('search', String(filters.search).trim())

  return params.toString()
}

export async function fetchPostsOverview(filters, options = {}) {
  const qs = buildQuery(filters)
  return await getJSON(`/api/posts/overview?${qs}`, { retries: 2, ...options })
}

export async function fetchPostsList(filters, { page = 1, pageSize = 20 } = {}, options = {}) {
  const qs = buildQuery(filters)
  const paging = new URLSearchParams()
  paging.set('page', String(page))
  paging.set('page_size', String(pageSize))
  return await getJSON(`/api/posts/list?${qs}&${paging.toString()}`, { retries: 2, ...options })
}

export async function fetchPostDetail({ projectId, postId }, options = {}) {
  const qs = new URLSearchParams()
  qs.set('project_id', String(projectId))
  qs.set('post_id', String(postId))
  return await getJSON(`/api/posts/detail?${qs.toString()}`, { retries: 1, ...options })
}
