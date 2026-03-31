import { getJSON } from './http'

export async function fetchProjectConfig(projectId, options = {}) {
  const data = await getJSON(`/api/projects/${projectId}/config`, options)
  return {
    project: data?.project || null,
    brands: Array.isArray(data?.brands) ? data.brands : [],
    platforms: Array.isArray(data?.platforms) ? data.platforms : [],
    keywords: Array.isArray(data?.keywords) ? data.keywords : [],
  }
}

