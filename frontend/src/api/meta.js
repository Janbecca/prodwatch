import { getJSON } from './http'

export async function fetchBrands(options = {}) {
  const data = await getJSON('/api/meta/brands', options)
  return Array.isArray(data?.brands) ? data.brands : []
}

export async function fetchPlatforms(options = {}) {
  const data = await getJSON('/api/meta/platforms', options)
  return Array.isArray(data?.platforms) ? data.platforms : []
}

