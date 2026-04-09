// 作用：前端 API：项目相关后端接口调用封装。

import { getJSON } from './http'

// Use /list to make newly created inactive projects visible in the UI.
// /enabled remains available for "only active projects" use-cases.
const PROJECTS_LIST_PATH = '/api/projects/list'

export async function fetchEnabledProjects() {
  const data = await getJSON(PROJECTS_LIST_PATH)
  if (Array.isArray(data)) return data
  if (Array.isArray(data?.projects)) return data.projects
  if (Array.isArray(data?.items)) return data.items
  return []
}
