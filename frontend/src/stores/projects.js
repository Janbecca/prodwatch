// 作用：前端状态：项目相关状态管理（store）。

import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { fetchEnabledProjects } from '../api/projects'

const LS_KEY = 'prodwatch_active_project_id'

function readPersistedId() {
  const raw = localStorage.getItem(LS_KEY)
  if (!raw) return null
  const n = Number(raw)
  return Number.isFinite(n) ? n : null
}

function writePersistedId(id) {
  if (id == null) {
    localStorage.removeItem(LS_KEY)
    return
  }
  localStorage.setItem(LS_KEY, String(id))
}

export const useProjectsStore = defineStore('projects', () => {
  const projects = ref([])
  const loading = ref(false)
  const error = ref(null)

  // Requirement (1): first enter -> prefer restoring from localStorage
  const activeProjectId = ref(readPersistedId())
  const issues = ref([])

  const activeProject = computed(() => {
    return projects.value.find((p) => p.id === activeProjectId.value) || null
  })

  function selfCheck(context) {
    const nextIssues = []
    const ids = new Set(projects.value.map((p) => p.id))

    if (projects.value.length === 0) {
      if (activeProjectId.value != null) {
        nextIssues.push(`[${context}] projects empty but activeProjectId=${activeProjectId.value}`)
      }
    } else {
      if (activeProjectId.value == null) {
        nextIssues.push(`[${context}] activeProjectId is null but projects not empty`)
      } else if (!ids.has(activeProjectId.value)) {
        nextIssues.push(`[${context}] activeProjectId=${activeProjectId.value} not in projects list`)
      }
    }

    issues.value = nextIssues
    if (nextIssues.length > 0) {
      // eslint-disable-next-line no-console
      console.warn('[projectsStore selfCheck]', ...nextIssues)
    }
  }

  async function fetchProjects() {
    loading.value = true
    error.value = null
    try {
      const list = await fetchEnabledProjects()
      projects.value = Array.isArray(list) ? list : []

      const persisted = readPersistedId()
      const ids = new Set(projects.value.map((p) => p.id))

      if (projects.value.length === 0) {
        activeProjectId.value = null
        writePersistedId(null)
        selfCheck('fetchProjects(empty)')
        return
      }

      if (persisted != null && ids.has(persisted)) {
        activeProjectId.value = persisted
        writePersistedId(persisted)
        selfCheck('fetchProjects(persisted_ok)')
        return
      }

      // Requirement (2): persisted id invalid -> fallback to first available project
      const firstId = projects.value[0]?.id ?? null
      activeProjectId.value = firstId
      writePersistedId(firstId)
      selfCheck('fetchProjects(fallback_first)')
    } catch (e) {
      error.value = e?.message || String(e)
      projects.value = []
      activeProjectId.value = null
      issues.value = []
    } finally {
      loading.value = false
    }
  }

  function setActiveProject(id) {
    const next = id == null ? null : Number(id)
    const ids = new Set(projects.value.map((p) => p.id))
    if (next == null || !ids.has(next)) return
    activeProjectId.value = next
    writePersistedId(next)
    selfCheck('setActiveProject')
  }

  return {
    projects,
    activeProjectId,
    activeProject,
    loading,
    error,
    issues,
    fetchProjects,
    setActiveProject,
  }
})
