import { defineStore } from 'pinia'
import { computed, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'

import { fetchProjectConfig } from '../api/projectConfig'
import { manualRefreshProject } from '../api/projectRefresh'
import { useProjectsStore } from './projects'

function uniqNums(arr) {
  const out = []
  const seen = new Set()
  for (const x of arr || []) {
    const n = Number(x)
    if (!Number.isFinite(n)) continue
    if (seen.has(n)) continue
    seen.add(n)
    out.push(n)
  }
  return out
}

function normalizeTimeKey(value) {
  const v = String(value || '')
  if (v === '7d' || v === '14d' || v === '30d' || v === 'custom') return v
  return '14d'
}

export const useDashboardStore = defineStore('dashboard', () => {
  const projectsStore = useProjectsStore()

  const projectId = ref(null)
  const scopeLoading = ref(false)
  const scopeError = ref('')

  const brandOptions = ref([]) // [{id,name,...}]
  const platformOptions = ref([]) // [{id,code,name,...}]

  const brandIds = ref([])
  const platformIds = ref([])

  const timeKey = ref('14d') // 7d | 14d | 30d | custom
  const customRange = ref([]) // ['YYYY-MM-DD','YYYY-MM-DD']

  const reloadSeq = ref(0)
  const refreshLoading = ref(false)

  const activeProjectId = computed(() => projectsStore.activeProjectId)
  const activeProject = computed(() => projectsStore.activeProject)
  const enabledProjectId = computed(() => {
    const p = activeProject.value
    if (!p) return null
    if (Number(p.is_active || 0) !== 1) return null
    return Number(p.id)
  })

  const timeQuery = computed(() => {
    const k = normalizeTimeKey(timeKey.value)
    if (k === '7d') return { days: 7 }
    if (k === '14d') return { days: 14 }
    if (k === '30d') return { days: 30 }
    const [start, end] = customRange.value || []
    if (!start || !end) return { start_date: null, end_date: null }
    return { start_date: start, end_date: end }
  })

  async function loadScope(pid) {
    if (!pid) return
    scopeLoading.value = true
    scopeError.value = ''
    // Avoid briefly showing stale filters when switching projects.
    brandOptions.value = []
    platformOptions.value = []
    brandIds.value = []
    platformIds.value = []
    try {
      const data = await fetchProjectConfig(pid)
      brandOptions.value = Array.isArray(data?.brands) ? data.brands : []
      platformOptions.value = Array.isArray(data?.platforms) ? data.platforms : []

      // Reasonable defaults: select all platforms; select up to 4 brands.
      const nextBrandIds = brandOptions.value.map((b) => b.id).slice(0, 4)
      const nextPlatformIds = platformOptions.value.map((p) => p.id)
      brandIds.value = uniqNums(nextBrandIds)
      platformIds.value = uniqNums(nextPlatformIds)
    } catch (e) {
      scopeError.value = e?.message || String(e)
      brandOptions.value = []
      platformOptions.value = []
      brandIds.value = []
      platformIds.value = []
    } finally {
      scopeLoading.value = false
    }
  }

  async function ensureProject(pid) {
    const n = pid == null ? null : Number(pid)
    if (!n) return
    if (projectId.value === n && (brandOptions.value.length > 0 || platformOptions.value.length > 0)) return
    projectId.value = n
    await loadScope(n)
  }

  function clearScope() {
    projectId.value = null
    scopeLoading.value = false
    scopeError.value = ''
    brandOptions.value = []
    platformOptions.value = []
    brandIds.value = []
    platformIds.value = []
  }

  function setBrandIds(next) {
    const ids = uniqNums(next)
    if (ids.length > 4) {
      ElMessage.warning('品牌最多选择 4 个')
      brandIds.value = ids.slice(0, 4)
      return
    }
    brandIds.value = ids
  }

  function setPlatformIds(next) {
    platformIds.value = uniqNums(next)
  }

  function setTimeKey(next) {
    timeKey.value = normalizeTimeKey(next)
  }

  function setCustomRange(next) {
    if (!Array.isArray(next)) {
      customRange.value = []
      return
    }
    customRange.value = next.slice(0, 2)
  }

  async function manualRefresh() {
    const pid = enabledProjectId.value
    if (!pid) return
    refreshLoading.value = true
    try {
      const res = await manualRefreshProject(pid, {})
      reloadSeq.value += 1
      ElMessage.success(`已触发刷新（job_id=${res?.crawl_job_id ?? '-'}）`)
    } catch (e) {
      ElMessage.error(e?.message || String(e))
      throw e
    } finally {
      refreshLoading.value = false
    }
  }

  // Keep scope in sync with the global active project.
  watch(
    () => activeProject.value,
    (p) => {
      if (!p) {
        clearScope()
        return
      }
      if (Number(p.is_active || 0) !== 1) {
        clearScope()
        return
      }
      ensureProject(p.id)
    },
    { immediate: true }
  )

  return {
    // scope
    projectId,
    scopeLoading,
    scopeError,
    brandOptions,
    platformOptions,

    // filters
    brandIds,
    platformIds,
    timeKey,
    customRange,
    timeQuery,

    // refresh
    reloadSeq,
    refreshLoading,
    manualRefresh,
    enabledProjectId,

    // setters
    ensureProject,
    setBrandIds,
    setPlatformIds,
    setTimeKey,
    setCustomRange,
  }
})
