// 作用：前端状态：仪表盘相关状态管理（store）。
import { defineStore } from 'pinia'
import { computed, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'

import { fetchProjectConfig } from '../api/projectConfig'
import { fetchCrawlJobStatus } from '../api/crawlJobs'
import { fetchProjectRefreshStatus, manualRefreshProject } from '../api/projectRefresh'
import { useProjectsStore } from './projects'
import { useRefreshStore } from './refresh'

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
  const refreshStore = useRefreshStore()

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
  const lastRefreshJobId = ref(null)

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
      const msg = e?.message || String(e)
      scopeError.value = msg
      brandOptions.value = []
      platformOptions.value = []
      brandIds.value = []
      platformIds.value = []

      // Project might be deleted / not found; try to refresh global project list to recover.
      if (String(msg).includes('HTTP 404')) {
        scopeError.value = '项目不存在或已删除，请刷新项目列表'
        try {
          await projectsStore.fetchProjects()
        } catch {
          // ignore secondary errors
        }
      }
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
    if (refreshLoading.value) return
    refreshLoading.value = true
    refreshStore.startRefreshing(pid, { bannerAck: false })
    try {
      // Preflight status check: avoid calling /refresh when it's already running.
      // This prevents browsers from logging a 409 request stack trace in console.
      const st = await fetchProjectRefreshStatus(pid)
      if (st && st.running) {
        refreshStore.startRefreshing(pid, { bannerAck: false })
        const jobId = Number(st.crawl_job_id)
        if (Number.isFinite(jobId) && jobId > 0) {
          ElMessage.warning(`该项目正在刷新中（任务编号=${jobId}），请稍后重试。`)
        } else {
          ElMessage.warning('该项目正在刷新中，请稍后重试。')
        }
        return null
      }

      const res = await manualRefreshProject(pid, {})
      // Request returned quickly (refresh runs in background). Clear optimistic window;
      // /refresh/status polling decides final state.
      refreshStore.clearOptimistic(pid)
      if (res && res.skipped && Number(res.status) === 409) {
        ElMessage.warning(String(res.detail || '该项目正在刷新中，请稍后重试。'))
        return null
      }

      const jobId = Number(res?.crawl_job_id)
      lastRefreshJobId.value = Number.isFinite(jobId) && jobId > 0 ? jobId : null
      // Persist job id into refreshStore state so the completion watcher can fetch
      // `/api/crawl_jobs/status` even if `/refresh/status` later omits crawl_job_id (running=false).
      try {
        const st2 = refreshStore.getState(pid)
        if (st2 && lastRefreshJobId.value) st2.crawl_job_id = lastRefreshJobId.value
      } catch {
        // ignore
      }

      // 记录刷新来源（mock/llm/unknown），便于后续分析不同来源的刷新表现差异。
      // 注意该日志可能会失败（例如 res 结构不符合预期），但不应影响用户正常使用，因此放在独立的
      try {
        const pg = res?.post_generation_plan || res?.post_generation || null
        const by = String(pg?.generated_by || '').toLowerCase()
        const label = by === 'mock' ? 'mock' : by === 'llm' ? 'llm' : 'unknown'
        console.info('[prodwatch] manual refresh post generation:', {
          project_id: pid,
          crawl_job_id: lastRefreshJobId.value,
          generated_by: label,
          provider: pg?.provider || '',
          model: pg?.model || '',
          prompt_version: pg?.prompt_version || '',
        })
      } catch {
        // ignore logging failures
      }

      if (lastRefreshJobId.value) {
        ElMessage.success(`已触发刷新任务（任务编号=${lastRefreshJobId.value}），正在后台处理…`)
      } else {
        ElMessage.success('已触发刷新，正在后台处理…')
      }
    } catch (e) {
      const msg = e?.message || String(e)
      // 409 means refresh is skipped due to an in-flight job (expected behavior).
      if (String(msg).includes('HTTP 409')) {
        ElMessage.warning(msg.replace(/^HTTP 409:\s*/i, ''))
        return null
      }
      ElMessage.error(msg)
      // Do not rethrow; avoid unhandled promise rejection in UI event handlers.
      return null
    } finally {
      refreshLoading.value = false
      refreshStore.clearOptimistic(pid)
      // 确保刷新状态被正确清理
      // （例如在 /refresh/status 返回 running=false 但未携带 crawl_job_id 时，
      // 仍能正确停止刷新状态）。
      try {
        await refreshStore.syncStatus(pid)
      } catch {
        // ignore
      }
      if (!refreshStore.isRefreshing(pid)) refreshStore.stopRefreshing(pid)
    }
  }

  // 保持 scope 与 activeProject 同步；activeProject 变化时自动加载 scope。
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

  // Auto-reload dashboard panels after refresh finishes (running: true -> false).
  watch(
    () => {
      const pid = enabledProjectId.value
      if (!pid) return { pid: null, running: false, jobId: null }
      const st = refreshStore.getState(pid)
      const jobId = Number(st?.crawl_job_id)
      return {
        pid,
        running: Boolean(st?.running),
        jobId: Number.isFinite(jobId) && jobId > 0 ? jobId : null,
      }
    },
    async (cur, prev) => {
      if (!prev) return
      if (!cur?.pid || prev.pid !== cur.pid) return
      if (prev.running && !cur.running) {
        reloadSeq.value += 1
        if (cur.jobId) {
          try {
            const res = await fetchCrawlJobStatus(cur.jobId)
            const item = res?.item || null
            const st = String(item?.status || '')
            if (st === 'failed') {
              try {
                console.error('[prodwatch] manual refresh failed:', {
                  project_id: cur.pid,
                  crawl_job_id: cur.jobId,
                  status: st,
                  error_message: item?.error_message || '',
                })
              } catch {
                // ignore console failures
              }
              ElMessage.error(`刷新失败（任务编号=${cur.jobId}）：${item?.error_message || '未知原因'}`)
            } else {
              ElMessage.success(`刷新完成（任务编号=${cur.jobId}）`)
            }
          } catch {
            ElMessage.success('刷新完成')
          }
        } else {
          ElMessage.success('刷新完成')
        }
      }
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
    lastRefreshJobId,

    // setters
    ensureProject,
    setBrandIds,
    setPlatformIds,
    setTimeKey,
    setCustomRange,
  }
})
