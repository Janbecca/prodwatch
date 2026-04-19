// 作用：前端状态：拆分链路的项目操作（模拟生成帖子 / 分析帖子）。
//
// 设计：
// - 复用 refreshStore 的“项目正在运行”状态与轮询（/refresh/status）。
// - 只负责触发后端任务 + 记录最后一次 simulate 的 crawl_job_id（便于 analyze 默认使用）。
import { defineStore } from 'pinia'
import { computed, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'

import { fetchProjectRefreshStatus, manualAnalyzeProject, manualSimulateProject } from '../api/projectRefresh'
import { useProjectsStore } from './projects'
import { useRefreshStore } from './refresh'

export const usePipelineOpsStore = defineStore('pipelineOps', () => {
  const projectsStore = useProjectsStore()
  const refreshStore = useRefreshStore()

  const simulateLoading = ref(false)
  const analyzeLoading = ref(false)
  const lastSimulateJobIdByProject = reactive(new Map()) // projectId -> crawl_job_id

  const activeProjectId = computed(() => projectsStore.activeProjectId)
  const enabledProjectId = computed(() => {
    const p = projectsStore.activeProject
    if (!p) return null
    if (Number(p.is_active || 0) !== 1) return null
    return Number(p.id)
  })

  function lastSimulateJobId(projectId) {
    const pid = Number(projectId)
    if (!Number.isFinite(pid) || pid <= 0) return null
    const v = Number(lastSimulateJobIdByProject.get(Math.trunc(pid)))
    return Number.isFinite(v) && v > 0 ? v : null
  }

  async function _preflight(projectId) {
    const pid = Number(projectId)
    if (!Number.isFinite(pid) || pid <= 0) return null
    try {
      return await fetchProjectRefreshStatus(pid)
    } catch {
      return null
    }
  }

  async function simulate(projectId, payload = {}) {
    const pid = Number(projectId ?? enabledProjectId.value ?? activeProjectId.value)
    if (!Number.isFinite(pid) || pid <= 0) return null
    if (simulateLoading.value) return null
    if (refreshStore.isRefreshing(pid)) return null
    simulateLoading.value = true
    refreshStore.startRefreshing(pid, { bannerAck: false })
    try {
      const st = await _preflight(pid)
      if (st?.running) {
        ElMessage.warning('任务正在运行中，请稍后再试')
        refreshStore.startRefreshing(pid, { bannerAck: false })
        return null
      }

      const res = await manualSimulateProject(pid, payload || {})
      refreshStore.clearOptimistic(pid)

      if (res?.skipped) {
        ElMessage.warning(String(res?.detail || '任务被跳过'))
        return null
      }

      const jobId = Number(res?.crawl_job_id)
      if (Number.isFinite(jobId) && jobId > 0) {
        lastSimulateJobIdByProject.set(pid, jobId)
        try {
          const st2 = refreshStore.getState(pid)
          if (st2) st2.crawl_job_id = jobId
        } catch {
          // ignore
        }
        ElMessage.success(`已触发“爬虫任务”（任务编号=${jobId}），正在后台处理…`)
      } else {
        ElMessage.success('已触发“爬虫任务”，正在后台处理…')
      }
      return res || null
    } catch (e) {
      const msg = e?.message || String(e)
      if (String(msg).includes('HTTP 409')) {
        ElMessage.warning(msg.replace(/^HTTP 409:\s*/i, ''))
        return null
      }
      ElMessage.error(msg)
      return null
    } finally {
      simulateLoading.value = false
      refreshStore.clearOptimistic(pid)
      try {
        await refreshStore.syncStatus(pid)
      } catch {
        // ignore
      }
      if (!refreshStore.isRefreshing(pid)) refreshStore.stopRefreshing(pid)
    }
  }

  async function analyze(projectId, payload = {}) {
    const pid = Number(projectId ?? enabledProjectId.value ?? activeProjectId.value)
    if (!Number.isFinite(pid) || pid <= 0) return null
    if (analyzeLoading.value) return null
    if (refreshStore.isRefreshing(pid)) return null
    analyzeLoading.value = true
    refreshStore.startRefreshing(pid, { bannerAck: false })
    try {
      const st = await _preflight(pid)
      if (st?.running) {
        ElMessage.warning('任务正在运行中，请稍后再试')
        refreshStore.startRefreshing(pid, { bannerAck: false })
        return null
      }

      const src = payload?.source_crawl_job_id ? Number(payload.source_crawl_job_id) : lastSimulateJobId(pid)
      const body = { ...(payload || {}) }
      if (!body.source_crawl_job_id && src) body.source_crawl_job_id = src

      const res = await manualAnalyzeProject(pid, body)
      refreshStore.clearOptimistic(pid)

      if (res?.skipped) {
        ElMessage.warning(String(res?.detail || '任务被跳过'))
        return null
      }

      const jobId = Number(res?.crawl_job_id)
      if (Number.isFinite(jobId) && jobId > 0) {
        try {
          const st2 = refreshStore.getState(pid)
          if (st2) st2.crawl_job_id = jobId
        } catch {
          // ignore
        }
        ElMessage.success(`已触发“分析帖子”（任务编号=${jobId}），正在后台处理…`)
      } else {
        ElMessage.success('已触发“分析帖子”，正在后台处理…')
      }
      return res || null
    } catch (e) {
      const msg = e?.message || String(e)
      if (String(msg).includes('HTTP 409')) {
        ElMessage.warning(msg.replace(/^HTTP 409:\s*/i, ''))
        return null
      }
      ElMessage.error(msg)
      return null
    } finally {
      analyzeLoading.value = false
      refreshStore.clearOptimistic(pid)
      try {
        await refreshStore.syncStatus(pid)
      } catch {
        // ignore
      }
      if (!refreshStore.isRefreshing(pid)) refreshStore.stopRefreshing(pid)
    }
  }

  return {
    simulateLoading,
    analyzeLoading,
    lastSimulateJobIdByProject,
    lastSimulateJobId,
    simulate,
    analyze,
  }
})

