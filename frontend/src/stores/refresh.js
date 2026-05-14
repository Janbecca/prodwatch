// 作用：前端全局状态：项目刷新状态（按 project_id 维度）。
//
// 目标：
// - 点击“手动刷新”后立即进入 refreshing=true
// - 轮询 GET /api/projects/{id}/refresh/status，直到 running=false 再解除
// - 为布局提示条、页面遮罩、按钮禁用提供统一的数据来源
import { defineStore } from 'pinia'
import { reactive } from 'vue'
import { fetchProjectRefreshStatus } from '../api/projectRefresh'
import { fetchCrawlJobProgress } from '../api/crawlJobs'

function nowTs() {
  return Date.now()
}

export const useRefreshStore = defineStore('refresh', () => {
  // Map projectId -> state
  // Use reactive(Map) so Map.set/get mutations are tracked by Vue.
  const byProjectId = reactive(new Map())
  const pollTimers = reactive(new Map())

  function _ensure(projectId) {
    const pid = Number(projectId)
    if (!Number.isFinite(pid) || pid <= 0) return null
    const id = Math.trunc(pid)
    if (!byProjectId.has(id)) {
      byProjectId.set(id, {
        project_id: id,
        refreshing: false,
        running: false,
        reason: '',
        crawl_job_id: null,
        stage: 'unknown',
        stage_started_at: null,
        stage_updated_at: null,
        stage_message: null,
        stage_meta: null,
        started_at: null,
        last_checked_at: 0,
        last_error: '',
        banner_ack: false,
        optimistic_until_ts: 0,
      })
    }
    return byProjectId.get(id)
  }

  function getState(projectId) {
    const st = _ensure(projectId)
    return st || null
  }

  function isRefreshing(projectId) {
    const st = byProjectId.get(Number(projectId))
    return Boolean(st && (st.refreshing || st.running))
  }

  function setBannerAck(projectId, ack = true) {
    const st = _ensure(projectId)
    if (!st) return
    st.banner_ack = Boolean(ack)
  }

  async function syncStatus(projectId) {
    const st = _ensure(projectId)
    if (!st) return null
    try {
      const res = await fetchProjectRefreshStatus(st.project_id)
      st.last_checked_at = nowTs()
      st.last_error = ''
      st.running = Boolean(res?.running)
      st.reason = res?.reason ? String(res.reason) : ''
      const jobId = Number(res?.crawl_job_id)
      // Preserve the last known job id when backend reports not running (status endpoint omits crawl_job_id).
      if (Number.isFinite(jobId) && jobId > 0) {
        st.crawl_job_id = jobId
      }
      st.started_at = res?.started_at != null ? String(res.started_at) : null
      // When backend confirms running, stop the optimistic window.
      if (st.running) st.optimistic_until_ts = 0

      // 保持刷新状态，直到 backend 确认不再 running。
      // 即使请求失败，也保持刷新状态（但不保持 running 状态）
      // 因为用户可能网络波动导致请求失败，但实际上刷新仍在进行中。
      if (!st.running) {
        const now = nowTs()
        if (st.optimistic_until_ts && now < st.optimistic_until_ts) {
          st.refreshing = true
        } else {
          st.refreshing = false
        }
      }
      return res
    } catch (e) {
      st.last_checked_at = nowTs()
      st.last_error = e?.message || String(e)
      //如果请求失败，不要轻易取消刷新状态，因为可能是网络问题导致请求失败，但实际上刷新仍在进行中。、
      st.running = false
      const now = nowTs()
      if (st.optimistic_until_ts && now < st.optimistic_until_ts) {
        st.refreshing = true
      } else {
        st.refreshing = false
      }
      return null
    }
  }

  async function syncProgress(projectId) {
    const st = _ensure(projectId)
    if (!st) return null
    const jobId = Number(st.crawl_job_id)
    if (!Number.isFinite(jobId) || jobId <= 0) return null

    try {
      const res = await fetchCrawlJobProgress(jobId)
      const item = res?.item || null
      const nextStage = String(item?.stage || 'unknown').trim().toLowerCase() || 'unknown'
      const prevStage = String(st.stage || 'unknown').trim().toLowerCase() || 'unknown'
      const prevUpdatedAt = st.stage_updated_at != null ? String(st.stage_updated_at) : null
      const prevMsg = st.stage_message != null ? String(st.stage_message) : null

      st.stage = nextStage
      st.stage_started_at = item?.stage_started_at != null ? String(item.stage_started_at) : null
      st.stage_updated_at = item?.stage_updated_at != null ? String(item.stage_updated_at) : null
      st.stage_message = item?.message != null ? String(item.message) : null
      st.stage_meta = item?.meta ?? null

      const nextUpdatedAt = st.stage_updated_at != null ? String(st.stage_updated_at) : null
      const nextMsg = st.stage_message != null ? String(st.stage_message) : null

      // Console progress logs:
      // - stage transitions
      // - message/meta updates within the same stage (backend uses a single-row progress state)
      if ((nextStage !== prevStage || nextUpdatedAt !== prevUpdatedAt || nextMsg !== prevMsg) && import.meta.env.DEV) {
        try {
          console.info('[prodwatch] refresh stage:', {
            project_id: st.project_id,
            crawl_job_id: jobId,
            stage: nextStage,
            updated_at: st.stage_updated_at,
            message: st.stage_message,
            meta: st.stage_meta,
          })
        } catch {
          // ignore console failures
        }
      }

      return res
    } catch {
      return null
    }
  }

  function startRefreshing(projectId, { bannerAck = false } = {}) {
    const st = _ensure(projectId)
    if (!st) return
    st.refreshing = true
    st.running = true // optimistic: request may not have created crawl_job yet
    st.banner_ack = Boolean(bannerAck)
    st.optimistic_until_ts = nowTs() + 15_000
    _startPolling(st.project_id)
  }

  function clearOptimistic(projectId) {
    const st = _ensure(projectId)
    if (!st) return
    st.optimistic_until_ts = 0
  }

  function stopRefreshing(projectId) {
    const st = _ensure(projectId)
    if (!st) return
    st.refreshing = false
    st.running = false
    st.optimistic_until_ts = 0
    _stopPolling(st.project_id)
  }

  function _startPolling(projectId, { intervalMs = 1000 } = {}) {
    const pid = Number(projectId)
    if (!Number.isFinite(pid) || pid <= 0) return
    const id = Math.trunc(pid)
    if (pollTimers.has(id)) return

    const t = setInterval(async () => {
      const st = byProjectId.get(id)
      // If state disappeared or no longer refreshing, stop polling.
      if (!st || (!st.refreshing && !st.running)) {
        _stopPolling(id)
        return
      }
      await syncStatus(id)
      // Poll progress more frequently while running to achieve near real-time stage updates.
      await syncProgress(id)
      // When backend stops running, stop polling.
      const st2 = byProjectId.get(id)
      if (st2 && !st2.running) {
        _stopPolling(id)
      }
    }, Number(intervalMs || 2000))
    pollTimers.set(id, t)
  }

  function _stopPolling(projectId) {
    const pid = Number(projectId)
    if (!Number.isFinite(pid) || pid <= 0) return
    const id = Math.trunc(pid)
    const t = pollTimers.get(id)
    if (t) clearInterval(t)
    pollTimers.delete(id)
  }

  return {
    byProjectId,
    getState,
    isRefreshing,
    setBannerAck,
    syncStatus,
    syncProgress,
    startRefreshing,
    clearOptimistic,
    stopRefreshing,
  }
})
