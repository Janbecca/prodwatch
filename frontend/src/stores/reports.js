import { defineStore } from 'pinia'
import { computed, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

import { createReport, deleteReport, fetchReportDetail, fetchReportEvidenceList, fetchReportsList, generateReport } from '../api/reports'
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

function splitCsv(raw) {
  if (raw == null) return []
  return String(raw)
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s)
}

export const useReportsStore = defineStore('reports', () => {
  const projectsStore = useProjectsStore()
  const activeProjectId = computed(() => projectsStore.activeProjectId)
  const activeProject = computed(() => projectsStore.activeProject)

  const draft = ref({
    projectId: null,
    reportType: '',
    dataRange: [],
    createdRange: [],
    search: '',
  })

  const followActiveProject = ref(true)

  const applied = ref(null)
  const queried = ref(false)

  const loading = ref(false)
  const error = ref('')
  const items = ref([])
  const total = ref(0)
  const page = ref(1)
  const pageSize = ref(20)

  // Create drawer state (for "copy generate")
  const createOpen = ref(false)
  const createPrefill = ref(null)

  // Detail drawer state
  const detailOpen = ref(false)
  const detailLoading = ref(false)
  const detailError = ref('')
  const detail = ref(null)

  // Evidence dialog state
  const evidenceOpen = ref(false)
  const evidenceLoading = ref(false)
  const evidenceError = ref('')
  const evidenceItems = ref([])
  const evidenceTotal = ref(0)
  const evidencePage = ref(1)
  const evidencePageSize = ref(20)
  const evidenceReportId = ref(null)

  let ac = null
  let detailAc = null
  let evidenceAc = null

  const projectNameById = computed(() => {
    const map = {}
    for (const p of projectsStore.projects || []) map[Number(p.id)] = p.name
    return map
  })

  function resetDraft() {
    draft.value = {
      projectId: activeProjectId.value || null,
      reportType: '',
      dataRange: [],
      createdRange: [],
      search: '',
    }
    followActiveProject.value = true
  }

  function buildApplied() {
    return {
      projectId: draft.value.projectId || activeProjectId.value || null,
      reportType: draft.value.reportType || '',
      dataRange: Array.isArray(draft.value.dataRange) ? draft.value.dataRange.slice(0, 2) : [],
      createdRange: Array.isArray(draft.value.createdRange) ? draft.value.createdRange.slice(0, 2) : [],
      search: String(draft.value.search || '').trim(),
    }
  }

  function setDraftProjectId(next) {
    const n = next == null ? null : Number(next)
    draft.value.projectId = Number.isFinite(n) && n > 0 ? n : null
    followActiveProject.value = false
  }

  async function fetchList() {
    if (!applied.value) {
      items.value = []
      total.value = 0
      error.value = ''
      loading.value = false
      return
    }

    if (ac) ac.abort()
    ac = new AbortController()
    loading.value = true
    error.value = ''
    try {
      const res = await fetchReportsList(applied.value, { page: page.value, pageSize: pageSize.value }, { signal: ac.signal })
      items.value = Array.isArray(res?.items) ? res.items : []
      total.value = Number(res?.total || 0)
    } catch (e) {
      if (e?.name === 'AbortError') return
      error.value = e?.message || String(e)
      items.value = []
      total.value = 0
    } finally {
      loading.value = false
    }
  }

  async function runQuery() {
    applied.value = buildApplied()
    queried.value = true
    page.value = 1
    await fetchList()
  }

  function setPage(next) {
    const n = Number(next)
    if (!Number.isFinite(n) || n < 1) return
    const nextPage = Math.trunc(n)
    if (nextPage === page.value) return
    page.value = nextPage
    fetchList()
  }

  function setPageSize(next) {
    const n = Number(next)
    if (!Number.isFinite(n) || n < 1) return
    const nextSize = Math.trunc(n)
    if (nextSize === pageSize.value) return
    pageSize.value = nextSize
    page.value = 1
    fetchList()
  }

  async function openDetail(row) {
    const reportId = Number(row?.id)
    if (!Number.isFinite(reportId) || reportId <= 0) return
    detailOpen.value = true
    detailLoading.value = true
    detailError.value = ''
    detail.value = row || null

    if (detailAc) detailAc.abort()
    detailAc = new AbortController()
    try {
      const res = await fetchReportDetail(reportId, { signal: detailAc.signal })
      detail.value = res?.item || null
    } catch (e) {
      if (e?.name === 'AbortError') return
      detailError.value = e?.message || String(e)
    } finally {
      detailLoading.value = false
    }
  }

  function closeDetail() {
    detailOpen.value = false
  }

  async function onExport(row) {
    ElMessage.info(`Export not implemented (report_id=${row?.id ?? '-'})`)
  }

  async function onEvidence(row) {
    const rid = Number(row?.id)
    if (!Number.isFinite(rid) || rid <= 0) return
    evidenceReportId.value = rid
    evidencePage.value = 1
    evidenceOpen.value = true
    await fetchEvidence()
  }

  async function onCopyGenerate(row) {
    const reportId = Number(row?.id)
    if (!Number.isFinite(reportId) || reportId <= 0) return
    detailLoading.value = true
    try {
      const res = await fetchReportDetail(reportId)
      const it = res?.item || null
      if (!it) throw new Error('report detail missing')

      // Prefer parsing config from report_config when present.
      const cfg = it.config || {}
      createPrefill.value = {
        projectId: Number(it.project_id) || null,
        title: `${it.title || 'Report'} (copy)`,
        type: it.report_type || 'daily',
        dateRange: [it.data_start_date, it.data_end_date].filter(Boolean),
        platformIds: uniqNums(splitCsv(cfg.platform_ids)),
        brandIds: uniqNums(splitCsv(cfg.brand_ids)),
        keywords: splitCsv(cfg.keywords),
        modules: [
          cfg.include_sentiment ? 'sentiment' : null,
          cfg.include_trend ? 'trend' : null,
          cfg.include_topics ? 'topics' : null,
          cfg.include_feature_analysis ? 'feature' : null,
          cfg.include_spam ? 'spam' : null,
          cfg.include_competitor_compare ? 'competitor' : null,
          cfg.include_strategy ? 'strategy' : null,
        ].filter(Boolean),
      }
      createOpen.value = true
    } catch (e) {
      ElMessage.error(e?.message || String(e))
    } finally {
      detailLoading.value = false
    }
  }

  async function onGenerate(row) {
    const reportId = Number(row?.id)
    if (!Number.isFinite(reportId) || reportId <= 0) return
    detailLoading.value = true
    try {
      await generateReport(reportId)
      ElMessage.success(`Generated report #${reportId}`)
      if (queried.value) await fetchList()
      // If detail drawer is showing this report, refresh it.
      if (detailOpen.value && Number(detail.value?.id) === reportId) {
        const res = await fetchReportDetail(reportId)
        detail.value = res?.item || null
      }
    } catch (e) {
      ElMessage.error(e?.message || String(e))
    } finally {
      detailLoading.value = false
    }
  }

  async function submitCreate(payload) {
    const res = await createReport(payload)
    const rid = Number(res?.report_id)
    if (!Number.isFinite(rid) || rid <= 0) throw new Error('create failed')
    ElMessage.success(`Created report #${rid}`)
    createOpen.value = false
    createPrefill.value = null
    if (queried.value) await fetchList()
    return rid
  }

  async function fetchEvidence() {
    const rid = evidenceReportId.value
    if (!rid) return
    if (evidenceAc) evidenceAc.abort()
    evidenceAc = new AbortController()
    evidenceLoading.value = true
    evidenceError.value = ''
    try {
      const res = await fetchReportEvidenceList(
        rid,
        { page: evidencePage.value, pageSize: evidencePageSize.value },
        { signal: evidenceAc.signal }
      )
      evidenceItems.value = Array.isArray(res?.items) ? res.items : []
      evidenceTotal.value = Number(res?.total || 0)
    } catch (e) {
      if (e?.name === 'AbortError') return
      evidenceError.value = e?.message || String(e)
      evidenceItems.value = []
      evidenceTotal.value = 0
    } finally {
      evidenceLoading.value = false
    }
  }

  function setEvidencePage(next) {
    const n = Number(next)
    if (!Number.isFinite(n) || n < 1) return
    const p = Math.trunc(n)
    if (p === evidencePage.value) return
    evidencePage.value = p
    fetchEvidence()
  }

  function setEvidencePageSize(next) {
    const n = Number(next)
    if (!Number.isFinite(n) || n < 1) return
    const s = Math.trunc(n)
    if (s === evidencePageSize.value) return
    evidencePageSize.value = s
    evidencePage.value = 1
    fetchEvidence()
  }

  function closeEvidence() {
    evidenceOpen.value = false
  }

  async function onDelete(row) {
    const reportId = Number(row?.id)
    if (!Number.isFinite(reportId) || reportId <= 0) return
    await ElMessageBox.confirm(`Delete report #${reportId}?`, 'Confirm', { type: 'warning' })
    try {
      await deleteReport(reportId)
      ElMessage.success('Deleted')
      await fetchList()
      if (items.value.length === 0 && page.value > 1) {
        page.value -= 1
        await fetchList()
      }
    } catch (e) {
      ElMessage.error(e?.message || String(e))
    }
  }

  // Keep default project consistent with the whole app: activeProjectId is the filter project.
  watch(
    () => activeProjectId.value,
    (pid) => {
      if (followActiveProject.value) {
        draft.value.projectId = pid || null
      }
      // changing project invalidates current list context when following.
      if (!followActiveProject.value) return
      items.value = []
      total.value = 0
      error.value = ''
      loading.value = false
      applied.value = null
      queried.value = false
      page.value = 1
    },
    { immediate: true }
  )

  return {
    activeProjectId,
    activeProject,
    projectNameById,

    draft,
    applied,
    queried,
    followActiveProject,

    loading,
    error,
    items,
    total,
    page,
    pageSize,

    runQuery,
    fetchList,
    setPage,
    setPageSize,
    setDraftProjectId,
    resetDraft,

    // actions
    openDetail,
    closeDetail,
    onExport,
    onEvidence,
    onCopyGenerate,
    onGenerate,
    onDelete,
    submitCreate,

    // detail drawer
    detailOpen,
    detailLoading,
    detailError,
    detail,

    // create drawer
    createOpen,
    createPrefill,

    // evidence
    evidenceOpen,
    evidenceLoading,
    evidenceError,
    evidenceItems,
    evidenceTotal,
    evidencePage,
    evidencePageSize,
    evidenceReportId,
    fetchEvidence,
    setEvidencePage,
    setEvidencePageSize,
    closeEvidence,
  }
})
