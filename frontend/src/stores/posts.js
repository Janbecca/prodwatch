// 作用：前端状态：帖子相关状态管理（store）。

import { defineStore } from 'pinia'
import { computed, ref, watch } from 'vue'

import { fetchProjectConfig } from '../api/projectConfig'
import { fetchPostDetail, fetchPostsList, fetchPostsOverview } from '../api/posts'
import { resolveDateRange } from '../utils/date'
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

function clampNum(v, lo, hi) {
  const n = Number(v)
  if (!Number.isFinite(n)) return null
  return Math.min(hi, Math.max(lo, n))
}

function normalizeInt(v) {
  if (v == null || v === '') return null
  const n = Number(v)
  if (!Number.isFinite(n)) return null
  return Math.trunc(n)
}

export const usePostsStore = defineStore('posts', () => {
  const projectsStore = useProjectsStore()

  const scopeLoading = ref(false)
  const scopeError = ref('')
  const brandOptions = ref([])
  const platformOptions = ref([])
  const keywordOptions = ref([])

  const draft = ref({
    dateRange: [], // ['YYYY-MM-DD','YYYY-MM-DD']
    platformIds: [],
    brandIds: [],
    keywords: [],
    sentiments: [],
    sentimentScoreRange: [-1, 1],
    spam: null, // null | 'spam' | 'normal'
    isValid: null, // null | true | false
    likeMin: null,
    likeMax: null,
    commentMin: null,
    commentMax: null,
    shareMin: null,
    shareMax: null,
    search: '',
  })

  const applied = ref(null)
  const queried = ref(false)

  const overviewLoading = ref(false)
  const overviewError = ref('')
  const overview = ref(null)

  const listLoading = ref(false)
  const listError = ref('')
  const items = ref([])
  const total = ref(0)
  const page = ref(1)
  const pageSize = ref(20)

  const detailOpen = ref(false)
  const detailLoading = ref(false)
  const detailError = ref('')
  const detail = ref(null)

  let ac = null
  let listAc = null
  let detailAc = null

  const activeProjectId = computed(() => projectsStore.activeProjectId)
  const activeProject = computed(() => projectsStore.activeProject)

  const hasEnabledProject = computed(() => {
    const p = activeProject.value
    return !!p && Number(p.is_active || 0) === 1
  })

  function defaultDraftForScope({ brands, platforms } = {}) {
    const dr = resolveDateRange({ days: 14 })
    const dateRange = dr ? [dr.startDate, dr.endDate] : []
    return {
      ...draft.value,
      dateRange,
      platformIds: uniqNums((platforms || []).map((p) => p.id)),
      brandIds: uniqNums((brands || []).map((b) => b.id)),
      keywords: [],
      sentiments: [],
      sentimentScoreRange: [-1, 1],
      spam: null,
      isValid: null,
      likeMin: null,
      likeMax: null,
      commentMin: null,
      commentMax: null,
      shareMin: null,
      shareMax: null,
      search: '',
    }
  }

  async function loadScope(projectId) {
    const pid = Number(projectId)
    if (!Number.isFinite(pid) || pid <= 0) return

    scopeLoading.value = true
    scopeError.value = ''
    brandOptions.value = []
    platformOptions.value = []
    keywordOptions.value = []
    overview.value = null
    overviewError.value = ''
    items.value = []
    total.value = 0
    listError.value = ''
    resetPaging()
    queried.value = false
    applied.value = null
    try {
      const data = await fetchProjectConfig(pid)
      brandOptions.value = Array.isArray(data?.brands) ? data.brands : []
      platformOptions.value = Array.isArray(data?.platforms) ? data.platforms : []
      // API returns keywords as rows [{keyword, keyword_type, ...}], but UI <el-option> needs string label/value.
      keywordOptions.value = Array.isArray(data?.keywords)
        ? data.keywords
            .map((k) => String(k?.keyword || '').trim())
            .filter((s) => s !== '')
        : []

      draft.value = defaultDraftForScope({
        brands: brandOptions.value,
        platforms: platformOptions.value,
      })
    } catch (e) {
      scopeError.value = e?.message || String(e)
      brandOptions.value = []
      platformOptions.value = []
      keywordOptions.value = []
      draft.value = defaultDraftForScope()
    } finally {
      scopeLoading.value = false
    }
  }

  function resetDraft() {
    draft.value = defaultDraftForScope({
      brands: brandOptions.value,
      platforms: platformOptions.value,
    })
  }

  function buildAppliedFilters() {
    const pid = activeProjectId.value
    if (!pid || !hasEnabledProject.value) return null
    const [startDate, endDate] = draft.value.dateRange || []
    if (!startDate || !endDate) return null

    const scoreLo = clampNum(draft.value.sentimentScoreRange?.[0], -1, 1)
    const scoreHi = clampNum(draft.value.sentimentScoreRange?.[1], -1, 1)
    const fullScore = scoreLo === -1 && scoreHi === 1

    return {
      projectId: pid,
      startDate,
      endDate,
      platformIds: uniqNums(draft.value.platformIds),
      brandIds: uniqNums(draft.value.brandIds),
      keywords: (draft.value.keywords || []).map(String).filter((s) => s),
      sentiments: (draft.value.sentiments || []).map(String).filter((s) => s),
      spamLabels: draft.value.spam ? [draft.value.spam] : [],
      isValid: draft.value.isValid,
      sentimentScoreMin: fullScore ? null : scoreLo,
      sentimentScoreMax: fullScore ? null : scoreHi,
      likeMin: normalizeInt(draft.value.likeMin),
      likeMax: normalizeInt(draft.value.likeMax),
      commentMin: normalizeInt(draft.value.commentMin),
      commentMax: normalizeInt(draft.value.commentMax),
      shareMin: normalizeInt(draft.value.shareMin),
      shareMax: normalizeInt(draft.value.shareMax),
      search: String(draft.value.search || '').trim(),
    }
  }

  async function runQuery() {
    const next = buildAppliedFilters()
    applied.value = next
    queried.value = true
    page.value = 1
    await Promise.all([fetchOverview(), fetchList()])
  }

  function resetPaging() {
    page.value = 1
    pageSize.value = 20
  }

  async function fetchOverview() {
    if (!applied.value) {
      overview.value = null
      overviewError.value = ''
      overviewLoading.value = false
      return
    }

    if (ac) ac.abort()
    ac = new AbortController()

    overviewLoading.value = true
    overviewError.value = ''
    try {
      const res = await fetchPostsOverview(applied.value, { signal: ac.signal })
      overview.value = res?.overview || null
    } catch (e) {
      if (e?.name === 'AbortError') return
      overviewError.value = e?.message || String(e)
      overview.value = null
    } finally {
      overviewLoading.value = false
    }
  }

  async function fetchList() {
    if (!applied.value) {
      items.value = []
      total.value = 0
      listError.value = ''
      listLoading.value = false
      return
    }

    if (listAc) listAc.abort()
    listAc = new AbortController()

    listLoading.value = true
    listError.value = ''
    try {
      const res = await fetchPostsList(applied.value, { page: page.value, pageSize: pageSize.value }, { signal: listAc.signal })
      items.value = Array.isArray(res?.items) ? res.items : []
      total.value = Number(res?.total || 0)
    } catch (e) {
      if (e?.name === 'AbortError') return
      listError.value = e?.message || String(e)
      items.value = []
      total.value = 0
    } finally {
      listLoading.value = false
    }
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

  function closeDetail() {
    detailOpen.value = false
  }

  async function openDetail(row) {
    const pid = activeProjectId.value
    if (!pid) return
    const postId = Number(row?.id)
    if (!Number.isFinite(postId) || postId <= 0) return

    detailOpen.value = true
    detailLoading.value = true
    detailError.value = ''
    detail.value = row || null

    if (detailAc) detailAc.abort()
    detailAc = new AbortController()
    try {
      const res = await fetchPostDetail({ projectId: pid, postId }, { signal: detailAc.signal })
      detail.value = res?.item || null
    } catch (e) {
      if (e?.name === 'AbortError') return
      detailError.value = e?.message || String(e)
    } finally {
      detailLoading.value = false
    }
  }

  watch(
    () => activeProjectId.value,
    (pid) => {
      if (!pid) return
      loadScope(pid)
    },
    { immediate: true }
  )

  return {
    // project
    activeProjectId,
    activeProject,
    hasEnabledProject,

    // scope options
    scopeLoading,
    scopeError,
    brandOptions,
    platformOptions,
    keywordOptions,

    // filters
    draft,
    applied,
    queried,
    resetDraft,
    resetPaging,
    runQuery,

    // overview
    overviewLoading,
    overviewError,
    overview,

    // list
    listLoading,
    listError,
    items,
    total,
    page,
    pageSize,
    fetchList,
    setPage,
    setPageSize,

    // detail
    detailOpen,
    detailLoading,
    detailError,
    detail,
    openDetail,
    closeDetail,
  }
})
