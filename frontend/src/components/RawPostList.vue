<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import api from '../api/axios'
import PageHeader from './PageHeader.vue'
import { ElMessage } from 'element-plus'
import { useProjectsStore } from '../stores/projects'

const route = useRoute()
const router = useRouter()
const projectsStore = useProjectsStore()

const enabledProjects = computed(() => projectsStore.enabledProjects || [])
const activeProjectId = computed({
  get: () => projectsStore.activeProjectId,
  set: (v) => projectsStore.setActiveProjectId(v),
})
const activeProject = computed(() => projectsStore.activeProject)

// options
const options = ref({ brands: [], platforms: [] })
const keywordOptions = ref([])

// filters
const timeKey = ref('last14') // last7 | last14 | last30 | custom
const customStart = ref(null)
const customEnd = ref(null)
const platformId = ref('')
const brandIds = ref([])
const keywordId = ref('')
const polarity = ref('') // '' | positive | neutral | negative
const spamLabel = ref('') // '' | normal | spam | suspect
const isValid = ref('') // '' | '1' | '0'
const intensityMin = ref('')
const intensityMax = ref('')
const likeMin = ref('')
const likeMax = ref('')
const commentMin = ref('')
const commentMax = ref('')
const shareMin = ref('')
const shareMax = ref('')
const qRaw = ref('')
const qClean = ref('')
const qTopic = ref('')
const qEntity = ref('')
const advancedOpen = ref(false)

// pagination
const page = ref(1)
const pageSize = ref(20)

// data
const stats = ref({
  total_posts: 0,
  valid_posts: 0,
  negative_posts: 0,
  spam_posts: 0,
  hot_topics: 0,
  entities: 0,
})
const total = ref(0)
const items = ref([])
const loading = ref(false)
const error = ref('')

// detail drawer
const drawerVisible = ref(false)
const activeRow = ref(null)
const linkedReports = ref([])
const linkedLoading = ref(false)

// polling (optional quick refresh)
const autoRefresh = ref(false)
const autoRefreshSeconds = 15
let timer = null

const toDateOnly = (d) => {
  if (!d) return null
  const dt = new Date(d)
  if (Number.isNaN(dt.getTime())) return null
  const y = dt.getFullYear()
  const m = String(dt.getMonth() + 1).padStart(2, '0')
  const day = String(dt.getDate()).padStart(2, '0')
  return `${y}-${m}-${day}`
}

const timeQuery = computed(() => {
  if (timeKey.value === 'custom') {
    return {
      start_date: toDateOnly(customStart.value),
      end_date: toDateOnly(customEnd.value),
    }
  }
  const days = Number(String(timeKey.value).replace('last', ''))
  return { days: Number.isFinite(days) ? days : 14 }
})

const isCustomRangeReady = computed(() => {
  if (timeKey.value !== 'custom') return true
  return Boolean(toDateOnly(customStart.value) && toDateOnly(customEnd.value))
})

const visibleBrands = computed(() => {
  const all = options.value?.brands || []
  const p = activeProject.value
  if (!p) return all
  const allow = new Set((p.brand_ids || []).map((x) => Number(x)))
  return all.filter((b) => allow.has(Number(b.id)))
})

const visiblePlatforms = computed(() => {
  const all = options.value?.platforms || []
  const p = activeProject.value
  if (!p) return all
  const allowIds = (p.enabled_platform_ids || []).map((x) => Number(x)).filter((x) => Number.isFinite(x))
  if (!allowIds.length) return all
  const allow = new Set(allowIds)
  return all.filter((pl) => allow.has(Number(pl.id)))
})

const buildParams = (obj) => {
  const params = new URLSearchParams()
  for (const [k, v] of Object.entries(obj || {})) {
    if (v === undefined || v === null || v === '') continue
    if (Array.isArray(v)) {
      for (const item of v) params.append(k, String(item))
      continue
    }
    params.append(k, String(v))
  }
  return params
}

const parseApiError = (e) => {
  const detail = e?.response?.data?.detail
  if (typeof detail === 'string' && detail) return detail
  return e?.message || '请求失败'
}

const queryParams = computed(() => ({
  mode: 'all',
  project_id: activeProjectId.value || undefined,
  platform_id: platformId.value === '' ? undefined : Number(platformId.value),
  brand_ids: brandIds.value.length ? brandIds.value : undefined,
  keyword_id: keywordId.value === '' ? undefined : Number(keywordId.value),
  polarity: polarity.value || undefined,
  spam_label: spamLabel.value || undefined,
  is_valid: isValid.value === '' ? undefined : Boolean(Number(isValid.value)),
  intensity_min: intensityMin.value === '' ? undefined : Number(intensityMin.value),
  intensity_max: intensityMax.value === '' ? undefined : Number(intensityMax.value),
  like_min: likeMin.value === '' ? undefined : Number(likeMin.value),
  like_max: likeMax.value === '' ? undefined : Number(likeMax.value),
  comment_min: commentMin.value === '' ? undefined : Number(commentMin.value),
  comment_max: commentMax.value === '' ? undefined : Number(commentMax.value),
  share_min: shareMin.value === '' ? undefined : Number(shareMin.value),
  share_max: shareMax.value === '' ? undefined : Number(shareMax.value),
  q_raw: String(qRaw.value || '').trim() || undefined,
  q_clean: String(qClean.value || '').trim() || undefined,
  q_topic: String(qTopic.value || '').trim() || undefined,
  q_entity: String(qEntity.value || '').trim() || undefined,
  ...timeQuery.value,
}))

const fetchOptions = async () => {
  const { data } = await api.get('/api/dashboard/options')
  options.value = { brands: data?.brands || [], platforms: data?.platforms || [] }
}

const fetchKeywords = async () => {
  keywordOptions.value = []
  if (!activeProjectId.value) return
  try {
    const { data } = await api.get('/api/posts/keywords', { params: { project_id: activeProjectId.value } })
    keywordOptions.value = Array.isArray(data) ? data : []
  } catch {
    keywordOptions.value = []
  }
}

const fetchStats = async () => {
  if (!activeProjectId.value) return
  const { data } = await api.get('/api/posts/stats', { params: buildParams(queryParams.value) })
  stats.value = data || stats.value
}

const fetchPage = async () => {
  if (!activeProjectId.value) return
  loading.value = true
  error.value = ''
  try {
    if (!isCustomRangeReady.value) return
    const { data } = await api.get('/api/posts/page', {
      params: buildParams({ ...queryParams.value, page: page.value, page_size: pageSize.value }),
    })
    total.value = Number(data?.total) || 0
    items.value = Array.isArray(data?.items) ? data.items : []
  } catch (e) {
    error.value = parseApiError(e) || '加载帖子失败'
    items.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

const fetchAll = async () => {
  await Promise.all([fetchStats(), fetchPage()])
}

const manualRefresh = async () => {
  if (!activeProjectId.value) return
  loading.value = true
  error.value = ''
  try {
    const p = activeProject.value
    const ids = (brandIds.value && brandIds.value.length ? brandIds.value : (p?.brand_ids || []))
      .map((x) => Number(x))
      .filter((x) => Number.isFinite(x))

    const platformIds = platformId.value === '' ? (activeProject.value?.enabled_platform_ids || []) : [Number(platformId.value)]

    await api.post('/api/dashboard/manual_refresh', {
      brand_ids: ids.length ? ids : undefined,
      project_id: activeProjectId.value,
      platform_ids: (platformIds || []).map((x) => Number(x)).filter((x) => Number.isFinite(x)),
      max_posts_per_run: 30,
      sentiment_model: 'rule-based',
      trigger_type: 'manual',
    })

    page.value = 1
    await Promise.all([fetchKeywords(), fetchAll()])
    ElMessage.success('已刷新并生成新数据')
  } catch (e) {
    ElMessage.error(parseApiError(e) || '手动刷新失败')
  } finally {
    loading.value = false
  }
}

const applyProjectDefaults = () => {
  const p = activeProject.value
  if (!p) return
  const ids = (p.brand_ids || []).map((x) => Number(x)).filter((x) => Number.isFinite(x))
  brandIds.value = ids
}

const startPolling = () => {
  stopPolling()
  if (!autoRefresh.value) return
  timer = setInterval(() => fetchAll(), autoRefreshSeconds * 1000)
}

const stopPolling = () => {
  if (timer) clearInterval(timer)
  timer = null
}

const resetFilters = async () => {
  timeKey.value = 'last14'
  customStart.value = null
  customEnd.value = null
  platformId.value = ''
  keywordId.value = ''
  polarity.value = ''
  spamLabel.value = ''
  isValid.value = ''
  intensityMin.value = ''
  intensityMax.value = ''
  likeMin.value = ''
  likeMax.value = ''
  commentMin.value = ''
  commentMax.value = ''
  shareMin.value = ''
  shareMax.value = ''
  qRaw.value = ''
  qClean.value = ''
  qTopic.value = ''
  qEntity.value = ''
  page.value = 1
  applyProjectDefaults()
  await fetchAll()
}

const openDetail = async (row) => {
  activeRow.value = row
  drawerVisible.value = true
  linkedReports.value = []
  linkedLoading.value = true
  try {
    const { data } = await api.get(`/api/posts/${row.id}/linked_reports`)
    linkedReports.value = Array.isArray(data) ? data : []
  } catch {
    linkedReports.value = []
  } finally {
    linkedLoading.value = false
  }
}

const snippet = (t, n = 80) => {
  const s = String(t || '').trim()
  if (!s) return '-'
  return s.length > n ? `${s.slice(0, n)}…` : s
}

const polarityText = (p) => {
  const x = String(p || '').toLowerCase()
  if (x === 'positive') return '正面'
  if (x === 'negative') return '负面'
  if (x === 'neutral') return '中性'
  return '-'
}

const polarityType = (p) => {
  const x = String(p || '').toLowerCase()
  if (x === 'positive') return 'success'
  if (x === 'negative') return 'danger'
  if (x === 'neutral') return 'info'
  return ''
}

const gotoReport = (rid) => {
  if (!rid) return
  router.push({ path: '/report', query: { report_id: rid } })
}

// init from route query (Dashboard/Report jumps)
const parseIds = (v) => {
  if (v === undefined || v === null) return []
  const arr = Array.isArray(v) ? v : [v]
  return arr.map((x) => Number(x)).filter((x) => Number.isFinite(x)).map((x) => Math.trunc(x))
}

watch(
  () => route.query,
  async (q) => {
    if (q?.project_id !== undefined && q?.project_id !== null && q?.project_id !== '') {
      const pid = Number(q.project_id)
      if (Number.isFinite(pid)) projectsStore.setActiveProjectId(pid)
    }
    const qDays = q?.days !== undefined && q?.days !== null && q?.days !== '' ? Number(q.days) : null
    const qStart = q?.start_date ? new Date(String(q.start_date)) : null
    const qEnd = q?.end_date ? new Date(String(q.end_date)) : null
    if (qStart && qEnd && !Number.isNaN(qStart.getTime()) && !Number.isNaN(qEnd.getTime())) {
      timeKey.value = 'custom'
      customStart.value = qStart
      customEnd.value = qEnd
    } else if ([7, 14, 30].includes(qDays)) {
      timeKey.value = `last${qDays}`
      customStart.value = null
      customEnd.value = null
    }

    const b = parseIds(q?.brand_ids)
    if (b.length) brandIds.value = b
    page.value = 1
    await fetchAll()
  },
  { immediate: true, deep: true }
)

watch(
  () => projectsStore.activeProjectId,
  async () => {
    applyProjectDefaults()
    page.value = 1
    await fetchKeywords()
    await fetchAll()
  }
)

watch([page, pageSize], () => fetchPage())
watch([autoRefresh], () => startPolling())

onMounted(async () => {
  await projectsStore.fetch()
  await fetchOptions()
  applyProjectDefaults()
  await fetchKeywords()
  await fetchAll()
  startPolling()
})

onBeforeUnmount(() => stopPolling())
</script>

<template>
  <section class="page">
    <PageHeader content="帖子浏览" />

    <el-alert
      v-if="enabledProjects.length === 0"
      title="暂无已启用项目，请先到【项目配置】启用项目后再浏览帖子。"
      type="warning"
      show-icon
      :closable="false"
    />

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />

    <el-card class="card" shadow="hover">
      <div class="filters">
        <div class="field">
          <div class="label">启用项目</div>
          <el-select
            v-model="activeProjectId"
            placeholder="请选择启用项目"
            style="width: 260px; max-width: 100%"
            :disabled="enabledProjects.length === 0"
          >
            <el-option
              v-for="p in enabledProjects"
              :key="p.id"
              :label="p.product_category ? `${p.name}（${p.product_category}）` : p.name"
              :value="p.id"
            />
          </el-select>
        </div>

        <div class="field">
          <div class="label">时间范围</div>
          <div class="inline">
            <el-select v-model="timeKey" style="width: 140px; max-width: 100%">
              <el-option label="近 7 天" value="last7" />
              <el-option label="近 14 天" value="last14" />
              <el-option label="近 30 天" value="last30" />
              <el-option label="自定义" value="custom" />
            </el-select>
            <template v-if="timeKey === 'custom'">
              <el-date-picker v-model="customStart" type="date" placeholder="开始" />
              <el-date-picker v-model="customEnd" type="date" placeholder="结束" />
            </template>
          </div>
        </div>

        <div class="field">
          <div class="label">平台</div>
          <el-select v-model="platformId" clearable placeholder="全部平台" style="width: 160px; max-width: 100%">
            <el-option v-for="p in visiblePlatforms" :key="p.id" :label="p.name" :value="String(p.id)" />
          </el-select>
        </div>

        <div class="field">
          <div class="label">品牌</div>
          <el-select v-model="brandIds" multiple collapse-tags filterable placeholder="全部品牌" style="width: 260px; max-width: 100%">
            <el-option v-for="b in visibleBrands" :key="b.id" :label="b.name" :value="b.id" />
          </el-select>
        </div>

        <div class="field">
          <div class="label">关键词</div>
          <el-select v-model="keywordId" clearable placeholder="全部关键词" style="width: 180px; max-width: 100%">
            <el-option v-for="k in keywordOptions" :key="k.id" :label="k.keyword" :value="String(k.id)" />
          </el-select>
        </div>

        <div class="field">
          <div class="label">情感</div>
          <el-select v-model="polarity" clearable placeholder="全部" style="width: 140px; max-width: 100%">
            <el-option label="正面" value="positive" />
            <el-option label="中性" value="neutral" />
            <el-option label="负面" value="negative" />
          </el-select>
        </div>

        <div class="field">
          <div class="label">水军</div>
          <el-select v-model="spamLabel" clearable placeholder="全部" style="width: 140px; max-width: 100%">
            <el-option label="normal" value="normal" />
            <el-option label="suspect" value="suspect" />
            <el-option label="spam" value="spam" />
          </el-select>
        </div>

        <div class="field">
          <div class="label">有效</div>
          <el-select v-model="isValid" clearable placeholder="全部" style="width: 120px; max-width: 100%">
            <el-option label="有效" value="1" />
            <el-option label="无效" value="0" />
          </el-select>
        </div>

        <div class="field">
          <div class="label">原文搜索</div>
          <el-input v-model="qRaw" placeholder="原文全文搜索" style="width: 220px; max-width: 100%" />
        </div>

        <div class="field">
          <div class="label">清洗搜索</div>
          <el-input v-model="qClean" placeholder="清洗文本搜索" style="width: 220px; max-width: 100%" />
        </div>

        <div class="field">
          <div class="label">主题搜索</div>
          <el-input v-model="qTopic" placeholder="主题关键词搜索" style="width: 180px; max-width: 100%" />
        </div>

        <div class="field">
          <div class="label">实体搜索</div>
          <el-input v-model="qEntity" placeholder="实体关键词搜索" style="width: 180px; max-width: 100%" />
        </div>

        <div class="field" style="min-width: 220px">
          <div class="label">更多筛选</div>
          <el-switch v-model="advancedOpen" active-text="展开" inactive-text="收起" />
        </div>

        <div class="actions">
          <el-button type="primary" :loading="loading" :disabled="!isCustomRangeReady" @click="page = 1; fetchAll()">
            查询
          </el-button>
          <el-button type="primary" plain :loading="loading" :disabled="!activeProjectId" @click="manualRefresh">
            手动刷新
          </el-button>
          <el-button :disabled="loading" @click="resetFilters">重置</el-button>
          <el-switch v-model="autoRefresh" active-text="自动刷新" />
        </div>
      </div>

      <el-divider v-if="advancedOpen" />
      <div v-if="advancedOpen" class="filters">
        <div class="field">
          <div class="label">情感强度</div>
          <div class="inline">
            <el-input v-model="intensityMin" placeholder="min(0~1)" style="width: 110px" />
            <span class="muted">~</span>
            <el-input v-model="intensityMax" placeholder="max(0~1)" style="width: 110px" />
          </div>
        </div>
        <div class="field">
          <div class="label">点赞数</div>
          <div class="inline">
            <el-input v-model="likeMin" placeholder="min" style="width: 110px" />
            <span class="muted">~</span>
            <el-input v-model="likeMax" placeholder="max" style="width: 110px" />
          </div>
        </div>
        <div class="field">
          <div class="label">评论数</div>
          <div class="inline">
            <el-input v-model="commentMin" placeholder="min" style="width: 110px" />
            <span class="muted">~</span>
            <el-input v-model="commentMax" placeholder="max" style="width: 110px" />
          </div>
        </div>
        <div class="field">
          <div class="label">转发数</div>
          <div class="inline">
            <el-input v-model="shareMin" placeholder="min" style="width: 110px" />
            <span class="muted">~</span>
            <el-input v-model="shareMax" placeholder="max" style="width: 110px" />
          </div>
        </div>
      </div>
    </el-card>

    <el-card class="card" shadow="hover">
      <div class="stats">
        <div class="stat"><div class="k">帖子总数</div><div class="v">{{ stats.total_posts }}</div></div>
        <div class="stat"><div class="k">有效帖子</div><div class="v">{{ stats.valid_posts }}</div></div>
        <div class="stat"><div class="k">负面帖子</div><div class="v danger">{{ stats.negative_posts }}</div></div>
        <div class="stat"><div class="k">水军帖子</div><div class="v">{{ stats.spam_posts }}</div></div>
        <div class="stat"><div class="k">热点主题数</div><div class="v">{{ stats.hot_topics }}</div></div>
        <div class="stat"><div class="k">实体数量</div><div class="v">{{ stats.entities }}</div></div>
      </div>
    </el-card>

    <el-card class="card" shadow="hover">
      <el-table :data="items" row-key="id" stripe size="small" v-loading="loading" class="table" @row-click="openDetail">
        <el-table-column label="帖子内容" min-width="320" show-overflow-tooltip>
          <template #default="{ row }">
            <div class="content">{{ snippet(row.raw_text, 90) }}</div>
            <div class="sub muted">清洗：{{ snippet(row.clean_text, 60) }}</div>
          </template>
        </el-table-column>
        <el-table-column prop="platform_name" label="平台" width="110" />
        <el-table-column prop="brand_name" label="品牌" width="120" show-overflow-tooltip />
        <el-table-column prop="publish_time" label="发布时间" min-width="170" />
        <el-table-column prop="like_count" label="赞" width="70" />
        <el-table-column prop="comment_count" label="评" width="70" />
        <el-table-column prop="share_count" label="转" width="70" />
        <el-table-column label="情感" width="90">
          <template #default="{ row }">
            <el-tag v-if="row.polarity" size="small" :type="polarityType(row.polarity)">{{ polarityText(row.polarity) }}</el-tag>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="强度" width="80">
          <template #default="{ row }">
            {{ row.intensity != null ? Number(row.intensity).toFixed(3) : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="水军" width="100">
          <template #default="{ row }">
            <el-tag v-if="row.spam_label" size="small" :type="String(row.spam_label).toLowerCase() === 'spam' ? 'danger' : 'info'">{{ row.spam_label }}</el-tag>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="topic_name" label="主题" min-width="140" show-overflow-tooltip />
        <el-table-column prop="entity_texts" label="实体" min-width="160" show-overflow-tooltip />
      </el-table>

      <div class="pager">
        <div class="muted">共 {{ total }} 条</div>
        <el-pagination
          v-model:current-page="page"
          v-model:page-size="pageSize"
          :total="total"
          :page-sizes="[10, 20, 50, 100]"
          layout="sizes, prev, pager, next"
          background
        />
      </div>
    </el-card>

    <el-drawer v-model="drawerVisible" title="帖子详情" size="720px">
      <div v-if="!activeRow" class="muted">未选择帖子</div>
      <template v-else>
        <el-descriptions :column="1" border>
          <el-descriptions-item label="原始文本">{{ activeRow.raw_text || '-' }}</el-descriptions-item>
          <el-descriptions-item label="平台帖子ID">{{ activeRow.platform_post_id || '-' }}</el-descriptions-item>
          <el-descriptions-item label="平台">{{ activeRow.platform_name || '-' }}</el-descriptions-item>
          <el-descriptions-item label="品牌">{{ activeRow.brand_name || '-' }}</el-descriptions-item>
          <el-descriptions-item label="作者">{{ activeRow.author_nickname || '-' }}</el-descriptions-item>
          <el-descriptions-item label="发布时间">{{ activeRow.publish_time || '-' }}</el-descriptions-item>
          <el-descriptions-item label="点赞/评论/转发">
            {{ activeRow.like_count ?? 0 }} / {{ activeRow.comment_count ?? 0 }} / {{ activeRow.share_count ?? 0 }}
          </el-descriptions-item>
          <el-descriptions-item label="清洗文本">{{ activeRow.clean_text || '-' }}</el-descriptions-item>
          <el-descriptions-item label="文本哈希">{{ activeRow.text_hash || '-' }}</el-descriptions-item>
          <el-descriptions-item label="是否有效">
            <el-tag size="small" :type="Number(activeRow.clean_is_valid) === 1 ? 'success' : 'info'">
              {{ Number(activeRow.clean_is_valid) === 1 ? '有效' : '无效' }}
            </el-tag>
            <span v-if="Number(activeRow.clean_is_valid) !== 1 && activeRow.clean_invalid_reason" class="muted">（{{ activeRow.clean_invalid_reason }}）</span>
          </el-descriptions-item>
          <el-descriptions-item label="情感">
            <el-tag v-if="activeRow.polarity" size="small" :type="polarityType(activeRow.polarity)">{{ polarityText(activeRow.polarity) }}</el-tag>
            <span class="muted" v-if="activeRow.confidence != null"> confidence={{ Number(activeRow.confidence).toFixed(3) }}</span>
            <span class="muted" v-if="activeRow.intensity != null"> intensity={{ Number(activeRow.intensity).toFixed(3) }}</span>
            <div class="muted" v-if="activeRow.emotions">emotions={{ activeRow.emotions }}</div>
          </el-descriptions-item>
          <el-descriptions-item label="水军识别">
            <el-tag v-if="activeRow.spam_label" size="small" :type="String(activeRow.spam_label).toLowerCase() === 'spam' ? 'danger' : 'info'">{{ activeRow.spam_label }}</el-tag>
            <span class="muted" v-if="activeRow.spam_score != null"> score={{ Number(activeRow.spam_score).toFixed(3) }}</span>
            <div class="muted" v-if="activeRow.spam_rule_hits">rule_hits={{ activeRow.spam_rule_hits }}</div>
          </el-descriptions-item>
          <el-descriptions-item label="主题识别">
            <span>{{ activeRow.topic_name || '-' }}</span>
            <span class="muted" v-if="activeRow.topic_score != null"> score={{ activeRow.topic_score }}</span>
          </el-descriptions-item>
        </el-descriptions>

        <div class="block">
          <div class="block-title">实体识别</div>
          <el-empty v-if="!(activeRow.entities || []).length" description="暂无实体结果" />
          <el-table v-else :data="activeRow.entities" size="small" stripe>
            <el-table-column prop="entity_type" label="类型" width="120" />
            <el-table-column prop="entity_text" label="实体" min-width="160" />
            <el-table-column prop="normalized" label="归一化" min-width="160" />
            <el-table-column label="置信度" width="120">
              <template #default="{ row }">{{ row.confidence != null ? Number(row.confidence).toFixed(3) : '-' }}</template>
            </el-table-column>
          </el-table>
        </div>

        <div class="block">
          <div class="block-title">关联追溯</div>
          <div class="inline" style="margin-bottom: 8px">
            <el-button
              size="small"
              :disabled="!activeRow?.brand_id"
              @click="brandIds = activeRow?.brand_id ? [Number(activeRow.brand_id)] : brandIds; page = 1; fetchAll()"
            >
              同品牌
            </el-button>
            <el-button
              size="small"
              :disabled="!activeRow?.platform_id"
              @click="platformId = activeRow?.platform_id != null ? String(activeRow.platform_id) : platformId; page = 1; fetchAll()"
            >
              同平台
            </el-button>
            <el-button
              size="small"
              :disabled="!activeRow?.topic_name"
              @click="qTopic = activeRow?.topic_name || ''; page = 1; fetchAll()"
            >
              同主题
            </el-button>
            <el-button
              size="small"
              :disabled="!(activeRow?.entities || []).length"
              @click="qEntity = (activeRow?.entities || [])[0]?.entity_text || ''; page = 1; fetchAll()"
            >
              同实体
            </el-button>
          </div>
          <el-skeleton v-if="linkedLoading" :rows="3" animated />
          <el-empty v-else-if="!linkedReports.length" description="暂无关联报告引用" />
          <div v-else class="links">
            <div v-for="r in linkedReports" :key="r.id" class="link-row">
              <span class="link-title">{{ r.title || `报告#${r.id}` }}</span>
              <el-button link type="primary" @click="gotoReport(r.id)">打开报告</el-button>
            </div>
          </div>
        </div>
      </template>
    </el-drawer>
  </section>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.card {
  border-radius: 10px;
}

.filters {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  align-items: flex-end;
}

.field {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-width: 180px;
  max-width: 100%;
}

.label {
  font-size: 12px;
  color: #4b5563;
}

.inline {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  align-items: center;
}

.actions {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  align-items: center;
}

.stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 10px;
}

.stat {
  background: #f9fafb;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 10px 12px;
}

.stat .k {
  font-size: 12px;
  color: #6b7280;
}

.stat .v {
  margin-top: 6px;
  font-size: 18px;
  font-weight: 700;
  color: #111827;
}

.stat .v.danger {
  color: #b91c1c;
}

.table {
  width: 100%;
}

.content {
  font-size: 13px;
  color: #111827;
  line-height: 1.5;
}

.sub {
  margin-top: 4px;
}

.muted {
  color: #6b7280;
  font-size: 12px;
}

.pager {
  margin-top: 10px;
  display: flex;
  justify-content: space-between;
  gap: 10px;
  flex-wrap: wrap;
  align-items: center;
}

.block {
  margin-top: 14px;
}

.block-title {
  font-weight: 600;
  margin-bottom: 8px;
}

.links {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.link-row {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  align-items: center;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 8px 10px;
  background: #fff;
}

.link-title {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

@media (max-width: 640px) {
  .field {
    min-width: 100%;
  }
}
</style>
