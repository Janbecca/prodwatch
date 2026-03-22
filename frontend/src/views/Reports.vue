<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as echarts from 'echarts'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '../api/axios'
import PageHeader from '../components/PageHeader.vue'
import { useProjectsStore } from '../stores/projects'

const projectsStore = useProjectsStore()
const projects = computed(() => projectsStore.projects || [])

const loading = ref(false)
const error = ref('')
const reports = ref([])

// filters
const filterProjectId = ref('')
const filterBrandId = ref('')
const filterPlatformId = ref('')
const filterReportType = ref('')
const filterTimeRange = ref([]) // 数据时间范围 [Date, Date]
const filterCreatedRange = ref([]) // 创建时间范围 [Date, Date]
const filterTitleKeyword = ref('')

// options
const optionsLoading = ref(false)
const platformOptions = ref([])
const brandOptions = ref([])

// drawers
const detailVisible = ref(false)
const detailLoading = ref(false)
const activeReport = ref(null)

const citationsVisible = ref(false)
const citationsLoading = ref(false)
const citations = ref([])

const createVisible = ref(false)
const creating = ref(false)

// create form
const formProjectId = ref('')
const formTitle = ref('')
const formReportType = ref('daily') // daily | weekly | monthly | special
const formTimeMode = ref('last14') // last7 | last14 | last30 | custom
const formCustomRange = ref([]) // [Date, Date]
const formPlatformIds = ref([])
const formBrandIds = ref([])
const formKeywordIds = ref([])
const formIncludeSections = ref([
  'sentiment_analysis',
  'sentiment_trends',
  'hot_topics',
  'entities',
  'spam',
  'competitor_compare',
  'suggestions',
])
const formExportFormats = ref(['word'])

const keywordOptions = ref([])
const keywordLoading = ref(false)

const reportTypeOptions = [
  { label: '日报', value: 'daily' },
  { label: '周报', value: 'weekly' },
  { label: '月报', value: 'monthly' },
  { label: '专题报告', value: 'special' },
]

const timeModeOptions = [
  { label: '近 7 天', value: 'last7' },
  { label: '近 14 天', value: 'last14' },
  { label: '近 30 天', value: 'last30' },
  { label: '自定义', value: 'custom' },
]

const sectionOptions = [
  { label: '情感分析', value: 'sentiment_analysis' },
  { label: '情感趋势', value: 'sentiment_trends' },
  { label: '热点话题', value: 'hot_topics' },
  { label: '实体/功能点分析', value: 'entities' },
  { label: '水军识别', value: 'spam' },
  { label: '竞品对比', value: 'competitor_compare' },
  { label: '战略建议', value: 'suggestions' },
]

const exportFormatOptions = [
  { label: 'PDF', value: 'pdf' },
  { label: 'Word', value: 'word' },
  { label: 'PPT', value: 'ppt' },
]

const parseApiError = (e) => e?.response?.data?.detail || e?.message || '请求失败'

const toDateOnly = (d) => {
  if (!d) return null
  const dt = new Date(d)
  if (Number.isNaN(dt.getTime())) return null
  const y = dt.getFullYear()
  const m = String(dt.getMonth() + 1).padStart(2, '0')
  const day = String(dt.getDate()).padStart(2, '0')
  return `${y}-${m}-${day}`
}

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

const activeProjectBrandIds = computed(() => {
  const pid = Number(formProjectId.value)
  if (!Number.isFinite(pid) || pid <= 0) return []
  const p = (projects.value || []).find((x) => Number(x?.id) === pid)
  const ids = Array.isArray(p?.brand_ids) ? p.brand_ids : []
  return ids.map((x) => Number(x)).filter((x) => Number.isFinite(x))
})

const filteredBrandOptions = computed(() => {
  const brandIds = new Set(activeProjectBrandIds.value || [])
  if (!formProjectId.value) return brandOptions.value || []
  if (brandIds.size === 0) return []
  return (brandOptions.value || []).filter((b) => brandIds.has(Number(b.id)))
})

const filterProjectBrandIds = computed(() => {
  const pid = Number(filterProjectId.value)
  if (!Number.isFinite(pid) || pid <= 0) return []
  const p = (projects.value || []).find((x) => Number(x?.id) === pid)
  const ids = Array.isArray(p?.brand_ids) ? p.brand_ids : []
  return ids.map((x) => Number(x)).filter((x) => Number.isFinite(x))
})

const filteredFilterBrandOptions = computed(() => {
  const pid = Number(filterProjectId.value)
  if (!Number.isFinite(pid) || pid <= 0) return brandOptions.value || []
  const allow = new Set(filterProjectBrandIds.value || [])
  if (!allow.size) return []
  return (brandOptions.value || []).filter((b) => allow.has(Number(b.id)))
})

const filterProjectEnabledPlatformIds = computed(() => {
  const pid = Number(filterProjectId.value)
  if (!Number.isFinite(pid) || pid <= 0) return []
  const p = (projects.value || []).find((x) => Number(x?.id) === pid)
  const ids = Array.isArray(p?.enabled_platform_ids) ? p.enabled_platform_ids : []
  return ids.map((x) => Number(x)).filter((x) => Number.isFinite(x))
})

const filteredFilterPlatformOptions = computed(() => {
  const pid = Number(filterProjectId.value)
  if (!Number.isFinite(pid) || pid <= 0) return platformOptions.value || []
  const allow = new Set(filterProjectEnabledPlatformIds.value || [])
  if (!allow.size) return platformOptions.value || []
  return (platformOptions.value || []).filter((p) => allow.has(Number(p.id)))
})

const brandNameById = computed(() => {
  const m = new Map()
  for (const b of brandOptions.value || []) m.set(Number(b.id), b.name)
  return m
})

const platformNameById = computed(() => {
  const m = new Map()
  for (const p of platformOptions.value || []) m.set(Number(p.id), p.name)
  return m
})

const triggerTypeLabel = (t) => {
  const s = String(t || '').toLowerCase()
  if (s === 'manual') return '手动'
  if (s === 'schedule') return '定时'
  if (s === 'custom') return '自定义'
  return s || '-'
}

const fetchOptions = async () => {
  optionsLoading.value = true
  try {
    const { data } = await api.get('/api/dashboard/options')
    platformOptions.value = Array.isArray(data?.platforms) ? data.platforms : []
    brandOptions.value = Array.isArray(data?.brands) ? data.brands : []
  } catch (e) {
    ElMessage.error(parseApiError(e) || '加载选项失败')
  } finally {
    optionsLoading.value = false
  }
}

const fetchKeywords = async (projectId) => {
  const pid = Number(projectId)
  if (!Number.isFinite(pid) || pid <= 0) {
    keywordOptions.value = []
    return
  }
  keywordLoading.value = true
  try {
    const { data } = await api.get('/api/posts/keywords', { params: { project_id: pid } })
    keywordOptions.value = Array.isArray(data) ? data : []
  } catch (e) {
    keywordOptions.value = []
  } finally {
    keywordLoading.value = false
  }
}

const fetchReports = async () => {
  loading.value = true
  error.value = ''
  try {
    const timeFrom = toDateOnly(filterTimeRange.value?.[0])
    const timeTo = toDateOnly(filterTimeRange.value?.[1])
    const createdFrom = toDateOnly(filterCreatedRange.value?.[0])
    const createdTo = toDateOnly(filterCreatedRange.value?.[1])

    const params = buildParams({
      project_id: filterProjectId.value ? Number(filterProjectId.value) : undefined,
      brand_id: filterBrandId.value ? Number(filterBrandId.value) : undefined,
      platform_id: filterPlatformId.value ? Number(filterPlatformId.value) : undefined,
      report_type: filterReportType.value || undefined,
      time_from: timeFrom || undefined,
      time_to: timeTo || undefined,
      created_from: createdFrom || undefined,
      created_to: createdTo || undefined,
      title_keyword: String(filterTitleKeyword.value || '').trim() || undefined,
    })
    const { data } = await api.get('/api/reports', { params })
    reports.value = Array.isArray(data) ? data : []
  } catch (e) {
    error.value = parseApiError(e) || '加载报告列表失败'
  } finally {
    loading.value = false
  }
}

const resetFilters = async () => {
  filterProjectId.value = ''
  filterBrandId.value = ''
  filterPlatformId.value = ''
  filterReportType.value = ''
  filterTimeRange.value = []
  filterCreatedRange.value = []
  filterTitleKeyword.value = ''
  await fetchReports()
}

const openDetail = async (row) => {
  if (!row?.id) return
  detailVisible.value = true
  detailLoading.value = true
  activeReport.value = null
  try {
    const { data } = await api.get(`/api/reports/${row.id}`)
    activeReport.value = data || null
  } catch (e) {
    ElMessage.error(parseApiError(e) || '加载报告失败')
    detailVisible.value = false
  } finally {
    detailLoading.value = false
  }
}

const openCitations = async (row) => {
  if (!row?.id) return
  citationsVisible.value = true
  citationsLoading.value = true
  citations.value = []
  try {
    const { data } = await api.get(`/api/reports/${row.id}/citations`)
    citations.value = Array.isArray(data) ? data : []
  } catch (e) {
    ElMessage.error(parseApiError(e) || '加载引用证据失败')
    citationsVisible.value = false
  } finally {
    citationsLoading.value = false
  }
}

const confirmDelete = async (row) => {
  if (!row?.id) return
  try {
    await ElMessageBox.confirm('确认删除该报告？删除后不可恢复。', '操作确认', {
      confirmButtonText: '删除',
      cancelButtonText: '取消',
      type: 'warning',
    })
  } catch {
    return
  }
  try {
    await api.delete(`/api/reports/${row.id}`)
    ElMessage.success('已删除')
    await fetchReports()
  } catch (e) {
    ElMessage.error(parseApiError(e) || '删除失败')
  }
}

const downloadExport = async (row, format) => {
  if (!row?.id) return
  const fmt = String(format || 'word').toLowerCase()
  try {
    const { data, headers } = await api.get(`/api/reports/${row.id}/export`, {
      params: { format: fmt },
      responseType: 'blob',
    })
    const blob = new Blob([data], { type: headers?.['content-type'] || 'application/octet-stream' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    const safeTitle = String(row.title || `report_${row.id}`).replace(/[\\/:*?"<>|]+/g, '_')
    const ext = fmt === 'pdf' ? 'pdf' : fmt === 'ppt' ? 'ppt' : 'doc'
    link.download = `${safeTitle}.${ext}`
    link.click()
    URL.revokeObjectURL(url)
  } catch (e) {
    ElMessage.error(parseApiError(e) || '导出失败')
  }
}

const openCreate = () => {
  createVisible.value = true
  formProjectId.value = String(projectsStore.activeProjectId || '')
  formTitle.value = ''
  formReportType.value = 'daily'
  formTimeMode.value = 'last14'
  formCustomRange.value = []
  formPlatformIds.value = []
  formBrandIds.value = []
  formKeywordIds.value = []
  formIncludeSections.value = sectionOptions.map((x) => x.value)
  formExportFormats.value = ['word']
  if (!platformOptions.value?.length || !brandOptions.value?.length) fetchOptions()
  fetchKeywords(formProjectId.value)
}

const openCopyCreate = async (row) => {
  if (!row?.id) return
  try {
    const { data } = await api.get(`/api/reports/${row.id}`)
    const cfg = data?.config || {}
    createVisible.value = true
    formProjectId.value = String(cfg.projectId || data?.projectId || projectsStore.activeProjectId || '')
    formTitle.value = String(cfg.title || data?.title || '').trim() ? `${cfg.title || data?.title}（复制）` : '复制报告'
    formReportType.value = String(cfg.reportType || data?.reportType || 'daily')

    // time
    if (cfg.startDate && cfg.endDate) {
      formTimeMode.value = 'custom'
      formCustomRange.value = [new Date(cfg.startDate), new Date(cfg.endDate)]
    } else {
      formTimeMode.value = 'last14'
      formCustomRange.value = []
    }

    formPlatformIds.value = Array.isArray(cfg.platformIds) ? cfg.platformIds : []
    formBrandIds.value = Array.isArray(cfg.brandIds) ? cfg.brandIds : []
    formKeywordIds.value = Array.isArray(cfg.keywordIds) ? cfg.keywordIds : []

    if (Array.isArray(cfg.includeSections)) formIncludeSections.value = cfg.includeSections
    else if (cfg.include && typeof cfg.include === 'object') {
      formIncludeSections.value = Object.entries(cfg.include)
        .filter(([, v]) => Boolean(v))
        .map(([k]) => k)
    } else formIncludeSections.value = sectionOptions.map((x) => x.value)

    formExportFormats.value = Array.isArray(cfg.exportFormats) ? cfg.exportFormats : ['word']
    if (!platformOptions.value?.length || !brandOptions.value?.length) await fetchOptions()
    await fetchKeywords(formProjectId.value)
  } catch (e) {
    ElMessage.error(parseApiError(e) || '复制失败')
  }
}

const buildCreatePayload = () => {
  const pid = Number(formProjectId.value)
  if (!Number.isFinite(pid) || pid <= 0) return { error: '请选择所属项目' }
  const title = String(formTitle.value || '').trim()
  if (!title) return { error: '请输入报告标题' }

  const payload = {
    projectId: pid,
    title,
    reportType: formReportType.value,
    platformIds: (formPlatformIds.value || []).map((x) => Number(x)).filter((x) => Number.isFinite(x)),
    brandIds: (formBrandIds.value || []).map((x) => Number(x)).filter((x) => Number.isFinite(x)),
    keywordIds: (formKeywordIds.value || []).map((x) => Number(x)).filter((x) => Number.isFinite(x)),
    includeSections: [...(formIncludeSections.value || [])],
    exportFormats: [...(formExportFormats.value || [])],
  }

  if (formTimeMode.value === 'custom') {
    const s = toDateOnly(formCustomRange.value?.[0])
    const e = toDateOnly(formCustomRange.value?.[1])
    if (!s || !e) return { error: '请选择自定义时间范围' }
    payload.startDate = s
    payload.endDate = e
    return { payload }
  }

  if (formTimeMode.value === 'last7') payload.days = 7
  else if (formTimeMode.value === 'last30') payload.days = 30
  else payload.days = 14
  return { payload }
}

const submitCreate = async () => {
  const { payload, error: err } = buildCreatePayload()
  if (err) return ElMessage.warning(err)
  creating.value = true
  try {
    const { data } = await api.post('/api/reports', payload)
    ElMessage.success('已生成报告')
    createVisible.value = false
    await fetchReports()
    if (data?.id) openDetail({ id: data.id })
  } catch (e) {
    ElMessage.error(parseApiError(e) || '生成失败')
  } finally {
    creating.value = false
  }
}

// charts in detail drawer
const trendChartEl = ref(null)
const compareChartEl = ref(null)
let trendChart = null
let compareChart = null

const disposeCharts = () => {
  if (trendChart) {
    trendChart.dispose()
    trendChart = null
  }
  if (compareChart) {
    compareChart.dispose()
    compareChart = null
  }
}

const renderCharts = async () => {
  await nextTick()
  disposeCharts()

  const content = activeReport.value?.content || {}
  const trends = content?.sentiment_trends || {}
  const compare = Array.isArray(content?.competitor_compare) ? content.competitor_compare : []

  if (trendChartEl.value && Array.isArray(trends?.dates) && trends.dates.length) {
    trendChart = echarts.init(trendChartEl.value)
    trendChart.setOption({
      tooltip: { trigger: 'axis' },
      legend: { data: ['正面', '中性', '负面'] },
      grid: { left: 40, right: 16, top: 30, bottom: 24, containLabel: true },
      xAxis: { type: 'category', data: trends.dates },
      yAxis: { type: 'value', min: 0, max: 1 },
      series: [
        { name: '正面', type: 'line', smooth: true, data: trends.positive || [] },
        { name: '中性', type: 'line', smooth: true, data: trends.neutral || [] },
        { name: '负面', type: 'line', smooth: true, data: trends.negative || [] },
      ],
    })
  }

  if (compareChartEl.value && compare.length) {
    compareChart = echarts.init(compareChartEl.value)
    compareChart.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 40, right: 16, top: 20, bottom: 70, containLabel: true },
      xAxis: {
        type: 'category',
        data: compare.map((x) => x.brand_name || `brand_${x.brand_id}`),
        axisLabel: { interval: 0, rotate: 30 },
      },
      yAxis: { type: 'value', min: 0, max: 1 },
      series: [{ name: '负面率', type: 'bar', data: compare.map((x) => Number(x.negative_ratio || 0)) }],
    })
  }
}

watch(
  () => [detailVisible.value, activeReport.value?.id],
  async () => {
    if (!detailVisible.value) return
    await renderCharts()
  }
)

watch(
  () => formProjectId.value,
  async (pid) => {
    // reset brand/keyword when project changes
    formBrandIds.value = []
    formKeywordIds.value = []
    await fetchKeywords(pid)
  }
)

watch(
  () => filterProjectId.value,
  async () => {
    // reset list filters when project changes
    filterBrandId.value = ''
    filterPlatformId.value = ''
    await fetchReports()
  }
)

onBeforeUnmount(() => {
  disposeCharts()
})

onMounted(async () => {
  await projectsStore.fetch()
  await fetchOptions()
  await fetchReports()
})
</script>

<template>
  <div class="page">
    <PageHeader content="报告中心" />

    <el-card class="block" shadow="never">
      <el-form :inline="true" class="filters" @submit.prevent>
        <el-form-item label="项目">
          <el-select v-model="filterProjectId" placeholder="全部" clearable filterable style="width: 220px">
            <el-option v-for="p in projects" :key="p.id" :label="p.name" :value="String(p.id)" />
          </el-select>
        </el-form-item>

        <el-form-item label="品牌">
          <el-select v-model="filterBrandId" placeholder="全部" clearable filterable style="width: 180px">
            <el-option v-for="b in filteredFilterBrandOptions" :key="b.id" :label="b.name" :value="String(b.id)" />
          </el-select>
        </el-form-item>

        <el-form-item label="平台">
          <el-select v-model="filterPlatformId" placeholder="全部" clearable filterable style="width: 180px">
            <el-option v-for="p in filteredFilterPlatformOptions" :key="p.id" :label="p.name" :value="String(p.id)" />
          </el-select>
        </el-form-item>

        <el-form-item label="类型">
          <el-select v-model="filterReportType" placeholder="全部" clearable style="width: 180px">
            <el-option v-for="t in reportTypeOptions" :key="t.value" :label="t.label" :value="t.value" />
          </el-select>
        </el-form-item>

        <el-form-item label="数据时间">
          <el-date-picker v-model="filterTimeRange" type="daterange" start-placeholder="开始" end-placeholder="结束" />
        </el-form-item>

        <el-form-item label="创建时间">
          <el-date-picker v-model="filterCreatedRange" type="daterange" start-placeholder="开始" end-placeholder="结束" />
        </el-form-item>

        <el-form-item label="标题">
          <el-input v-model="filterTitleKeyword" placeholder="关键词" clearable style="width: 200px" />
        </el-form-item>

        <el-form-item>
          <el-button type="primary" :loading="loading" @click="fetchReports">查询</el-button>
          <el-button :disabled="loading" @click="resetFilters">重置</el-button>
          <el-button type="success" :disabled="projectsStore.loading" @click="openCreate">新建报告</el-button>
        </el-form-item>
      </el-form>

      <el-alert v-if="error" class="mt12" type="error" :title="error" show-icon />

      <el-table class="mt12" :data="reports" v-loading="loading" row-key="id" style="width: 100%">
        <el-table-column prop="title" label="报告标题" min-width="220" show-overflow-tooltip />
        <el-table-column prop="reportTypeLabel" label="报告类型" width="110">
          <template #default="{ row }">
            {{
              reportTypeOptions.find((x) => x.value === row.reportType)?.label ||
              (row.reportType === 'custom' ? '专题报告' : row.reportType || '-')
            }}
          </template>
        </el-table-column>
        <el-table-column prop="projectName" label="所属项目" min-width="160" show-overflow-tooltip />
        <el-table-column label="品牌" width="140" show-overflow-tooltip>
          <template #default="{ row }">
            <span>{{ brandNameById.get(Number(row.brandId)) || (row.brandId != null ? `#${row.brandId}` : '-') }}</span>
          </template>
        </el-table-column>
        <el-table-column label="平台" width="140" show-overflow-tooltip>
          <template #default="{ row }">
            <span>{{ platformNameById.get(Number(row.platformId)) || (row.platformId != null ? `#${row.platformId}` : '-') }}</span>
          </template>
        </el-table-column>
        <el-table-column label="生成方式" width="110">
          <template #default="{ row }">
            <span>{{ triggerTypeLabel(row.triggerType) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="数据时间范围" min-width="200">
          <template #default="{ row }">
            <span>{{ row.timeStart ? String(row.timeStart).slice(0, 10) : row.rangeFrom || '-' }}</span>
            <span> ~ </span>
            <span>{{ row.timeEnd ? String(row.timeEnd).slice(0, 10) : row.rangeTo || '-' }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="createdAt" label="生成时间" width="180">
          <template #default="{ row }">
            <span>{{ row.createdAt ? String(row.createdAt).replace('T', ' ').slice(0, 19) : '-' }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="status" label="生成状态" width="110" />
        <el-table-column prop="summary" label="报告摘要" min-width="260" show-overflow-tooltip />

        <el-table-column label="操作" width="330" fixed="right">
          <template #default="{ row }">
            <el-button link type="primary" @click="openDetail(row)">查看详情</el-button>
            <el-button link @click="openCitations(row)">引用证据</el-button>

            <el-dropdown @command="(cmd) => downloadExport(row, cmd)">
              <el-button link type="success">导出</el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item command="pdf">导出 PDF</el-dropdown-item>
                  <el-dropdown-item command="word">导出 Word</el-dropdown-item>
                  <el-dropdown-item command="ppt">导出 PPT</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>

            <el-button link type="warning" @click="openCopyCreate(row)">复制生成</el-button>
            <el-button link type="danger" @click="confirmDelete(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- Create drawer -->
    <el-drawer v-model="createVisible" title="新建报告" size="560px" :close-on-click-modal="false">
      <el-form label-width="92px" @submit.prevent>
        <el-divider content-position="left">基本信息</el-divider>
        <el-form-item label="报告标题">
          <el-input v-model="formTitle" placeholder="请输入" maxlength="80" show-word-limit />
        </el-form-item>
        <el-form-item label="报告类型">
          <el-select v-model="formReportType" style="width: 100%">
            <el-option v-for="t in reportTypeOptions" :key="t.value" :label="t.label" :value="t.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="所属项目">
          <el-select v-model="formProjectId" placeholder="请选择" filterable style="width: 100%">
            <el-option v-for="p in projects" :key="p.id" :label="p.name" :value="String(p.id)" />
          </el-select>
        </el-form-item>

        <el-divider content-position="left">数据范围</el-divider>
        <el-form-item label="时间范围">
          <el-select v-model="formTimeMode" style="width: 100%">
            <el-option v-for="t in timeModeOptions" :key="t.value" :label="t.label" :value="t.value" />
          </el-select>
        </el-form-item>
        <el-form-item v-if="formTimeMode === 'custom'" label="自定义">
          <el-date-picker v-model="formCustomRange" type="daterange" start-placeholder="开始" end-placeholder="结束" style="width: 100%" />
        </el-form-item>

        <el-form-item label="平台">
          <el-select v-model="formPlatformIds" multiple filterable clearable :loading="optionsLoading" style="width: 100%">
            <el-option v-for="p in platformOptions" :key="p.id" :label="p.name" :value="p.id" />
          </el-select>
        </el-form-item>

        <el-form-item label="品牌">
          <el-select v-model="formBrandIds" multiple filterable clearable :loading="optionsLoading" style="width: 100%">
            <el-option v-for="b in filteredBrandOptions" :key="b.id" :label="b.name" :value="b.id" />
          </el-select>
          <div v-if="formProjectId && filteredBrandOptions.length === 0" class="hint">当前项目未配置监控品牌或未获取到品牌数据。</div>
        </el-form-item>

        <el-form-item label="关键词">
          <el-select v-model="formKeywordIds" multiple filterable clearable :loading="keywordLoading" style="width: 100%">
            <el-option v-for="k in keywordOptions" :key="k.id" :label="k.keyword" :value="k.id" />
          </el-select>
        </el-form-item>

        <el-divider content-position="left">分析选项</el-divider>
        <el-form-item label="展示模块">
          <el-checkbox-group v-model="formIncludeSections">
            <el-checkbox v-for="s in sectionOptions" :key="s.value" :value="s.value">{{ s.label }}</el-checkbox>
          </el-checkbox-group>
        </el-form-item>

        <el-divider content-position="left">输出配置</el-divider>
        <el-form-item label="导出格式">
          <el-checkbox-group v-model="formExportFormats">
            <el-checkbox v-for="f in exportFormatOptions" :key="f.value" :value="f.value">{{ f.label }}</el-checkbox>
          </el-checkbox-group>
        </el-form-item>

        <div class="drawer-actions">
          <el-button @click="createVisible = false">取消</el-button>
          <el-button type="primary" :loading="creating" @click="submitCreate">生成报告</el-button>
        </div>
      </el-form>
    </el-drawer>

    <!-- Detail drawer -->
    <el-drawer v-model="detailVisible" title="报告详情" size="72%">
      <div v-loading="detailLoading">
        <template v-if="activeReport">
          <el-card shadow="never" class="block">
            <div class="title-row">
              <div class="title">{{ activeReport.title }}</div>
              <div class="meta">
                <span>项目：{{ activeReport.projectName || activeReport.projectId }}</span>
                <span class="sep">|</span>
                <span>
                  数据时间：{{ activeReport.timeStart ? String(activeReport.timeStart).slice(0, 10) : activeReport.rangeFrom }} ~
                  {{ activeReport.timeEnd ? String(activeReport.timeEnd).slice(0, 10) : activeReport.rangeTo }}
                </span>
              </div>
              <div class="summary">{{ activeReport.summary || '' }}</div>
            </div>
          </el-card>

          <el-card v-if="activeReport.content?.executive_summary" shadow="never" class="block mt12">
            <template #header>执行摘要</template>
            <el-descriptions :column="1" border>
              <el-descriptions-item label="舆情总体趋势">{{ activeReport.content.executive_summary.overall_trend }}</el-descriptions-item>
              <el-descriptions-item label="主要风险点">{{ activeReport.content.executive_summary.main_risks }}</el-descriptions-item>
              <el-descriptions-item label="关键用户反馈">{{ activeReport.content.executive_summary.key_feedback }}</el-descriptions-item>
              <el-descriptions-item label="战略建议">
                <ul class="ul">
                  <li v-for="(s, idx) in activeReport.content.executive_summary.strategic_suggestions || []" :key="idx">{{ s }}</li>
                </ul>
              </el-descriptions-item>
            </el-descriptions>
          </el-card>

          <el-card v-if="activeReport.content?.overview" shadow="never" class="block mt12">
            <template #header>舆情概况</template>
            <el-descriptions :column="3" border>
              <el-descriptions-item label="总帖子数">{{ activeReport.content.overview.total_posts }}</el-descriptions-item>
              <el-descriptions-item label="有效帖子数">{{ activeReport.content.overview.valid_posts }}</el-descriptions-item>
              <el-descriptions-item label="水军帖子数">{{ activeReport.content.overview.spam_posts }}</el-descriptions-item>
              <el-descriptions-item label="正面帖子数">{{ activeReport.content.overview.positive_posts }}</el-descriptions-item>
              <el-descriptions-item label="中性帖子数">{{ activeReport.content.overview.neutral_posts }}</el-descriptions-item>
              <el-descriptions-item label="负面帖子数">{{ activeReport.content.overview.negative_posts }}</el-descriptions-item>
            </el-descriptions>
          </el-card>

          <el-card v-if="activeReport.content?.sentiment_trends?.dates?.length" shadow="never" class="block mt12">
            <template #header>情感趋势分析</template>
            <div ref="trendChartEl" class="chart" />
          </el-card>

          <el-card v-if="activeReport.content?.hot_topics?.length" shadow="never" class="block mt12">
            <template #header>热点话题分析（TopN）</template>
            <el-table :data="activeReport.content.hot_topics" size="small">
              <el-table-column prop="topic_name" label="主题" min-width="180" />
              <el-table-column prop="count" label="讨论量" width="120" />
            </el-table>
          </el-card>

          <el-card v-if="activeReport.content?.entities?.length" shadow="never" class="block mt12">
            <template #header>实体/功能点分析（TopN）</template>
            <el-table :data="activeReport.content.entities" size="small">
              <el-table-column prop="entity_text" label="实体" min-width="180" />
              <el-table-column prop="count" label="出现次数" width="120" />
            </el-table>
          </el-card>

          <el-card v-if="activeReport.content?.spam" shadow="never" class="block mt12">
            <template #header>水军分析</template>
            <el-descriptions :column="2" border>
              <el-descriptions-item label="水军帖子数">{{ activeReport.content.spam.spam_posts }}</el-descriptions-item>
              <el-descriptions-item label="水军比例">{{ (Number(activeReport.content.spam.spam_ratio || 0) * 100).toFixed(1) }}%</el-descriptions-item>
            </el-descriptions>
          </el-card>

          <el-card v-if="activeReport.content?.competitor_compare?.length" shadow="never" class="block mt12">
            <template #header>竞品对比</template>
            <div ref="compareChartEl" class="chart" />
            <el-table class="mt12" :data="activeReport.content.competitor_compare" size="small">
              <el-table-column prop="brand_name" label="品牌" min-width="160" />
              <el-table-column prop="total_posts" label="舆情数量" width="120" />
              <el-table-column label="负面率" width="120">
                <template #default="{ row }">{{ (Number(row.negative_ratio || 0) * 100).toFixed(1) }}%</template>
              </el-table-column>
            </el-table>
          </el-card>

          <el-card v-if="activeReport.content?.strategic_suggestions?.length" shadow="never" class="block mt12">
            <template #header>战略建议</template>
            <ul class="ul">
              <li v-for="(s, idx) in activeReport.content.strategic_suggestions" :key="idx">{{ s }}</li>
            </ul>
          </el-card>
        </template>

        <el-empty v-else description="暂无数据" />
      </div>
    </el-drawer>

    <!-- Citations drawer -->
    <el-drawer v-model="citationsVisible" title="引用证据" size="66%">
      <el-table :data="citations" v-loading="citationsLoading" row-key="id" style="width: 100%">
        <el-table-column prop="quoteText" label="帖子内容" min-width="260" show-overflow-tooltip />
        <el-table-column prop="platformName" label="来源平台" width="120" />
        <el-table-column prop="publishTime" label="发布时间" width="170">
          <template #default="{ row }">
            <span>{{ row.publishTime ? String(row.publishTime).replace('T', ' ').slice(0, 19) : '-' }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="sentimentLabel" label="情感标签" width="110" />
        <el-table-column prop="spamLabel" label="水军标签" width="110" />
        <el-table-column prop="reason" label="引用原因" min-width="160" show-overflow-tooltip />
      </el-table>
    </el-drawer>
  </div>
</template>

<style scoped>
.page {
  width: 100%;
  padding: 12px;
  box-sizing: border-box;
}

.block {
  width: 100%;
  max-width: 100%;
}

.filters :deep(.el-form-item) {
  margin-bottom: 10px;
}

.mt12 {
  margin-top: 12px;
}

.hint {
  margin-top: 6px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.drawer-actions {
  position: sticky;
  bottom: 0;
  padding: 12px 0 0;
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  background: var(--el-bg-color);
}

.chart {
  width: 100%;
  height: 320px;
  max-width: 100%;
}

.title-row .title {
  font-size: 16px;
  font-weight: 600;
  line-height: 1.4;
}

.title-row .meta {
  margin-top: 6px;
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.title-row .meta .sep {
  margin: 0 6px;
}

.title-row .summary {
  margin-top: 8px;
  color: var(--el-text-color-regular);
  font-size: 13px;
  line-height: 1.5;
}

.ul {
  margin: 0;
  padding-left: 18px;
}

@media (max-width: 920px) {
  .page {
    padding: 8px;
  }
  .chart {
    height: 260px;
  }
}
</style>
