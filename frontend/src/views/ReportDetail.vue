<template>
  <el-space direction="vertical" :size="12" fill>
    <PageSection title="Report Detail">
      <template #extra>
        <el-button
          v-if="report"
          size="small"
          type="success"
          plain
          :loading="genLoading"
          :disabled="!canGenerate"
          @click="onGenerate"
        >
          Generate
        </el-button>
      </template>
      <el-alert
        v-if="error"
        type="error"
        :title="error"
        :closable="false"
        show-icon
        style="margin-bottom: 10px"
      />

      <el-skeleton v-if="loading" :rows="4" animated />

      <template v-else-if="report">
        <el-descriptions :column="2" border>
          <el-descriptions-item label="Title">{{ report.title || '-' }}</el-descriptions-item>
          <el-descriptions-item label="Type">
            <el-tag size="small" type="info">{{ report.report_type || '-' }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="Project">{{ projectName }}</el-descriptions-item>
          <el-descriptions-item label="Status">
            <el-tag :type="statusType(report.status)" size="small">{{ report.status || '-' }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="Data Range">{{ fmtRange(report.data_start_date, report.data_end_date) }}</el-descriptions-item>
          <el-descriptions-item label="Created At">{{ fmtTime(report.created_at) }}</el-descriptions-item>
          <el-descriptions-item label="Summary" :span="2">{{ report.summary || '-' }}</el-descriptions-item>
        </el-descriptions>
      </template>
    </PageSection>

    <PageSection title="Content (Markdown)">
      <el-empty v-if="!report" description="No report" />
      <template v-else>
        <SafeMarkdown :markdown="report.content_markdown || ''" />
      </template>
    </PageSection>

    <el-alert
      v-if="report && report.status && report.status !== 'success' && report.status !== 'done'"
      type="warning"
      :closable="false"
      show-icon
      :title="`Report status is ${report.status}. Aggregated panels are available after success.`"
    />

    <template v-if="report && (report.status === 'success' || report.status === 'done')">
      <PageSection title="Public Opinion Overview">
        <el-skeleton v-if="aggLoading" :rows="2" animated />
        <el-alert v-else-if="aggError" type="error" :title="aggError" :closable="false" show-icon />
        <el-empty v-else-if="!overviewItems.length" description="No data" />
        <el-table v-else :data="overviewRows" border>
          <el-table-column prop="brand" label="Brand" min-width="160" />
          <el-table-column prop="posts" label="Posts" width="120" />
          <el-table-column prop="pos" label="Positive %" width="120" />
          <el-table-column prop="neg" label="Negative %" width="120" />
        </el-table>
      </PageSection>

      <PageSection title="Sentiment Trend">
        <template #extra>
          <el-radio-group v-model="trendMode" size="small">
            <el-radio-button value="positive">Positive</el-radio-button>
            <el-radio-button value="negative">Negative</el-radio-button>
          </el-radio-group>
        </template>
        <el-skeleton v-if="aggLoading" :rows="2" animated />
        <el-alert v-else-if="aggError" type="error" :title="aggError" :closable="false" show-icon />
        <el-empty v-else-if="!trendDates.length" description="No data" />
        <SentimentTrendChart
          v-else
          :height="'320px'"
          :mode="trendMode"
          :dates="trendDates"
          :series="trendSeries"
          :brand-name-by-id="brandNameById"
        />
      </PageSection>

      <PageSection title="Keyword Analysis">
        <el-skeleton v-if="aggLoading" :rows="2" animated />
        <el-alert v-else-if="aggError" type="error" :title="aggError" :closable="false" show-icon />
        <el-empty v-else-if="!kwDates.length" description="No data" />
        <KeywordStackedBarChart v-else :height="'320px'" :dates="kwDates" :series="kwSeries" />
      </PageSection>

      <PageSection title="Feature Analysis">
        <el-skeleton v-if="aggLoading" :rows="2" animated />
        <el-alert v-else-if="aggError" type="error" :title="aggError" :closable="false" show-icon />
        <el-empty v-else-if="!featDates.length" description="No data" />
        <KeywordStackedBarChart
          v-else
          :height="'320px'"
          :dates="featDates"
          :series="featSeriesForChart"
        />
      </PageSection>

      <PageSection title="Competitor Compare">
        <el-empty v-if="!overviewItems.length" description="No data" />
        <el-table v-else :data="overviewRows" border>
          <el-table-column prop="brand" label="Brand" min-width="160" />
          <el-table-column prop="posts" label="Posts" width="120" />
          <el-table-column prop="pos" label="Positive %" width="120" />
          <el-table-column prop="neg" label="Negative %" width="120" />
        </el-table>
      </PageSection>
    </template>
  </el-space>
</template>

<script setup>
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import PageSection from '../components/common/PageSection.vue'
import SafeMarkdown from '../components/common/SafeMarkdown.vue'
import SentimentTrendChart from '../components/charts/SentimentTrendChart.vue'
import KeywordStackedBarChart from '../components/charts/KeywordStackedBarChart.vue'

import { fetchProjectConfig } from '../api/projectConfig'
import { fetchReportDetail, generateReport } from '../api/reports'
import {
  fetchDashboardFeatureMonitorStacked,
  fetchDashboardKeywordMonitorStacked,
  fetchDashboardOverviewByBrand,
  fetchDashboardSentimentTrendDailyByBrand,
} from '../api/dashboard'
import { useProjectsStore } from '../stores/projects'

const route = useRoute()

const loading = ref(false)
const error = ref('')
const report = ref(null)
const genLoading = ref(false)

const scopeLoading = ref(false)
const brandOptions = ref([])
const platformOptions = ref([])

const aggLoading = ref(false)
const aggError = ref('')
const overviewItems = ref([])
const trendDates = ref([])
const trendSeries = ref([])
const kwDates = ref([])
const kwSeries = ref([])
const featDates = ref([])
const featSeries = ref([]) // [{feature,data}]

const trendMode = ref('positive')

const canGenerate = computed(() => {
  const s = String(report.value?.status || '')
  if (!report.value) return false
  if (genLoading.value) return false
  return s !== 'running' && s !== 'success' && s !== 'done'
})

let ac = null
let aggAc = null

const projectsStore = useProjectsStore()
const projectNameById = computed(() => {
  const map = {}
  for (const p of projectsStore.projects || []) map[Number(p.id)] = p.name
  return map
})

const projectName = computed(() => {
  const pid = Number(report.value?.project_id)
  return projectNameById.value[pid] || (pid ? `#${pid}` : '-')
})

const brandNameById = computed(() => {
  const map = {}
  for (const b of brandOptions.value || []) map[Number(b.id)] = b.name
  return map
})

function fmtTime(t) {
  if (!t) return '-'
  return String(t).slice(0, 19).replace('T', ' ')
}
function fmtRange(a, b) {
  if (!a && !b) return '-'
  return `${a || '-'} ~ ${b || '-'}`
}
function statusType(s) {
  if (s === 'success' || s === 'done') return 'success'
  if (s === 'failed' || s === 'error') return 'danger'
  if (s === 'pending' || s === 'running') return 'warning'
  return 'info'
}

function ratio(n, d) {
  const a = Number(n || 0)
  const b = Number(d || 0)
  if (!b) return 0
  return a / b
}
function pct(x) {
  return `${(Number(x || 0) * 100).toFixed(1)}%`
}

const overviewRows = computed(() => {
  return (overviewItems.value || []).map((it) => {
    const bid = Number(it?.brand_id)
    const total = Number(it?.total_post_count || 0)
    const pos = Number(it?.positive_count || 0)
    const neg = Number(it?.negative_count || 0)
    return {
      brand: brandNameById.value[bid] || `#${bid || '-'}`,
      posts: total.toLocaleString(),
      pos: pct(ratio(pos, total)),
      neg: pct(ratio(neg, total)),
    }
  })
})

const featSeriesForChart = computed(() => {
  return (featSeries.value || []).map((s) => ({ keyword: s.feature, data: s.data }))
})

function parseCsvInts(raw) {
  if (raw == null || raw === '') return []
  return String(raw)
    .split(',')
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n) && n > 0)
}
function parseCsvStrs(raw) {
  if (raw == null || raw === '') return []
  return String(raw)
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s)
}

async function loadReport() {
  const rid = Number(route.params.id)
  if (!Number.isFinite(rid) || rid <= 0) {
    error.value = 'Invalid report id'
    return
  }
  if (ac) ac.abort()
  ac = new AbortController()
  loading.value = true
  error.value = ''
  try {
    const res = await fetchReportDetail(rid, { signal: ac.signal })
    report.value = res?.item || null
  } catch (e) {
    if (e?.name === 'AbortError') return
    error.value = e?.message || String(e)
    report.value = null
  } finally {
    loading.value = false
  }
}

async function onGenerate() {
  const rid = Number(route.params.id)
  if (!Number.isFinite(rid) || rid <= 0) {
    ElMessage.error('Invalid report id')
    return
  }
  genLoading.value = true
  try {
    await generateReport(rid)
    ElMessage.success(`Generated report #${rid}`)
    await loadReport()
    await loadAggregates()
  } catch (e) {
    ElMessage.error(e?.message || String(e))
    await loadReport()
  } finally {
    genLoading.value = false
  }
}

async function loadScope() {
  const pid = Number(report.value?.project_id)
  if (!pid) return
  scopeLoading.value = true
  try {
    const cfg = await fetchProjectConfig(pid)
    brandOptions.value = Array.isArray(cfg?.brands) ? cfg.brands : []
    platformOptions.value = Array.isArray(cfg?.platforms) ? cfg.platforms : []
  } finally {
    scopeLoading.value = false
  }
}

async function loadAggregates() {
  const r = report.value
  if (!r) return
  if (!(r.status === 'success' || r.status === 'done')) return

  const pid = Number(r.project_id)
  const startDate = r.data_start_date
  const endDate = r.data_end_date
  const cfg = r.config || {}
  const platformIds = parseCsvInts(cfg.platform_ids)
  const brandIds = parseCsvInts(cfg.brand_ids)

  if (aggAc) aggAc.abort()
  aggAc = new AbortController()
  aggLoading.value = true
  aggError.value = ''
  try {
    const [ov, tr, kw, feat] = await Promise.all([
      fetchDashboardOverviewByBrand({ projectId: pid, startDate, endDate, platformIds, brandIds }, { signal: aggAc.signal }),
      fetchDashboardSentimentTrendDailyByBrand({ projectId: pid, startDate, endDate, platformIds, brandIds, topN: 4 }, { signal: aggAc.signal }),
      fetchDashboardKeywordMonitorStacked({ projectId: pid, startDate, endDate, platformIds, brandIds, topN: 15 }, { signal: aggAc.signal }),
      fetchDashboardFeatureMonitorStacked({ projectId: pid, startDate, endDate, brandIds, topN: 15 }, { signal: aggAc.signal }),
    ])
    overviewItems.value = Array.isArray(ov?.items) ? ov.items : []
    trendDates.value = Array.isArray(tr?.dates) ? tr.dates : []
    trendSeries.value = Array.isArray(tr?.series) ? tr.series : []
    kwDates.value = Array.isArray(kw?.dates) ? kw.dates : []
    kwSeries.value = Array.isArray(kw?.series) ? kw.series : []
    featDates.value = Array.isArray(feat?.dates) ? feat.dates : []
    featSeries.value = Array.isArray(feat?.series) ? feat.series : []
  } catch (e) {
    if (e?.name === 'AbortError') return
    aggError.value = e?.message || String(e)
    overviewItems.value = []
    trendDates.value = []
    trendSeries.value = []
    kwDates.value = []
    kwSeries.value = []
    featDates.value = []
    featSeries.value = []
  } finally {
    aggLoading.value = false
  }
}

watch(
  () => route.params.id,
  async () => {
    await loadReport()
    await loadScope()
    await loadAggregates()
  },
  { immediate: true }
)

onBeforeUnmount(() => {
  if (ac) ac.abort()
  if (aggAc) aggAc.abort()
})
</script>
