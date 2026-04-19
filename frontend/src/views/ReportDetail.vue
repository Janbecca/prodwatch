<!-- 作用：前端页面：报告详情（结构化看板 + LLM 摘要） -->

<template>
  <el-space direction="vertical" :size="12" fill>
    <PageSection title="报告详情">
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
          生成
        </el-button>
      </template>

      <el-alert v-if="error" type="error" :title="error" :closable="false" show-icon style="margin-bottom: 10px" />
      <el-skeleton v-if="loading" :rows="4" animated />

      <template v-else-if="report">
        <el-descriptions :column="2" border>
          <el-descriptions-item label="标题">{{ report.title || '-' }}</el-descriptions-item>
          <el-descriptions-item label="类型">
            <el-tag size="small" type="info">{{ typeText(report.report_type || '-') }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="项目">{{ projectName }}</el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="statusType(report.status)" size="small">{{ statusText(report.status) }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="数据范围">{{ fmtRange(report.data_start_date, report.data_end_date) }}</el-descriptions-item>
          <el-descriptions-item label="创建时间">{{ fmtTime(report.created_at) }}</el-descriptions-item>
          <el-descriptions-item label="摘要" :span="2">{{ report.summary || '-' }}</el-descriptions-item>
        </el-descriptions>
      </template>
    </PageSection>

    <el-alert
      v-if="report && report.status && report.status !== 'success' && report.status !== 'done'"
      type="warning"
      :closable="false"
      show-icon
      :title="`当前状态为 ${statusText(report.status)}，报告看板将在生成成功后可用。`"
    />
    <el-alert
      v-if="report && report.error_message"
      type="error"
      :closable="false"
      show-icon
      :title="`生成失败：${String(report.error_message).slice(0, 300)}`"
      style="margin-top: 10px"
    />

    <template v-if="report && (report.status === 'success' || report.status === 'done')">
      <!-- 1) 执行摘要：仅 LLM 文本摘要 -->
      <PageSection title="执行摘要">
        <el-skeleton v-if="aggLoading" :rows="3" animated />
        <el-alert v-else-if="aggError" type="error" :title="aggError" :closable="false" show-icon />
        <el-space v-else direction="vertical" :size="10" fill>
          <el-alert v-if="ai.summary" type="info" :closable="false" show-icon :title="ai.summary" />
          <el-card shadow="never">
            <template #header><el-text tag="b">文字摘要</el-text></template>
            <SafeMarkdown :markdown="ai.executive_summary_md || ''" />
          </el-card>
        </el-space>
      </PageSection>

      <!-- 2) 舆情趋势：情感趋势图 + LLM 解读 -->
      <PageSection v-if="showTrend" title="舆情趋势">
        <template #extra>
          <el-radio-group v-model="trendMode" size="small">
            <el-radio-button value="positive">正向</el-radio-button>
            <el-radio-button value="negative">负向</el-radio-button>
          </el-radio-group>
        </template>

        <el-skeleton v-if="aggLoading" :rows="2" animated />
        <el-alert v-else-if="aggError" type="error" :title="aggError" :closable="false" show-icon />
        <el-empty v-else-if="!trendDates.length && !overviewItems.length" description="暂无数据" />
        <template v-else>
          <el-row :gutter="12">
            <el-col :xs="24" :md="16">
              <SentimentTrendChart
                v-if="trendDates.length"
                :height="'320px'"
                :mode="trendMode"
                :dates="trendDates"
                :series="trendSeries"
                :brand-name-by-id="brandNameById"
              />
              <el-empty v-else description="暂无趋势数据" />
            </el-col>
            <el-col :xs="24" :md="8">
              <el-card shadow="never" class="fill-card">
                <template #header><el-text tag="b">趋势解读</el-text></template>
                <SafeMarkdown :markdown="ai.trend_summary_md || ''" />
              </el-card>
            </el-col>
          </el-row>

          <el-row v-if="overviewItems.length" :gutter="12" style="margin-top: 12px">
            <el-col :xs="24" :md="12">
              <el-card shadow="never">
                <template #header><el-text tag="b">声量（按品牌）</el-text></template>
                <ECharts :option="overviewBarOption" height="260px" />
              </el-card>
            </el-col>
            <el-col :xs="24" :md="12">
              <el-card shadow="never">
                <template #header><el-text tag="b">情感分布（汇总）</el-text></template>
                <ECharts :option="sentimentPieOption" height="260px" />
              </el-card>
            </el-col>
          </el-row>
        </template>
      </PageSection>

      <!-- 3) 风险点看板 -->
      <PageSection v-if="showFeature" title="风险点">
        <el-skeleton v-if="aggLoading" :rows="2" animated />
        <el-alert v-else-if="aggError" type="error" :title="aggError" :closable="false" show-icon />
        <el-empty v-else-if="!topRiskRows.length" description="暂无数据" />
        <el-row v-else :gutter="12">
          <el-col :xs="24" :md="16">
            <el-card shadow="never">
              <template #header><el-text tag="b">风险看板（Top 8）</el-text></template>
              <ECharts :option="riskBarOption" height="320px" />
            </el-card>
          </el-col>
          <el-col :xs="24" :md="8">
            <el-card shadow="never" class="fill-card">
              <template #header><el-text tag="b">风险摘要</el-text></template>
              <SafeMarkdown :markdown="ai.risk_points_md || ''" />
            </el-card>
          </el-col>
        </el-row>
      </PageSection>

      <!-- 4) 关键用户反馈看板 -->
      <PageSection v-if="showFeedback" title="关键用户反馈">
        <template #extra>
          <el-button v-if="report" size="small" @click="openEvidence">查看全部证据</el-button>
        </template>

        <el-skeleton v-if="aggLoading" :rows="2" animated />
        <el-alert v-else-if="aggError" type="error" :title="aggError" :closable="false" show-icon />
        <el-empty v-else-if="!keyFeedbackRows.length" description="暂无证据" />
        <el-row v-else :gutter="12">
          <el-col :xs="24" :md="16">
            <el-table :data="keyFeedbackRows" border style="width: 100%">
              <el-table-column prop="content" label="内容摘要" min-width="280" show-overflow-tooltip />
              <el-table-column prop="platform" label="平台" width="120" />
              <el-table-column prop="publishTime" label="发布时间" width="160" />
              <el-table-column prop="sentiment" label="情感" width="110">
                <template #default="{ row }">
                  <el-tag :type="sentimentType(row.sentiment)" size="small">{{ sentimentText(row.sentiment) }}</el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="reason" label="原因" min-width="180" show-overflow-tooltip />
            </el-table>
          </el-col>
          <el-col :xs="24" :md="8">
            <el-card shadow="never" class="fill-card">
              <template #header><el-text tag="b">反馈摘要</el-text></template>
              <SafeMarkdown :markdown="ai.key_user_feedback_md || ''" />
            </el-card>
          </el-col>
        </el-row>
      </PageSection>

      <!-- 5) 竞品对比看板 -->
      <PageSection v-if="showCompetitor" title="竞品对比">
        <el-skeleton v-if="aggLoading" :rows="2" animated />
        <el-alert v-else-if="aggError" type="error" :title="aggError" :closable="false" show-icon />
        <el-empty v-else-if="!overviewRows.length" description="暂无数据" />
        <el-row v-else :gutter="12">
          <el-col :xs="24" :md="16">
            <el-table :data="overviewRows" border style="width: 100%">
              <el-table-column prop="brand" label="品牌" min-width="160" />
              <el-table-column prop="posts" label="帖子数" width="110" />
              <el-table-column prop="pos" label="正向%" width="110" />
              <el-table-column prop="neg" label="负向%" width="110" />
            </el-table>
          </el-col>
          <el-col :xs="24" :md="8">
            <el-card shadow="never" class="fill-card">
              <template #header><el-text tag="b">对比摘要</el-text></template>
              <SafeMarkdown :markdown="ai.competitor_compare_md || ''" />
            </el-card>
          </el-col>
        </el-row>
      </PageSection>

      <!-- 6) 热点话题看板 -->
      <PageSection v-if="showTopics" title="热点话题">
        <el-skeleton v-if="aggLoading" :rows="2" animated />
        <el-alert v-else-if="aggError" type="error" :title="aggError" :closable="false" show-icon />
        <el-empty v-else-if="!topicDates.length" description="暂无数据" />
        <el-row v-else :gutter="12">
          <el-col :xs="24" :md="16">
            <KeywordStackedBarChart :height="'320px'" :dates="topicDates" :series="topicSeries" />
          </el-col>
          <el-col :xs="24" :md="8">
            <el-card shadow="never" class="fill-card">
              <template #header><el-text tag="b">热点摘要</el-text></template>
              <SafeMarkdown :markdown="ai.hot_topics_md || ''" />
            </el-card>
          </el-col>
        </el-row>
      </PageSection>

      <!-- 7) 策略建议 -->
      <PageSection v-if="showStrategy" title="策略建议">
        <SafeMarkdown :markdown="ai.strategy_suggestions_md || ''" />
      </PageSection>

      <!-- 原始内容（可选） -->
      <el-collapse>
        <el-collapse-item title="原始内容（Markdown，调试用）" name="raw">
          <SafeMarkdown :markdown="rawMarkdown || ''" />
        </el-collapse-item>
      </el-collapse>
    </template>

    <ReportEvidenceDialog />
  </el-space>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'

import PageSection from '../components/common/PageSection.vue'
import SafeMarkdown from '../components/common/SafeMarkdown.vue'
import ECharts from '../components/charts/ECharts.vue'
import SentimentTrendChart from '../components/charts/SentimentTrendChart.vue'
import KeywordStackedBarChart from '../components/charts/KeywordStackedBarChart.vue'
import ReportEvidenceDialog from '../components/reports/ReportEvidenceDialog.vue'
import { useReportsStore } from '../stores/reports'
import { useProjectsStore } from '../stores/projects'

import { fetchProjectConfig } from '../api/projectConfig'
import { fetchReportBoards, fetchReportDetail, fetchReportEvidenceList, generateReport } from '../api/reports'

const route = useRoute()
const reportsStore = useReportsStore()
const projectsStore = useProjectsStore()

const loading = ref(false)
const error = ref('')
const report = ref(null)
const genLoading = ref(false)

const aggLoading = ref(false)
const aggError = ref('')
const overviewItems = ref([])
const trendDates = ref([])
const trendSeries = ref([])
const topicDates = ref([])
const topicSeries = ref([])
const featSeries = ref([]) // risk keywords -> [{feature,data:[count]}]
const keyFeedbackItems = ref([]) // evidence-derived top feedback posts

const evidenceLoading = ref(false)
const evidenceError = ref('')
const evidenceItems = ref([])

const trendMode = ref('positive')

let ac = null
let aggAc = null
let evidenceAc = null

const cfg = computed(() => report.value?.config || {})
function isOn(v) {
  return v === true || Number(v || 0) === 1
}

const showTrend = computed(() => isOn(cfg.value?.include_sentiment) && isOn(cfg.value?.include_trend))
const showTopics = computed(() => isOn(cfg.value?.include_topics))
const showFeature = computed(() => isOn(cfg.value?.include_feature_analysis))
const showCompetitor = computed(() => isOn(cfg.value?.include_sentiment) && isOn(cfg.value?.include_competitor_compare))
const showStrategy = computed(() => isOn(cfg.value?.include_strategy))
const showFeedback = computed(() => isOn(cfg.value?.include_spam))

const canGenerate = computed(() => {
  const s = String(report.value?.status || '')
  if (!report.value) return false
  if (genLoading.value) return false
  return s !== 'running' && s !== 'success' && s !== 'done'
})

const projectNameById = computed(() => {
  const map = {}
  for (const p of projectsStore.projects || []) map[Number(p.id)] = p.name
  return map
})

const projectName = computed(() => {
  const pid = Number(report.value?.project_id)
  return projectNameById.value[pid] || (pid ? `#${pid}` : '-')
})

// brand/platform options are from project-config endpoint
const brandOptions = ref([])
const platformOptions = ref([])
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
function statusText(v) {
  if (v === 'pending') return '待处理'
  if (v === 'running') return '生成中'
  if (v === 'success' || v === 'done') return '成功'
  if (v === 'failed') return '失败'
  if (v === 'error') return '错误'
  return v || '-'
}
function typeText(v) {
  if (v === 'daily') return '日报'
  if (v === 'weekly') return '周报'
  if (v === 'monthly') return '月报'
  if (v === 'special') return '专题'
  return v || '-'
}

function sentimentType(s) {
  if (s === 'positive') return 'success'
  if (s === 'negative') return 'danger'
  if (s === 'neutral') return 'info'
  return 'info'
}
function sentimentText(s) {
  if (s === 'positive') return '正向'
  if (s === 'neutral') return '中性'
  if (s === 'negative') return '负向'
  return s || '-'
}

function trimText(s) {
  return String(s || '').replace(/\s+/g, ' ').trim()
}

function parseIntList(raw) {
  if (raw == null) return null
  const s = String(raw).trim()
  if (!s) return []
  // Try JSON array first (report_config stores JSON text).
  try {
    const arr = JSON.parse(s)
    if (Array.isArray(arr)) {
      const out = []
      for (const x of arr) {
        const n = Number(x)
        if (Number.isFinite(n)) out.push(n)
      }
      return out
    }
  } catch {
    // ignore
  }
  // Fallback CSV
  const out = []
  for (const part of s.split(',')) {
    const n = Number(String(part || '').trim())
    if (Number.isFinite(n)) out.push(n)
  }
  return out
}

function parseAiBlocks(md) {
  const src = String(md || '')
  const m = src.match(/<!--PRODWATCH_AI_BLOCKS\s*[\r\n]+([\s\S]*?)[\r\n]+-->/)
  if (!m) return {}
  try {
    return JSON.parse(m[1] || '{}') || {}
  } catch {
    return {}
  }
}

const ai = computed(() => {
  const blocks = parseAiBlocks(report.value?.content_markdown || '')
  return {
    summary: String(blocks?.summary || '').trim(),
    executive_summary_md: String(blocks?.executive_summary_md || '').trim(),
    trend_summary_md: String(blocks?.trend_summary_md || '').trim(),
    risk_points_md: String(blocks?.risk_points_md || '').trim(),
    key_user_feedback_md: String(blocks?.key_user_feedback_md || '').trim(),
    competitor_compare_md: String(blocks?.competitor_compare_md || '').trim(),
    hot_topics_md: String(blocks?.hot_topics_md || '').trim(),
    strategy_suggestions_md: String(blocks?.strategy_suggestions_md || '').trim(),
  }
})

const rawMarkdown = computed(() => {
  const src = String(report.value?.content_markdown || '')
  return src.replace(/<!--PRODWATCH_AI_BLOCKS[\s\S]*?-->/g, '').trim()
})

const overviewRows = computed(() => {
  return (overviewItems.value || []).map((it) => {
    const total = Number(it?.total_post_count || 0)
    const pos = Number(it?.positive_count || 0)
    const neg = Number(it?.negative_count || 0)
    return {
      brand: brandNameById.value?.[Number(it?.brand_id)] || (it?.brand_id ? `品牌 ${it.brand_id}` : '-'),
      posts: total,
      pos: total ? `${((pos / total) * 100).toFixed(1)}%` : '0.0%',
      neg: total ? `${((neg / total) * 100).toFixed(1)}%` : '0.0%',
    }
  })
})

const overviewBarOption = computed(() => {
  const rows = (overviewItems.value || []).slice().sort((a, b) => Number(b?.total_post_count || 0) - Number(a?.total_post_count || 0))
  const names = rows.map((it) => brandNameById.value?.[Number(it?.brand_id)] || `品牌 ${it?.brand_id}`)
  const data = rows.map((it) => Number(it?.total_post_count || 0))
  return {
    grid: { left: 46, right: 12, top: 10, bottom: 30 },
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'category', data: names, axisLabel: { rotate: names.length > 6 ? 25 : 0 } },
    yAxis: { type: 'value', splitLine: { lineStyle: { type: 'dashed' } } },
    series: [{ type: 'bar', data, itemStyle: { color: '#409EFF' } }],
  }
})

const sentimentPieOption = computed(() => {
  let pos = 0
  let neu = 0
  let neg = 0
  for (const it of overviewItems.value || []) {
    pos += Number(it?.positive_count || 0)
    neu += Number(it?.neutral_count || 0)
    neg += Number(it?.negative_count || 0)
  }
  const total = pos + neu + neg
  return {
    tooltip: { trigger: 'item' },
    legend: { bottom: 0 },
    series: [
      {
        type: 'pie',
        radius: ['45%', '72%'],
        avoidLabelOverlap: true,
        label: { formatter: total ? '{b}: {d}%' : '{b}' },
        data: [
          { name: '正向', value: pos, itemStyle: { color: '#67C23A' } },
          { name: '中性', value: neu, itemStyle: { color: '#909399' } },
          { name: '负向', value: neg, itemStyle: { color: '#F56C6C' } },
        ],
      },
    ],
  }
})

function seriesTotals(series, keyName) {
  const rows = []
  for (const s of series || []) {
    const name = String(s?.[keyName] || s?.feature || s?.keyword || '')
    const arr = Array.isArray(s?.data) ? s.data : []
    const total = arr.reduce((acc, v) => acc + Number(v || 0), 0)
    if (!name) continue
    rows.push({ name, total })
  }
  rows.sort((a, b) => b.total - a.total)
  return rows
}

const topRiskRows = computed(() => seriesTotals(featSeries.value, 'feature').slice(0, 8))
const riskBarOption = computed(() => {
  const rows = topRiskRows.value
  const names = rows.map((r) => r.name)
  const data = rows.map((r) => r.total)
  return {
    grid: { left: 46, right: 12, top: 10, bottom: 30 },
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'category', data: names, axisLabel: { rotate: names.length > 6 ? 25 : 0 } },
    yAxis: { type: 'value', splitLine: { lineStyle: { type: 'dashed' } } },
    series: [{ type: 'bar', data, itemStyle: { color: '#E6A23C' } }],
  }
})

const keyFeedbackRows = computed(() => {
  const rows = []
  for (const it of keyFeedbackItems.value || []) {
    rows.push({
      content: trimText(it?.content_excerpt || it?.content || it?.title || '').slice(0, 180) || '-',
      platform: it?.platform_name || (it?.platform_id ? `平台 ${it.platform_id}` : '-'),
      publishTime: fmtTime(it?.publish_time),
      sentiment: it?.sentiment,
      reason: trimText(it?.reason || it?.quote_reason || '-'),
    })
  }
  return rows
    .map((r) => ({ ...r, platform: String(r?.platform || '').replace('骞冲彴', '平台') || '-' }))
    .slice(0, 10)
})

async function loadReport(reportId) {
  if (ac) ac.abort()
  ac = new AbortController()

  loading.value = true
  error.value = ''
  try {
    const res = await fetchReportDetail(reportId, { signal: ac.signal })
    report.value = res?.item || null
  } catch (e) {
    if (e?.name === 'AbortError') return
    error.value = e?.message || String(e)
    report.value = null
  } finally {
    loading.value = false
  }
}

async function loadScopeOptions() {
  const pid = Number(report.value?.project_id)
  if (!pid) return
  try {
    const res = await fetchProjectConfig(pid)
    brandOptions.value = Array.isArray(res?.brands) ? res.brands : []
    platformOptions.value = Array.isArray(res?.platforms) ? res.platforms : []
  } catch {
    brandOptions.value = []
    platformOptions.value = []
  }
}

async function loadAggregates() {
  if (!report.value) return
  if (aggAc) aggAc.abort()
  aggAc = new AbortController()

  aggLoading.value = true
  aggError.value = ''
  try {
    const rid = Number(report.value?.id)
    const start = String(report.value?.data_start_date || '')
    const end = String(report.value?.data_end_date || '')
    if (!rid || !start || !end) throw new Error('报告数据范围缺失')

    const b = await fetchReportBoards(rid, {}, { signal: aggAc.signal })

    overviewItems.value = Array.isArray(b?.overview_by_brand?.items) ? b.overview_by_brand.items : []

    const tr = b?.sentiment_trend_daily_by_brand || {}
    trendDates.value = showTrend.value && Array.isArray(tr?.dates) ? tr.dates : []
    trendSeries.value = showTrend.value && Array.isArray(tr?.series) ? tr.series : []

    const topics = b?.topic_monitor_stacked || {}
    topicDates.value = showTopics.value && Array.isArray(topics?.dates) ? topics.dates : []
    topicSeries.value = showTopics.value && Array.isArray(topics?.series) ? topics.series : []

    const risks = Array.isArray(b?.risk_keywords) ? b.risk_keywords : []
    featSeries.value = showFeature.value
      ? risks.map((it) => ({ feature: it?.keyword, data: [Number(it?.post_count || 0)] })).filter((x) => x.feature)
      : []

    keyFeedbackItems.value = showFeedback.value && Array.isArray(b?.key_user_feedback?.items) ? b.key_user_feedback.items : []
  } catch (e) {
    if (e?.name === 'AbortError') return
    aggError.value = e?.message || String(e)
    overviewItems.value = []
    trendDates.value = []
    trendSeries.value = []
    topicDates.value = []
    topicSeries.value = []
    featSeries.value = []
    keyFeedbackItems.value = []
  } finally {
    aggLoading.value = false
  }
}

async function loadEvidence() {
  if (!report.value) return
  if (evidenceAc) evidenceAc.abort()
  evidenceAc = new AbortController()
  evidenceLoading.value = true
  evidenceError.value = ''
  try {
    const rid = Number(report.value?.id)
    const res = await fetchReportEvidenceList(rid, { page: 1, pageSize: 100 }, { signal: evidenceAc.signal })
    evidenceItems.value = Array.isArray(res?.items) ? res.items : []
  } catch (e) {
    if (e?.name === 'AbortError') return
    evidenceError.value = e?.message || String(e)
    evidenceItems.value = []
  } finally {
    evidenceLoading.value = false
  }
}

async function onGenerate() {
  if (!report.value) return
  genLoading.value = true
  try {
    await generateReport(Number(report.value.id))
    ElMessage.success('已触发生成')
    await loadReport(Number(report.value.id))
    if (report.value?.status === 'success' || report.value?.status === 'done') {
      await Promise.all([loadScopeOptions(), loadAggregates(), loadEvidence()])
    }
  } catch (e) {
    ElMessage.error(e?.message || String(e))
  } finally {
    genLoading.value = false
  }
}

function openEvidence() {
  if (!report.value) return
  reportsStore.onEvidence({ id: Number(report.value.id) })
}

watch(
  () => route.params?.id,
  async (id) => {
    const rid = Number(id)
    if (!Number.isFinite(rid) || rid <= 0) return
    await loadReport(rid)
    if (!report.value) return
    await loadScopeOptions()
    if (report.value?.status === 'success' || report.value?.status === 'done') {
      await Promise.all([loadAggregates(), loadEvidence()])
    }
  },
  { immediate: true }
)

onMounted(() => {
  projectsStore.fetchProjects()
})

onBeforeUnmount(() => {
  ac?.abort()
  aggAc?.abort()
  evidenceAc?.abort()
})
</script>

<style scoped>
.fill-card {
  height: 100%;
}
</style>
