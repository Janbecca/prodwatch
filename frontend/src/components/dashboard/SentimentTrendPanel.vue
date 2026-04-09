<!-- 作用：前端组件：仪表盘模块组件（SentimentTrendPanel）。 -->

<template>
  <PageSection title="情感趋势">
    <template #extra>
      <el-radio-group v-model="mode" size="small">
        <el-radio-button value="positive">正向</el-radio-button>
        <el-radio-button value="negative">负向</el-radio-button>
      </el-radio-group>
    </template>

    <el-alert
      v-if="error"
      type="error"
      :title="error"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-skeleton v-if="loading" :rows="3" animated />

    <template v-else>
      <el-empty v-if="!hasFilters" description="至少选择 1 个品牌和 1 个平台" />
      <el-empty v-else-if="!hasData" description="暂无数据" />
      <SentimentTrendChart
        v-else
        :height="'300px'"
        :mode="mode"
        :dates="dates"
        :series="series"
        :brand-name-by-id="brandNameById"
      />
    </template>
  </PageSection>
</template>

<script setup>
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import PageSection from '../common/PageSection.vue'
import SentimentTrendChart from '../charts/SentimentTrendChart.vue'

import { useDashboardStore } from '../../stores/dashboard'
import { resolveDateRange } from '../../utils/date'
import { fetchDashboardSentimentTrendDailyByBrand } from '../../api/dashboard'

const dashboard = useDashboardStore()

const mode = ref('positive')
const loading = ref(false)
const error = ref('')

const dates = ref([])
const series = ref([])

let ac = null
let timer = null

const brandNameById = computed(() => {
  const map = {}
  for (const b of dashboard.brandOptions || []) {
    const id = Number(b?.id)
    if (!Number.isFinite(id)) continue
    map[id] = b?.name ?? `品牌 ${id}`
  }
  return map
})

const hasData = computed(() => {
  return Array.isArray(dates.value) && dates.value.length > 0 && Array.isArray(series.value) && series.value.length > 0
})

const hasFilters = computed(() => {
  return (dashboard.brandIds || []).length > 0 && (dashboard.platformIds || []).length > 0
})

async function load() {
  const pid = dashboard.enabledProjectId
  if (!pid) {
    dates.value = []
    series.value = []
    error.value = ''
    loading.value = false
    return
  }

  if (!hasFilters.value) {
    if (ac) ac.abort()
    dates.value = []
    series.value = []
    error.value = ''
    loading.value = false
    return
  }

  const dr = resolveDateRange(dashboard.timeQuery)
  if (!dr) {
    dates.value = []
    series.value = []
    error.value = '日期范围无效（请选择自定义范围，或使用最近 7/14/30 天）。'
    loading.value = false
    return
  }

  if (ac) ac.abort()
  ac = new AbortController()

  loading.value = true
  error.value = ''
  try {
    const res = await fetchDashboardSentimentTrendDailyByBrand(
      {
        projectId: pid,
        startDate: dr.startDate,
        endDate: dr.endDate,
        platformIds: dashboard.platformIds,
        brandIds: dashboard.brandIds,
        topN: 4,
      },
      { signal: ac.signal }
    )
    dates.value = Array.isArray(res?.dates) ? res.dates : []
    series.value = Array.isArray(res?.series) ? res.series : []
  } catch (e) {
    if (e?.name === 'AbortError') return
    error.value = e?.message || String(e)
    dates.value = []
    series.value = []
  } finally {
    loading.value = false
  }
}

function scheduleLoad() {
  if (timer) clearTimeout(timer)
  timer = setTimeout(() => load(), 60)
}

onBeforeUnmount(() => {
  if (timer) clearTimeout(timer)
  if (ac) ac.abort()
})

watch(
  () => [
    dashboard.enabledProjectId,
    dashboard.reloadSeq,
    (dashboard.brandIds || []).join(','),
    (dashboard.platformIds || []).join(','),
    JSON.stringify(dashboard.timeQuery || {}),
  ],
  () => scheduleLoad(),
  { immediate: true }
)
</script>
