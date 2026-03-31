<template>
  <PageSection title="Keyword Monitor">
    <template #extra>
      <el-input-number v-model="topN" :min="5" :max="50" size="small" controls-position="right" />
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
      <el-empty v-if="!hasFilters" description="Select at least 1 brand and 1 platform" />
      <el-empty v-else-if="!hasData" description="No data" />
      <KeywordStackedBarChart v-else :height="'300px'" :dates="dates" :series="series" />
    </template>
  </PageSection>
</template>

<script setup>
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import PageSection from '../common/PageSection.vue'
import KeywordStackedBarChart from '../charts/KeywordStackedBarChart.vue'

import { useDashboardStore } from '../../stores/dashboard'
import { resolveDateRange } from '../../utils/date'
import { fetchDashboardKeywordMonitorStacked } from '../../api/dashboard'

const dashboard = useDashboardStore()

const topN = ref(15)
const loading = ref(false)
const error = ref('')
const dates = ref([])
const series = ref([])

let ac = null
let timer = null

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
    error.value = 'Invalid date range (select a custom range, or use last 7/14/30 days).'
    loading.value = false
    return
  }

  if (ac) ac.abort()
  ac = new AbortController()

  loading.value = true
  error.value = ''
  try {
    const res = await fetchDashboardKeywordMonitorStacked(
      {
        projectId: pid,
        startDate: dr.startDate,
        endDate: dr.endDate,
        platformIds: dashboard.platformIds,
        brandIds: dashboard.brandIds,
        topN: topN.value,
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
  timer = setTimeout(() => load(), 80)
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
    topN.value,
  ],
  () => scheduleLoad(),
  { immediate: true }
)
</script>
