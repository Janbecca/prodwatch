<template>
  <ECharts :option="option" :height="height" />
</template>

<script setup>
import { computed } from 'vue'
import ECharts from './ECharts.vue'

const props = defineProps({
  height: { type: String, default: '280px' },
  mode: { type: String, default: 'positive' }, // positive | negative
  dates: { type: Array, default: () => [] }, // ['YYYY-MM-DD',...]
  series: { type: Array, default: () => [] }, // [{brand_id, total_post_count:[], positive_count:[], negative_count:[]}]
  brandNameById: { type: Object, default: () => ({}) }, // { [id]: name }
})

function ratio(num, den) {
  const n = Number(num || 0)
  const d = Number(den || 0)
  if (!d) return 0
  return n / d
}

const option = computed(() => {
  const isPos = String(props.mode) !== 'negative'
  const key = isPos ? 'positive_count' : 'negative_count'
  const title = isPos ? 'Positive share' : 'Negative share'

  const lines = (props.series || []).map((s) => {
    const bid = Number(s?.brand_id)
    const name = props.brandNameById?.[bid] || `Brand ${bid}`
    const totals = Array.isArray(s?.total_post_count) ? s.total_post_count : []
    const counts = Array.isArray(s?.[key]) ? s[key] : []
    const data = (props.dates || []).map((_, i) => ratio(counts[i], totals[i]))
    return {
      name,
      type: 'line',
      smooth: true,
      symbol: 'circle',
      symbolSize: 6,
      showSymbol: false,
      data,
    }
  })

  return {
    title: { text: title, left: 'left', textStyle: { fontSize: 12, fontWeight: 600 } },
    tooltip: {
      trigger: 'axis',
      valueFormatter: (v) => `${(Number(v || 0) * 100).toFixed(1)}%`,
    },
    legend: { top: 0, left: 'center' },
    grid: { left: 40, right: 20, top: 36, bottom: 32 },
    xAxis: { type: 'category', data: props.dates || [], axisLabel: { rotate: 0 } },
    yAxis: {
      type: 'value',
      min: 0,
      max: 1,
      axisLabel: { formatter: (v) => `${Math.round(Number(v) * 100)}%` },
      splitLine: { lineStyle: { type: 'dashed' } },
    },
    series: lines,
  }
})
</script>

