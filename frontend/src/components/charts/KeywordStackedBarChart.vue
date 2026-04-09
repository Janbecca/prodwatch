<!-- 作用：前端组件：图表模块组件（KeywordStackedBarChart）。 -->

<template>
  <ECharts :option="option" :height="height" />
</template>

<script setup>
import { computed } from 'vue'
import ECharts from './ECharts.vue'

const props = defineProps({
  height: { type: String, default: '280px' },
  dates: { type: Array, default: () => [] }, // ['YYYY-MM-DD',...]
  series: { type: Array, default: () => [] }, // [{keyword, data:[...]}]
})

const option = computed(() => {
  const items = Array.isArray(props.series) ? props.series : []
  const legendData = items.map((it) => String(it?.keyword ?? ''))
  return {
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { top: 0, left: 'center', data: legendData },
    grid: { left: 40, right: 20, top: 36, bottom: 32 },
    xAxis: { type: 'category', data: props.dates || [] },
    yAxis: { type: 'value', splitLine: { lineStyle: { type: 'dashed' } } },
    series: items.map((it) => ({
      name: String(it?.keyword ?? ''),
      type: 'bar',
      stack: 'kw',
      emphasis: { focus: 'series' },
      data: Array.isArray(it?.data) ? it.data : [],
    })),
  }
})
</script>

