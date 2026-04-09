<!-- 作用：前端组件：图表模块组件（ECharts）。 -->

<template>
  <div ref="el" class="ec" />
</template>

<script setup>
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as echarts from 'echarts'

const props = defineProps({
  option: { type: Object, required: true },
  height: { type: String, default: '280px' },
})

const el = ref(null)
let chart = null
let ro = null

function ensureChart() {
  if (chart || !el.value) return
  chart = echarts.init(el.value)
  chart.setOption(props.option || {}, { notMerge: true })
}

function resize() {
  if (!chart) return
  chart.resize()
}

onMounted(() => {
  ensureChart()
  if (typeof ResizeObserver !== 'undefined' && el.value) {
    ro = new ResizeObserver(() => resize())
    ro.observe(el.value)
  } else {
    window.addEventListener('resize', resize)
  }
})

onBeforeUnmount(() => {
  if (ro) {
    ro.disconnect()
    ro = null
  } else {
    window.removeEventListener('resize', resize)
  }
  if (chart) {
    chart.dispose()
    chart = null
  }
})

watch(
  () => props.option,
  (opt) => {
    ensureChart()
    if (!chart) return
    chart.setOption(opt || {}, { notMerge: true })
  },
  { deep: true }
)
</script>

<style scoped>
.ec {
  height: v-bind(height);
  width: 100%;
}
</style>

