<script setup>
import { ref, onMounted } from 'vue'
import api from '../api/axios'

const report = ref(null)

onMounted(async () => {
  const { data } = await api.get('/api/report')
  report.value = data
})
</script>

<template>
  <div style="padding:16px;">
    <h2>报告生成与导出</h2>
    <pre v-if="report">{{ report }}</pre>
  </div>
</template>
