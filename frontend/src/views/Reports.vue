<script setup>
import { ref } from 'vue'
import api from '../api/axios'

const summary = ref(null)
const error = ref('')
const loading = ref(false)

const fetchReport = async () => {
  loading.value = true
  error.value = ''
  try {
    const { data } = await api.get('/api/report')
    summary.value = data
  } catch (e) {
    error.value = e?.response?.data?.detail || '获取报告失败'
  } finally {
    loading.value = false
  }
}

fetchReport()
</script>

<template>
  <section class="page">
    <el-page-header content="报告摘要" />
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <div class="title-row">
      <h2>报告中心</h2>
      <el-button type="primary" :loading="loading" @click="fetchReport">{{ loading ? '加载中...' : '重新加载' }}</el-button>
    </div>
    <el-card class="card" shadow="hover">
      <pre>{{ summary }}</pre>
    </el-card>
  </section>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.title-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.card { border-radius: 10px; }
</style>
