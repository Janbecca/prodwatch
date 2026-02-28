<script setup>
import { ref } from 'vue'
import api from '../api/axios'

const kpis = ref(null)
const trends = ref(null)
const ranking = ref([])
const alerts = ref([])
const error = ref('')
const loading = ref(false)

const loadAll = async () => {
  loading.value = true
  error.value = ''
  try {
    const [{ data: k }, { data: t }, { data: r }, { data: a }] = await Promise.all([
      api.get('/api/dashboard/kpis'),
      api.get('/api/dashboard/trends'),
      api.get('/api/dashboard/ranking'),
      api.get('/api/dashboard/alerts'),
    ])
    kpis.value = k
    trends.value = t
    ranking.value = r
    alerts.value = a
  } catch (e) {
    error.value = e?.response?.data?.detail || '加载仪表盘数据失败'
  } finally {
    loading.value = false
  }
}

loadAll()
</script>

<template>
  <section class="page">
    <el-page-header content="仪表盘接口概览" />
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <div class="title-row">
      <h2>仪表盘</h2>
      <el-button type="primary" :loading="loading" @click="loadAll">{{ loading ? '加载中...' : '重新加载' }}</el-button>
    </div>

    <div class="grid">
      <el-card class="card" shadow="hover">
        <h3>KPI 指标</h3>
        <pre>{{ kpis }}</pre>
      </el-card>
      <el-card class="card" shadow="hover">
        <h3>趋势数据</h3>
        <pre>{{ trends }}</pre>
      </el-card>
      <el-card class="card" shadow="hover">
        <h3>热度排行</h3>
        <ul>
          <li v-for="r in ranking" :key="r.product">{{ r.product }} / {{ r.score }}</li>
        </ul>
      </el-card>
      <el-card class="card" shadow="hover">
        <h3>预警列表</h3>
        <ul>
          <li v-for="a in alerts" :key="`${a.product}-${a.level}`">{{ a.level }} - {{ a.product }} - {{ a.reason }}</li>
        </ul>
      </el-card>
    </div>
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

.grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 12px;
}

.card { border-radius: 10px; }
</style>
