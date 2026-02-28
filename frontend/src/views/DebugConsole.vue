<script setup>
import { ref } from 'vue'
import api from '../api/axios'

const sources = ref([])
const sourceDetail = ref(null)
const sourceId = ref('weibo')
const importResult = ref(null)
const reportSummary = ref(null)
const apiLog = ref([])
const loading = ref(false)
const error = ref('')

const pushLog = (label, payload) => {
  apiLog.value.unshift({
    at: new Date().toLocaleString(),
    label,
    payload,
  })
  apiLog.value = apiLog.value.slice(0, 20)
}

const wrap = async (label, fn) => {
  loading.value = true
  error.value = ''
  try {
    const data = await fn()
    pushLog(label, data)
    return data
  } catch (e) {
    const detail = e?.response?.data?.detail || e.message || '请求失败'
    error.value = `${label}: ${detail}`
    pushLog(`${label} (error)`, detail)
    return null
  } finally {
    loading.value = false
  }
}

const loadSources = async () => {
  const data = await wrap('GET /api/sources', async () => {
    const resp = await api.get('/api/sources')
    return resp.data
  })
  if (data) {
    sources.value = data
  }
}

const loadSourceDetail = async () => {
  const data = await wrap(`GET /api/sources/${sourceId.value}`, async () => {
    const resp = await api.get(`/api/sources/${sourceId.value}`)
    return resp.data
  })
  if (data) {
    sourceDetail.value = data
  }
}

const runImport = async () => {
  const data = await wrap('POST /api/sources/import_excel', async () => {
    const resp = await api.post('/api/sources/import_excel')
    return resp.data
  })
  if (data) {
    importResult.value = data
  }
}

const loadReportSummary = async () => {
  const data = await wrap('GET /api/report', async () => {
    const resp = await api.get('/api/report')
    return resp.data
  })
  if (data) {
    reportSummary.value = data
  }
}

loadSources()
loadReportSummary()
</script>

<template>
  <section class="page">
    <el-page-header content="接口联调控制台" />
    <div class="title-row">
      <h2>调试控制台</h2>
      <span v-if="loading">请求中...</span>
    </div>
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />

    <div class="grid">
      <el-card class="card" shadow="hover">
        <h3>来源列表</h3>
        <div class="actions">
          <el-button @click="loadSources">GET /api/sources</el-button>
        </div>
        <pre>{{ sources }}</pre>
      </el-card>

      <el-card class="card" shadow="hover">
        <h3>来源详情</h3>
        <div class="actions">
          <el-input v-model="sourceId" placeholder="来源 ID" style="max-width: 200px" />
          <el-button @click="loadSourceDetail">GET /api/sources/{id}</el-button>
        </div>
        <pre>{{ sourceDetail }}</pre>
      </el-card>

      <el-card class="card" shadow="hover">
        <h3>Excel 导入</h3>
        <el-button type="primary" @click="runImport">POST /api/sources/import_excel</el-button>
        <pre>{{ importResult }}</pre>
      </el-card>

      <el-card class="card" shadow="hover">
        <h3>报告摘要</h3>
        <el-button @click="loadReportSummary">GET /api/report</el-button>
        <pre>{{ reportSummary }}</pre>
      </el-card>

      <el-card class="card full" shadow="hover">
        <h3>接口日志</h3>
        <div v-for="(item, idx) in apiLog" :key="idx" class="log-item">
          <div>{{ item.at }} | {{ item.label }}</div>
          <pre>{{ item.payload }}</pre>
        </div>
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
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 12px;
}

.card { border-radius: 10px; }

.full {
  grid-column: 1 / -1;
}

.actions {
  display: flex;
  gap: 8px;
  margin-bottom: 8px;
}

.log-item {
  border-top: 1px solid #e5e7eb;
  padding-top: 8px;
  margin-top: 8px;
}

</style>
