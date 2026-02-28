<script setup>
import { ref } from 'vue'
import api from '../api/axios'

const datasources = ref([])
const projects = ref([])
const error = ref('')
const saving = ref(false)
const saveResult = ref(null)

const loadDatasources = async () => {
  error.value = ''
  try {
    const { data } = await api.get('/api/settings/datasources')
    datasources.value = data
  } catch (e) {
    error.value = e?.response?.data?.detail || '加载数据源失败'
  }
}

const saveDatasources = async () => {
  error.value = ''
  saving.value = true
  try {
    const { data } = await api.post('/api/settings/datasources', datasources.value)
    saveResult.value = data
  } catch (e) {
    error.value = e?.response?.data?.detail || '保存数据源失败'
  } finally {
    saving.value = false
  }
}

const loadProjects = async () => {
  error.value = ''
  try {
    const { data } = await api.get('/api/settings/users')
    projects.value = data
  } catch (e) {
    error.value = e?.response?.data?.detail || '加载项目列表失败'
  }
}

loadDatasources()
loadProjects()
</script>

<template>
  <section class="page">
    <el-page-header content="系统设置调试" />
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />

    <el-card class="card" shadow="hover">
      <div class="row">
        <h3>数据源配置</h3>
        <div class="actions">
          <el-button @click="loadDatasources">重新加载</el-button>
          <el-button type="primary" :loading="saving" @click="saveDatasources">{{ saving ? '保存中...' : '保存' }}</el-button>
        </div>
      </div>
      <div v-for="d in datasources" :key="d.id" class="item-row">
        <span>{{ d.id }}</span>
        <el-input v-model="d.freq" />
      </div>
      <pre>{{ saveResult }}</pre>
    </el-card>

    <el-card class="card" shadow="hover">
      <div class="row">
        <h3>项目列表</h3>
        <el-button @click="loadProjects">重新加载</el-button>
      </div>
      <pre>{{ projects }}</pre>
    </el-card>
  </section>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.card { border-radius: 10px; }

.row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.actions {
  display: flex;
  gap: 8px;
}

.item-row {
  display: grid;
  grid-template-columns: 100px 1fr;
  gap: 8px;
  margin: 8px 0;
}

</style>
