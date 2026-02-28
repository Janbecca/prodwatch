<script setup>
import { ref } from 'vue'
import api from '../api/axios'

const overview = ref(null)
const product = ref('')
const list = ref([])
const error = ref('')

const loadOverview = async () => {
  try {
    const { data } = await api.get('/api/moderation/spam/overview')
    overview.value = data
  } catch (e) {
    error.value = e?.response?.data?.detail || '加载总览失败'
  }
}

const loadList = async () => {
  try {
    const { data } = await api.get('/api/moderation/spam/list', {
      params: { product: product.value || undefined },
    })
    list.value = data
  } catch (e) {
    error.value = e?.response?.data?.detail || '加载列表失败'
  }
}

loadOverview()
loadList()
</script>

<template>
  <section class="page">
    <el-page-header content="水军识别调试" />
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <el-card class="card" shadow="hover">
      <el-button type="primary" plain @click="loadOverview">刷新总览</el-button>
      <pre>{{ overview }}</pre>
    </el-card>

    <el-card class="card" shadow="hover">
      <div class="actions">
        <el-input v-model="product" placeholder="按产品筛选（可选）" style="max-width: 280px" />
        <el-button @click="loadList">查询列表</el-button>
      </div>
      <ul>
        <li v-for="item in list" :key="item.id">{{ item.id }} - {{ item.product }} - {{ item.reason }}</li>
      </ul>
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

.actions {
  display: flex;
  gap: 8px;
  margin-bottom: 8px;
}

</style>
