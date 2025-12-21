<script setup>
import { ref } from 'vue'
import api from '../api/axios'

const projectId = ref(1)
const platform = ref(['weibo'])
const keyword = ref('萤石')
const model = ref('rule-based')
const results = ref([])
const loading = ref(false)

const run = async () => {
  loading.value = true
  try {
    const { data } = await api.post('/api/analysis/run', null, {
      params: {
        project_id: projectId.value,
        platform: platform.value,
        keyword: keyword.value,
        model: model.value
      }
    })
    results.value = data.items
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div style="padding:16px;">
    <h2>动态分析调试</h2>

    <div>
      <label>平台：</label>
      <select multiple v-model="platform">
        <option value="weibo">微博</option>
        <option value="xhs">小红书</option>
        <option value="douyin">抖音</option>
      </select>

      <label>模型：</label>
      <select v-model="model">
        <option value="rule-based">Rule-based</option>
        <option value="gpt-4o-mini">gpt-4o-mini</option>
      </select>

      <input v-model="keyword" placeholder="关键词" />
      <button @click="run">运行</button>
    </div>

    <div v-if="loading">运行中...</div>
    <ul>
      <li v-for="r in results" :key="r.id">
        [{{ r.polarity }}] {{ r.emotions }}
      </li>
    </ul>
  </div>
</template>
