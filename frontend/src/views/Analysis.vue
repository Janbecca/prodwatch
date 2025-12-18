<script setup>
import { ref } from 'vue'
import api from '../api/axios'

const product = ref('CamX')
const sentiment = ref('')
const summary = ref(null)
const comments = ref([])

const load = async () => {
  const { data: s } = await api.get('/api/analysis/summary', { params: { products: [product.value] } })
  summary.value = s
  const { data: c } = await api.get('/api/analysis/comments', { params: { product: product.value, sentiment: sentiment.value || undefined } })
  comments.value = c
}

load()
</script>

<template>
  <div style="padding:16px;">
    <h2>数据分析与情感分析</h2>
    <div style="margin-bottom:8px;">
      <input v-model="product" placeholder="竞品（如 CamX）" />
      <select v-model="sentiment">
        <option value="">全部</option>
        <option value="positive">正面</option>
        <option value="neutral">中性</option>
        <option value="negative">负面</option>
      </select>
      <button @click="load">查询</button>
    </div>
    <pre v-if="summary">{{ summary }}</pre>
    <ul>
      <li v-for="c in comments" :key="c.id">[{{ c.sentiment }}] {{ c.text }}</li>
    </ul>
  </div>
</template>
