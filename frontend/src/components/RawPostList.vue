<script setup>
import { ref, onMounted } from 'vue'
import axios from 'axios'

const posts = ref([])
const loading = ref(false)
const error = ref('')

onMounted(async () => {
  loading.value = true
  try {
    const { data } = await axios.get('/api/posts')
    posts.value = data
  } catch (e) {
    error.value = '加载失败'
  } finally {
    loading.value = false
  }
})
</script>

<template>
  <div style="padding:16px;">
    <h2>原始贴文</h2>
    <div v-if="loading">加载中...</div>
    <div v-else-if="error">{{ error }}</div>
    <ul v-else>
      <li v-for="p in posts" :key="p.id">{{ p.title }} - {{ p.source }}</li>
    </ul>
  </div>
</template>
