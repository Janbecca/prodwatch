<script setup>
import { ref, onMounted } from 'vue'
import api from '../api/axios'

const overview = ref(null)
const items = ref([])

onMounted(async () => {
  const { data: o } = await api.get('/api/moderation/spam/overview')
  overview.value = o
  const { data: l } = await api.get('/api/moderation/spam/list')
  items.value = l
})
</script>

<template>
  <div style="padding:16px;">
    <h2>水军识别与数据验证</h2>
    <pre v-if="overview">{{ overview }}</pre>
    <ul>
      <li v-for="i in items" :key="i.id">{{ i.product }} - {{ i.reason }}</li>
    </ul>
  </div>
</template>
