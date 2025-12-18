<script setup>
import { ref, onMounted } from 'vue'
import api from '../api/axios'

const kpis = ref(null)
const ranking = ref([])
const alerts = ref([])

onMounted(async () => {
  const [{ data: k }, { data: r }, { data: a }] = await Promise.all([
    api.get('/api/dashboard/kpis'),
    api.get('/api/dashboard/ranking'),
    api.get('/api/dashboard/alerts'),
  ])
  kpis.value = k
  ranking.value = r
  alerts.value = a
})
</script>

<template>
  <div style="padding:16px;">
    <h2>仪表盘</h2>
    <pre v-if="kpis">{{ kpis }}</pre>
    <h3>热度排行</h3>
    <ul>
      <li v-for="r in ranking" :key="r.product">{{ r.product }} - {{ r.score }}</li>
    </ul>
    <h3>告警</h3>
    <ul>
      <li v-for="a in alerts" :key="a.product">{{ a.level }} - {{ a.product }} - {{ a.reason }}</li>
    </ul>
  </div>
</template>
