<script setup>
import { ref, onMounted } from 'vue'
import api from '../api/axios'

const datasources = ref([])

onMounted(async () => {
  const { data } = await api.get('/api/settings/datasources')
  datasources.value = data
})

const save = async () => {
  await api.post('/api/settings/datasources', datasources.value)
  alert('已保存')
}
</script>

<template>
  <div style="padding:16px;">
    <h2>系统设置与管理</h2>
    <ul>
      <li v-for="d in datasources" :key="d.id">
        {{ d.id }} 频率：<input v-model="d.freq" />
      </li>
    </ul>
    <button @click="save">保存</button>
  </div>
</template>
