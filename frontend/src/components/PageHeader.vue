<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { getLastTransitionRef, NavSource } from '../navContext'

defineProps({
  content: { type: String, required: true },
})

const router = useRouter()
const route = useRoute()
const lastTransition = getLastTransitionRef()

const canShowBack = computed(() => {
  const t = lastTransition.value
  if (!t || t.toFullPath !== route.fullPath) return false
  if (t.source === NavSource.Sidebar) return false
  if (!t.fromFullPath || t.fromFullPath === route.fullPath) return false
  if (t.fromFullPath === '/login' || t.fromFullPath === '/register') return false
  return true
})

const onBack = () => {
  router.back()
}
</script>

<template>
  <div class="wrap">
    <el-page-header v-if="canShowBack" :content="content" @back="onBack" />
    <h2 v-else class="title">{{ content }}</h2>
  </div>
</template>

<style scoped>
.wrap {
  display: flex;
  align-items: center;
  min-height: 32px;
}

.title {
  margin: 0;
  font-size: 16px;
  font-weight: 600;
}
</style>
