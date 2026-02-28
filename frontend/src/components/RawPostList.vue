<script setup>
import { computed, ref } from 'vue'
import api from '../api/axios'

const mode = ref('latest_run')
const projectId = ref('')
const platformId = ref('')
const posts = ref([])
const loading = ref(false)
const error = ref('')

const queryParams = computed(() => ({
  mode: mode.value || 'latest_run',
  project_id: projectId.value === '' ? undefined : Number(projectId.value),
  platform_id: platformId.value === '' ? undefined : Number(platformId.value),
}))

const loadPosts = async () => {
  loading.value = true
  error.value = ''
  try {
    const { data } = await api.get('/api/posts', { params: queryParams.value })
    posts.value = data
  } catch (e) {
    error.value = e?.response?.data?.detail || '加载帖子失败'
  } finally {
    loading.value = false
  }
}

loadPosts()
</script>

<template>
  <section class="page">
    <el-page-header content="原始帖子浏览" />
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <el-card class="toolbar" shadow="hover">
      <label>模式</label>
      <el-select v-model="mode" style="width: 140px">
        <el-option label="latest_run" value="latest_run" />
        <el-option label="all" value="all" />
      </el-select>
      <label>项目 ID</label>
      <el-input v-model="projectId" type="number" placeholder="可选" style="width: 120px" />
      <label>平台 ID</label>
      <el-input v-model="platformId" type="number" placeholder="可选" style="width: 120px" />
      <el-button type="primary" :loading="loading" @click="loadPosts">{{ loading ? '加载中...' : '查询 /api/posts' }}</el-button>
    </el-card>
    <div class="summary">总数：{{ posts.length }}</div>
    <div class="list">
      <el-card v-for="p in posts" :key="p.id" class="item" shadow="hover">
        <div><strong>#{{ p.id }}</strong> run={{ p.pipeline_run_id }} project={{ p.project_id }}</div>
        <div>platform={{ p.platform_id }} keyword_id={{ p.keyword_id }} post_id={{ p.platform_post_id }}</div>
        <div>publish_time={{ p.publish_time }}</div>
        <div>{{ p.raw_text }}</div>
        <div>likes={{ p.like_count }} comments={{ p.comment_count }} shares={{ p.share_count }}</div>
      </el-card>
    </div>
  </section>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.toolbar {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  align-items: center;
  border-radius: 10px;
}

.summary {
  color: #374151;
}

.list {
  display: grid;
  gap: 8px;
}

.item {
  border-radius: 8px;
}
</style>
