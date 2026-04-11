<!-- 作用：前端组件：帖子模块组件（PostsOverview）。 -->

<template>
  <PageSection title="概览">
    <el-alert
      v-if="store.overviewError"
      type="error"
      :title="store.overviewError"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-skeleton v-if="store.overviewLoading" :rows="2" animated />

    <template v-else>
      <el-empty v-if="!store.queried" description="点击“查询”加载概览" />
      <el-empty v-else-if="!store.overview" description="暂无数据" />

      <el-row v-else :gutter="12">
        <el-col v-for="c in cards" :key="c.key" :span="4">
          <el-card shadow="never" class="card">
            <el-text type="info">{{ c.label }}</el-text>
            <div class="val">{{ c.value }}</div>
          </el-card>
        </el-col>
      </el-row>
    </template>
  </PageSection>
</template>

<script setup>
import { computed } from 'vue'
import PageSection from '../common/PageSection.vue'
import { usePostsStore } from '../../stores/posts'

const store = usePostsStore()

const cards = computed(() => {
  const o = store.overview || {}
  const fmt = (n) => Number(n || 0).toLocaleString()
  return [
    { key: 'total', label: '总帖子数', value: fmt(o.total) },
    { key: 'valid', label: '有效帖子数', value: fmt(o.valid_count) },
    { key: 'negative', label: '负向帖子数', value: fmt(o.negative_count) },
    { key: 'spam', label: '垃圾帖子数', value: fmt(o.spam_count) },
    { key: 'kw', label: '关键词命中数', value: fmt(o.hot_keyword_count) },
    { key: 'topic', label: '热点话题数', value: fmt(o.hot_topic_count) },
  ]
})
</script>

<style scoped>
.card {
  border: 1px solid var(--el-border-color-lighter);
}
.val {
  margin-top: 6px;
  font-size: 22px;
  font-weight: 700;
}
</style>
