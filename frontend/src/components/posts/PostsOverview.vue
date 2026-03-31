<template>
  <PageSection title="Overview">
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
      <el-empty v-if="!store.queried" description="Click Query to load overview" />
      <el-empty v-else-if="!store.overview" description="No data" />

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
    { key: 'total', label: 'Total Posts', value: fmt(o.total) },
    { key: 'valid', label: 'Valid Posts', value: fmt(o.valid_count) },
    { key: 'negative', label: 'Negative Posts', value: fmt(o.negative_count) },
    { key: 'spam', label: 'Spam Posts', value: fmt(o.spam_count) },
    { key: 'kw', label: 'Hot Keywords', value: fmt(o.hot_keyword_count) },
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

