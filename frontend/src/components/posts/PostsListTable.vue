<template>
  <PageSection title="Posts">
    <el-alert
      v-if="store.listError"
      type="error"
      :title="store.listError"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-table
      v-loading="store.listLoading"
      :data="tableRows"
      border
      style="width: 100%"
      @row-click="onRowClick"
    >
      <el-table-column prop="id" label="ID" width="90" />
      <el-table-column label="Summary" min-width="320">
        <template #default="{ row }">
          <div class="summary">
            <div class="summary__title" v-if="row.title">{{ row.title }}</div>
            <div class="summary__content">{{ row.summary }}</div>
          </div>
        </template>
      </el-table-column>
      <el-table-column prop="platformName" label="Platform" width="110" />
      <el-table-column prop="brandName" label="Brand" width="140" />
      <el-table-column prop="publishTime" label="Publish Time" width="150" />
      <el-table-column label="Keywords" min-width="200">
        <template #default="{ row }">
          <el-space wrap>
            <el-tag v-for="k in row.keywords" :key="k" size="small">{{ k }}</el-tag>
          </el-space>
        </template>
      </el-table-column>
      <el-table-column prop="likeCount" label="Likes" width="90" />
      <el-table-column prop="commentCount" label="Comments" width="100" />
      <el-table-column prop="shareCount" label="Shares" width="90" />
      <el-table-column prop="viewCount" label="Views" width="90" />
      <el-table-column label="Sentiment" width="120">
        <template #default="{ row }">
          <el-tag :type="sentimentType(row.sentiment)" size="small">{{ row.sentiment || '-' }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="sentimentScore" label="Score" width="90" />
      <el-table-column prop="emotionIntensity" label="Intensity" width="110" />
      <el-table-column label="Spam" width="90">
        <template #default="{ row }">
          <el-tag :type="row.spamLabel === 'spam' ? 'danger' : 'info'" size="small">{{ row.spamLabel || '-' }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="Valid" width="90">
        <template #default="{ row }">
          <el-tag :type="row.isValid === 1 ? 'success' : row.isValid === 0 ? 'warning' : 'info'" size="small">
            {{ row.isValid === 1 ? 'valid' : row.isValid === 0 ? 'invalid' : '-' }}
          </el-tag>
        </template>
      </el-table-column>
    </el-table>

    <div style="margin-top: 10px">
      <PostsPagination />
    </div>
  </PageSection>
</template>

<script setup>
import { computed } from 'vue'
import PageSection from '../common/PageSection.vue'
import PostsPagination from './PostsPagination.vue'
import { usePostsStore } from '../../stores/posts'

const store = usePostsStore()

function trimText(s) {
  return String(s || '').replace(/\s+/g, ' ').trim()
}

function uniqStrings(arr) {
  const out = []
  const seen = new Set()
  for (const x of arr || []) {
    const s = String(x || '').trim()
    if (!s) continue
    if (seen.has(s)) continue
    seen.add(s)
    out.push(s)
  }
  return out
}

function truncate(s, n = 120) {
  const t = trimText(s)
  if (t.length <= n) return t
  return `${t.slice(0, n)}…`
}

function sentimentType(s) {
  if (s === 'positive') return 'success'
  if (s === 'negative') return 'danger'
  if (s === 'neutral') return 'info'
  return 'info'
}

const brandNameById = computed(() => {
  const map = {}
  for (const b of store.brandOptions || []) map[Number(b.id)] = b.name
  return map
})
const platformNameById = computed(() => {
  const map = {}
  for (const p of store.platformOptions || []) map[Number(p.id)] = p.name
  return map
})

const tableRows = computed(() => {
  return (store.items || []).map((it) => {
    const id = Number(it?.id)
    const platformId = Number(it?.platform_id)
    const brandId = Number(it?.brand_id)
    return {
      raw: it,
      id,
      title: trimText(it?.title),
      summary: truncate(it?.content || it?.clean_text || it?.title || ''),
      platformName: platformNameById.value[platformId] || `#${platformId || '-'}`,
      brandName: brandNameById.value[brandId] || `#${brandId || '-'}`,
      publishTime: it?.publish_time ? String(it.publish_time).slice(0, 16).replace('T', ' ') : '-',
      keywords: uniqStrings(Array.isArray(it?.keywords) ? it.keywords : []).slice(0, 6),
      likeCount: Number(it?.like_count || 0),
      commentCount: Number(it?.comment_count || 0),
      shareCount: Number(it?.share_count || 0),
      viewCount: it?.view_count == null ? '-' : Number(it.view_count || 0),
      sentiment: it?.sentiment,
      sentimentScore: it?.sentiment_score == null ? '-' : Number(it.sentiment_score).toFixed(2),
      emotionIntensity: it?.emotion_intensity == null ? '-' : Number(it.emotion_intensity).toFixed(2),
      spamLabel: it?.spam_label || 'normal',
      isValid: it?.is_valid,
    }
  })
})

function onRowClick(row) {
  store.openDetail(row.raw)
}
</script>

<style scoped>
.summary__title {
  font-weight: 700;
  margin-bottom: 4px;
}
.summary__content {
  color: var(--el-text-color-regular);
}
</style>
