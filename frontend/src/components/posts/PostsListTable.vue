<!-- 作用：前端组件：帖子模块组件（PostsListTable）。 -->

<template>
  <PageSection title="帖子列表">
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
      <el-table-column prop="id" label="编号" width="90" />
      <el-table-column label="摘要" min-width="320">
        <template #default="{ row }">
          <div class="summary">
            <div class="summary__title" v-if="row.title">{{ row.title }}</div>
            <div class="summary__content">{{ row.summary }}</div>
          </div>
        </template>
      </el-table-column>
      <el-table-column prop="platformName" label="平台" width="110" />
      <el-table-column prop="brandName" label="品牌" width="140" />
      <el-table-column prop="publishTime" label="发布时间" width="150" />
      <el-table-column label="关键词" min-width="200">
        <template #default="{ row }">
          <el-space wrap>
            <el-tag v-for="k in row.keywords" :key="k" size="small">{{ k }}</el-tag>
          </el-space>
        </template>
      </el-table-column>
      <el-table-column prop="likeCount" label="点赞" width="90" />
      <el-table-column prop="commentCount" label="评论" width="100" />
      <el-table-column prop="shareCount" label="分享" width="90" />
      <el-table-column prop="viewCount" label="浏览" width="90" />
      <el-table-column label="情感" width="120">
        <template #default="{ row }">
          <el-tag :type="sentimentType(row.sentiment)" size="small">{{ sentimentText(row.sentiment) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="sentimentScore" label="分数" width="90" />
      <el-table-column prop="emotionIntensity" label="强度" width="110" />
      <el-table-column label="垃圾" width="90">
        <template #default="{ row }">
          <el-tag :type="row.spamLabel === 'spam' ? 'danger' : 'info'" size="small">{{ spamText(row.spamLabel) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="有效" width="90">
        <template #default="{ row }">
          <el-tag :type="row.isValid === 1 ? 'success' : row.isValid === 0 ? 'warning' : 'info'" size="small">
            {{ row.isValid === 1 ? '有效' : row.isValid === 0 ? '无效' : '-' }}
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

function sentimentText(s) {
  if (s === 'positive') return '正向'
  if (s === 'neutral') return '中性'
  if (s === 'negative') return '负向'
  return s || '-'
}

function spamText(s) {
  const v = String(s || 'normal').toLowerCase()
  if (v === 'spam') return '垃圾'
  if (v === 'normal') return '正常'
  return s || '-'
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
