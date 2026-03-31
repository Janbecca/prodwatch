<template>
  <el-drawer v-model="open" size="620px" title="Post Detail" :with-header="true">
    <el-alert
      v-if="store.detailError"
      type="error"
      :title="store.detailError"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-skeleton v-if="store.detailLoading" :rows="6" animated />

    <el-empty v-else-if="!item" description="No detail" />

    <template v-else>
      <el-descriptions :column="1" border>
        <el-descriptions-item label="Post ID">{{ item.id }}</el-descriptions-item>
        <el-descriptions-item label="Title">{{ item.title || '-' }}</el-descriptions-item>
        <el-descriptions-item label="Post URL">
          <el-link v-if="item.post_url" :href="item.post_url" target="_blank">{{ item.post_url }}</el-link>
          <span v-else>-</span>
        </el-descriptions-item>
        <el-descriptions-item label="Publish Time">{{ fmtTime(item.publish_time) }}</el-descriptions-item>
        <el-descriptions-item label="Crawled At">{{ fmtTime(item.crawled_at) }}</el-descriptions-item>
        <el-descriptions-item label="Platform">{{ platformName }}</el-descriptions-item>
        <el-descriptions-item label="Brand">{{ brandName }}</el-descriptions-item>
        <el-descriptions-item label="Counts">
          <el-space wrap>
            <el-tag type="info">Likes: {{ num(item.like_count) }}</el-tag>
            <el-tag type="info">Comments: {{ num(item.comment_count) }}</el-tag>
            <el-tag type="info">Shares: {{ num(item.share_count) }}</el-tag>
            <el-tag type="info">Views: {{ item.view_count == null ? '-' : num(item.view_count) }}</el-tag>
          </el-space>
        </el-descriptions-item>
        <el-descriptions-item label="Sentiment">
          <el-space wrap>
            <el-tag :type="sentimentType(item.sentiment)">{{ item.sentiment || '-' }}</el-tag>
            <el-tag type="info">score: {{ score(item.sentiment_score) }}</el-tag>
            <el-tag type="info">intensity: {{ score(item.emotion_intensity) }}</el-tag>
          </el-space>
        </el-descriptions-item>
        <el-descriptions-item label="Spam">
          <el-tag :type="(item.spam_label || 'normal') === 'spam' ? 'danger' : 'info'">
            {{ item.spam_label || 'normal' }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="Valid">
          <el-tag :type="item.is_valid === 1 ? 'success' : item.is_valid === 0 ? 'warning' : 'info'">
            {{ item.is_valid === 1 ? 'valid' : item.is_valid === 0 ? 'invalid' : '-' }}
          </el-tag>
          <el-text v-if="item.invalid_reason" type="info" style="margin-left: 8px">
            {{ item.invalid_reason }}
          </el-text>
        </el-descriptions-item>
        <el-descriptions-item label="Hit Keywords">
          <el-space wrap>
            <el-tag v-for="k in item.keywords || []" :key="k" size="small">{{ k }}</el-tag>
            <el-text v-if="!item.keywords || item.keywords.length === 0" type="info">-</el-text>
          </el-space>
        </el-descriptions-item>
        <el-descriptions-item label="Features">
          <el-space wrap>
            <el-tag v-for="f in featureTags" :key="f.key" :type="f.type" size="small">{{ f.text }}</el-tag>
            <el-text v-if="featureTags.length === 0" type="info">-</el-text>
          </el-space>
        </el-descriptions-item>
      </el-descriptions>

      <el-divider />

      <el-text tag="b">Raw Text</el-text>
      <pre class="block">{{ item.content || item.title || '-' }}</pre>

      <el-text tag="b">Clean Text</el-text>
      <pre class="block">{{ item.clean_text || '-' }}</pre>
    </template>
  </el-drawer>
</template>

<script setup>
import { computed } from 'vue'
import { usePostsStore } from '../../stores/posts'

const store = usePostsStore()

const open = computed({
  get: () => store.detailOpen,
  set: (v) => {
    if (!v) store.closeDetail()
  },
})

const item = computed(() => store.detail)

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

const brandName = computed(() => {
  const bid = Number(item.value?.brand_id)
  return brandNameById.value[bid] || (bid ? `#${bid}` : '-')
})
const platformName = computed(() => {
  const pid = Number(item.value?.platform_id)
  return platformNameById.value[pid] || (pid ? `#${pid}` : '-')
})

const featureTags = computed(() => {
  const list = Array.isArray(item.value?.features) ? item.value.features : []
  const out = []
  const seen = new Set()
  for (let idx = 0; idx < list.length; idx++) {
    const f = list[idx]
    const name = String(f?.feature_name ?? '').trim()
    if (!name) continue
    if (seen.has(name)) continue
    seen.add(name)
    const s = f?.feature_sentiment
    const type = s === 'positive' ? 'success' : s === 'negative' ? 'danger' : 'info'
    const conf = f?.confidence == null ? '' : ` (${Number(f.confidence).toFixed(2)})`
    out.push({ key: `${name}-${idx}`, type, text: `${name}${conf}` })
  }
  return out
})

function fmtTime(t) {
  if (!t) return '-'
  return String(t).slice(0, 19).replace('T', ' ')
}
function num(v) {
  return Number(v || 0).toLocaleString()
}
function score(v) {
  if (v == null) return '-'
  const n = Number(v)
  if (!Number.isFinite(n)) return '-'
  return n.toFixed(2)
}
function sentimentType(s) {
  if (s === 'positive') return 'success'
  if (s === 'negative') return 'danger'
  if (s === 'neutral') return 'info'
  return 'info'
}
</script>

<style scoped>
.block {
  margin: 8px 0 18px;
  padding: 10px 12px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 8px;
  background: var(--el-fill-color-blank);
  max-height: 220px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-word;
}
</style>
