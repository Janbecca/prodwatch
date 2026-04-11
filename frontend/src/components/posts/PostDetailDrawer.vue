<!-- 作用：前端组件：帖子模块组件（PostDetailDrawer）。 -->

<template>
  <el-drawer v-model="open" size="620px" title="帖子详情" :with-header="true">
    <el-alert
      v-if="store.detailError"
      type="error"
      :title="store.detailError"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-skeleton v-if="store.detailLoading" :rows="6" animated />

    <el-empty v-else-if="!item" description="暂无详情" />

    <template v-else>
      <el-descriptions :column="1" border>
        <el-descriptions-item label="帖子编号">{{ item.id }}</el-descriptions-item>
        <el-descriptions-item label="标题">{{ item.title || '-' }}</el-descriptions-item>
        <el-descriptions-item label="帖子链接">
          <el-link v-if="item.post_url" :href="item.post_url" target="_blank">{{ item.post_url }}</el-link>
          <span v-else>-</span>
        </el-descriptions-item>
        <el-descriptions-item label="发布时间">{{ fmtTime(item.publish_time) }}</el-descriptions-item>
        <el-descriptions-item label="抓取时间">{{ fmtTime(item.crawled_at) }}</el-descriptions-item>
        <el-descriptions-item label="平台">{{ platformName }}</el-descriptions-item>
        <el-descriptions-item label="品牌">{{ brandName }}</el-descriptions-item>
        <el-descriptions-item label="互动数据">
          <el-space wrap>
            <el-tag type="info">点赞: {{ num(item.like_count) }}</el-tag>
            <el-tag type="info">评论: {{ num(item.comment_count) }}</el-tag>
            <el-tag type="info">分享: {{ num(item.share_count) }}</el-tag>
            <el-tag type="info">浏览: {{ item.view_count == null ? '-' : num(item.view_count) }}</el-tag>
          </el-space>
        </el-descriptions-item>
        <el-descriptions-item label="情感">
          <el-space wrap>
            <el-tag :type="sentimentType(item.sentiment)">{{ sentimentText(item.sentiment) }}</el-tag>
            <el-tag type="info">分数: {{ score(item.sentiment_score) }}</el-tag>
            <el-tag type="info">强度: {{ score(item.emotion_intensity) }}</el-tag>
          </el-space>
        </el-descriptions-item>
        <el-descriptions-item label="垃圾">
          <el-tag :type="(item.spam_label || 'normal') === 'spam' ? 'danger' : 'info'">
            {{ spamText(item.spam_label || 'normal') }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="有效性">
          <el-tag :type="item.is_valid === 1 ? 'success' : item.is_valid === 0 ? 'warning' : 'info'">
            {{ item.is_valid === 1 ? '有效' : item.is_valid === 0 ? '无效' : '-' }}
          </el-tag>
          <el-text v-if="item.invalid_reason" type="info" style="margin-left: 8px">
            {{ item.invalid_reason }}
          </el-text>
        </el-descriptions-item>
        <el-descriptions-item label="关键词命中">
          <el-space wrap>
            <el-tag v-for="k in item.keywords || []" :key="k" size="small">{{ k }}</el-tag>
            <el-text v-if="!item.keywords || item.keywords.length === 0" type="info">-</el-text>
          </el-space>
        </el-descriptions-item>
        <el-descriptions-item label="话题分析">
          <el-space wrap>
            <el-tag v-for="t in topicTags" :key="t" size="small" type="success">{{ t }}</el-tag>
            <el-text v-if="topicTags.length === 0" type="info">-</el-text>
          </el-space>
        </el-descriptions-item>
        <el-descriptions-item v-if="entityTags.length" label="实体">
          <el-space wrap>
            <el-tag v-for="t in entityTags" :key="t" size="small" type="info">{{ t }}</el-tag>
          </el-space>
        </el-descriptions-item>
        <el-descriptions-item v-if="issueTags.length" label="问题归纳">
          <el-space wrap>
            <el-tag v-for="t in issueTags" :key="t" size="small" type="warning">{{ t }}</el-tag>
          </el-space>
        </el-descriptions-item>
        <el-descriptions-item label="特征">
          <el-space wrap>
            <el-tag v-for="f in featureTags" :key="f.key" :type="f.type" size="small">{{ f.text }}</el-tag>
            <el-text v-if="featureTags.length === 0" type="info">-</el-text>
          </el-space>
        </el-descriptions-item>
      </el-descriptions>

      <el-divider />

      <el-text tag="b">原始文本</el-text>
      <pre class="block">{{ item.content || item.title || '-' }}</pre>

      <el-text tag="b">清洗文本</el-text>
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

function takeTextList(v, limit = 6) {
  const out = []
  if (Array.isArray(v)) {
    for (const it of v) {
      if (it == null) continue
      if (typeof it === 'string') out.push(it)
      else if (typeof it === 'object') {
        if (it.text) out.push(String(it.text))
        else if (it.topic) out.push(String(it.topic))
        else if (it.name) out.push(String(it.name))
      }
    }
  }
  return uniqStrings(out).slice(0, limit)
}

const topicTags = computed(() => {
  return takeTextList(item.value?.topics || item.value?.analysis_result?.topics, 12)
})

const entityTags = computed(() => {
  return takeTextList(item.value?.entities || item.value?.analysis_result?.entities, 12)
})

const issueTags = computed(() => {
  return takeTextList(item.value?.issues || item.value?.analysis_result?.issues, 12)
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
