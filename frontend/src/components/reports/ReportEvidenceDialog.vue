<!-- 作用：前端组件：报告模块组件（ReportEvidenceDialog）。 -->

<template>
  <el-dialog v-model="open" title="证据" width="860px">
    <el-alert
      v-if="store.evidenceError"
      type="error"
      :title="store.evidenceError"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-table v-loading="store.evidenceLoading" :data="rows" border style="width: 100%">
      <el-table-column prop="content" label="帖子内容" min-width="320" show-overflow-tooltip />
      <el-table-column prop="platform" label="平台" width="120" />
      <el-table-column prop="publishTime" label="发布时间" width="160" />
      <el-table-column prop="sentiment" label="情感" width="110">
        <template #default="{ row }">
          <el-tag :type="sentimentType(row.sentiment)" size="small">{{ sentimentText(row.sentiment) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="spam" label="垃圾" width="90">
        <template #default="{ row }">
          <el-tag :type="row.spam === 'spam' ? 'danger' : 'info'" size="small">{{ spamText(row.spam || 'normal') }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="reason" label="原因" min-width="200" show-overflow-tooltip />
      <el-table-column prop="section" label="章节" width="140" />
    </el-table>

    <div style="margin-top: 10px; display: flex; justify-content: flex-end">
      <el-pagination
        :current-page="store.evidencePage"
        :page-size="store.evidencePageSize"
        :page-sizes="[10, 20, 50, 100]"
        layout="total, sizes, prev, pager, next"
        :total="store.evidenceTotal"
        background
        :disabled="store.evidenceLoading"
        @update:current-page="store.setEvidencePage"
        @update:page-size="store.setEvidencePageSize"
      />
    </div>
  </el-dialog>
</template>

<script setup>
import { computed } from 'vue'
import { useReportsStore } from '../../stores/reports'

const store = useReportsStore()

const open = computed({
  get: () => store.evidenceOpen,
  set: (v) => {
    if (!v) store.closeEvidence()
  },
})

function trimText(s) {
  return String(s || '').replace(/\s+/g, ' ').trim()
}

function fmtTime(t) {
  if (!t) return '-'
  return String(t).slice(0, 19).replace('T', ' ')
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

const rows = computed(() => {
  return (store.evidenceItems || []).map((it) => ({
    content: trimText(it?.content || it?.title || '').slice(0, 160) || '-',
    platform: it?.platform_name || (it?.platform_id ? `平台 ${it.platform_id}` : '-'),
    publishTime: fmtTime(it?.publish_time),
    sentiment: it?.sentiment,
    spam: it?.spam_label || 'normal',
    reason: trimText(it?.quote_reason || '-'),
    section: it?.section_name || '-',
  }))
})
</script>
