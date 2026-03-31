<template>
  <el-drawer v-model="open" size="720px" title="Report Detail" :with-header="true">
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
        <el-descriptions-item label="Title">{{ item.title || '-' }}</el-descriptions-item>
        <el-descriptions-item label="Type">
          <el-tag size="small" type="info">{{ item.report_type || '-' }}</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="Project">{{ projectName }}</el-descriptions-item>
        <el-descriptions-item label="Data Range">{{ fmtRange(item.data_start_date, item.data_end_date) }}</el-descriptions-item>
        <el-descriptions-item label="Created At">{{ fmtTime(item.created_at) }}</el-descriptions-item>
        <el-descriptions-item label="Status">
          <el-tag :type="statusType(item.status)" size="small">{{ item.status || '-' }}</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="Summary">{{ item.summary || '-' }}</el-descriptions-item>
      </el-descriptions>

      <el-divider />
      <el-text tag="b">Content (Markdown)</el-text>
      <pre class="md">{{ item.content_markdown || '-' }}</pre>
    </template>
  </el-drawer>
</template>

<script setup>
import { computed } from 'vue'
import { useReportsStore } from '../../stores/reports'

const store = useReportsStore()

const open = computed({
  get: () => store.detailOpen,
  set: (v) => {
    if (!v) store.closeDetail()
  },
})

const item = computed(() => store.detail)

const projectName = computed(() => {
  const pid = Number(item.value?.project_id)
  return store.projectNameById?.[pid] || (pid ? `#${pid}` : '-')
})

function fmtTime(t) {
  if (!t) return '-'
  return String(t).slice(0, 19).replace('T', ' ')
}

function fmtRange(a, b) {
  if (!a && !b) return '-'
  return `${a || '-'} ~ ${b || '-'}`
}

function statusType(s) {
  if (s === 'success' || s === 'done') return 'success'
  if (s === 'failed' || s === 'error') return 'danger'
  if (s === 'pending' || s === 'running') return 'warning'
  return 'info'
}
</script>

<style scoped>
.md {
  margin: 8px 0 0;
  padding: 10px 12px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 8px;
  background: var(--el-fill-color-blank);
  max-height: 520px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-word;
}
</style>

