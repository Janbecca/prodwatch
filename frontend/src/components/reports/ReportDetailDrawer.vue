<!-- 作用：前端组件：报告模块组件（ReportDetailDrawer）。 -->

<template>
  <el-drawer v-model="open" size="720px" title="报告详情" :with-header="true">
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
        <el-descriptions-item label="标题">{{ item.title || '-' }}</el-descriptions-item>
        <el-descriptions-item label="类型">
          <el-tag size="small" type="info">{{ typeText(item.report_type || '-') }}</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="项目">{{ projectName }}</el-descriptions-item>
        <el-descriptions-item label="数据范围">{{ fmtRange(item.data_start_date, item.data_end_date) }}</el-descriptions-item>
        <el-descriptions-item label="创建时间">{{ fmtTime(item.created_at) }}</el-descriptions-item>
        <el-descriptions-item label="状态">
          <el-tag :type="statusType(item.status)" size="small">{{ statusText(item.status) }}</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="摘要">{{ item.summary || '-' }}</el-descriptions-item>
      </el-descriptions>

      <el-divider />
      <el-text tag="b">内容（标记文本）</el-text>
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

function typeText(v) {
  if (v === 'daily') return '日报'
  if (v === 'weekly') return '周报'
  if (v === 'monthly') return '月报'
  if (v === 'special') return '专题'
  return v || '-'
}

function statusText(v) {
  if (v === 'pending') return '待处理'
  if (v === 'running') return '生成中'
  if (v === 'success' || v === 'done') return '成功'
  if (v === 'failed') return '失败'
  if (v === 'error') return '错误'
  return v || '-'
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
