<!-- 作用：前端组件：报告模块组件（ReportsTable）。 -->

<template>
  <PageSection title="报告列表">
    <el-alert
      v-if="store.error"
      type="error"
      :title="store.error"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-empty v-if="!store.queried && !store.loading" description="点击“查询”加载报告列表" />

    <el-table
      v-else
      v-loading="store.loading"
      :data="rows"
      border
      style="width: 100%"
      @row-dblclick="(row) => store.openDetail(row.raw)"
    >
      <el-table-column prop="title" label="标题" min-width="240" show-overflow-tooltip />
      <el-table-column prop="type" label="类型" width="110">
        <template #default="{ row }">
          <el-tag size="small" type="info">{{ row.type }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="projectName" label="项目" width="160" show-overflow-tooltip />
      <el-table-column prop="dataRange" label="数据范围" width="210" />
      <el-table-column prop="createdAt" label="创建时间" width="170" />
      <el-table-column prop="summary" label="摘要" min-width="220" show-overflow-tooltip />
      <el-table-column prop="status" label="状态" width="120">
        <template #default="{ row }">
          <el-tag :type="statusType(row.status)" size="small">{{ row.statusLabel }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="440" fixed="right">
        <template #default="{ row }">
          <el-button size="small" type="primary" plain @click="goDetail(row.raw)">详情</el-button>
          <el-button size="small" type="success" plain :disabled="row.status === 'running'" @click="store.onGenerate(row.raw)">
            生成
          </el-button>
          <el-button size="small" plain @click="store.onEvidence(row.raw)">证据</el-button>
          <el-button size="small" plain @click="store.onExport(row.raw)">导出</el-button>
          <el-button size="small" type="success" plain @click="store.onCopyGenerate(row.raw)">复制</el-button>
          <el-button size="small" type="danger" plain @click="store.onDelete(row.raw)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>

    <div class="pager">
      <ReportsPagination />
    </div>
  </PageSection>
</template>

<script setup>
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import PageSection from '../common/PageSection.vue'
import ReportsPagination from './ReportsPagination.vue'
import { useReportsStore } from '../../stores/reports'

const store = useReportsStore()
const router = useRouter()

function trimText(s) {
  return String(s || '').replace(/\s+/g, ' ').trim()
}

function truncate(s, n = 120) {
  const t = trimText(s)
  if (t.length <= n) return t
  return `${t.slice(0, n)}…`
}

function fmtRange(a, b) {
  if (!a && !b) return '-'
  return `${a || '-'} ~ ${b || '-'}`
}

function fmtTime(t) {
  if (!t) return '-'
  return String(t).slice(0, 19).replace('T', ' ')
}

function statusType(s) {
  if (s === 'success' || s === 'done') return 'success'
  if (s === 'failed' || s === 'error') return 'danger'
  if (s === 'pending' || s === 'running') return 'warning'
  return 'info'
}

const rows = computed(() => {
  const nameById = store.projectNameById || {}
  return (store.items || []).map((it) => {
    const pid = Number(it?.project_id)
    const typeRaw = it?.report_type || '-'
    const statusRaw = it?.status
    return {
      raw: it,
      id: Number(it?.id),
      title: trimText(it?.title) || `报告 #${it?.id ?? '-'}`,
      type: typeText(typeRaw),
      projectName: nameById[pid] || `#${pid || '-'}`,
      dataRange: fmtRange(it?.data_start_date, it?.data_end_date),
      createdAt: fmtTime(it?.created_at),
      summary: truncate(it?.summary || it?.content_markdown || ''),
      status: statusRaw,
      statusLabel: statusText(statusRaw),
    }
  })
})

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

function goDetail(raw) {
  const id = Number(raw?.id)
  if (!Number.isFinite(id) || id <= 0) return
  router.push({ name: 'report-detail', params: { id } })
}
</script>

<style scoped>
.pager {
  margin-top: 12px;
  display: flex;
  justify-content: flex-end;
}
</style>
