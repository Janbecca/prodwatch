<template>
  <PageSection title="Reports">
    <el-alert
      v-if="store.error"
      type="error"
      :title="store.error"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-empty v-if="!store.queried && !store.loading" description="Click Query to load reports" />

    <el-table
      v-else
      v-loading="store.loading"
      :data="rows"
      border
      style="width: 100%"
      @row-dblclick="(row) => store.openDetail(row.raw)"
    >
      <el-table-column prop="title" label="Title" min-width="240" show-overflow-tooltip />
      <el-table-column prop="type" label="Type" width="110">
        <template #default="{ row }">
          <el-tag size="small" type="info">{{ row.type }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="projectName" label="Project" width="160" show-overflow-tooltip />
      <el-table-column prop="dataRange" label="Data Range" width="210" />
      <el-table-column prop="createdAt" label="Created At" width="170" />
      <el-table-column prop="summary" label="Summary" min-width="220" show-overflow-tooltip />
      <el-table-column prop="status" label="Status" width="120">
        <template #default="{ row }">
          <el-tag :type="statusType(row.status)" size="small">{{ row.status || '-' }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="Actions" width="440" fixed="right">
        <template #default="{ row }">
          <el-button size="small" type="primary" plain @click="goDetail(row.raw)">Detail</el-button>
          <el-button size="small" type="success" plain :disabled="row.status === 'running'" @click="store.onGenerate(row.raw)">
            Generate
          </el-button>
          <el-button size="small" plain @click="store.onEvidence(row.raw)">Evidence</el-button>
          <el-button size="small" plain @click="store.onExport(row.raw)">Export</el-button>
          <el-button size="small" type="success" plain @click="store.onCopyGenerate(row.raw)">Copy</el-button>
          <el-button size="small" type="danger" plain @click="store.onDelete(row.raw)">Delete</el-button>
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
    return {
      raw: it,
      id: Number(it?.id),
      title: trimText(it?.title) || `Report #${it?.id ?? '-'}`,
      type: it?.report_type || '-',
      projectName: nameById[pid] || `#${pid || '-'}`,
      dataRange: fmtRange(it?.data_start_date, it?.data_end_date),
      createdAt: fmtTime(it?.created_at),
      summary: truncate(it?.summary || it?.content_markdown || ''),
      status: it?.status,
    }
  })
})

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
