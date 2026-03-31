<template>
  <el-space direction="vertical" :size="12" fill>
    <el-empty v-if="!hasProjects" description="No enabled project" />
    <template v-else>
      <ReportsFilters @open-create="openCreate" />
      <ReportsTable />
    </template>
  </el-space>

  <ReportCreateDrawer />
  <ReportDetailDrawer />
  <ReportEvidenceDialog />
</template>

<script setup>
import { computed } from 'vue'
import { useProjectsStore } from '../stores/projects'
import { useReportsStore } from '../stores/reports'

import ReportsFilters from '../components/reports/ReportsFilters.vue'
import ReportsTable from '../components/reports/ReportsTable.vue'
import ReportCreateDrawer from '../components/reports/ReportCreateDrawer.vue'
import ReportDetailDrawer from '../components/reports/ReportDetailDrawer.vue'
import ReportEvidenceDialog from '../components/reports/ReportEvidenceDialog.vue'

const projectsStore = useProjectsStore()
const hasProjects = computed(() => projectsStore.projects.length > 0)
const store = useReportsStore()

function openCreate() {
  store.createPrefill = null
  store.createOpen = true
}
</script>
