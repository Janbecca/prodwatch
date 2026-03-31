<template>
  <el-space direction="vertical" :size="12" fill>
    <el-empty v-if="!hasProjects" description="No enabled project" />
    <template v-else>
      <DashboardFilters />
      <DashboardOverviewCards />
      <SentimentTrendPanel />
      <KeywordMonitorPanel />
    </template>
  </el-space>
</template>

<script setup>
import { computed } from 'vue'
import { useProjectsStore } from '../stores/projects'

import DashboardFilters from '../components/dashboard/DashboardFilters.vue'
import DashboardOverviewCards from '../components/dashboard/DashboardOverviewCards.vue'
import SentimentTrendPanel from '../components/dashboard/SentimentTrendPanel.vue'
import KeywordMonitorPanel from '../components/dashboard/KeywordMonitorPanel.vue'

const projectsStore = useProjectsStore()
const hasProjects = computed(() => projectsStore.projects.length > 0)
</script>

