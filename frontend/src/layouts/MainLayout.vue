<!-- 作用：前端布局：页面通用布局与导航框架。 -->

<template>
  <el-container class="app-shell">
      <el-aside class="app-aside" width="220px">
        <div class="brand">
        <div class="brand__title">产品舆情监测</div>
        <div class="brand__sub">舆情系统</div>
      </div>
      <SidebarNav />
    </el-aside>

    <el-container>
      <el-header class="app-header">
        <div class="header-left">
          <el-text tag="b">舆情系统</el-text>
          <el-divider direction="vertical" />
          <!-- <el-text type="info">前端骨架（模拟数据）</el-text> -->
        </div>
        <div class="header-right">
          <el-tag type="info">版本 0</el-tag>
        </div>
      </el-header>

      <el-main class="app-main">
        <el-alert
          v-if="showRefreshBanner"
          type="warning"
          :closable="false"
          show-icon
          class="refresh-banner"
        >
          <template #title>
            <el-space :size="8" alignment="center" wrap>
              <span class="spin-dot" />
              <span>项目正在刷新中</span>
              <span v-if="bannerJobId">（任务编号={{ bannerJobId }}）</span>
              <span v-if="bannerStartedAt">开始时间={{ bannerStartedAt }}</span>
            </el-space>
          </template>
        </el-alert>

        <div class="main-content">
          <RouterView />
        </div>
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup>
import { computed, onMounted, watch } from 'vue'
import { RouterView } from 'vue-router'
import SidebarNav from '../components/SidebarNav.vue'
import { useProjectsStore } from '../stores/projects'
import { useRefreshStore } from '../stores/refresh'

const projectsStore = useProjectsStore()
const refreshStore = useRefreshStore()

const activeProjectId = computed(() => projectsStore.activeProjectId)
const activeRefresh = computed(() => refreshStore.getState(activeProjectId.value))
const isRefreshing = computed(() => refreshStore.isRefreshing(activeProjectId.value))

const showRefreshBanner = computed(() => Boolean(isRefreshing.value))

const bannerJobId = computed(() => {
  const st = activeRefresh.value
  const n = Number(st?.crawl_job_id)
  return Number.isFinite(n) && n > 0 ? n : null
})
const bannerStartedAt = computed(() => {
  const st = activeRefresh.value
  return st?.started_at ? String(st.started_at) : ''
})

onMounted(() => {
  projectsStore.fetchProjects()
})

// Keep refresh state in sync with current active project.
watch(
  () => activeProjectId.value,
  async (pid) => {
    if (!pid) return
    const res = await refreshStore.syncStatus(pid)
    if (res?.running) {
      refreshStore.startRefreshing(pid, { bannerAck: false })
      refreshStore.clearOptimistic(pid)
    } else {
      refreshStore.stopRefreshing(pid)
    }
  },
  { immediate: true }
)
</script>

<style scoped>
.app-shell {
  height: 100vh;
}
.app-aside {
  border-right: 1px solid var(--el-border-color);
  background: var(--el-bg-color);
  padding: 12px 10px;
}
.brand {
  padding: 8px 10px 12px;
}
.brand__title {
  font-weight: 700;
  font-size: 16px;
}
.brand__sub {
  margin-top: 2px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.app-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--el-border-color);
  background: var(--el-bg-color);
}
.header-right {
  display: flex;
  align-items: center;
  gap: 8px;
}
.app-main {
  background: var(--el-bg-color-page);
}
.refresh-banner {
  margin-bottom: 10px;
}
.main-content {
  min-height: calc(100vh - 120px);
}
.spin-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  border: 2px solid var(--el-color-warning-light-5);
  border-top-color: var(--el-color-warning);
  display: inline-block;
  animation: spin 0.9s linear infinite;
}
@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
