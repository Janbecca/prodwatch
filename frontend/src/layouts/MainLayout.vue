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
        <RouterView />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup>
import { onMounted } from 'vue'
import { RouterView } from 'vue-router'
import SidebarNav from '../components/SidebarNav.vue'
import { useProjectsStore } from '../stores/projects'

const projectsStore = useProjectsStore()
onMounted(() => {
  projectsStore.fetchProjects()
})
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
</style>
