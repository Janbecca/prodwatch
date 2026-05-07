<!-- 作用：前端组件：侧边栏导航（SidebarNav） -->

<template>
  <el-menu :default-active="activePath" class="nav" router>
    <el-menu-item index="/dashboard"><span>仪表盘</span></el-menu-item>
    <el-menu-item index="/posts"><span>帖子浏览</span></el-menu-item>
    <el-menu-item index="/reports"><span>报告中心</span></el-menu-item>
    <el-menu-item index="/project-config"><span>项目配置</span></el-menu-item>
    <el-menu-item index="/llm-config"><span>智能体配置</span></el-menu-item>
    <el-menu-item index="/system-overview"><span>系统功能概览</span></el-menu-item>
  </el-menu>
</template>

<script setup>
import { computed } from 'vue'
import { useRoute } from 'vue-router'

const route = useRoute()
const activePath = computed(() => route.path)
</script>

<style scoped>
.nav {
  border-right: 0;
}
</style>

