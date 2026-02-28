<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthStore } from './stores/auth'

const auth = useAuthStore()
const router = useRouter()
const route = useRoute()
const isAuthed = computed(() => Boolean(auth.token || localStorage.getItem('token')))
const showShell = computed(() => !['/login', '/register'].includes(route.path))
const activeMenu = computed(() => route.path)

const menus = [
  { path: '/dashboard', title: '仪表盘' },
  { path: '/analysis', title: '分析调试' },
  { path: '/posts', title: '帖子浏览' },
  { path: '/moderation', title: '水军识别' },
  { path: '/report', title: '报告中心' },
  { path: '/settings', title: '系统设置' },
  { path: '/debug', title: '接口控制台' },
]

const logout = () => {
  auth.logout()
  router.push('/login')
}
</script>

<template>
  <div v-if="showShell" class="app-shell">
    <el-container class="shell-container">
      <el-aside width="220px" class="sidebar">
        <div class="brand">
          <div class="brand-title">ProdWatch</div>
          <div class="brand-sub">舆情联动调试台</div>
        </div>
        <el-menu :default-active="activeMenu" router class="menu">
          <el-menu-item v-for="item in menus" :key="item.path" :index="item.path">
            {{ item.title }}
          </el-menu-item>
        </el-menu>
      </el-aside>
      <el-container>
        <el-header class="header">
          <div>当前页面：{{ menus.find((m) => m.path === activeMenu)?.title || '未知' }}</div>
          <div class="auth-actions">
            <el-button v-if="!isAuthed" type="primary" plain @click="router.push('/login')">登录</el-button>
            <el-button v-if="!isAuthed" plain @click="router.push('/register')">注册</el-button>
            <el-button v-if="isAuthed" type="danger" plain @click="logout">退出登录</el-button>
          </div>
        </el-header>
        <el-main class="main">
          <router-view />
        </el-main>
      </el-container>
    </el-container>
  </div>
  <div v-else class="auth-layout">
    <div class="auth-top">
      <router-link to="/login">登录</router-link>
      <router-link to="/register">注册</router-link>
    </div>
    <main class="auth-main">
      <router-view />
    </main>
  </div>
</template>

<style scoped>
.app-shell {
  min-height: 100vh;
}

.shell-container {
  min-height: 100vh;
}

.sidebar {
  border-right: 1px solid #ebeef5;
  background: linear-gradient(180deg, #0f172a 0%, #111827 100%);
  color: #e5e7eb;
  padding-top: 10px;
}

.brand {
  padding: 8px 16px 14px;
}

.brand-title {
  font-size: 20px;
  font-weight: 700;
  letter-spacing: 0.5px;
}

.brand-sub {
  margin-top: 4px;
  font-size: 12px;
  color: #93c5fd;
}

.menu {
  border-right: none;
  background: transparent;
}

:deep(.el-menu-item) {
  color: #d1d5db;
}

:deep(.el-menu-item:hover) {
  background: rgba(255, 255, 255, 0.08);
}

:deep(.el-menu-item.is-active) {
  color: #fff;
  background: rgba(37, 99, 235, 0.45);
}

.header {
  border-bottom: 1px solid #ebeef5;
  background: #fff;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.auth-actions {
  display: flex;
  gap: 8px;
}

.main {
  background: #f4f6fb;
}

.auth-layout {
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}

.auth-top {
  padding: 10px 16px;
  border-bottom: 1px solid #ebeef5;
  background: #fff;
  display: flex;
  gap: 12px;
  justify-content: flex-end;
}

.auth-main {
  flex: 1;
  background: radial-gradient(circle at top left, #dbeafe, #f4f6fb 45%);
}
</style>
