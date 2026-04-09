// 作用：前端路由：页面路由表与导航守卫配置。

import { createRouter, createWebHistory } from 'vue-router'

import MainLayout from '../layouts/MainLayout.vue'
import Dashboard from '../views/Dashboard.vue'
import Posts from '../views/Posts.vue'
import Reports from '../views/Reports.vue'
import ReportDetail from '../views/ReportDetail.vue'
import ProjectConfig from '../views/ProjectConfig.vue'
import LLMConfig from '../views/LLMConfig.vue'

const routes = [
  {
    path: '/',
    component: MainLayout,
    redirect: '/dashboard',
    children: [
      { path: 'dashboard', name: 'dashboard', component: Dashboard },
      { path: 'posts', name: 'posts', component: Posts },
      { path: 'reports', name: 'reports', component: Reports },
      { path: 'reports/:id', name: 'report-detail', component: ReportDetail },
      { path: 'project-config', name: 'project-config', component: ProjectConfig },
      { path: 'llm-config', name: 'llm-config', component: LLMConfig },
    ],
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

export default router
