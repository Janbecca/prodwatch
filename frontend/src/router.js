import { createRouter, createWebHistory } from 'vue-router'
import AnalysisList from './components/AnalysisList.vue'
import RawPostList from './components/RawPostList.vue'
import ReportDownload from './components/ReportDownload.vue'
import Login from './views/Login.vue'
import Register from './views/Register.vue'

const Dashboard = () => import('./views/Dashboard.vue')
const Analysis = () => import('./views/Analysis.vue')
const Moderation = () => import('./views/Moderation.vue')
const Reports = () => import('./views/Reports.vue')
const Settings = () => import('./views/Settings.vue')

const routes = [
  { path: '/', redirect: '/dashboard' },
  { path: '/login', component: Login },
  { path: '/register', component: Register },
  { path: '/dashboard', component: Dashboard, meta: { requiresAuth: true } },
  { path: '/analysis', component: Analysis, meta: { requiresAuth: true } },
  { path: '/posts', component: RawPostList, meta: { requiresAuth: true } },
  { path: '/moderation', component: Moderation, meta: { requiresAuth: true } },
  { path: '/report', component: Reports, meta: { requiresAuth: true } },
  { path: '/settings', component: Settings, meta: { requiresAuth: true } },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

router.beforeEach((to, _from, next) => {
  if (to.meta.requiresAuth) {
    const token = localStorage.getItem('token')
    if (!token) return next('/login')
  }
  next()
})

export default router
