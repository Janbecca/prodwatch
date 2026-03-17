import { createRouter, createWebHistory } from 'vue-router'
import RawPostList from './components/RawPostList.vue'
import Login from './views/Login.vue'
import Register from './views/Register.vue'
import { consumeNextNavigationSource, setLastTransition } from './navContext'

const Dashboard = () => import('./views/Dashboard.vue')
const Reports = () => import('./views/Reports.vue')
const DebugConsole = () => import('./views/DebugConsole.vue')
const Projects = () => import('./views/Projects.vue')

const routes = [
  { path: '/', redirect: '/dashboard' },
  { path: '/login', component: Login },
  { path: '/register', component: Register },
  { path: '/dashboard', component: Dashboard, meta: { requiresAuth: true } },
  { path: '/posts', component: RawPostList, meta: { requiresAuth: true } },
  { path: '/report', component: Reports, meta: { requiresAuth: true } },
  { path: '/projects', component: Projects, meta: { requiresAuth: true } },
  { path: '/debug', component: DebugConsole, meta: { requiresAuth: true } },
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

router.afterEach((to, from) => {
  setLastTransition({
    toFullPath: to.fullPath,
    fromFullPath: from?.fullPath,
    source: consumeNextNavigationSource(),
  })
})

export default router
