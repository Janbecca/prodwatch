import { defineStore } from 'pinia'
import api from '../api/axios'

export const useProjectsStore = defineStore('projects', {
  state: () => ({
    projects: [],
    activeProjectId: Number(localStorage.getItem('activeProjectId') || '') || null,
    loading: false,
    error: '',
  }),
  getters: {
    enabledProjects(state) {
      return (state.projects || []).filter((p) => Boolean(p.is_active))
    },
    activeProject(state) {
      const id = state.activeProjectId
      if (!id) return null
      return (state.projects || []).find((p) => p.id === id) || null
    },
    enabledCount() {
      return this.enabledProjects.length
    },
  },
  actions: {
    async fetch() {
      this.loading = true
      this.error = ''
      try {
        const { data } = await api.get('/api/projects')
        this.projects = data || []
        const enabled = this.enabledProjects
        if (enabled.length === 0) {
          this.activeProjectId = null
        } else if (!this.activeProjectId || !enabled.some((p) => p.id === this.activeProjectId)) {
          this.activeProjectId = enabled[0].id
        }

        if (this.activeProjectId) localStorage.setItem('activeProjectId', String(this.activeProjectId))
        else localStorage.removeItem('activeProjectId')
      } catch (e) {
        this.error = e?.response?.data?.detail || e?.message || '加载项目失败'
      } finally {
        this.loading = false
      }
    },
    setActiveProjectId(id) {
      const n = Number(id)
      this.activeProjectId = Number.isFinite(n) ? n : null
      if (this.activeProjectId) localStorage.setItem('activeProjectId', String(this.activeProjectId))
      else localStorage.removeItem('activeProjectId')
    },
  },
})

