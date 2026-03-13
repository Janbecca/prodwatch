import { defineStore } from 'pinia'
import api from '../api/axios'

export const useAuthStore = defineStore('auth', {
  state: () => ({ token: localStorage.getItem('token') || '', user: null }),
  actions: {
    async login(username, password) {
      const { data } = await api.post('/api/auth/login_json', { username, password })
      this.token = data.access_token
      localStorage.setItem('token', this.token)
      await this.fetchMe()
    },
    async register(payload) {
      await api.post('/api/auth/register', payload)
    },
    async fetchMe() {
      const { data } = await api.get('/api/auth/me')
      this.user = data
    },
    logout() {
      this.token = ''
      this.user = null
      localStorage.removeItem('token')
    },
  },
})
