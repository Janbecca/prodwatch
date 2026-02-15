<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore } from '../stores/auth'

const username = ref('')
const password = ref('')
const loading = ref(false)
const error = ref('')
const router = useRouter()
const auth = useAuthStore()

const submit = async () => {
  loading.value = true
  error.value = ''
  try {
    await auth.login(username.value, password.value)
    router.push('/dashboard')
  } catch (e) {
    error.value = '登录失败'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div style="max-width:360px;margin:60px auto;">
    <h2>登录</h2>
    <div v-if="error" style="color:red">{{ error }}</div>
    <div>
      <input v-model="username" placeholder="用户名" style="width:100%;margin:8px 0;padding:8px;" />
      <input v-model="password" type="password" placeholder="密码" style="width:100%;margin:8px 0;padding:8px;" />
      <button :disabled="loading" @click="submit" style="width:100%;padding:10px;">
        {{ loading ? '登录中...' : '登录' }}
      </button>
    </div>
    <p style="margin-top:12px;">
      还没有账号？<router-link to="/register">去注册</router-link>
    </p>
  </div>
</template>
