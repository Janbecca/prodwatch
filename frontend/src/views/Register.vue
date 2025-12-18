<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import api from '../api/axios'

const email = ref('')
const phone = ref('')
const username = ref('')
const password = ref('')
const loading = ref(false)
const error = ref('')
const router = useRouter()

const submit = async () => {
  loading.value = true
  error.value = ''
  try {
    await api.post('/api/auth/register', { email: email.value, phone: phone.value, username: username.value, password: password.value })
    router.push('/login')
  } catch (e) {
    error.value = '注册失败'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div style="max-width:360px;margin:60px auto;">
    <h2>注册</h2>
    <div v-if="error" style="color:red">{{ error }}</div>
    <div>
      <input v-model="email" placeholder="邮箱" style="width:100%;margin:8px 0;padding:8px;" />
      <input v-model="phone" placeholder="手机号（可选）" style="width:100%;margin:8px 0;padding:8px;" />
      <input v-model="username" placeholder="用户名" style="width:100%;margin:8px 0;padding:8px;" />
      <input v-model="password" type="password" placeholder="密码" style="width:100%;margin:8px 0;padding:8px;" />
      <button :disabled="loading" @click="submit" style="width:100%;padding:10px;">{{ loading ? '提交中...' : '注册' }}</button>
    </div>
    <p style="margin-top:12px;">
      已有账号？<router-link to="/login">去登录</router-link>
    </p>
  </div>
</template>
