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
const success = ref('')
const router = useRouter()

const submit = async () => {
  loading.value = true
  error.value = ''
  success.value = ''
  try {
    await api.post('/api/auth/register', {
      email: email.value,
      phone: phone.value || null,
      username: username.value,
      password: password.value,
    })
    success.value = '注册成功，正在跳转登录页...'
    setTimeout(() => router.push('/login'), 800)
  } catch (e) {
    error.value = e?.response?.data?.detail || '注册失败，请检查输入'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="auth-page">
    <el-card class="auth-card" shadow="hover">
      <template #header>
        <div class="header">注册</div>
      </template>
      <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
      <el-alert v-if="success" :title="success" type="success" show-icon :closable="false" />
      <el-form label-position="top" class="form">
        <el-form-item label="邮箱">
          <el-input v-model="email" placeholder="请输入邮箱" />
        </el-form-item>
        <el-form-item label="手机号（可选）">
          <el-input v-model="phone" placeholder="请输入手机号" />
        </el-form-item>
        <el-form-item label="用户名">
          <el-input v-model="username" placeholder="请输入用户名" />
        </el-form-item>
        <el-form-item label="密码">
          <el-input v-model="password" type="password" show-password placeholder="请输入密码" />
        </el-form-item>
        <el-button type="primary" :loading="loading" @click="submit">{{ loading ? '提交中...' : '注册' }}</el-button>
      </el-form>
      <p>已有账号？<router-link to="/login">去登录</router-link></p>
    </el-card>
  </div>
</template>

<style scoped>
.auth-page {
  max-width: 420px;
  margin: 60px auto;
}

.auth-card {
  border-radius: 12px;
}

.header {
  font-size: 18px;
  font-weight: 600;
}

.form {
  margin: 14px 0 6px;
}
</style>
