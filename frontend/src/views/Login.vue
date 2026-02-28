<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore } from '../stores/auth'

const username = ref('admin')
const password = ref('admin123')
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
    error.value = e?.response?.data?.detail || '登录失败，请检查账号密码'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="auth-page">
    <el-card class="auth-card" shadow="hover">
      <template #header>
        <div class="header">登录</div>
      </template>
      <p class="hint">默认账号：admin / admin123</p>
      <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
      <el-form label-position="top" class="form">
        <el-form-item label="用户名">
          <el-input v-model="username" placeholder="请输入用户名" />
        </el-form-item>
        <el-form-item label="密码">
          <el-input v-model="password" type="password" show-password placeholder="请输入密码" />
        </el-form-item>
        <el-button type="primary" :loading="loading" @click="submit">{{ loading ? '提交中...' : '登录' }}</el-button>
      </el-form>
      <p>没有账号？<router-link to="/register">去注册</router-link></p>
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

.hint {
  font-size: 12px;
  color: #6b7280;
  margin-bottom: 10px;
}
</style>
