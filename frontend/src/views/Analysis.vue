<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import api from '../api/axios'

const router = useRouter()
const projectId = ref(1)
const platform = ref(['weibo'])
const keyword = ref('萤石')
const model = ref('rule-based')
const loading = ref(false)
const error = ref('')
const response = ref(null)

const runAnalysis = async () => {
  loading.value = true
  error.value = ''
  response.value = null
  try {
    const { data } = await api.post('/api/analysis/run', null, {
      params: {
        project_id: projectId.value,
        platform: platform.value,
        keyword: keyword.value,
        model: model.value,
      },
    })
    response.value = data
  } catch (e) {
    error.value = e?.response?.data?.detail || '分析任务执行失败'
  } finally {
    loading.value = false
  }
}

const openLatestPosts = () => {
  router.push('/posts')
}
</script>

<template>
  <section class="page">
    <el-page-header content="分析任务调试" />
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
    <el-card class="panel" shadow="hover">
      <el-form label-position="top">
        <el-form-item label="项目 ID">
          <el-input-number v-model="projectId" :min="1" />
        </el-form-item>
        <el-form-item label="平台">
          <el-select v-model="platform" multiple placeholder="请选择平台" style="width: 320px">
            <el-option label="微博" value="weibo" />
            <el-option label="小红书" value="xhs" />
            <el-option label="抖音" value="douyin" />
          </el-select>
        </el-form-item>
        <el-form-item label="关键词">
          <el-input v-model="keyword" />
        </el-form-item>
        <el-form-item label="模型">
          <el-select v-model="model" style="width: 220px">
            <el-option label="规则模型" value="rule-based" />
            <el-option label="qwen-plus" value="qwen-plus" />
          </el-select>
        </el-form-item>
      </el-form>
      <div class="actions">
        <el-button type="primary" :loading="loading" @click="runAnalysis">{{ loading ? '执行中...' : '执行分析任务' }}</el-button>
        <el-button @click="openLatestPosts">查看最新帖子</el-button>
      </div>
    </el-card>

    <el-card class="panel" shadow="hover">
      <h3>接口返回</h3>
      <pre>{{ response }}</pre>
    </el-card>
  </section>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.panel { border-radius: 10px; }

.actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

</style>
