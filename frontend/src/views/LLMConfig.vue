<!-- 作用：前端页面：LLM 配置视图。 -->

<template>
  <el-space direction="vertical" :size="12" fill>
    <el-alert title="全局大模型配置" type="info" :closable="false" show-icon>
      <template #default>
        <div>当前配置对所有项目生效</div>
        <div>仅影响后续新任务</div>
      </template>
    </el-alert>

    <el-space wrap>
      <el-button :loading="loading" @click="load">刷新</el-button>
      <el-button type="primary" :loading="saving" :disabled="loading || rows.length === 0" @click="saveAll">
        保存配置
      </el-button>
      <el-button :loading="saving" :disabled="loading || rows.length === 0" @click="restoreCheapDefaults">
        恢复默认省钱方案
      </el-button>
      <el-text v-if="error" type="danger">{{ error }}</el-text>
    </el-space>

    <el-table v-loading="loading" :data="rows" border style="width: 100%">
      <el-table-column prop="task_type" label="任务类型" width="180" />
      <el-table-column prop="title" label="说明" min-width="170" />

      <el-table-column label="模型提供方" width="140">
        <template #default="{ row }">
          <el-select v-model="row.draft.provider" style="width: 120px" filterable>
            <el-option v-for="p in providers" :key="p" :label="p" :value="p" />
          </el-select>
        </template>
      </el-table-column>

      <el-table-column label="模型" min-width="170">
        <template #default="{ row }">
          <el-select
            v-model="row.draft.model"
            style="width: 100%"
            clearable
            filterable
            allow-create
            default-first-option
            placeholder="选择或输入模型（可留空）"
          >
            <el-option
              v-for="m in modelsFor(row.draft.provider)"
              :key="m"
              :label="m"
              :value="m"
            />
          </el-select>
        </template>
      </el-table-column>


      <el-table-column label="状态" width="140">
        <template #default="{ row }">
          <el-tag type="info">
            {{ row.effectiveLabel }}
          </el-tag>
        </template>
      </el-table-column>
    </el-table>
  </el-space>
</template>

<script setup>
import { ElMessage } from 'element-plus'
import { onMounted, ref } from 'vue'

import { fetchLLMConfig, fetchLLMModels, putLLMConfig } from '../api/llmConfig'

const loading = ref(false)
const saving = ref(false)
const error = ref('')
const providers = ref([])
const modelsByProvider = ref({})
const cheapDefaults = ref({})
const rows = ref([])

let currentController = null

function toDraft(effective) {
  return {
    provider: String(effective?.provider || ''),
    model: effective?.model == null ? null : String(effective.model),
  }
}

function normalizeDraft(draft, taskType) {
  return {
    task_type: String(taskType),
    provider: String(draft.provider || ''),
    model: draft.model == null || String(draft.model).trim() === '' ? null : String(draft.model).trim(),
  }
}

function modelsFor(provider) {
  const key = String(provider || '').trim().toLowerCase()
  const arr = modelsByProvider.value?.[key]
  return Array.isArray(arr) ? arr : []
}

async function load() {
  if (currentController) currentController.abort()
  currentController = new AbortController()

  loading.value = true
  error.value = ''
  try {
    const [modelsData, cfg] = await Promise.all([
      fetchLLMModels({ signal: currentController.signal }),
      fetchLLMConfig({ signal: currentController.signal }),
    ])
    providers.value = Array.isArray(modelsData?.providers) ? modelsData.providers : []
    modelsByProvider.value = modelsData?.models_by_provider || {}
    cheapDefaults.value = modelsData?.cheap_defaults || {}

    rows.value = (Array.isArray(cfg?.tasks) ? cfg.tasks : []).map((t) => {
      const c = t?.config || {}
      const provider = String(c?.provider || '')
      const model = c?.model == null ? null : String(c.model)
      return {
        task_type: String(t?.task_type || ''),
        title: String(t?.title || ''),
        draft: toDraft(c),
        effectiveLabel: `${provider}${model ? ` / ${model}` : ''}`,
      }
    })
  } catch (e) {
    error.value = e?.message || String(e)
  } finally {
    loading.value = false
  }
}

async function saveAll() {
  saving.value = true
  try {
    const items = rows.value.map((r) => normalizeDraft(r.draft, r.task_type))
    await putLLMConfig(items)
    ElMessage.success('已保存')
    await load()
  } catch (e) {
    ElMessage.error(e?.message || String(e))
  } finally {
    saving.value = false
  }
}

async function restoreCheapDefaults() {
  saving.value = true
  try {
    const defs = cheapDefaults.value || {}
    const items = rows.value.map((r) => {
      const d = defs?.[r.task_type] || {}
      return {
        task_type: r.task_type,
        provider: String(d?.provider || ''),
        model: d?.model == null ? null : String(d.model),
      }
    })
    await putLLMConfig(items)
    ElMessage.success('已恢复默认省钱方案')
    await load()
  } catch (e) {
    ElMessage.error(e?.message || String(e))
  } finally {
    saving.value = false
  }
}

onMounted(load)
</script>
