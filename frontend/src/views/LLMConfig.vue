<!-- 作用：前端页面：大模型配置（全局任务模型配置 + 提示词模板配置） -->

<template>
  <div class="pw-page-fit">
    <el-alert title="全局大模型配置" type="info" :closable="false" show-icon>
      <template #default>
        <div>当前配置对所有项目生效</div>
        <div>仅影响后续新任务</div>
      </template>
    </el-alert>

    <el-space wrap>
      <el-button :loading="loading" @click="loadAll">刷新</el-button>
      <el-button type="primary" :loading="saving" :disabled="loading || rows.length === 0" @click="saveAll">
        保存配置
      </el-button>
      <el-button :loading="saving" :disabled="loading || rows.length === 0" @click="restoreCheapDefaults">
        恢复默认省钱方案
      </el-button>
      <el-text v-if="error" type="danger">{{ error }}</el-text>
    </el-space>

    <div class="pw-grow table-wrap">
      <el-table v-loading="loading" :data="rows" border style="width: 100%" height="100%">
        <el-table-column prop="task_type" label="任务类型" width="190" />
        <el-table-column prop="title" label="说明" min-width="170" />

        <el-table-column label="模型提供方" width="160">
          <template #default="{ row }">
            <el-select v-model="row.draft.provider" style="width: 130px" filterable>
              <el-option v-for="p in providers" :key="p" :label="p" :value="p" />
            </el-select>
          </template>
        </el-table-column>

        <el-table-column label="模型" min-width="200">
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
              <el-option v-for="m in modelsFor(row.draft.provider)" :key="m" :label="m" :value="m" />
            </el-select>
          </template>
        </el-table-column>

        <el-table-column label="当前生效" width="170">
          <template #default="{ row }">
            <el-tag type="info">{{ row.effectiveLabel }}</el-tag>
          </template>
        </el-table-column>
      </el-table>
    </div>

    <el-collapse v-model="openPanels" class="prompt-collapse">
      <el-collapse-item name="prompts">
        <template #title>
          <span>提示词配置</span>
          <el-tag v-if="promptError" type="danger" style="margin-left: 10px">加载失败</el-tag>
          <el-tag v-else-if="promptsLoading" type="info" style="margin-left: 10px">加载中</el-tag>
        </template>

        <el-space direction="vertical" :size="12" fill>
          <el-alert type="warning" :closable="false" show-icon>
            <template #title>提示词模板会影响后端真实调用内容，请谨慎修改</template>
            <template #default>
              <div>显示内容与后端实际使用版本一致（来源：backend/llm/prompts/templates/*.json）</div>
              <div>提示词不可为空；保存后立即生效（同一后端进程）</div>
            </template>
          </el-alert>

          <el-space wrap>
            <el-button :loading="promptsLoading" @click="loadPrompts">刷新提示词</el-button>
            <el-text v-if="promptError" type="danger">{{ promptError }}</el-text>
          </el-space>

          <el-tabs v-model="activePromptTaskType" type="border-card" class="pw-grow">
            <el-tab-pane
              v-for="t in promptTabs"
              :key="t.task_type"
              :name="t.task_type"
              :label="t.title || t.task_type"
            >
              <template v-if="activePrompt">
                <el-space direction="vertical" :size="10" fill>
                  <el-space wrap>
                    <el-tag>版本：{{ activePrompt.version || '-' }}</el-tag>
                    <el-tag type="info">修改时间：{{ activePrompt.updated_at || '-' }}</el-tag>
                  </el-space>

                  <div class="prompt-grid">
                    <div class="prompt-block">
                      <div class="block-title">输出格式（只读 JSON）</div>
                      <el-input
                        :model-value="activeSections.outputJson"
                        type="textarea"
                        readonly
                        :autosize="{ minRows: 6, maxRows: 14 }"
                      />
                    </div>
                    <div class="prompt-block">
                      <div class="block-title">输入格式（只读 JSON）</div>
                      <el-input
                        :model-value="activeSections.inputJson"
                        type="textarea"
                        readonly
                        :autosize="{ minRows: 6, maxRows: 14 }"
                      />
                    </div>
                    <div class="prompt-block span-2">
                      <div class="block-title">提示词（可编辑）</div>
                      <el-form label-position="top">
                        <el-form-item :error="promptErrorText">
                          <el-input
                            v-model="draftPrompt.prompt"
                            type="textarea"
                            :autosize="{ minRows: 10, maxRows: 24 }"
                            placeholder="提示词不可为空"
                          />
                        </el-form-item>
                      </el-form>
                    </div>
                  </div>

                  <el-space wrap>
                    <el-button
                      type="primary"
                      :loading="promptSaving"
                      :disabled="!canSavePrompt"
                      @click="savePrompt"
                    >
                      保存该任务提示词
                    </el-button>
                    <el-button :disabled="promptSaving" @click="confirmResetPrompt">重置为后端当前版本</el-button>
                    <el-text type="info">task_type：{{ activePrompt.task_type }}</el-text>
                  </el-space>
                </el-space>
              </template>
              <el-empty v-else description="暂无提示词数据" />
            </el-tab-pane>
          </el-tabs>
        </el-space>
      </el-collapse-item>
    </el-collapse>
  </div>
</template>

<script setup>
import { ElMessage, ElMessageBox } from 'element-plus'
import { computed, onMounted, reactive, ref, watch } from 'vue'

import { fetchLLMConfig, fetchLLMModels, fetchLLMPrompts, putLLMConfig, putLLMPrompt } from '../api/llmConfig'

const loading = ref(false)
const saving = ref(false)
const error = ref('')

const providers = ref([])
const modelsByProvider = ref({})
const cheapDefaults = ref({})
const rows = ref([])

const openPanels = ref(['prompts'])

const promptsLoading = ref(false)
const promptSaving = ref(false)
const promptError = ref('')
const promptsByTask = ref({})
const activePromptTaskType = ref('')

const draftPrompt = reactive({ prompt: '' })

let currentController = null
let promptController = null

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

async function loadAll() {
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

    // Init prompt tab to the first task (if not already selected).
    if (!activePromptTaskType.value && rows.value.length) activePromptTaskType.value = rows.value[0].task_type
  } catch (e) {
    if (e?.name === 'AbortError') return
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
    await loadAll()
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
    await loadAll()
  } catch (e) {
    ElMessage.error(e?.message || String(e))
  } finally {
    saving.value = false
  }
}

async function loadPrompts() {
  if (promptController) promptController.abort()
  promptController = new AbortController()

  promptsLoading.value = true
  promptError.value = ''
  try {
    const data = await fetchLLMPrompts({ signal: promptController.signal })
    const items = Array.isArray(data?.items) ? data.items : []
    const map = {}
    for (const it of items) {
      const tt = String(it?.task_type || '')
      if (!tt) continue
      map[tt] = {
        task_type: tt,
        version: String(it?.version || ''),
        template: String(it?.template || ''),
        updated_at: it?.updated_at ? String(it.updated_at) : '',
      }
    }
    promptsByTask.value = map

    // Select a default tab when prompts arrive.
    if (!activePromptTaskType.value) {
      const keys = Object.keys(map)
      activePromptTaskType.value = keys.length ? keys[0] : ''
    } else if (!map?.[activePromptTaskType.value]) {
      const keys = Object.keys(map)
      activePromptTaskType.value = keys.length ? keys[0] : ''
    }
    resetPromptDraft()
  } catch (e) {
    if (e?.name === 'AbortError') return
    promptError.value = e?.message || String(e)
  } finally {
    promptsLoading.value = false
  }
}

const activePrompt = computed(() => {
  const tt = String(activePromptTaskType.value || '')
  return tt ? promptsByTask.value?.[tt] : null
})

const titleByTaskType = computed(() => {
  const m = {}
  for (const r of rows.value || []) {
    const tt = String(r?.task_type || '')
    if (!tt) continue
    m[tt] = String(r?.title || '')
  }
  return m
})

const promptTabs = computed(() => {
  const map = promptsByTask.value || {}
  const keys = Object.keys(map).sort()
  if (!keys.length) {
    return (rows.value || []).map((r) => ({ task_type: r.task_type, title: r.title }))
  }
  return keys.map((tt) => ({ task_type: tt, title: titleByTaskType.value?.[tt] || '' }))
})

function splitTemplateSections(template) {
  const s = String(template || '')
  const OUT_S = '[OUTPUT_JSON]'
  const OUT_E = '[/OUTPUT_JSON]'
  const IN_S = '[INPUT_JSON]'
  const IN_E = '[/INPUT_JSON]'
  const P_S = '[PROMPT]'
  const P_E = '[/PROMPT]'

  try {
    const o1 = s.indexOf(OUT_S) + OUT_S.length
    const o2 = s.indexOf(OUT_E, o1)
    const i1 = s.indexOf(IN_S, o2) + IN_S.length
    const i2 = s.indexOf(IN_E, i1)
    const p1 = s.indexOf(P_S, i2) + P_S.length
    const p2 = s.indexOf(P_E, p1)
    return {
      outputJson: s.slice(o1, o2).trim(),
      inputJson: s.slice(i1, i2).trim(),
      prompt: s.slice(p1, p2).trim(),
    }
  } catch {
    return { outputJson: '', inputJson: '', prompt: s.trim() }
  }
}

const activeSections = computed(() => splitTemplateSections(activePrompt.value?.template))

function resetPromptDraft() {
  draftPrompt.prompt = String(activeSections.value?.prompt || '')
}

watch(
  () => activePromptTaskType.value,
  () => resetPromptDraft()
)

const promptErrorText = computed(() => {
  if (!openPanels.value.includes('prompts')) return ''
  return String(draftPrompt.prompt || '').trim() === '' ? '提示词不可为空' : ''
})
const canSavePrompt = computed(() => !promptErrorText.value && !!activePrompt.value)

async function savePrompt() {
  const p = activePrompt.value
  if (!p) return

  const prompt = String(draftPrompt.prompt || '')
  if (String(prompt).trim() === '') {
    ElMessage.error('提示词不可为空')
    return
  }

  promptSaving.value = true
  try {
    const res = await putLLMPrompt(p.task_type, { prompt })
    const it = res?.item
    if (it?.task_type) {
      promptsByTask.value = {
        ...(promptsByTask.value || {}),
        [String(it.task_type)]: {
          task_type: String(it.task_type),
          version: String(it.version || ''),
          template: String(it.template || ''),
          updated_at: it.updated_at ? String(it.updated_at) : '',
        },
      }
      resetPromptDraft()
    } else {
      await loadPrompts()
    }
    ElMessage.success('提示词已保存并生效')
  } catch (e) {
    ElMessage.error(e?.message || String(e))
  } finally {
    promptSaving.value = false
  }
}

async function confirmResetPrompt() {
  try {
    await ElMessageBox.confirm('将丢弃本地未保存的修改，并从后端重新加载当前生效的提示词。是否继续？', '确认重置', {
      type: 'warning',
      confirmButtonText: '继续',
      cancelButtonText: '取消',
    })
  } catch {
    return
  }
  await loadPrompts()
  resetPromptDraft()
}

onMounted(async () => {
  await loadAll()
  await loadPrompts()
})
</script>

<style scoped>
.table-wrap {
  min-height: 260px;
}
.prompt-collapse :deep(.el-collapse-item__header) {
  font-weight: 700;
}
.prompt-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}
.prompt-block {
  min-width: 0;
}
.prompt-block.span-2 {
  grid-column: span 2;
}
.block-title {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-bottom: 6px;
}

@media (max-width: 900px) {
  .prompt-grid {
    grid-template-columns: 1fr;
  }
  .prompt-block.span-2 {
    grid-column: auto;
  }
}
</style>
