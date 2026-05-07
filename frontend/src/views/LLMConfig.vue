<!-- 作用：前端页面：大模型与提示词配置（按任务一一对应） -->

<template>
  <div class="pw-page-fit" v-loading="loading">

    <el-space wrap class="top-actions">
      <el-button :loading="loading || promptsLoading" @click="refreshAll">刷新</el-button>
      <el-button
        type="primary"
        :loading="saving"
        :disabled="loading || saving || tasks.length === 0"
        @click="saveAllModels"
      >
        保存模型配置
      </el-button>
      <el-button :loading="saving" :disabled="loading || saving || tasks.length === 0" @click="restoreCheapDefaults">
        恢复默认省钱方案
      </el-button>
      <!-- <el-tag type="success" effect="plain">可配置：帖子分析、报告生成</el-tag> -->
      <el-text v-if="error" type="danger">{{ error }}</el-text>
      <el-text v-else-if="promptError" type="danger">{{ promptError }}</el-text>
    </el-space>


    <el-row v-if="taskCards.length" :gutter="8" class="task-grid">
      <el-col v-for="t in taskCards" :key="t.task_type" :xs="24" :md="12" :xl="12">
        <el-card shadow="hover" class="task-card" :body-style="{ padding: '10px' }">
          <template #header>
            <div class="card-header">
              <div class="card-title">
                <div class="title">{{ t.title || t.task_type }}</div>
                <div class="sub muted">{{ t.task_type }}</div>
              </div>
              <el-tag type="info" effect="plain">全局</el-tag>
            </div>
          </template>

          <el-space direction="vertical" :size="12" fill>
            <div class="section">
              <div class="section-title">模型配置</div>
              <el-form label-width="90px" class="model-form">
                <el-form-item label="提供方">
                  <el-select v-model="t.draft.provider" style="width: 100%" filterable>
                    <el-option v-for="p in providers" :key="p" :label="p" :value="p" />
                  </el-select>
                </el-form-item>
                <el-form-item label="模型">
                  <el-select
                    v-model="t.draft.model"
                    style="width: 100%"
                    clearable
                    filterable
                    allow-create
                    default-first-option
                    placeholder="选择或输入模型（可留空）"
                  >
                    <el-option v-for="m in modelsFor(t.draft.provider)" :key="m" :label="m" :value="m" />
                  </el-select>
                </el-form-item>
              </el-form>

              <el-space wrap>
                <el-button
                  type="primary"
                  size="small"
                  :loading="modelSavingByTask[t.task_type]"
                  :disabled="saving || loading"
                  @click="saveModel(t.task_type)"
                >
                  保存模型
                </el-button>
                <el-tag type="info" effect="plain">当前生效：{{ t.effectiveLabel }}</el-tag>
              </el-space>
            </div>

            <el-divider />

            <div class="section" v-loading="promptsLoading">
              <div class="section-title">提示词配置</div>

              <template v-if="t.prompt">
                <el-space wrap>
                  <el-tag>版本：{{ t.prompt.version || '-' }}</el-tag>
                  <el-tag type="info" effect="plain">修改时间：{{ t.prompt.updated_at || '-' }}</el-tag>
                </el-space>

                <div class="prompt-grid">
                  <div class="prompt-block">
                    <div class="block-title">输出格式（只读）</div>
                    <el-input
                      :model-value="t.sections.outputJson"
                      type="textarea"
                      readonly
                      :autosize="{ minRows: 4, maxRows: 12 }"
                    />
                  </div>
                  <div class="prompt-block">
                    <div class="block-title">输入格式（只读）</div>
                    <el-input
                      :model-value="t.sections.inputJson"
                      type="textarea"
                      readonly
                      :autosize="{ minRows: 4, maxRows: 12 }"
                    />
                  </div>
                  <div class="prompt-block span-2">
                    <div class="block-title">提示词（可编辑）</div>
                    <el-input
                      v-model="promptDraftByTask[t.task_type]"
                      type="textarea"
                      :readonly="!isPromptEditing(t.task_type)"
                      :autosize="{ minRows: 10, maxRows: 30 }"
                      placeholder="提示词不可为空"
                    />
                    <el-text
                      v-if="isPromptEditing(t.task_type) && promptErrorText(t.task_type)"
                      type="danger"
                    >
                      {{ promptErrorText(t.task_type) }}
                    </el-text>
                  </div>
                </div>

                <el-space wrap>
                  <template v-if="!isPromptEditing(t.task_type)">
                    <el-button type="primary" size="small" :disabled="saving || loading" @click="enterPromptEdit(t.task_type)">
                      修改
                    </el-button>
                  </template>
                  <template v-else>
                    <el-button
                      type="primary"
                      size="small"
                      :loading="promptSavingByTask[t.task_type]"
                      :disabled="!canSavePrompt(t.task_type) || saving || loading"
                      @click="savePrompt(t.task_type)"
                    >
                      保存
                    </el-button>
                    <el-button size="small" :disabled="promptSavingByTask[t.task_type]" @click="cancelPromptEdit(t.task_type)">
                      取消
                    </el-button>
                  </template>
                </el-space>
              </template>
              <el-empty v-else description="暂无提示词数据" />
            </div>
          </el-space>
        </el-card>
      </el-col>
    </el-row>

    <el-empty v-else-if="!loading" description="暂无可配置任务" />
  </div>
</template>

<script setup>
import { ElMessage } from 'element-plus'
import { computed, onMounted, reactive, ref } from 'vue'

import { fetchLLMConfig, fetchLLMModels, fetchLLMPrompts, putLLMConfig, putLLMPrompt } from '../api/llmConfig'

const VISIBLE_TASK_TYPES = new Set(['post_analysis', 'report_generation'])

const loading = ref(false)
const saving = ref(false)
const error = ref('')

const providers = ref([])
const modelsByProvider = ref({})
const cheapDefaults = ref({})

const tasks = ref([])

const promptsLoading = ref(false)
const promptError = ref('')
const promptsByTask = ref({})

const promptDraftByTask = reactive({})
const modelSavingByTask = reactive({})
const promptSavingByTask = reactive({})
const promptEditModeByTask = reactive({})

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

function effectiveLabelFromConfig(cfg) {
  const provider = String(cfg?.provider || '')
  const model = cfg?.model == null ? null : String(cfg.model)
  return `${provider}${model ? ` / ${model}` : ''}`
}

async function loadAllModels() {
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

    tasks.value = (Array.isArray(cfg?.tasks) ? cfg.tasks : [])
      .map((t) => {
        const c = t?.config || {}
        const task_type = String(t?.task_type || '')
        return {
          task_type,
          title: String(t?.title || ''),
          draft: toDraft(c),
          effectiveLabel: effectiveLabelFromConfig(c),
        }
      })
      .filter((t) => VISIBLE_TASK_TYPES.has(t.task_type))
  } catch (e) {
    if (e?.name === 'AbortError') return
    error.value = e?.message || String(e)
  } finally {
    loading.value = false
  }
}

async function saveAllModels() {
  saving.value = true
  try {
    const items = tasks.value.map((t) => normalizeDraft(t.draft, t.task_type))
    await putLLMConfig(items)
    ElMessage.success('模型配置已保存')
    await loadAllModels()
  } catch (e) {
    ElMessage.error(e?.message || String(e))
  } finally {
    saving.value = false
  }
}

async function saveModel(taskType) {
  const tt = String(taskType || '').trim()
  if (!tt) return
  const t = (tasks.value || []).find((x) => String(x?.task_type || '') === tt)
  if (!t) return

  modelSavingByTask[tt] = true
  try {
    await putLLMConfig([normalizeDraft(t.draft, tt)])
    ElMessage.success('模型配置已保存')
    await loadAllModels()
  } catch (e) {
    ElMessage.error(e?.message || String(e))
  } finally {
    modelSavingByTask[tt] = false
  }
}

async function restoreCheapDefaults() {
  saving.value = true
  try {
    const defs = cheapDefaults.value || {}
    const items = tasks.value.map((t) => {
      const d = defs?.[t.task_type] || {}
      return {
        task_type: t.task_type,
        provider: String(d?.provider || ''),
        model: d?.model == null ? null : String(d.model),
      }
    })
    await putLLMConfig(items)
    ElMessage.success('已恢复默认省钱方案')
    await loadAllModels()
  } catch (e) {
    ElMessage.error(e?.message || String(e))
  } finally {
    saving.value = false
  }
}

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

function ensurePromptDraft(taskType) {
  const tt = String(taskType || '').trim()
  if (!tt) return
  if (Object.prototype.hasOwnProperty.call(promptDraftByTask, tt)) return
  const tpl = promptsByTask.value?.[tt]?.template
  const sections = splitTemplateSections(tpl)
  promptDraftByTask[tt] = String(sections.prompt || '')
  promptEditModeByTask[tt] = 'view'
}

function resetPromptDraft(taskType) {
  const tt = String(taskType || '').trim()
  if (!tt) return
  const tpl = promptsByTask.value?.[tt]?.template
  const sections = splitTemplateSections(tpl)
  promptDraftByTask[tt] = String(sections.prompt || '')
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
      const tt = String(it?.task_type || '').trim()
      if (!tt) continue
      if (!VISIBLE_TASK_TYPES.has(tt)) continue
      map[tt] = {
        task_type: tt,
        version: String(it?.version || ''),
        template: String(it?.template || ''),
        updated_at: it?.updated_at ? String(it.updated_at) : '',
      }
    }
    promptsByTask.value = map
    for (const t of tasks.value || []) ensurePromptDraft(t.task_type)
  } catch (e) {
    if (e?.name === 'AbortError') return
    promptError.value = e?.message || String(e)
  } finally {
    promptsLoading.value = false
  }
}

const taskCards = computed(() => {
  return (tasks.value || []).map((t) => {
    const p = promptsByTask.value?.[t.task_type] || null
    return {
      ...t,
      prompt: p,
      sections: splitTemplateSections(p?.template),
    }
  })
})

function promptErrorText(taskType) {
  const tt = String(taskType || '').trim()
  if (!tt) return ''
  if (!promptsByTask.value?.[tt]) return ''
  return String(promptDraftByTask?.[tt] || '').trim() === '' ? '提示词不可为空' : ''
}

function canSavePrompt(taskType) {
  const tt = String(taskType || '').trim()
  if (!tt) return false
  return !promptErrorText(tt) && !!promptsByTask.value?.[tt]
}

function isPromptEditing(taskType) {
  const tt = String(taskType || '').trim()
  if (!tt) return false
  return String(promptEditModeByTask?.[tt] || 'view') === 'edit'
}

function enterPromptEdit(taskType) {
  const tt = String(taskType || '').trim()
  if (!tt) return
  if (!promptsByTask.value?.[tt]) return
  ensurePromptDraft(tt)
  promptEditModeByTask[tt] = 'edit'
}

function cancelPromptEdit(taskType) {
  const tt = String(taskType || '').trim()
  if (!tt) return
  resetPromptDraft(tt)
  promptEditModeByTask[tt] = 'view'
}

async function savePrompt(taskType) {
  const tt = String(taskType || '').trim()
  if (!tt) return
  if (!promptsByTask.value?.[tt]) return

  const prompt = String(promptDraftByTask?.[tt] || '')
  if (String(prompt).trim() === '') {
    ElMessage.error('提示词不可为空')
    return
  }

  promptSavingByTask[tt] = true
  try {
    const res = await putLLMPrompt(tt, { prompt })
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
      resetPromptDraft(tt)
    } else {
      await loadPrompts()
    }
    ElMessage.success('提示词已保存并生效')
    promptEditModeByTask[tt] = 'view'
  } catch (e) {
    ElMessage.error(e?.message || String(e))
  } finally {
    promptSavingByTask[tt] = false
  }
}

async function confirmResetPrompt(taskType) {
  const tt = String(taskType || '').trim()
  if (!tt) return
  try {
    await ElMessageBox.confirm('将丢弃本地未保存的修改。是否继续？', '确认重置', {
      type: 'warning',
      confirmButtonText: '继续',
      cancelButtonText: '取消',
    })
  } catch {
    return
  }
  resetPromptDraft(tt)
}

async function refreshAll() {
  await loadAllModels()
  await loadPrompts()
}

onMounted(async () => {
  await refreshAll()
})
</script>

<style scoped>
.top-actions {
  margin-top: 10px;
}
.alert-lines {
  width: 100%;
  display: block;
}
.alert-lines :deep(.el-space__item) {
  min-width: 0;
  width: 100%;
  max-width: 100%;
}
.warn-alert {
  margin-top: 10px;
}
.page-alert {
  width: 100%;
  min-width: 0;
  box-sizing: border-box;
}
.page-alert :deep(.el-alert__content),
.page-alert :deep(.el-alert__title),
.page-alert :deep(.el-alert__description) {
  width: 100%;
  min-width: 0;
  white-space: normal;
  word-break: break-word;
  overflow-wrap: anywhere;
  overflow: visible;
}
.page-alert :deep(.el-alert__description) {
  margin: 0;
}
.warn-alert :deep(.el-alert__description) {
  white-space: normal;
  word-break: break-word;
}
.task-grid {
  margin-top: 10px;
}
.task-card {
  height: 100%;
}
.card-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
}
.card-title .title {
  font-size: 16px;
  font-weight: 700;
  line-height: 1.2;
}
.card-title .sub {
  font-size: 12px;
  margin-top: 2px;
}
.section-title {
  font-weight: 700;
  margin-bottom: 6px;
}
.prompt-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(320px, 1fr));
  gap: 16px;
}
.prompt-block {
  min-width: 0;
  width: 100%;
}
.prompt-block.span-2 {
  grid-column: span 2;
}
.prompt-block :deep(.el-input__inner) {
  min-width: 0;
}
.block-title {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-bottom: 6px;
}
.muted {
  color: var(--el-text-color-secondary);
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
