<template>
  <el-drawer v-model="open" size="560px" title="New Report" :with-header="true">
    <el-alert
      v-if="scopeError"
      type="error"
      :title="scopeError"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-form label-width="110px">
      <el-form-item label="Project">
        <el-select v-model="form.projectId" style="width: 100%" :disabled="projectsStore.loading">
          <el-option
            v-for="p in enabledProjects"
            :key="p.id"
            :label="p.name || `#${p.id}`"
            :value="p.id"
          />
        </el-select>
      </el-form-item>

      <el-form-item label="Title">
        <el-input v-model="form.title" placeholder="Report title" />
      </el-form-item>

      <el-form-item label="Type">
        <el-select v-model="form.type" style="width: 100%">
          <el-option label="daily" value="daily" />
          <el-option label="weekly" value="weekly" />
          <el-option label="monthly" value="monthly" />
          <el-option label="special" value="special" />
        </el-select>
      </el-form-item>

      <el-form-item label="Data Range">
        <el-date-picker
          v-model="form.dateRange"
          type="daterange"
          value-format="YYYY-MM-DD"
          range-separator="to"
          start-placeholder="Start"
          end-placeholder="End"
          style="width: 100%"
        />
      </el-form-item>

      <el-divider />
      <el-form-item label="Platforms">
        <el-select v-model="form.platformIds" multiple collapse-tags collapse-tags-tooltip style="width: 100%">
          <el-option v-for="p in platformOptions" :key="p.id" :label="p.name || `#${p.id}`" :value="p.id" />
        </el-select>
      </el-form-item>
      <el-form-item label="Brands">
        <el-select v-model="form.brandIds" multiple collapse-tags collapse-tags-tooltip style="width: 100%">
          <el-option v-for="b in brandOptions" :key="b.id" :label="b.name || `#${b.id}`" :value="b.id" />
        </el-select>
      </el-form-item>
      <el-form-item label="Keywords">
        <el-select v-model="form.keywords" multiple filterable collapse-tags collapse-tags-tooltip style="width: 100%">
          <el-option v-for="k in keywordOptions" :key="k" :label="k" :value="k" />
        </el-select>
      </el-form-item>

      <el-divider />
      <el-form-item label="Modules">
        <el-checkbox-group v-model="form.modules">
          <el-checkbox value="sentiment">Sentiment</el-checkbox>
          <el-checkbox value="trend">Trend</el-checkbox>
          <el-checkbox value="topics">Topics</el-checkbox>
          <el-checkbox value="feature">Feature</el-checkbox>
          <el-checkbox value="spam">Spam</el-checkbox>
          <el-checkbox value="competitor">Competitor</el-checkbox>
          <el-checkbox value="strategy">Strategy</el-checkbox>
        </el-checkbox-group>
      </el-form-item>

      <el-form-item>
        <el-button type="primary" @click="onCreate">Create</el-button>
        <el-button @click="open = false">Close</el-button>
      </el-form-item>
    </el-form>
  </el-drawer>
</template>

<script setup>
import { computed, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { useProjectsStore } from '../../stores/projects'
import { useReportsStore } from '../../stores/reports'
import { fetchProjectConfig } from '../../api/projectConfig'

const projectsStore = useProjectsStore()
const reportsStore = useReportsStore()

const enabledProjects = computed(() => {
  return (projectsStore.projects || []).filter((p) => Number(p?.is_active || 0) === 1)
})

const open = computed({
  get: () => reportsStore.createOpen,
  set: (v) => {
    if (!v) reportsStore.createOpen = false
  },
})

const targetProjectId = computed(() => {
  const fromPrefill = Number(reportsStore.createPrefill?.projectId)
  if (Number.isFinite(fromPrefill) && fromPrefill > 0) return fromPrefill
  const fromDraft = Number(reportsStore.draft?.projectId)
  if (Number.isFinite(fromDraft) && fromDraft > 0) return fromDraft
  const fromActive = Number(projectsStore.activeProjectId)
  if (Number.isFinite(fromActive) && fromActive > 0) return fromActive
  return null
})

const scopeError = ref('')
const brandOptions = ref([])
const platformOptions = ref([])
const keywordOptions = ref([])

const form = reactive({
  projectId: null,
  title: '',
  type: 'daily',
  dateRange: [],
  platformIds: [],
  brandIds: [],
  keywords: [],
  modules: ['sentiment', 'trend', 'topics', 'feature', 'spam', 'competitor', 'strategy'],
})

function uniqValidInts(arr) {
  const out = []
  const seen = new Set()
  for (const x of arr || []) {
    const n = Number(x)
    if (!Number.isFinite(n) || n <= 0) continue
    const v = Math.trunc(n)
    if (seen.has(v)) continue
    seen.add(v)
    out.push(v)
  }
  return out
}

function uniqNonEmptyStrings(arr) {
  const out = []
  const seen = new Set()
  for (const x of arr || []) {
    const s = String(x || '').trim()
    if (!s) continue
    if (seen.has(s)) continue
    seen.add(s)
    out.push(s)
  }
  return out
}

function resetForm() {
  form.projectId = null
  form.title = ''
  form.type = 'daily'
  form.dateRange = []
  form.platformIds = []
  form.brandIds = []
  form.keywords = []
  form.modules = ['sentiment', 'trend', 'topics', 'feature', 'spam', 'competitor', 'strategy']
}

function applyPrefill(prefill) {
  if (!prefill) return
  if (prefill.projectId != null) form.projectId = Number(prefill.projectId) || null
  if (prefill.title != null) form.title = String(prefill.title)
  if (prefill.type) form.type = String(prefill.type)
  if (Array.isArray(prefill.dateRange)) form.dateRange = prefill.dateRange.slice(0, 2)
  if (Array.isArray(prefill.platformIds)) form.platformIds = prefill.platformIds.slice()
  if (Array.isArray(prefill.brandIds)) form.brandIds = prefill.brandIds.slice()
  if (Array.isArray(prefill.keywords)) form.keywords = prefill.keywords.slice()
  if (Array.isArray(prefill.modules)) form.modules = prefill.modules.slice()
}

async function loadScope(pid) {
  scopeError.value = ''
  brandOptions.value = []
  platformOptions.value = []
  keywordOptions.value = []
  try {
    const data = await fetchProjectConfig(pid)
    brandOptions.value = Array.isArray(data?.brands) ? data.brands : []
    platformOptions.value = Array.isArray(data?.platforms) ? data.platforms : []
    keywordOptions.value = Array.isArray(data?.keywords) ? data.keywords : []
  } catch (e) {
    scopeError.value = e?.message || String(e)
  }
}

watch(
  () => [open.value, targetProjectId.value],
  async () => {
    if (!open.value) return
    const pid = targetProjectId.value
    if (!pid) return
    resetForm()
    form.projectId = pid
    await loadScope(pid)
    applyPrefill(reportsStore.createPrefill)
  },
  { immediate: true }
)

watch(
  () => form.projectId,
  (pid, prev) => {
    if (!open.value) return
    if (pid == null) return
    if (prev != null && Number(pid) === Number(prev)) return
    loadScope(pid)
  }
)

function onCreate() {
  doCreate()
}

async function doCreate() {
  const pid = Number(form.projectId)
  const [ds, de] = form.dateRange || []
  if (!pid) {
    ElMessage.error('Project is required')
    return
  }
  if (!form.title || String(form.title).trim() === '') {
    ElMessage.error('Title is required')
    return
  }
  if (!ds || !de) {
    ElMessage.error('Data range is required')
    return
  }

  const payload = {
    project_id: pid,
    title: String(form.title).trim(),
    report_type: String(form.type),
    data_start_date: ds,
    data_end_date: de,
    platform_ids: uniqValidInts(form.platformIds),
    brand_ids: uniqValidInts(form.brandIds),
    keywords: uniqNonEmptyStrings(form.keywords),
    include_sentiment: form.modules.includes('sentiment'),
    include_trend: form.modules.includes('trend'),
    include_topics: form.modules.includes('topics'),
    include_feature_analysis: form.modules.includes('feature'),
    include_spam: form.modules.includes('spam'),
    include_competitor_compare: form.modules.includes('competitor'),
    include_strategy: form.modules.includes('strategy'),
  }

  try {
    await reportsStore.submitCreate(payload)
  } catch (e) {
    ElMessage.error(e?.message || String(e))
  }
}
</script>
