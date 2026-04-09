<!-- 作用：前端组件：报告模块组件（ReportCreateDrawer）。 -->

<template>
  <el-drawer v-model="open" size="560px" title="新建报告" :with-header="true">
    <el-alert
      v-if="scopeError"
      type="error"
      :title="scopeError"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-form label-width="110px">
      <el-form-item label="项目">
        <el-select v-model="form.projectId" style="width: 100%" :disabled="projectsStore.loading">
          <el-option
            v-for="p in enabledProjects"
            :key="p.id"
            :label="p.name || `#${p.id}`"
            :value="p.id"
          />
        </el-select>
      </el-form-item>

      <el-form-item label="标题">
        <el-input v-model="form.title" placeholder="报告标题" />
      </el-form-item>

      <el-form-item label="类型">
        <el-select v-model="form.type" style="width: 100%">
          <el-option label="日报" value="daily" />
          <el-option label="周报" value="weekly" />
          <el-option label="月报" value="monthly" />
          <el-option label="专题" value="special" />
        </el-select>
      </el-form-item>

      <el-form-item label="数据范围">
        <el-date-picker
          v-model="form.dateRange"
          type="daterange"
          value-format="YYYY-MM-DD"
          range-separator="至"
          start-placeholder="开始日期"
          end-placeholder="结束日期"
          style="width: 100%"
        />
      </el-form-item>

      <el-divider />
      <el-form-item label="平台">
        <el-select v-model="form.platformIds" multiple collapse-tags collapse-tags-tooltip style="width: 100%">
          <el-option
            v-for="p in safePlatformOptions"
            :key="p.id"
            :label="p.name || `#${p.id}`"
            :value="p.id"
          />
        </el-select>
      </el-form-item>
      <el-form-item label="品牌">
        <el-select v-model="form.brandIds" multiple collapse-tags collapse-tags-tooltip style="width: 100%">
          <el-option v-for="b in safeBrandOptions" :key="b.id" :label="b.name || `#${b.id}`" :value="b.id" />
        </el-select>
      </el-form-item>
      <el-form-item label="关键词">
        <el-select v-model="form.keywords" multiple filterable collapse-tags collapse-tags-tooltip style="width: 100%">
          <el-option v-for="k in keywordOptions" :key="k" :label="k" :value="k" />
        </el-select>
      </el-form-item>

      <el-divider />
      <el-form-item label="模块">
        <el-checkbox-group v-model="form.modules">
          <el-checkbox value="sentiment">情感</el-checkbox>
          <el-checkbox value="trend">趋势</el-checkbox>
          <el-checkbox value="topics">话题</el-checkbox>
          <el-checkbox value="feature">特征</el-checkbox>
          <el-checkbox value="spam">垃圾</el-checkbox>
          <el-checkbox value="competitor">竞品</el-checkbox>
          <el-checkbox value="strategy">策略</el-checkbox>
        </el-checkbox-group>
      </el-form-item>

      <el-form-item>
        <el-button type="primary" :loading="reportsStore.creating" :disabled="reportsStore.creating" @click="onCreate">
          创建
        </el-button>
        <el-button :disabled="reportsStore.creating" @click="open = false">关闭</el-button>
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
import { fetchProjectRefreshStatus } from '../../api/projectRefresh'

const projectsStore = useProjectsStore()
const reportsStore = useReportsStore()

function normalizeOptionList(items) {
  const out = []
  const seen = new Set()
  for (const it of items || []) {
    const id = Number(it?.id)
    if (!Number.isFinite(id) || id <= 0) continue
    const rid = Math.trunc(id)
    if (seen.has(rid)) continue
    seen.add(rid)
    out.push({ id: rid, name: it?.name != null ? String(it.name) : '' })
  }
  return out
}

const enabledProjects = computed(() => {
  const out = []
  const seen = new Set()
  for (const p of projectsStore.projects || []) {
    const id = Number(p?.id)
    if (!Number.isFinite(id) || id <= 0) continue
    const rid = Math.trunc(id)
    if (seen.has(rid)) continue
    if (Number(p?.is_active || 0) !== 1) continue
    seen.add(rid)
    out.push({ id: rid, name: p?.name != null ? String(p.name) : '' })
  }
  return out
})

const safePlatformOptions = computed(() => normalizeOptionList(platformOptions.value))
const safeBrandOptions = computed(() => normalizeOptionList(brandOptions.value))

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
    // API returns keywords as rows [{keyword, keyword_type, ...}], but UI <el-option> needs string label/value.
    keywordOptions.value = Array.isArray(data?.keywords)
      ? data.keywords
          .map((k) => String(k?.keyword || '').trim())
          .filter((s) => s !== '')
      : []
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
    ElMessage.error('必须选择项目')
    return
  }
  if (!form.title || String(form.title).trim() === '') {
    ElMessage.error('标题不能为空')
    return
  }
  if (!ds || !de) {
    ElMessage.error('必须选择数据范围')
    return
  }

  // 预检查：项目刷新中时不要触发创建请求，避免 409/503 以及浏览器控制台报错刷屏。
  try {
    const st = await fetchProjectRefreshStatus(pid)
    if (st?.running) {
      ElMessage.warning('项目正在刷新中，请稍后再新建报告')
      return
    }
  } catch {
    // ignore
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
