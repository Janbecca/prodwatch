<script setup>
import { computed, ref } from 'vue'
import api from '../api/axios'
import PageHeader from '../components/PageHeader.vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useProjectsStore } from '../stores/projects'

const loading = ref(false)
const error = ref('')

const brands = ref([])
const projects = ref([])

let keySeq = 0
const makeKey = (prefix) => `${prefix}-${Date.now()}-${++keySeq}`

const activeProjectKey = ref(null)
const isEditMode = ref(false)
const projectsStore = useProjectsStore()
const draft = ref({
  key: null,
  id: null,
  name: '',
  product_category: '',
  brand_keys: [],
  is_active: true,
})

const brandOptions = computed(() => (brands.value || []).map((b) => ({ label: b.name, value: b.key })))

const activeProject = computed(() => (projects.value || []).find((p) => p.key === activeProjectKey.value) || null)

const setDraftFromProject = (p) => {
  if (!p) {
    draft.value = { key: null, id: null, name: '', product_category: '', brand_keys: [], is_active: true }
    return
  }
  draft.value = {
    key: p.key,
    id: p.id,
    name: p.name || '',
    product_category: p.product_category || '',
    brand_keys: Array.isArray(p.brand_keys) ? [...p.brand_keys] : [],
    is_active: Boolean(p.is_active),
  }
}

const deletedProjectIds = ref(new Set())
const deletedBrandIds = ref(new Set())

const pendingBrands = computed(() => (brands.value || []).filter((b) => b.is_pending))

const usedBrandKeysInSavedProjects = computed(() => {
  const used = new Set()
  for (const p of projects.value || []) {
    const pid = Number(p.id)
    if (!Number.isFinite(pid) || pid <= 0) continue
    for (const bk of p.brand_keys || []) used.add(bk)
  }
  return used
})

const projectIdText = computed(() => {
  const id = Number(draft.value?.id)
  if (!Number.isFinite(id)) return '（未选择）'
  if (id > 0) return String(id)
  return `${id}（新建）`
})

const canEditActiveProject = computed(() => {
  if (!isEditMode.value) return false
  if (!activeProject.value) return false
  return !activeProject.value.is_active
})

const enabledCount = computed(() => (projects.value || []).filter((p) => Boolean(p.is_active)).length)

const canEnable = computed(() => {
  const p = activeProject.value
  if (!p) return false
  if (p.id == null || Number(p.id) <= 0) return false
  if (isEditMode.value) return false
  if (p.is_active) return true
  const hasCategory = Boolean(String(p.product_category || '').trim())
  const hasBrands = Array.isArray(p.brand_keys) && p.brand_keys.length > 0
  if (!hasCategory || !hasBrands) return false
  if (enabledCount.value >= 3) return false
  return true
})

const loadAll = async () => {
  loading.value = true
  error.value = ''
  try {
    const [b, p] = await Promise.all([api.get('/api/dashboard/options'), api.get('/api/projects')])
    const dbBrands = b.data?.brands || []
    brands.value = dbBrands.map((x) => ({
      id: x.id,
      name: x.name,
      industry: x.industry || null,
      key: `b-${x.id}`,
      is_pending: false,
    }))

    const dbProjects = p.data || []
    projects.value = dbProjects.map((x) => ({
      id: x.id,
      name: x.name,
      product_category: x.product_category || '',
      is_active: Boolean(x.is_active),
      brand_keys: (x.brand_ids || []).map((bid) => `b-${bid}`),
      key: `p-${x.id}`,
    }))

    isEditMode.value = false
    if (activeProjectKey.value == null && projects.value.length) {
      activeProjectKey.value = projects.value[0].key
      setDraftFromProject(projects.value[0])
    } else {
      setDraftFromProject(activeProject.value)
    }
  } catch (e) {
    error.value = e?.response?.data?.detail || e?.message || '加载失败'
  } finally {
    loading.value = false
  }
}

const onSwitchProject = (pkey) => {
  activeProjectKey.value = pkey
  setDraftFromProject(activeProject.value)
  isEditMode.value = false
}

const addProject = () => {
  const minId = Math.min(0, ...(projects.value || []).map((x) => Number(x.id) || 0))
  const nextTempId = minId - 1
  const p = { id: nextTempId, name: '新项目', product_category: '', brand_keys: [], is_active: false, key: makeKey('n') }
  projects.value = [...projects.value, p]
  activeProjectKey.value = p.key
  setDraftFromProject(p)
  isEditMode.value = true
}

const applyDraftToList = () => {
  const idx = (projects.value || []).findIndex((p) => p.key === draft.value.key)
  if (idx === -1) return
  const next = [...projects.value]
  next[idx] = {
    key: draft.value.key,
    id: draft.value.id,
    name: draft.value.name,
    product_category: draft.value.product_category || null,
    brand_keys: [...(draft.value.brand_keys || [])],
    is_active: Boolean(draft.value.is_active),
  }
  projects.value = next
}

const removeBrandFromAllProjects = (brandKey) => {
  projects.value = (projects.value || []).map((p) => ({
    ...p,
    brand_keys: (p.brand_keys || []).filter((k) => k !== brandKey),
  }))
  if (Array.isArray(draft.value.brand_keys)) {
    draft.value.brand_keys = draft.value.brand_keys.filter((k) => k !== brandKey)
  }
}

const requestDeleteBrand = (b) => {
  if (!b) return
  if (!b.is_pending && usedBrandKeysInSavedProjects.value.has(b.key)) return

  // Pending brand: just drop it from local state.
  if (b.is_pending) {
    brands.value = (brands.value || []).filter((x) => x.key !== b.key)
    removeBrandFromAllProjects(b.key)
    return
  }

  // Persisted brand: mark for deletion; only applied on Save.
  if (b.id != null) deletedBrandIds.value.add(Number(b.id))
  brands.value = (brands.value || []).filter((x) => x.key !== b.key)
  removeBrandFromAllProjects(b.key)
}

const removeActive = async () => {
  if (activeProjectKey.value == null) return
  const p = activeProject.value
  if (!p) return
  if (p.is_active) return

  try {
    await ElMessageBox.confirm('确认删除该项目？', '二次确认', {
      confirmButtonText: '删除',
      cancelButtonText: '取消',
      type: 'warning',
    })
  } catch {
    return
  }

  // mark for deletion; only persisted on Save
  if (p.id != null && Number(p.id) > 0) deletedProjectIds.value.add(Number(p.id))
  projects.value = projects.value.filter((x) => x.key !== p.key)
  activeProjectKey.value = projects.value[0]?.key || null
  setDraftFromProject(activeProject.value)
  isEditMode.value = false
}

const startEdit = () => {
  const p = activeProject.value
  if (!p) return
  if (p.is_active) return
  isEditMode.value = true
}

const onToggleActive = async (val) => {
  const p = activeProject.value
  if (!p || p.id == null || Number(p.id) <= 0) return

  if (val === true && !canEnable.value) {
    // revert
    p.is_active = false
    setDraftFromProject(p)
    if (enabledCount.value >= 3) ElMessage.warning('最多同时启用3个项目')
    else ElMessage.warning('产品品类和监控品牌为空，则不允许启用')
    return
  }

  loading.value = true
  error.value = ''
  try {
    await api.post(`/api/projects/${p.id}/activate`, { active: Boolean(val) })
    await loadAll()
    if (val === true) {
      ElMessage.success('该项目已启用，可以在仪表盘中查看舆情情况')
      projectsStore.setActiveProjectId(Number(p.id))
    }
    await projectsStore.fetch()
  } catch (e) {
    error.value = e?.response?.data?.detail || e?.message || '启用失败'
    await loadAll()
    await projectsStore.fetch()
  } finally {
    loading.value = false
  }
}

const saveAll = async () => {
  applyDraftToList()
  loading.value = true
  error.value = ''
  try {
    // 0) Persist deleted brands (only when not used by saved projects)
    for (const bid of Array.from(deletedBrandIds.value.values())) {
      await api.delete(`/api/brands/${bid}`)
    }

    // 1) Create pending brands (only on Save)
    const tempKeyToId = {}
    for (const b of pendingBrands.value) {
      const resp = await api.post('/api/brands', { name: b.name, industry: null })
      tempKeyToId[b.key] = resp.data?.id
    }

    // 2) Apply brand mapping and build project payload
    const keyToId = {}
    for (const b of brands.value || []) {
      if (b.is_pending) continue
      keyToId[b.key] = b.id
    }
    for (const [k, v] of Object.entries(tempKeyToId)) keyToId[k] = v

    const payload = (projects.value || []).map((p) => {
      const brandIds = (p.brand_keys || [])
        .map((k) => keyToId[k])
        .filter((x) => typeof x === 'number' && Number.isFinite(x))

      return {
        id: p.id,
        name: p.name,
        product_category: p.product_category || null,
        brand_ids: brandIds,
        is_active: Boolean(p.is_active),
      }
    })

    // 3) Persist deletions
    for (const id of Array.from(deletedProjectIds.value.values())) {
      const pid = Number(id)
      if (!Number.isFinite(pid) || pid <= 0) continue
      try {
        await api.delete(`/api/projects/${pid}`)
      } catch (e) {
        // Treat "already deleted / not found" as success.
        if (e?.response?.status !== 404) throw e
      }
    }

    // 4) Persist upserts
    await api.post('/api/projects', payload)

    // 5) Reload and reset local state
    deletedProjectIds.value = new Set()
    deletedBrandIds.value = new Set()
    await loadAll()
    await projectsStore.fetch()
    // best-effort: keep active selection by name
    if (draft.value?.name) {
      const found = (projects.value || []).find((x) => x.name === draft.value.name)
      if (found) {
        activeProjectKey.value = found.key
        setDraftFromProject(found)
      }
    }
  } catch (e) {
    error.value = e?.response?.data?.detail || e?.message || '保存失败'
  } finally {
    loading.value = false
  }
}

const normalizeBrandSelection = () => {
  const selected = Array.isArray(draft.value.brand_keys) ? [...draft.value.brand_keys] : []
  const knownKeys = new Set((brands.value || []).map((b) => b.key))
  const byLowerName = new Map((brands.value || []).map((b) => [String(b.name || '').trim().toLowerCase(), b.key]))

  const nextKeys = []
  for (const item of selected) {
    if (knownKeys.has(item)) {
      nextKeys.push(item)
      continue
    }
    const name = String(item || '').trim()
    if (!name) continue
    const lower = name.toLowerCase()
    const existingKey = byLowerName.get(lower)
    if (existingKey) {
      nextKeys.push(existingKey)
      continue
    }
    const k = makeKey('nb')
    brands.value = [
      ...brands.value,
      { id: null, name, industry: null, key: k, is_pending: true },
    ]
    knownKeys.add(k)
    byLowerName.set(lower, k)
    nextKeys.push(k)
  }

  // de-dup
  draft.value.brand_keys = Array.from(new Set(nextKeys))
  applyDraftToList()
}

loadAll()
</script>

<template>
  <section class="page">
    <PageHeader content="监控项目配置" />
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />

    <el-card class="card" shadow="hover">
      <div class="row">
        <div class="left">
          <div class="label">当前项目</div>
          <el-select
            v-model="activeProjectKey"
            placeholder="请选择项目"
            style="width: 320px; max-width: 100%"
            @change="onSwitchProject"
          >
            <el-option v-for="p in projects" :key="p.key" :label="p.name" :value="p.key" />
          </el-select>
        </div>
        <div class="actions">
          <el-button @click="loadAll">刷新</el-button>
          <el-switch
            v-if="activeProject"
            :model-value="Boolean(activeProject.is_active)"
            :disabled="isEditMode || (!activeProject.is_active && !canEnable) || Number(activeProject.id) <= 0"
            active-text="启用"
            @change="onToggleActive"
          />
          <el-button :disabled="!activeProject || activeProject.is_active" @click="startEdit">修改</el-button>
          <el-button @click="addProject">新增</el-button>
        </div>
      </div>

      <el-divider />

      <el-form label-position="top" class="form">
        <div class="meta">项目ID：{{ projectIdText }}</div>
        <el-form-item label="项目名称">
          <el-input v-model="draft.name" :disabled="!canEditActiveProject" placeholder="例如：摄像头舆情监控" @change="applyDraftToList" />
        </el-form-item>
        <el-form-item label="产品品类">
          <el-input v-model="draft.product_category" :disabled="!canEditActiveProject" placeholder="例如：摄像头" @change="applyDraftToList" />
        </el-form-item>
        <el-form-item label="监控品牌（可多选，支持筛选/新建草稿）">
          <el-select
            v-model="draft.brand_keys"
            multiple
            filterable
            allow-create
            default-first-option
            placeholder="选择或输入新品牌名称后回车"
            style="width: 520px; max-width: 100%"
            :disabled="!canEditActiveProject"
            @change="normalizeBrandSelection"
          >
            <el-option v-for="b in brands" :key="b.key" :label="b.name" :value="b.key">
              <div class="brand-opt">
                <span>{{ b.name }}</span>
                <span
                  class="brand-del"
                  :class="{ disabled: !b.is_pending && usedBrandKeysInSavedProjects.has(b.key) }"
                  title="删除品牌（保存后生效）"
                  @click.stop="requestDeleteBrand(b)"
                >
                  删除
                </span>
              </div>
            </el-option>
          </el-select>
          <div class="muted" v-if="pendingBrands.length" style="margin-top: 6px;">待保存新品牌：{{ pendingBrands.length }}</div>
        </el-form-item>
      </el-form>

      <div class="bottom-actions">
        <el-button type="danger" plain :disabled="activeProjectKey == null || activeProject?.is_active" :loading="loading" @click="removeActive">删除</el-button>
        <el-button type="primary" :disabled="!canEditActiveProject" :loading="loading" @click="saveAll">保存</el-button>
      </div>
    </el-card>
  </section>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.card { border-radius: 10px; }

.row {
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  gap: 10px;
  flex-wrap: wrap;
}

.left {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.label {
  font-size: 12px;
  color: #6b7280;
}

.actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.form {
  max-width: 720px;
}

@media (max-width: 640px) {
  .form {
    max-width: 100%;
  }
}

.meta {
  font-size: 12px;
  color: #6b7280;
  margin-bottom: 8px;
}

.bottom-actions {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 14px;
}

.brand-opt {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

.muted {
  font-size: 12px;
  color: #6b7280;
}

.brand-del {
  font-size: 12px;
  color: #ef4444;
  cursor: pointer;
  padding-left: 10px;
}

.brand-del.disabled {
  color: #9ca3af;
  cursor: not-allowed;
}
</style>
