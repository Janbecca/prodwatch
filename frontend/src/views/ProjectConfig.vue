<template>
  <el-space direction="vertical" :size="12" fill>
    <el-space wrap>
      <el-select
        v-model="projectModel"
        style="width: 260px"
        :disabled="mode !== 'view' || projectsStore.loading || projectsStore.projects.length === 0"
        placeholder="请选择项目"
      >
        <el-option v-for="p in projectsStore.projects" :key="p.id" :label="p.name" :value="p.id" />
      </el-select>

      <el-button :loading="projectsStore.loading" :disabled="mode !== 'view'" @click="projectsStore.fetchProjects()">
        刷新项目
      </el-button>

      <el-tag v-if="projectsStore.activeProject" type="info">
        ID: {{ projectsStore.activeProject.id }} |
        {{ Number(projectsStore.activeProject.is_active || 0) === 1 ? '启用' : '停用' }}
      </el-tag>
      <el-text v-else type="info">未选择项目</el-text>

      <el-text v-if="projectsStore.error" type="danger">{{ projectsStore.error }}</el-text>
    </el-space>

    <el-alert
      v-if="projectsStore.issues && projectsStore.issues.length"
      title="项目状态自检发现问题"
      type="warning"
      :closable="false"
      show-icon
    >
      <template #default>
        <div v-for="it in projectsStore.issues" :key="it">{{ it }}</div>
      </template>
    </el-alert>
   
    <el-empty v-if="!hasProjects" description="暂无项目" />
    <template v-else>
      <ProjectConfigActions
        :mode="mode"
        :loading="loading"
        :project="project"
        :can-edit="canEdit"
        @create="enterCreate"
        @edit="enterEdit"
        @save="save"
        @cancel="cancel"
        @toggle-active="toggleActive"
        @delete="remove"
      />

      <el-alert v-if="error" type="error" :title="error" show-icon :closable="false" />

      <template v-if="mode === 'view'">
        <ProjectBasicInfo :project="project" :loading="loading" />
        <ProjectBrandList :rows="brands" :loading="loading" />
        <ProjectPlatformList :rows="platforms" :loading="loading" />
        <ProjectKeywordList :rows="keywords" :loading="loading" />
      </template>

      <template v-else>
        <ProjectEditForm
          ref="editFormRef"
          :mode="mode"
          :model="editModel"
          :brand-options="brandOptions"
          :platform-options="platformOptions"
        />
      </template>
    </template>
  </el-space>
</template>

<script setup>
import { ElMessage } from 'element-plus'
import { computed, reactive, ref, watch } from 'vue'
import { useProjectsStore } from '../stores/projects'


import ProjectBasicInfo from '../components/project-config/ProjectBasicInfo.vue'
import ProjectBrandList from '../components/project-config/ProjectBrandList.vue'
import ProjectPlatformList from '../components/project-config/ProjectPlatformList.vue'
import ProjectKeywordList from '../components/project-config/ProjectKeywordList.vue'
import ProjectConfigActions from '../components/project-config/ProjectConfigActions.vue'
import ProjectEditForm from '../components/project-config/ProjectEditForm.vue'

import { fetchProjectConfig } from '../api/projectConfig'
import { fetchBrands, fetchPlatforms } from '../api/meta'
import { createProject, deleteProject, setProjectActivation, updateProject } from '../api/projectMutations'

const projectsStore = useProjectsStore()
const hasProjects = computed(() => projectsStore.projects.length > 0)
const projectModel = computed({
  get: () => projectsStore.activeProjectId,
  set: (v) => projectsStore.setActiveProject(v),
})

const loading = ref(false)
const error = ref('')
const project = ref(null)
const brands = ref([])
const platforms = ref([])
const keywords = ref([])

const brandOptions = ref([])
const platformOptions = ref([])

const mode = ref('view') // view | edit | create
const canEdit = computed(() => !!project.value && String(project.value.status || '') === 'inactive')

let currentController = null

async function load(projectId) {
  if (!projectId) return
  if (currentController) currentController.abort()
  currentController = new AbortController()

  loading.value = true
  error.value = ''
  try {
    const data = await fetchProjectConfig(projectId, { signal: currentController.signal })
    project.value = data.project
    brands.value = data.brands
    platforms.value = data.platforms
    keywords.value = data.keywords
  } catch (e) {
    if (e?.name === 'AbortError') return
    error.value = e?.message || String(e)
    ElMessage.error(error.value)
    project.value = null
    brands.value = []
    platforms.value = []
    keywords.value = []
  } finally {
    loading.value = false
  }
}

async function loadMetaOptions() {
  try {
    const [b, p] = await Promise.all([fetchBrands(), fetchPlatforms()])
    brandOptions.value = b
    platformOptions.value = p
  } catch (e) {
    // meta endpoints are optional; keep empty options on failure
    brandOptions.value = []
    platformOptions.value = []
    ElMessage.error(e?.message || String(e))
  }
}

watch(
  () => projectsStore.activeProjectId,
  (next) => {
    if (!next) {
      if (currentController) currentController.abort()
      project.value = null
      brands.value = []
      platforms.value = []
      keywords.value = []
      return
    }
    if (mode.value !== 'view') return
    load(next)
  },
  { immediate: true }
)

watch(
  () => mode.value,
  (m) => {
    // entering view mode -> reload to ensure latest
    if (m === 'view' && projectsStore.activeProjectId) load(projectsStore.activeProjectId)
  }
)

const editModel = reactive({
  id: null,
  name: '',
  product_category: '',
  description: '',
  our_brand_id: null,
  status: 'inactive',
  is_active: 0,
  refresh_mode: 'manual',
  refresh_cron: '',
  brand_ids: [],
  platform_ids: [],
  keywords: [],
})

const editFormRef = ref()

function fillEditModelFromLoaded() {
  editModel.id = project.value?.id ?? null
  editModel.name = project.value?.name ?? ''
  editModel.product_category = project.value?.product_category ?? ''
  editModel.description = project.value?.description ?? ''
  editModel.our_brand_id = project.value?.our_brand_id ?? null
  editModel.status = project.value?.status ?? 'inactive'
  editModel.is_active = Number(project.value?.is_active || 0)
  editModel.refresh_mode = project.value?.refresh_mode ?? 'manual'
  editModel.refresh_cron = project.value?.refresh_cron ?? ''
  editModel.brand_ids = brands.value.map((b) => b.id)
  editModel.platform_ids = platforms.value.map((p) => p.id)
  editModel.keywords = keywords.value.map((k) => ({
    keyword: k.keyword ?? '',
    keyword_type: k.keyword_type ?? '',
    weight: k.weight ?? 0,
    is_enabled: Number(k.is_enabled || 0),
  }))
}

function resetEditModelForCreate() {
  editModel.id = null
  editModel.name = ''
  editModel.product_category = ''
  editModel.description = ''
  editModel.our_brand_id = null
  editModel.status = 'inactive'
  editModel.is_active = 0
  editModel.refresh_mode = 'manual'
  editModel.refresh_cron = ''
  editModel.brand_ids = []
  editModel.platform_ids = []
  editModel.keywords = [{ keyword: '', keyword_type: '', weight: 0, is_enabled: 1 }]
}

async function enterCreate() {
  await loadMetaOptions()
  mode.value = 'create'
  resetEditModelForCreate()
}

async function enterEdit() {
  if (!canEdit.value) {
    ElMessage.warning('仅允许对“停用状态(inactive)项目”进行编辑')
    return
  }
  await loadMetaOptions()
  mode.value = 'edit'
  fillEditModelFromLoaded()
}

function cancel() {
  mode.value = 'view'
}

function payloadFromEditModel() {
  return {
    name: String(editModel.name || '').trim(),
    product_category: editModel.product_category || null,
    description: editModel.description || null,
    our_brand_id: editModel.our_brand_id,
    status: editModel.status || null,
    refresh_mode: editModel.refresh_mode || null,
    refresh_cron: editModel.refresh_cron || null,
    brand_ids: editModel.brand_ids.map((x) => Number(x)),
    platform_ids: editModel.platform_ids.map((x) => Number(x)),
    keywords: editModel.keywords.map((k) => ({
      keyword: String(k.keyword || '').trim(),
      keyword_type: k.keyword_type || null,
      weight: k.weight ?? null,
      is_enabled: Number(k.is_enabled || 0),
    })),
  }
}

async function save() {
  error.value = ''
  try {
    await editFormRef.value.validate()
  } catch (e) {
    error.value = e?.message || String(e)
    ElMessage.error(error.value)
    return
  }

  loading.value = true
  try {
    const payload = payloadFromEditModel()
    if (mode.value === 'create') {
      const res = await createProject(payload)
      const newId = res?.project_id ?? res?.id
      if (newId) {
        await projectsStore.fetchProjects()
        projectsStore.setActiveProject(newId)
        await load(newId)
      } else {
        await projectsStore.fetchProjects()
      }
      mode.value = 'view'
      ElMessage.success('新建项目成功')
      return
    }
    if (mode.value === 'edit') {
      const pid = Number(project.value?.id)
      await updateProject(pid, payload)
      await load(pid)
      await projectsStore.fetchProjects()
      mode.value = 'view'
      ElMessage.success('保存成功')
      return
    }
  } catch (e) {
    error.value = e?.message || String(e)
    ElMessage.error(error.value)
  } finally {
    loading.value = false
  }
}

async function toggleActive() {
  if (!project.value) return
  loading.value = true
  error.value = ''
  try {
    const nextStatus = String(project.value.status || '') === 'active' ? 'inactive' : 'active'
    const nextIsActive = nextStatus === 'active' ? 1 : 0
    await setProjectActivation(project.value.id, nextIsActive, nextStatus)
    await projectsStore.fetchProjects()
    await load(projectsStore.activeProjectId)
    ElMessage.success(nextStatus === 'active' ? '已启用项目（active）' : '已停用项目（inactive）')
  } catch (e) {
    error.value = e?.message || String(e)
    ElMessage.error(error.value)
  } finally {
    loading.value = false
  }
}

async function remove() {
  if (!project.value) return
  loading.value = true
  error.value = ''
  try {
    await deleteProject(project.value.id)
    mode.value = 'view'
    await projectsStore.fetchProjects()
    // store will auto-fallback to a valid project; watcher will reload
    ElMessage.success('删除成功')
  } catch (e) {
    error.value = e?.message || String(e)
    ElMessage.error(error.value)
  } finally {
    loading.value = false
  }
}
</script>
