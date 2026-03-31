<template>
  <PageSection title="Filters">
    <el-form :inline="true" label-width="120px">
      <el-form-item label="Project">
        <el-space wrap>
          <el-select
            :model-value="projectModel"
            style="width: 260px"
            placeholder="Select a project"
            :disabled="enabledProjects.length === 0 || projectsStore.loading"
            @update:model-value="store.setDraftProjectId"
          >
            <el-option v-for="p in enabledProjects" :key="p.id" :label="p.name || `#${p.id}`" :value="p.id" />
          </el-select>
          <el-button :loading="projectsStore.loading" @click="projectsStore.fetchProjects()">Reload</el-button>
        </el-space>
      </el-form-item>

      <el-form-item label="Report Type">
        <el-select v-model="store.draft.reportType" style="width: 200px" clearable>
          <el-option label="daily" value="daily" />
          <el-option label="weekly" value="weekly" />
          <el-option label="monthly" value="monthly" />
          <el-option label="special" value="special" />
        </el-select>
      </el-form-item>

      <el-form-item label="Data Range">
        <el-date-picker
          v-model="store.draft.dataRange"
          type="daterange"
          value-format="YYYY-MM-DD"
          range-separator="to"
          start-placeholder="Start"
          end-placeholder="End"
          style="width: 280px"
        />
      </el-form-item>

      <el-form-item label="Created Range">
        <el-date-picker
          v-model="store.draft.createdRange"
          type="daterange"
          value-format="YYYY-MM-DD"
          range-separator="to"
          start-placeholder="Start"
          end-placeholder="End"
          style="width: 280px"
        />
      </el-form-item>

      <el-form-item label="Search">
        <el-input v-model="store.draft.search" placeholder="title / content" clearable style="width: 260px" />
      </el-form-item>

      <el-form-item>
        <el-button type="primary" :loading="store.loading" @click="store.runQuery()">Query</el-button>
        <el-button :disabled="store.loading" @click="store.resetDraft()">Reset</el-button>
        <el-button type="success" @click="emit('open-create')">New</el-button>
      </el-form-item>
    </el-form>
  </PageSection>
</template>

<script setup>
import { computed, watch } from 'vue'
import PageSection from '../common/PageSection.vue'
import { useProjectsStore } from '../../stores/projects'
import { useReportsStore } from '../../stores/reports'

const emit = defineEmits(['open-create'])

const projectsStore = useProjectsStore()
const store = useReportsStore()

const enabledProjects = computed(() => {
  return (projectsStore.projects || []).filter((p) => Number(p?.is_active || 0) === 1)
})

const enabledProjectIds = computed(() => new Set(enabledProjects.value.map((p) => p.id)))

const isActiveProjectEnabled = computed(() => {
  const pid = projectsStore.activeProjectId
  return pid != null && enabledProjectIds.value.has(pid)
})

// Default: keep draft.projectId aligned with activeProjectId unless user manually changed it.
watch(
  () => [projectsStore.activeProjectId, enabledProjects.value.map((p) => p.id).join(',')],
  () => {
    if (!store.followActiveProject) return
    if (enabledProjects.value.length === 0) return
    if (isActiveProjectEnabled.value) {
      store.draft.projectId = projectsStore.activeProjectId
      return
    }
    // Active project not enabled -> follow first enabled.
    store.draft.projectId = enabledProjects.value[0].id
  },
  { immediate: true }
)

const projectModel = computed(() => store.draft.projectId)
</script>
