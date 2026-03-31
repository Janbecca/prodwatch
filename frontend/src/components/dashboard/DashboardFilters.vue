<template>
  <PageSection title="Filters">
    <el-form :inline="true" label-width="90px">
      <el-form-item label="Project">
        <el-space wrap>
          <el-select
            v-model="projectModel"
            :disabled="locked || store.loading || enabledProjects.length === 0"
            style="width: 260px"
            placeholder="Select an enabled project"
          >
            <el-option v-for="p in enabledProjects" :key="p.id" :label="p.name" :value="p.id" />
          </el-select>

          <el-button v-if="!locked" :loading="store.loading" @click="store.fetchProjects()">Reload</el-button>

          <el-tag v-if="store.activeProject && isActiveProjectEnabled" type="info">ID: {{ store.activeProject.id }}</el-tag>
          <el-text v-else type="info">No enabled project</el-text>
          <el-text v-if="store.error" type="danger">{{ store.error }}</el-text>
        </el-space>
      </el-form-item>

      <el-form-item label="Brands">
        <el-select
          :model-value="dashboard.brandIds"
          multiple
          collapse-tags
          collapse-tags-tooltip
          style="width: 260px"
          :loading="dashboard.scopeLoading"
          :disabled="!hasEnabledProject"
          @update:model-value="dashboard.setBrandIds"
        >
          <el-option v-for="b in dashboard.brandOptions" :key="b.id" :label="b.name" :value="b.id" />
        </el-select>
      </el-form-item>

      <el-form-item label="Platforms">
        <el-select
          :model-value="dashboard.platformIds"
          multiple
          collapse-tags
          collapse-tags-tooltip
          style="width: 260px"
          :loading="dashboard.scopeLoading"
          :disabled="!hasEnabledProject"
          @update:model-value="dashboard.setPlatformIds"
        >
          <el-option v-for="p in dashboard.platformOptions" :key="p.id" :label="p.name" :value="p.id" />
        </el-select>
      </el-form-item>

      <el-form-item label="Time">
        <el-select :model-value="dashboard.timeKey" style="width: 160px" @update:model-value="dashboard.setTimeKey">
          <el-option label="Last 7 days" value="7d" />
          <el-option label="Last 14 days" value="14d" />
          <el-option label="Last 30 days" value="30d" />
          <el-option label="Custom" value="custom" />
        </el-select>
      </el-form-item>

      <el-form-item v-if="dashboard.timeKey === 'custom'" label="Range">
        <el-date-picker
          :model-value="dashboard.customRange"
          type="daterange"
          value-format="YYYY-MM-DD"
          range-separator="to"
          start-placeholder="Start date"
          end-placeholder="End date"
          style="width: 280px"
          :disabled="!hasEnabledProject"
          @update:model-value="dashboard.setCustomRange"
        />
      </el-form-item>

      <el-form-item>
        <el-button
          :loading="dashboard.refreshLoading"
          type="primary"
          :disabled="!hasEnabledProject"
          @click="dashboard.manualRefresh()"
        >
          Manual refresh
        </el-button>
      </el-form-item>
    </el-form>

    <el-alert
      v-if="store.issues && store.issues.length"
      style="margin-top: 10px"
      title="Project self-check found issues"
      type="warning"
      :closable="false"
      show-icon
    >
      <template #default>
        <div v-for="it in store.issues" :key="it">{{ it }}</div>
      </template>
    </el-alert>

    <el-alert
      v-if="enabledProjects.length === 0 && !store.loading"
      style="margin-top: 10px"
      type="warning"
      title="No enabled projects. Enable one in Project Config first."
      show-icon
      :closable="false"
    />

    <el-alert
      v-if="dashboard.scopeError"
      style="margin-top: 10px"
      type="error"
      :title="dashboard.scopeError"
      show-icon
      :closable="false"
    />
  </PageSection>
</template>

<script setup>
import { computed, watch } from 'vue'
import PageSection from '../common/PageSection.vue'
import { useDashboardStore } from '../../stores/dashboard'
import { useProjectsStore } from '../../stores/projects'

const props = defineProps({
  locked: { type: Boolean, default: false },
})

const dashboard = useDashboardStore()
const store = useProjectsStore()

const enabledProjects = computed(() => {
  return (store.projects || []).filter((p) => Number(p?.is_active || 0) === 1)
})

const enabledProjectIds = computed(() => new Set(enabledProjects.value.map((p) => p.id)))

const isActiveProjectEnabled = computed(() => {
  const pid = store.activeProjectId
  return pid != null && enabledProjectIds.value.has(pid)
})

const hasEnabledProject = computed(() => enabledProjects.value.length > 0 && isActiveProjectEnabled.value)

// If the current activeProjectId is not enabled, auto-select the first enabled project.
watch(
  () => [store.activeProjectId, enabledProjects.value.map((p) => p.id).join(',')],
  () => {
    if (props.locked) return
    if (enabledProjects.value.length === 0) return
    if (isActiveProjectEnabled.value) return
    store.setActiveProject(enabledProjects.value[0].id)
  },
  { immediate: true }
)

const projectModel = computed({
  get: () => (isActiveProjectEnabled.value ? store.activeProjectId : enabledProjects.value[0]?.id ?? null),
  set: (v) => {
    if (props.locked) return
    store.setActiveProject(v)
  },
})
</script>

