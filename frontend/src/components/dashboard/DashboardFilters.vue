<!-- 作用：前端组件：仪表盘模块组件（DashboardFilters）。 -->

<template>
  <PageSection title="筛选">
    <el-form :inline="true" label-width="90px">
      <el-form-item label="项目">
        <el-space wrap>
          <el-select
            v-model="projectModel"
            :disabled="locked || store.loading || enabledProjects.length === 0"
            style="width: 260px"
            placeholder="请选择已启用项目"
          >
            <el-option v-for="p in enabledProjects" :key="p.id" :label="p.name" :value="p.id" />
          </el-select>

          <el-button v-if="!locked" :loading="store.loading" @click="store.fetchProjects()">重载</el-button>

          <el-tag v-if="store.activeProject && isActiveProjectEnabled" type="info">项目编号：{{ store.activeProject.id }}</el-tag>
          <el-text v-else type="info">暂无启用项目</el-text>
          <el-text v-if="store.error" type="danger">{{ store.error }}</el-text>
        </el-space>
      </el-form-item>

      <el-form-item label="品牌">
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

      <el-form-item label="平台">
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

      <el-form-item label="时间">
        <el-select :model-value="dashboard.timeKey" style="width: 160px" @update:model-value="dashboard.setTimeKey">
          <el-option label="最近 7 天" value="7d" />
          <el-option label="最近 14 天" value="14d" />
          <el-option label="最近 30 天" value="30d" />
          <el-option label="自定义" value="custom" />
        </el-select>
      </el-form-item>

      <el-form-item v-if="dashboard.timeKey === 'custom'" label="范围">
        <el-date-picker
          :model-value="dashboard.customRange"
          type="daterange"
          value-format="YYYY-MM-DD"
          range-separator="至"
          start-placeholder="开始日期"
          end-placeholder="结束日期"
          style="width: 280px"
          :disabled="!hasEnabledProject"
          @update:model-value="dashboard.setCustomRange"
        />
      </el-form-item>

      <el-form-item>
        <el-tooltip
          :disabled="!isRefreshing"
          content="项目正在刷新中，请稍后再操作"
          placement="top"
        >
          <el-button
            :loading="dashboard.refreshLoading"
            type="primary"
            :disabled="!hasEnabledProject || dashboard.refreshLoading || dashboard.scopeLoading || isRefreshing"
            @click="dashboard.manualRefresh()"
          >
            手动刷新
          </el-button>
        </el-tooltip>
      </el-form-item>
    </el-form>

    <el-alert
      v-if="store.issues && store.issues.length"
      style="margin-top: 10px"
      title="项目自检发现问题"
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
      title="暂无启用项目，请先在“项目配置”中启用一个项目。"
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
import { useRefreshStore } from '../../stores/refresh'

const props = defineProps({
  locked: { type: Boolean, default: false },
})

const dashboard = useDashboardStore()
const store = useProjectsStore()
const refreshStore = useRefreshStore()

const enabledProjects = computed(() => {
  return (store.projects || []).filter((p) => Number(p?.is_active || 0) === 1)
})

const enabledProjectIds = computed(() => new Set(enabledProjects.value.map((p) => p.id)))

const isActiveProjectEnabled = computed(() => {
  const pid = store.activeProjectId
  return pid != null && enabledProjectIds.value.has(pid)
})

const hasEnabledProject = computed(() => enabledProjects.value.length > 0 && isActiveProjectEnabled.value)
const isRefreshing = computed(() => refreshStore.isRefreshing(store.activeProjectId))

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
