<template>
  <PageSection title="Filters">
    <el-form :inline="true" label-width="110px">
      <el-form-item label="Project">
        <el-space wrap>
          <el-select
            v-model="projectModel"
            style="width: 260px"
            placeholder="Select an enabled project"
            :disabled="projectsStore.loading || enabledProjects.length === 0"
          >
            <el-option v-for="p in enabledProjects" :key="p.id" :label="p.name" :value="p.id" />
          </el-select>
          <el-button :loading="projectsStore.loading" @click="projectsStore.fetchProjects()">Reload</el-button>

          <el-tag v-if="store.activeProject && isActiveProjectEnabled" type="info">ID: {{ store.activeProjectId }}</el-tag>
          <el-text v-else type="info">No enabled project</el-text>
          <el-text v-if="store.scopeError" type="danger">{{ store.scopeError }}</el-text>
        </el-space>
      </el-form-item>

      <el-form-item label="Publish Time">
        <el-date-picker
          v-model="store.draft.dateRange"
          type="daterange"
          value-format="YYYY-MM-DD"
          range-separator="to"
          start-placeholder="Start date"
          end-placeholder="End date"
          style="width: 280px"
          :disabled="store.scopeLoading || !store.hasEnabledProject"
        />
      </el-form-item>

      <el-form-item label="Platforms">
        <el-select
          v-model="store.draft.platformIds"
          multiple
          collapse-tags
          collapse-tags-tooltip
          style="width: 260px"
          :disabled="store.scopeLoading || !store.hasEnabledProject"
        >
          <el-option v-for="p in store.platformOptions" :key="p.id" :label="p.name" :value="p.id" />
        </el-select>
      </el-form-item>

      <el-form-item label="Brands">
        <el-select
          v-model="store.draft.brandIds"
          multiple
          collapse-tags
          collapse-tags-tooltip
          style="width: 260px"
          :disabled="store.scopeLoading || !store.hasEnabledProject"
        >
          <el-option v-for="b in store.brandOptions" :key="b.id" :label="b.name" :value="b.id" />
        </el-select>
      </el-form-item>

      <el-form-item label="Keywords">
        <el-select
          v-model="store.draft.keywords"
          multiple
          filterable
          collapse-tags
          collapse-tags-tooltip
          style="width: 260px"
          :disabled="store.scopeLoading || !store.hasEnabledProject"
        >
          <el-option v-for="k in store.keywordOptions" :key="k" :label="k" :value="k" />
        </el-select>
      </el-form-item>

      <el-form-item label="Sentiment Type">
        <el-select
          v-model="store.draft.sentiments"
          multiple
          collapse-tags
          collapse-tags-tooltip
          style="width: 240px"
          :disabled="store.scopeLoading || !store.hasEnabledProject"
        >
          <el-option label="positive" value="positive" />
          <el-option label="neutral" value="neutral" />
          <el-option label="negative" value="negative" />
        </el-select>
      </el-form-item>

      <el-form-item label="Sentiment Score">
        <div style="width: 260px; padding: 0 6px">
          <el-slider v-model="store.draft.sentimentScoreRange" range :min="-1" :max="1" :step="0.05" />
        </div>
      </el-form-item>

      <el-form-item label="Spam">
        <el-select v-model="store.draft.spam" style="width: 160px" :disabled="store.scopeLoading || !store.hasEnabledProject">
          <el-option label="All" :value="null" />
          <el-option label="Spam" value="spam" />
          <el-option label="Normal" value="normal" />
        </el-select>
      </el-form-item>

      <el-form-item label="Valid">
        <el-select v-model="store.draft.isValid" style="width: 160px" :disabled="store.scopeLoading || !store.hasEnabledProject">
          <el-option label="All" :value="null" />
          <el-option label="Valid" :value="true" />
          <el-option label="Invalid" :value="false" />
        </el-select>
      </el-form-item>

      <el-form-item label="Likes">
        <el-space>
          <el-input-number v-model="store.draft.likeMin" :min="0" controls-position="right" placeholder="min" />
          <el-text type="info">-</el-text>
          <el-input-number v-model="store.draft.likeMax" :min="0" controls-position="right" placeholder="max" />
        </el-space>
      </el-form-item>

      <el-form-item label="Comments">
        <el-space>
          <el-input-number v-model="store.draft.commentMin" :min="0" controls-position="right" placeholder="min" />
          <el-text type="info">-</el-text>
          <el-input-number v-model="store.draft.commentMax" :min="0" controls-position="right" placeholder="max" />
        </el-space>
      </el-form-item>

      <el-form-item label="Shares">
        <el-space>
          <el-input-number v-model="store.draft.shareMin" :min="0" controls-position="right" placeholder="min" />
          <el-text type="info">-</el-text>
          <el-input-number v-model="store.draft.shareMax" :min="0" controls-position="right" placeholder="max" />
        </el-space>
      </el-form-item>

      <el-form-item label="Search">
        <el-input
          v-model="store.draft.search"
          placeholder="title / content"
          clearable
          style="width: 260px"
          :disabled="store.scopeLoading || !store.hasEnabledProject"
        />
      </el-form-item>

      <el-form-item>
        <el-button type="primary" :loading="store.overviewLoading" :disabled="!store.hasEnabledProject" @click="store.runQuery()">
          Query
        </el-button>
        <el-button :disabled="store.scopeLoading" @click="store.resetDraft()">Reset</el-button>
      </el-form-item>
    </el-form>

    <el-alert
      v-if="!store.hasEnabledProject"
      style="margin-top: 10px"
      type="warning"
      title="No enabled project selected"
      show-icon
      :closable="false"
    />
  </PageSection>
</template>

<script setup>
import { computed, watch } from 'vue'
import PageSection from '../common/PageSection.vue'
import { usePostsStore } from '../../stores/posts'
import { useProjectsStore } from '../../stores/projects'

const store = usePostsStore()
const projectsStore = useProjectsStore()

const enabledProjects = computed(() => {
  return (projectsStore.projects || []).filter((p) => Number(p?.is_active || 0) === 1)
})

const enabledProjectIds = computed(() => new Set(enabledProjects.value.map((p) => p.id)))

const isActiveProjectEnabled = computed(() => {
  const pid = projectsStore.activeProjectId
  return pid != null && enabledProjectIds.value.has(pid)
})

// Keep Posts default consistent with Dashboard: if active project is not enabled, pick the first enabled one.
watch(
  () => [projectsStore.activeProjectId, enabledProjects.value.map((p) => p.id).join(',')],
  () => {
    if (enabledProjects.value.length === 0) return
    if (isActiveProjectEnabled.value) return
    projectsStore.setActiveProject(enabledProjects.value[0].id)
  },
  { immediate: true }
)

const projectModel = computed({
  get: () => (isActiveProjectEnabled.value ? projectsStore.activeProjectId : enabledProjects.value[0]?.id ?? null),
  set: (v) => projectsStore.setActiveProject(v),
})
</script>
