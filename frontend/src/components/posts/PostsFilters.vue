<!-- 作用：前端组件：帖子模块组件（PostsFilters）。 -->

<template>
  <PageSection title="筛选" class="filters">
    <el-form :inline="true" label-width="110px">
      <el-form-item label="项目">
        <el-space wrap>
          <el-select
            v-model="projectModel"
            style="width: 260px"
            placeholder="请选择已启用项目"
            :disabled="projectsStore.loading || enabledProjects.length === 0"
          >
            <el-option v-for="p in enabledProjects" :key="p.id" :label="p.name" :value="p.id" />
          </el-select>
          <el-button :loading="projectsStore.loading" @click="projectsStore.fetchProjects()">重载</el-button>

          <el-tag v-if="store.activeProject && isActiveProjectEnabled" type="info">项目编号：{{ store.activeProjectId }}</el-tag>
          <el-text v-else type="info">暂无启用项目</el-text>
          <el-text v-if="store.scopeError" type="danger">{{ store.scopeError }}</el-text>
        </el-space>
      </el-form-item>

      <el-form-item label="发布时间">
        <el-date-picker
          v-model="store.draft.dateRange"
          type="daterange"
          value-format="YYYY-MM-DD"
          range-separator="至"
          start-placeholder="开始日期"
          end-placeholder="结束日期"
          style="width: 280px"
          :disabled="store.scopeLoading || !store.hasEnabledProject"
        />
      </el-form-item>

      <el-form-item label="平台">
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

      <el-form-item label="品牌">
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

      <el-form-item label="关键词命中">
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

      <el-form-item label="情感类型">
        <el-select
          v-model="store.draft.sentiments"
          multiple
          collapse-tags
          collapse-tags-tooltip
          style="width: 240px"
          :disabled="store.scopeLoading || !store.hasEnabledProject"
        >
          <el-option label="正向" value="positive" />
          <el-option label="中性" value="neutral" />
          <el-option label="负向" value="negative" />
        </el-select>
      </el-form-item>

      <el-form-item label="情感分数">
        <div style="width: 260px; padding: 0 6px">
          <el-slider v-model="store.draft.sentimentScoreRange" range :min="-1" :max="1" :step="0.05" />
        </div>
      </el-form-item>

      <!-- 细节 1：删除“垃圾”筛选项（仍保留列表展示垃圾标签）。 -->

      <el-form-item label="有效">
        <el-select v-model="validModel" style="width: 160px" :disabled="store.scopeLoading || !store.hasEnabledProject">
          <el-option label="全部" value="" />
          <el-option label="有效" value="true" />
          <el-option label="无效" value="false" />
        </el-select>
      </el-form-item>

      <el-form-item label="点赞">
        <el-space>
          <el-input-number v-model="store.draft.likeMin" :min="0" controls-position="right" placeholder="最小" />
          <el-text type="info">-</el-text>
          <el-input-number v-model="store.draft.likeMax" :min="0" controls-position="right" placeholder="最大" />
        </el-space>
      </el-form-item>

      <el-form-item label="评论">
        <el-space>
          <el-input-number v-model="store.draft.commentMin" :min="0" controls-position="right" placeholder="最小" />
          <el-text type="info">-</el-text>
          <el-input-number v-model="store.draft.commentMax" :min="0" controls-position="right" placeholder="最大" />
        </el-space>
      </el-form-item>

      <el-form-item label="分享">
        <el-space>
          <el-input-number v-model="store.draft.shareMin" :min="0" controls-position="right" placeholder="最小" />
          <el-text type="info">-</el-text>
          <el-input-number v-model="store.draft.shareMax" :min="0" controls-position="right" placeholder="最大" />
        </el-space>
      </el-form-item>

      <el-form-item label="搜索">
        <el-input
          v-model="store.draft.search"
          placeholder="标题 / 内容"
          clearable
          style="width: 260px"
          :disabled="store.scopeLoading || !store.hasEnabledProject"
        />
      </el-form-item>

      <el-form-item>
        <el-button
          type="primary"
          :loading="store.overviewLoading"
          :disabled="!store.hasEnabledProject"
          @click="store.runQuery()"
        >
          查询
        </el-button>
        <el-button :disabled="store.scopeLoading" @click="store.resetDraft()">重置</el-button>
      </el-form-item>

      <el-form-item>
        <el-tooltip :disabled="!isRefreshing" content="项目正在处理任务中，请稍后再试" placement="top">
          <el-space>
            <el-button
              :loading="isRefreshing"
              type="primary"
              :disabled="!store.hasEnabledProject || store.scopeLoading || isRefreshing"
              @click="dashboard.manualRefresh()"
            >
              手动刷新{{ stageSuffix }}
            </el-button>
          </el-space>
        </el-tooltip>
      </el-form-item>
    </el-form>

    <el-alert
      v-if="!store.hasEnabledProject"
      style="margin-top: 10px"
      type="warning"
      title="未选择已启用项目"
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
import { useRefreshStore } from '../../stores/refresh'
import { useDashboardStore } from '../../stores/dashboard'

const store = usePostsStore()
const projectsStore = useProjectsStore()
const refreshStore = useRefreshStore()
const dashboard = useDashboardStore()

const enabledProjects = computed(() => {
  return (projectsStore.projects || []).filter((p) => Number(p?.is_active || 0) === 1)
})

const enabledProjectIds = computed(() => new Set(enabledProjects.value.map((p) => p.id)))

const isActiveProjectEnabled = computed(() => {
  const pid = projectsStore.activeProjectId
  return pid != null && enabledProjectIds.value.has(pid)
})

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

const isRefreshing = computed(() => refreshStore.isRefreshing(projectsStore.activeProjectId))
const stageSuffix = computed(() => {
  const st = refreshStore.getState(projectsStore.activeProjectId)
  const stage = String(st?.stage || '').trim().toLowerCase()
  if (!isRefreshing.value) return ''
  if (stage === 'simulate') return '（生成中）'
  if (stage === 'analyze') return '（分析中）'
  if (stage === 'aggregate') return '（聚合中）'
  return '（处理中）'
})

const validModel = computed({
  get: () => (store.draft.isValid == null ? '' : store.draft.isValid ? 'true' : 'false'),
  set: (v) => {
    const s = String(v ?? '')
    if (s === '') store.draft.isValid = null
    else if (s === 'true') store.draft.isValid = true
    else if (s === 'false') store.draft.isValid = false
    else store.draft.isValid = null
  },
})
</script>

<style scoped>
.filters :deep(.el-form--inline) {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-start;
}
.filters :deep(.el-form--inline .el-form-item) {
  margin-right: 12px;
}
</style>
