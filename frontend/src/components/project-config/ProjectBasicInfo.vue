<!-- 作用：前端组件：项目配置模块组件（ProjectBasicInfo）。 -->

<template>
  <PageSection title="项目基础信息（只读）">
    <el-skeleton :loading="loading" animated>
      <template #template>
        <el-skeleton :rows="6" animated />
      </template>

      <template #default>
        <el-descriptions :column="2" border>
          <el-descriptions-item label="项目编号">{{ show(project?.id) }}</el-descriptions-item>
          <el-descriptions-item label="项目名称">{{ show(project?.name) }}</el-descriptions-item>
          <el-descriptions-item label="产品品类">{{ show(project?.product_category) }}</el-descriptions-item>
          <el-descriptions-item label="我方品牌">{{ show(project?.our_brand_name) }}</el-descriptions-item>
          <el-descriptions-item label="是否启用">
            <el-tag v-if="project" :type="project.is_active ? 'success' : 'info'">
              {{ project.is_active ? '启用' : '未启用' }}
            </el-tag>
            <span v-else>—</span>
          </el-descriptions-item>
          <el-descriptions-item label="项目状态">{{ showStatus(project?.status) }}</el-descriptions-item>
          <el-descriptions-item label="刷新方式">{{ showRefreshMode(project?.refresh_mode) }}</el-descriptions-item>
          <el-descriptions-item label="自动刷新表达式">{{ show(project?.refresh_cron) }}</el-descriptions-item>
          <el-descriptions-item label="创建时间">{{ show(project?.created_at) }}</el-descriptions-item>
          <el-descriptions-item label="更新时间">{{ show(project?.updated_at) }}</el-descriptions-item>
          <el-descriptions-item label="项目描述" :span="2">
            <div class="desc">{{ show(project?.description) }}</div>
          </el-descriptions-item>
        </el-descriptions>
      </template>
    </el-skeleton>
  </PageSection>
</template>

<script setup>
import PageSection from '../common/PageSection.vue'

defineProps({
  project: { type: Object, default: null },
  loading: { type: Boolean, default: false },
})

function show(v) {
  if (v === null || v === undefined || String(v).trim() === '') return '—'
  return v
}

function showStatus(v) {
  if (v === null || v === undefined || String(v).trim() === '') return '—'
  const s = String(v || '').toLowerCase()
  if (s === 'draft') return '草稿'
  if (s === 'inactive') return '停用'
  if (s === 'active') return '启用'
  if (s === 'archived') return '归档'
  return v
}

function showRefreshMode(v) {
  if (v === null || v === undefined || String(v).trim() === '') return '—'
  const s = String(v || '').toLowerCase()
  if (s === 'manual') return '手动'
  if (s === 'daily') return '每日'
  return v
}
</script>

<style scoped>
.desc {
  white-space: pre-wrap;
  color: var(--el-text-color-regular);
}
</style>
