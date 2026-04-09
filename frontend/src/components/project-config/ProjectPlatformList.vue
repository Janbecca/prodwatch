<!-- 作用：前端组件：项目配置模块组件（ProjectPlatformList）。 -->

<template>
  <PageSection title="已选平台列表（只读）">
    <el-skeleton :loading="loading" animated>
      <template #template>
        <el-skeleton :rows="5" animated />
      </template>
      <template #default>
        <el-empty v-if="!rows || rows.length === 0" description="暂无平台" />
        <el-table v-else :data="rows" stripe style="width: 100%">
          <el-table-column prop="id" label="编号" width="80" />
          <el-table-column prop="code" label="代码" width="140" />
          <el-table-column prop="name" label="名称" min-width="180" />
          <el-table-column prop="is_enabled" label="平台启用" width="120">
            <template #default="{ row }">
              <el-tag :type="row.is_enabled ? 'success' : 'info'">
                {{ row.is_enabled ? '启用' : '未启用' }}
              </el-tag>
            </template>
          </el-table-column>
        </el-table>
      </template>
    </el-skeleton>
  </PageSection>
</template>

<script setup>
import PageSection from '../common/PageSection.vue'

defineProps({
  rows: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
})
</script>
