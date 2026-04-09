<!-- 作用：前端组件：项目配置模块组件（ProjectKeywordList）。 -->

<template>
  <PageSection title="已配置关键词列表（只读）">
    <el-skeleton :loading="loading" animated>
      <template #template>
        <el-skeleton :rows="6" animated />
      </template>
      <template #default>
        <el-empty v-if="!rows || rows.length === 0" description="暂无关键词" />
        <el-table v-else :data="rows" stripe style="width: 100%">
          <el-table-column prop="keyword" label="关键词" min-width="160" />
          <el-table-column prop="keyword_type" label="类型" width="140" />
          <el-table-column prop="weight" label="权重" width="100" />
          <el-table-column prop="is_enabled" label="是否启用" width="120">
            <template #default="{ row }">
              <el-tag :type="row.is_enabled ? 'success' : 'info'">
                {{ row.is_enabled ? '启用' : '停用' }}
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
