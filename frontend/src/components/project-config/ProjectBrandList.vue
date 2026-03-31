<template>
  <PageSection title="已选品牌列表（只读）">
    <el-skeleton :loading="loading" animated>
      <template #template>
        <el-skeleton :rows="5" animated />
      </template>
      <template #default>
        <el-empty v-if="!rows || rows.length === 0" description="暂无品牌" />
        <el-table v-else :data="rows" stripe style="width: 100%">
          <el-table-column prop="id" label="ID" width="80" />
          <el-table-column prop="name" label="名称" min-width="160" />
          <el-table-column prop="alias" label="别名" min-width="140" />
          <el-table-column prop="category" label="分类" min-width="120" />
          <el-table-column prop="is_core_brand" label="核心" width="90">
            <template #default="{ row }">
              <el-tag :type="row.is_core_brand ? 'success' : 'info'">
                {{ row.is_core_brand ? '是' : '否' }}
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

