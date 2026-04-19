<!-- 作用：前端组件：通用模块组件（PageSection）。 -->

<template>
  <el-card class="section" shadow="never">
    <template #header>
      <div class="section__header">
        <el-text tag="b">{{ title }}</el-text>
        <div class="section__extra">
          <slot name="extra" />
        </div>
      </div>
    </template>
    <slot />
  </el-card>
</template>

<script setup>
defineProps({
  title: { type: String, required: true },
})
</script>

<style scoped>
.section {
  display: flex;
  flex-direction: column;
  min-height: 0;
}
.section :deep(.el-card__body) {
  flex: 1;
  min-height: 0;
}
.section__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}
.section__extra {
  display: flex;
  align-items: center;
  gap: 8px;
}
</style>
