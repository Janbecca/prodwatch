<!-- 作用：前端组件：通用模块组件（ChartPlaceholder）。 -->

<template>
  <div class="ph">
    <div class="ph__bar" />
    <el-text type="info">{{ text }}</el-text>
  </div>
</template>

<script setup>
defineProps({
  text: { type: String, default: 'Chart Placeholder' },
  height: { type: String, default: '260px' },
})
</script>

<style scoped>
.ph {
  height: v-bind(height);
  border: 1px dashed var(--el-border-color);
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  background: var(--el-fill-color-blank);
}
.ph__bar {
  width: 8px;
  height: 48px;
  background: var(--el-color-primary);
  border-radius: 6px;
  opacity: 0.65;
}
</style>

