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
.section__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}
.section__extra {
  display: flex;
  align-items: center;
  gap: 8px;
}
</style>

