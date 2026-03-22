<script setup>
defineProps({
  content: { type: String, required: true },
})
</script>

<template>
  <div class="wrap">
    <h2 class="title">{{ content }}</h2>
  </div>
</template>

<style scoped>
.wrap {
  display: flex;
  align-items: center;
  min-height: 32px;
}

.title {
  margin: 0;
  font-size: 16px;
  font-weight: 600;
}
</style>
