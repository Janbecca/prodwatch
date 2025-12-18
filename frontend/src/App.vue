<script setup>
</script>

<template>
  <div>
    <nav style="display:flex; gap:12px; padding:12px; border-bottom:1px solid #eee;">
      <router-link to="/analysis">分析</router-link>
      <router-link to="/posts">原始贴文</router-link>
      <router-link to="/report">报告</router-link>
    </nav>
    <router-view />
  </div>
</template>

<style scoped>
</style>
