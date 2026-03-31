<template>
  <PageSection title="项目切换">
    <el-space wrap>
      <el-select v-model="projectId" style="width: 260px">
        <el-option v-for="p in projects" :key="p.id" :label="p.name" :value="p.id" />
      </el-select>
      <el-tag type="info">Mock</el-tag>
    </el-space>
  </PageSection>
</template>

<script setup>
import { ref } from 'vue'
import PageSection from '../common/PageSection.vue'

const projects = [
  { id: 1, name: 'Demo Project' },
  { id: 2, name: 'Phone X 竞品监控' },
]
const projectId = ref(1)
</script>

