<!-- 作用：前端组件：项目模块组件（ProjectSwitcher）。 -->

<template>
  <PageSection title="项目切换">
    <el-space wrap>
      <el-select v-model="projectId" style="width: 260px">
        <el-option v-for="p in projects" :key="p.id" :label="p.name" :value="p.id" />
      </el-select>
      <el-tag type="info">演示</el-tag>
    </el-space>
  </PageSection>
</template>

<script setup>
import { ref } from 'vue'
import PageSection from '../common/PageSection.vue'

const projects = [
  { id: 1, name: '演示项目' },
  { id: 2, name: '手机旗舰款竞品监控' },
]
const projectId = ref(1)
</script>
