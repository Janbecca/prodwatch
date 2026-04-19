<!-- 作用：前端页面：系统功能概览（模块图 + 架构图，中英文切换） -->

<template>
  <div class="pw-page-fit">
    <el-space direction="vertical" :size="12" fill>
      <el-card shadow="never">
        <template #header>
          <div class="header">
            <div class="title">
              <el-text tag="b">系统功能概览</el-text>
              <el-text type="info" class="sub">前端/后端/服务层/LLM/数据层模块关系图</el-text>
            </div>

            <el-radio-group v-model="lang" size="small">
              <el-radio-button label="zh">中文</el-radio-button>
              <el-radio-button label="en">English</el-radio-button>
            </el-radio-group>
          </div>
        </template>

        <SystemModuleDiagram :lang="lang" />
      </el-card>

      <el-card shadow="never">
        <template #header>
          <div class="header">
            <div class="title">
              <el-text tag="b">系统架构图</el-text>
              <el-text type="info" class="sub">请求链路、长任务、调度、LLM 与数据层</el-text>
            </div>
          </div>
        </template>

        <SystemArchitectureDiagram :lang="lang" />
      </el-card>
    </el-space>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'
import SystemModuleDiagram from '../components/system/SystemModuleDiagram.vue'
import SystemArchitectureDiagram from '../components/system/SystemArchitectureDiagram.vue'

const STORAGE_KEY = 'pw.system_overview.lang'
const lang = ref(localStorage.getItem(STORAGE_KEY) || 'zh')
watch(
  () => lang.value,
  (v) => {
    localStorage.setItem(STORAGE_KEY, String(v || 'zh'))
  }
)
</script>

<style scoped>
.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}
.title {
  display: flex;
  align-items: baseline;
  gap: 10px;
}
.sub {
  font-size: 12px;
}
</style>

