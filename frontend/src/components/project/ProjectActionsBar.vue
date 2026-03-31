<template>
  <PageSection title="操作区">
    <el-space wrap>
      <el-button type="primary">保存配置</el-button>
      <el-button>恢复默认</el-button>
      <el-button type="success" plain>手动刷新</el-button>
      <el-button type="danger" plain>停用项目</el-button>
    </el-space>
  </PageSection>
</template>

<script setup>
import PageSection from '../common/PageSection.vue'
</script>

