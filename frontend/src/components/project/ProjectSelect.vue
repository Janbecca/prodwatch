<!-- 作用：前端组件：项目模块组件（ProjectSelect）。 -->

<template>
  <el-select v-model="model" :disabled="disabled || locked" style="width: 260px">
    <el-option v-for="p in options" :key="p.id" :label="p.name" :value="p.id" />
  </el-select>
</template>

<script setup>
import { useProjectSelect } from '../../composables/useProjectSelect'

defineProps({
  locked: { type: Boolean, default: false },
})

const { model, options, disabled } = useProjectSelect()
</script>
