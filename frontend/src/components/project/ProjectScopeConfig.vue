<!-- 作用：前端组件：项目模块组件（ProjectScopeConfig）。 -->

<template>
  <PageSection title="品牌/平台/关键词配置">
    <el-row :gutter="12">
      <el-col :span="8">
        <el-card shadow="never">
          <template #header><el-text tag="b">品牌</el-text></template>
          <el-checkbox-group v-model="form.brandIds">
            <el-space direction="vertical" alignment="start">
              <el-checkbox v-for="b in brands" :key="b.id" :label="b.id">{{ b.name }}</el-checkbox>
            </el-space>
          </el-checkbox-group>
        </el-card>
      </el-col>

      <el-col :span="8">
        <el-card shadow="never">
          <template #header><el-text tag="b">平台</el-text></template>
          <el-checkbox-group v-model="form.platformIds">
            <el-space direction="vertical" alignment="start">
              <el-checkbox v-for="p in platforms" :key="p.id" :label="p.id">{{ p.name }}</el-checkbox>
            </el-space>
          </el-checkbox-group>
        </el-card>
      </el-col>

      <el-col :span="8">
        <el-card shadow="never">
          <template #header><el-text tag="b">关键词</el-text></template>
          <el-select v-model="form.keywords" multiple collapse-tags collapse-tags-tooltip style="width: 100%">
            <el-option v-for="k in keywordOptions" :key="k" :label="k" :value="k" />
          </el-select>
          <el-divider />
          <el-input placeholder="新增关键词" />
        </el-card>
      </el-col>
    </el-row>
  </PageSection>
</template>

<script setup>
import { reactive } from 'vue'
import PageSection from '../common/PageSection.vue'

const brands = [
  { id: 1, name: '我方品牌' },
  { id: 2, name: '竞品A' },
  { id: 3, name: '竞品B' },
]
const platforms = [
  { id: 1, name: '微博' },
  { id: 2, name: '知乎' },
  { id: 3, name: '抖音' },
]
const keywordOptions = ['续航', '拍照', '价格', '卡顿', '发热']

const form = reactive({
  brandIds: [1, 2],
  platformIds: [1, 2, 3],
  keywords: ['续航', '发热'],
})
</script>
