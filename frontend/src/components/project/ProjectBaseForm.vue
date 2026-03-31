<template>
  <PageSection title="项目基础信息配置">
    <el-form label-width="110px">
      <el-row :gutter="12">
        <el-col :span="12">
          <el-form-item label="项目名称">
            <el-input v-model="form.name" />
          </el-form-item>
        </el-col>
        <el-col :span="12">
          <el-form-item label="品类">
            <el-input v-model="form.category" />
          </el-form-item>
        </el-col>
      </el-row>

      <el-form-item label="描述">
        <el-input v-model="form.desc" type="textarea" :rows="3" />
      </el-form-item>

      <el-row :gutter="12">
        <el-col :span="12">
          <el-form-item label="状态">
            <el-select v-model="form.status" style="width: 100%">
              <el-option label="draft" value="draft" />
              <el-option label="inactive" value="inactive" />
              <el-option label="active" value="active" />
              <el-option label="archived" value="archived" />
            </el-select>
          </el-form-item>
        </el-col>
        <el-col :span="12">
          <el-form-item label="刷新模式">
            <el-select v-model="form.refreshMode" style="width: 100%">
              <el-option label="manual" value="manual" />
              <el-option label="daily" value="daily" />
            </el-select>
          </el-form-item>
        </el-col>
      </el-row>
    </el-form>
  </PageSection>
</template>

<script setup>
import { reactive } from 'vue'
import PageSection from '../common/PageSection.vue'

const form = reactive({
  name: 'Demo Project',
  category: 'demo',
  desc: '这是一个用于展示前端骨架的 Mock 项目。',
  status: 'active',
  refreshMode: 'manual',
})
</script>

