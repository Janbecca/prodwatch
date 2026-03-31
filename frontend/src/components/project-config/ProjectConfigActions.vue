<template>
  <PageSection title="项目配置操作">
    <template #extra>
      <el-tag :type="modeTagType">{{ modeText }}</el-tag>
    </template>

    <el-space wrap>
      <el-button :disabled="loading" type="primary" plain @click="$emit('create')">新建</el-button>

      <el-tooltip content="仅允许停用项目编辑" placement="top">
        <el-button :disabled="!canEdit || loading" type="warning" plain @click="$emit('edit')">编辑</el-button>
      </el-tooltip>

      <el-button v-if="mode !== 'view'" :disabled="loading" type="primary" @click="$emit('save')">保存</el-button>
      <el-button v-if="mode !== 'view'" :disabled="loading" @click="$emit('cancel')">取消</el-button>

      <el-divider direction="vertical" />

      <el-button
        v-if="project && mode === 'view'"
        :disabled="loading"
        :type="String(project.status || '') === 'active' ? 'danger' : 'success'"
        plain
        @click="$emit('toggle-active')"
      >
        {{ String(project.status || '') === 'active' ? '停用项目' : '启用项目' }}
      </el-button>

      <el-popconfirm
        v-if="mode === 'edit' && project"
        title="确认删除该项目？"
        confirm-button-text="删除"
        cancel-button-text="取消"
        @confirm="$emit('delete')"
      >
        <template #reference>
          <el-button type="danger" plain :disabled="loading">删除</el-button>
        </template>
      </el-popconfirm>
    </el-space>
  </PageSection>
</template>

<script setup>
import { computed } from 'vue'
import PageSection from '../common/PageSection.vue'

const props = defineProps({
  mode: { type: String, default: 'view' }, // view | edit | create
  loading: { type: Boolean, default: false },
  project: { type: Object, default: null },
  canEdit: { type: Boolean, default: false },
})

defineEmits(['create', 'edit', 'save', 'cancel', 'toggle-active', 'delete'])

const modeText = computed(() => {
  if (props.mode === 'create') return '新建态'
  if (props.mode === 'edit') return '编辑态'
  return '查看态'
})
const modeTagType = computed(() => {
  if (props.mode === 'create') return 'success'
  if (props.mode === 'edit') return 'warning'
  return 'info'
})
</script>
