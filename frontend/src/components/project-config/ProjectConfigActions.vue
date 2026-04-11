<!-- 作用：前端组件：项目配置模块组件（ProjectConfigActions）。 -->

<template>
  <PageSection title="项目配置操作">
    <template #extra>
      <el-tag :type="modeTagType">{{ modeText }}</el-tag>
    </template>

    <el-space wrap>
      <el-tooltip :disabled="!isRefreshing" :content="blockHint" placement="top">
        <el-button :disabled="loading || isRefreshing" type="primary" plain @click="$emit('create')">新建</el-button>
      </el-tooltip>

      <el-tooltip content="仅允许停用项目编辑" placement="top">
        <el-button :disabled="!canEdit || loading || isRefreshing" type="warning" plain @click="$emit('edit')">编辑</el-button>
      </el-tooltip>

      <el-tooltip v-if="mode !== 'view'" :disabled="!isRefreshing" :content="blockHint" placement="top">
        <el-button :disabled="loading || isRefreshing" type="primary" @click="$emit('save')">保存</el-button>
      </el-tooltip>
      <el-button v-if="mode !== 'view'" :disabled="loading" @click="$emit('cancel')">取消</el-button>

      <el-divider direction="vertical" />

      <el-button
        v-if="project && mode === 'view'"
        :disabled="loading || isRefreshing"
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
          <el-tooltip :disabled="!isRefreshing" :content="blockHint" placement="top">
            <el-button type="danger" plain :disabled="loading || isRefreshing">删除</el-button>
          </el-tooltip>
        </template>
      </el-popconfirm>
    </el-space>
  </PageSection>
</template>

<script setup>
import { computed } from 'vue'
import PageSection from '../common/PageSection.vue'
import { useRefreshStore } from '../../stores/refresh'

const props = defineProps({
  mode: { type: String, default: 'view' }, // view | edit | create
  loading: { type: Boolean, default: false },
  project: { type: Object, default: null },
  canEdit: { type: Boolean, default: false },
})

defineEmits(['create', 'edit', 'save', 'cancel', 'toggle-active', 'delete'])

const refreshStore = useRefreshStore()
const isRefreshing = computed(() => refreshStore.isRefreshing(props.project?.id))
const blockHint = '项目正在刷新中，请稍后操作'

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
