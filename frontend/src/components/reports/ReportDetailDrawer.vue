<!-- 作用：前端组件：报告模块组件（ReportDetailDrawer）。 -->

<template>
  <el-drawer v-model="open" size="720px" title="报告详情" :with-header="true">
    <el-alert
      v-if="store.detailError"
      type="error"
      :title="store.detailError"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-skeleton v-if="store.detailLoading" :rows="6" animated />

    <el-empty v-else-if="!item" description="暂无详情" />

    <template v-else>
      <el-descriptions :column="1" border>
        <el-descriptions-item label="标题">{{ item.title || '-' }}</el-descriptions-item>
        <el-descriptions-item label="类型">
          <el-tag size="small" type="info">{{ typeText(item.report_type || '-') }}</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="项目">{{ projectName }}</el-descriptions-item>
        <el-descriptions-item label="数据范围">{{ fmtRange(item.data_start_date, item.data_end_date) }}</el-descriptions-item>
        <el-descriptions-item label="创建时间">{{ fmtTime(item.created_at) }}</el-descriptions-item>
        <el-descriptions-item label="状态">
          <el-tag :type="statusType(item.status)" size="small">{{ statusText(item.status) }}</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="摘要">{{ item.summary || '-' }}</el-descriptions-item>
      </el-descriptions>

      <el-divider />
      <el-text tag="b">内容（标记文本）</el-text>
      <el-space wrap style="margin: 10px 0">
        <el-button size="small" type="primary" :disabled="!item?.id" @click="goDetail">打开报告详情页</el-button>
      </el-space>

      <el-empty v-if="item.status !== 'success' && item.status !== 'done'" description="报告未生成成功" />

      <template v-else>
        <el-text tag="b">执行摘要</el-text>
        <SafeMarkdown :markdown="ai.executive_summary_md || ''" />

        <el-divider v-if="ai.trend_summary_md" />
        <template v-if="ai.trend_summary_md">
          <el-text tag="b">舆情趋势</el-text>
          <SafeMarkdown :markdown="ai.trend_summary_md || ''" />
        </template>

        <el-divider v-if="ai.risk_points_md" />
        <template v-if="ai.risk_points_md">
          <el-text tag="b">风险点</el-text>
          <SafeMarkdown :markdown="ai.risk_points_md || ''" />
        </template>

        <el-divider v-if="ai.key_user_feedback_md" />
        <template v-if="ai.key_user_feedback_md">
          <el-text tag="b">关键用户反馈</el-text>
          <SafeMarkdown :markdown="ai.key_user_feedback_md || ''" />
        </template>

        <el-divider v-if="ai.competitor_compare_md" />
        <template v-if="ai.competitor_compare_md">
          <el-text tag="b">竞品对比</el-text>
          <SafeMarkdown :markdown="ai.competitor_compare_md || ''" />
        </template>

        <el-divider v-if="ai.hot_topics_md" />
        <template v-if="ai.hot_topics_md">
          <el-text tag="b">热点话题</el-text>
          <SafeMarkdown :markdown="ai.hot_topics_md || ''" />
        </template>

        <el-divider v-if="ai.strategy_suggestions_md" />
        <template v-if="ai.strategy_suggestions_md">
          <el-text tag="b">策略建议</el-text>
          <SafeMarkdown :markdown="ai.strategy_suggestions_md || ''" />
        </template>

        <el-divider />
        <el-text type="info">原始内容（调试）</el-text>
        <pre class="md">{{ rawMarkdown || '-' }}</pre>
      </template>
    </template>
  </el-drawer>
</template>

<script setup>
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import { useReportsStore } from '../../stores/reports'
import SafeMarkdown from '../common/SafeMarkdown.vue'

const store = useReportsStore()
const router = useRouter()

const open = computed({
  get: () => store.detailOpen,
  set: (v) => {
    if (!v) store.closeDetail()
  },
})

const item = computed(() => store.detail)

function parseAiBlocks(md) {
  const src = String(md || '')
  const m = src.match(/<!--PRODWATCH_AI_BLOCKS\s*[\r\n]+([\s\S]*?)[\r\n]+-->/)
  if (!m) return {}
  try {
    return JSON.parse(m[1] || '{}') || {}
  } catch {
    return {}
  }
}

const ai = computed(() => {
  const blocks = parseAiBlocks(item.value?.content_markdown || '')
  return {
    executive_summary_md: String(blocks?.executive_summary_md || '').trim(),
    trend_summary_md: String(blocks?.trend_summary_md || '').trim(),
    risk_points_md: String(blocks?.risk_points_md || '').trim(),
    key_user_feedback_md: String(blocks?.key_user_feedback_md || '').trim(),
    competitor_compare_md: String(blocks?.competitor_compare_md || '').trim(),
    hot_topics_md: String(blocks?.hot_topics_md || '').trim(),
    strategy_suggestions_md: String(blocks?.strategy_suggestions_md || '').trim(),
  }
})

const rawMarkdown = computed(() => {
  const src = String(item.value?.content_markdown || '')
  return src.replace(/<!--PRODWATCH_AI_BLOCKS[\s\S]*?-->/g, '').trim()
})

function goDetail() {
  const rid = Number(item.value?.id)
  if (!rid) return
  router.push({ name: 'report-detail', params: { id: String(rid) } })
  store.closeDetail()
}

const projectName = computed(() => {
  const pid = Number(item.value?.project_id)
  return store.projectNameById?.[pid] || (pid ? `#${pid}` : '-')
})

function fmtTime(t) {
  if (!t) return '-'
  return String(t).slice(0, 19).replace('T', ' ')
}

function fmtRange(a, b) {
  if (!a && !b) return '-'
  return `${a || '-'} ~ ${b || '-'}`
}

function statusType(s) {
  if (s === 'success' || s === 'done') return 'success'
  if (s === 'failed' || s === 'error') return 'danger'
  if (s === 'pending' || s === 'running') return 'warning'
  return 'info'
}

function typeText(v) {
  if (v === 'daily') return '日报'
  if (v === 'weekly') return '周报'
  if (v === 'monthly') return '月报'
  if (v === 'special') return '专题'
  return v || '-'
}

function statusText(v) {
  if (v === 'pending') return '待处理'
  if (v === 'running') return '生成中'
  if (v === 'success' || v === 'done') return '成功'
  if (v === 'failed') return '失败'
  if (v === 'error') return '错误'
  return v || '-'
}
</script>

<style scoped>
.md {
  margin: 8px 0 0;
  padding: 10px 12px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 8px;
  background: var(--el-fill-color-blank);
  max-height: 520px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-word;
}
</style>
