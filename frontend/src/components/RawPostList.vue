<script setup>
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import api from '../api/axios'

const route = useRoute()

// filters
const mode = ref('latest_run') // latest_run | all
const projectId = ref('')
const platformId = ref('')
const brandIds = ref([])

// polling
const autoRefresh = ref(true)
const autoRefreshSeconds = 10
let timer = null

// time filter (cascader-like)
const timePopoverVisible = ref(false)
const timeKey = ref('last14') // last7 | last14 | last30 | custom
const customStart = ref(null) // Date | null
const customEnd = ref(null) // Date | null

const posts = ref([])
const loading = ref(false)
const error = ref('')

const parseIds = (v) => {
  if (v === undefined || v === null) return []
  const arr = Array.isArray(v) ? v : [v]
  return arr
    .map((x) => Number(x))
    .filter((x) => Number.isFinite(x))
    .map((x) => Math.trunc(x))
}

const toDateOnly = (d) => {
  if (!d) return undefined
  const dt = new Date(d)
  if (Number.isNaN(dt.getTime())) return undefined
  const y = dt.getFullYear()
  const m = String(dt.getMonth() + 1).padStart(2, '0')
  const day = String(dt.getDate()).padStart(2, '0')
  return `${y}-${m}-${day}`
}

const timeQuery = computed(() => {
  if (timeKey.value === 'custom') {
    return {
      start_date: toDateOnly(customStart.value),
      end_date: toDateOnly(customEnd.value),
    }
  }
  const days = Number(timeKey.value.replace('last', ''))
  return { days }
})

const isCustomRangeReady = computed(() => {
  if (timeKey.value !== 'custom') return true
  const start = toDateOnly(customStart.value)
  const end = toDateOnly(customEnd.value)
  return Boolean(start && end)
})

const timeText = computed(() => {
  if (timeKey.value === 'custom') return '自定义'
  const days = Number(timeKey.value.replace('last', ''))
  return `近${days}天`
})

const selectedRangeLabel = computed(() => {
  if (timeKey.value === 'custom') {
    const start = toDateOnly(customStart.value)
    const end = toDateOnly(customEnd.value)
    if (start && end) return `${start} ~ ${end}`
    return '请选择自定义日期范围'
  }
  const days = Number(timeKey.value.replace('last', ''))
  const end = new Date()
  const start = new Date()
  start.setDate(end.getDate() - (days - 1))
  return `${toDateOnly(start)} ~ ${toDateOnly(end)}`
})

const queryParams = computed(() => ({
  mode: mode.value || 'latest_run',
  brand_ids: brandIds.value.length ? brandIds.value : undefined,
  project_id: projectId.value === '' ? undefined : Number(projectId.value),
  platform_id: platformId.value === '' ? undefined : Number(platformId.value),
  ...timeQuery.value,
}))

const loadPosts = async () => {
  loading.value = true
  error.value = ''
  try {
    if (!isCustomRangeReady.value) return
    const { data } = await api.get('/api/posts', { params: queryParams.value })
    posts.value = Array.isArray(data) ? data : []
  } catch (e) {
    error.value = e?.response?.data?.detail || '加载帖子失败'
  } finally {
    loading.value = false
  }
}

const startPolling = () => {
  stopPolling()
  if (!autoRefresh.value) return
  timer = setInterval(() => {
    loadPosts()
  }, autoRefreshSeconds * 1000)
}

const stopPolling = () => {
  if (timer) clearInterval(timer)
  timer = null
}

const selectTimeKey = (k) => {
  timeKey.value = k
  if (k !== 'custom') {
    customStart.value = null
    customEnd.value = null
    timePopoverVisible.value = false
  }
}

const onPickCustomStart = (v) => {
  customStart.value = v || null
  if (customStart.value && customEnd.value) {
    if (new Date(customEnd.value).getTime() < new Date(customStart.value).getTime()) {
      customEnd.value = null
    }
  }
}

const onPickCustomEnd = (v) => {
  customEnd.value = v || null
  if (!customEnd.value) return
  if (!isCustomRangeReady.value) return
  timePopoverVisible.value = false
  loadPosts()
}

const formatScore = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '-'
  return n.toFixed(3)
}

watch(
  () => route.query,
  (q) => {
    if (typeof q?.mode === 'string' && q.mode) mode.value = q.mode
    brandIds.value = parseIds(q?.brand_ids)

    const qStart = q?.start_date ? new Date(String(q.start_date)) : null
    const qEnd = q?.end_date ? new Date(String(q.end_date)) : null
    const qDays = q?.days !== undefined && q?.days !== null && q?.days !== '' ? Number(q.days) : null

    if (qStart && qEnd && !Number.isNaN(qStart.getTime()) && !Number.isNaN(qEnd.getTime())) {
      timeKey.value = 'custom'
      customStart.value = qStart
      customEnd.value = qEnd
    } else if ([7, 14, 30].includes(qDays)) {
      timeKey.value = `last${qDays}`
      customStart.value = null
      customEnd.value = null
    }

    loadPosts()
    startPolling()
  },
  { immediate: true, deep: true }
)

watch([mode, timeKey], () => {
  loadPosts()
  startPolling()
})

watch([customStart, customEnd], () => {
  if (timeKey.value !== 'custom') return
  if (!isCustomRangeReady.value) return
  loadPosts()
  startPolling()
})

watch([autoRefresh], () => startPolling())

onBeforeUnmount(() => stopPolling())
</script>

<template>
  <section class="page">
    <el-page-header content="帖子浏览" />

    <div class="topbar">
      
    </div>

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />

    <el-card class="toolbar" shadow="hover">
      <label>品牌 IDs</label>
      <el-input :model-value="brandIds.join(',')" readonly placeholder="" style="width: 260px" />

      <label>项目 ID</label>
      <el-input v-model="projectId" type="number" placeholder="可选" style="width: 120px" />

      <label>平台 ID</label>
      <el-input v-model="platformId" type="number" placeholder="可选" style="width: 120px" />

      <el-button type="primary" :loading="loading" @click="loadPosts">
        {{ loading ? '加载中…' : '查询 /api/posts' }}
      </el-button>
    </el-card>

    <el-card class="refresh" shadow="hover">
      <div class="refresh-row">
        <label>模式</label>
        <el-select v-model="mode" style="width: 140px">
          <el-option label="最近分析" value="latest_run" />
          <el-option label="全部" value="all" />
        </el-select>

        <label>
          时间范围
          <span class="range">{{ selectedRangeLabel }}</span>
        </label>
        <el-popover v-model:visible="timePopoverVisible" trigger="manual" placement="bottom-start" width="420">
          <template #reference>
            <el-input
              readonly
              :model-value="timeText"
              style="width: 220px"
              placeholder="选择时间范围"
              @click="timePopoverVisible = !timePopoverVisible"
            />
          </template>
          <div class="time-pop">
            <div class="time-level1">
              <div class="time-item" :class="{ active: timeKey === 'last7' }" @click="selectTimeKey('last7')">
                近7天
              </div>
              <div class="time-item" :class="{ active: timeKey === 'last14' }" @click="selectTimeKey('last14')">
                近14天
              </div>
              <div class="time-item" :class="{ active: timeKey === 'last30' }" @click="selectTimeKey('last30')">
                近30天
              </div>
              <div class="time-item" :class="{ active: timeKey === 'custom' }" @click="selectTimeKey('custom')">
                自定义
              </div>
            </div>
            <div class="time-level2" v-if="timeKey === 'custom'">
              <div class="custom-row">
                <el-date-picker
                  v-model="customStart"
                  type="date"
                  placeholder="开始日期"
                  style="width: 100%"
                  :teleported="false"
                  @change="onPickCustomStart"
                />
                <el-date-picker
                  v-model="customEnd"
                  type="date"
                  placeholder="结束日期"
                  style="width: 100%"
                  :teleported="false"
                  @change="onPickCustomEnd"
                />
              </div>
            </div>
          </div>
        </el-popover>

        <div class="label">自动刷新（{{ autoRefreshSeconds }}秒）</div>
        <el-switch v-model="autoRefresh" />
        <el-button :loading="loading" @click="loadPosts">立即刷新</el-button>
      </div>
    </el-card>

    <div class="summary">总数：{{ posts.length }}</div>

    <el-card class="panel" shadow="hover">
      <el-table :data="posts" row-key="id" stripe size="small" class="table">
        <el-table-column type="expand">
          <template #default="{ row }">
            <div class="expand">
              <div class="raw">原文：{{ row.raw_text }}</div>
            </div>
          </template>
        </el-table-column>

        <el-table-column prop="id" label="ID" width="90" />
        <el-table-column prop="publish_time" label="发布时间" min-width="160" />

        <el-table-column label="品牌" min-width="120" show-overflow-tooltip>
          <template #default="{ row }">
            {{ row.brand_name || (row.brand_id ? `#${row.brand_id}` : '-') }}
          </template>
        </el-table-column>
        <el-table-column prop="keyword" label="关键词" min-width="140" show-overflow-tooltip />

        <el-table-column label="情感" width="90">
          <template #default="{ row }">{{ row.polarity ?? '-' }}</template>
        </el-table-column>
        <el-table-column label="置信度" width="100">
          <template #default="{ row }">{{ formatScore(row.confidence) }}</template>
        </el-table-column>
        <el-table-column label="强度" width="90">
          <template #default="{ row }">{{ formatScore(row.intensity) }}</template>
        </el-table-column>
        <el-table-column prop="emotions" label="情绪" min-width="160" show-overflow-tooltip />

        <el-table-column prop="project_id" label="项目" width="90" />
        <el-table-column prop="platform_id" label="平台" width="90" />
        <el-table-column prop="like_count" label="赞" width="70" />
        <el-table-column prop="comment_count" label="评" width="70" />
        <el-table-column prop="share_count" label="转" width="70" />
      </el-table>
    </el-card>
  </section>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.topbar {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  flex-wrap: wrap;
  color: #374151;
}


.range {
  margin-left: 8px;
  font-size: 12px;
  color: #6b7280;
}

.toolbar {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  align-items: center;
  border-radius: 10px;
}

.refresh {
  border-radius: 10px;
}

.refresh-row {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.refresh-row .label {
  font-size: 12px;
  color: #4b5563;
}

.summary {
  color: #374151;
}

.panel {
  border-radius: 10px;
}

.table {
  width: 100%;
}

.expand {
  padding: 6px 10px;
}

.raw {
  white-space: pre-wrap;
  word-break: break-word;
  line-height: 1.6;
}

.time-pop {
  display: flex;
  gap: 10px;
}

.time-level1 {
  width: 120px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.time-item {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 8px 10px;
  cursor: pointer;
  font-size: 13px;
  color: #111827;
  background: #fff;
}

.time-item.active {
  border-color: #409eff;
  background: #ecf5ff;
}

.time-level2 {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.custom-row {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
</style>

