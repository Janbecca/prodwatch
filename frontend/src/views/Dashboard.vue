<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import * as echarts from 'echarts'
import api from '../api/axios'
import PageHeader from '../components/PageHeader.vue'
import { useProjectsStore } from '../stores/projects'

const router = useRouter()
const projectsStore = useProjectsStore()

const loading = ref(false)
const error = ref('')

const options = ref({ brands: [], projects: [], platforms: [] })
const selectedBrandIds = ref([])
const selectedPlatformIds = ref([])

// manual refresh params
const refreshMaxPostsPerRun = ref(30)
const refreshSentimentModel = ref('rule-based')

const enabledProjects = computed(() => projectsStore.enabledProjects || [])
const activeProjectId = computed({
  get: () => projectsStore.activeProjectId,
  set: (v) => projectsStore.setActiveProjectId(v),
})
const activeProject = computed(() => projectsStore.activeProject)

const visibleBrands = computed(() => {
  const all = options.value?.brands || []
  const p = activeProject.value
  if (!p) return all
  const allow = new Set((p.brand_ids || []).map((x) => Number(x)))
  return all.filter((b) => allow.has(Number(b.id)))
})

const visiblePlatforms = computed(() => {
  const all = options.value?.platforms || []
  const p = activeProject.value
  if (!p) return all
  const allowIds = (p.enabled_platform_ids || []).map((x) => Number(x)).filter((x) => Number.isFinite(x))
  if (!allowIds.length) return all
  const allow = new Set(allowIds)
  return all.filter((pl) => allow.has(Number(pl.id)))
})

// 时间范围选择相关的状态
const timePopoverVisible = ref(false)
const timeKey = ref('last14') // last7 | last14 | last30 | custom
const customStart = ref(null) // Date | null
const customEnd = ref(null) // Date | null

const trendMetric = ref('negative_ratio') // negative_ratio | positive_ratio | spam_ratio

const overview = ref(null)
const trends = ref({ dates: [], series: [], metric: trendMetric.value })
const alerts = ref([])
const keywordFreq = ref({ top_keywords: [], brands: [], range: null })

const chartEl = ref(null)
let chart = null
const keywordChartEl = ref(null)
let keywordChart = null
let timer = null

const canCompare = computed(() => selectedBrandIds.value.length >= 2)

// 格式化 API 错误信息，优先显示后端返回的 detail 字段
const formatApiError = (e) => {
  const detail = e?.response?.data?.detail
  if (typeof detail === 'string') return detail
  if (Array.isArray(detail)) return JSON.stringify(detail)
  return e?.message || '请求失败'
}

// 将一个对象转换为 URLSearchParams，适用于 GET 请求的查询参数
const buildParams = (obj) => {
  const params = new URLSearchParams()
  for (const [k, v] of Object.entries(obj || {})) {
    if (v === undefined || v === null) continue
    if (Array.isArray(v)) {
      for (const item of v) params.append(k, String(item))
      continue
    }
    params.append(k, String(v))
  }
  return params
}

// 将一个日期字符串转换为 YYYY-MM-DD 格式，如果输入无效则返回 undefined
const toDateOnly = (d) => {
  if (!d) return undefined
  const dt = new Date(d)
  if (Number.isNaN(dt.getTime())) return undefined
  const y = dt.getFullYear()
  const m = String(dt.getMonth() + 1).padStart(2, '0')
  const day = String(dt.getDate()).padStart(2, '0')
  return `${y}-${m}-${day}`
}

// 确保 selectedBrandIds 不超过 4 个，并且至少有一个（如果 options 中有品牌的话）
// 注意：这里不能在每次 fetchAll 时无条件写 selectedBrandIds，否则会触发 watch 递归更新。
const clampSelectedBrands = () => {
  const allowList = visibleBrands.value || []
  const allow = new Set(allowList.map((b) => Number(b.id)).filter((x) => Number.isFinite(x)))

  const cur = Array.isArray(selectedBrandIds.value) ? selectedBrandIds.value : []
  let next = [...cur]

  if (allow.size) next = next.filter((x) => allow.has(Number(x)))
  if (next.length > 4) next = next.slice(0, 4)
  if (next.length === 0) {
    if (allowList.length) next = [allowList[0].id]
    else if ((options.value.brands || []).length) next = [options.value.brands[0].id]
  }

  const same = next.length === cur.length && next.every((v, i) => Number(v) === Number(cur[i]))
  if (!same) {
    suppressAutoFetchAll = true
    try {
      selectedBrandIds.value = next
    } finally {
      suppressAutoFetchAll = false
    }
  }
}

const clampSelectedPlatforms = () => {
  const allowList = visiblePlatforms.value || []
  const allow = new Set(allowList.map((p) => Number(p.id)).filter((x) => Number.isFinite(x)))

  const cur = Array.isArray(selectedPlatformIds.value) ? selectedPlatformIds.value : []
  let next = [...cur].map((x) => Number(x)).filter((x) => Number.isFinite(x))

  if (allow.size) next = next.filter((x) => allow.has(Number(x)))
  if (next.length === 0 && allowList.length) next = allowList.map((x) => Number(x.id)).filter((x) => Number.isFinite(x))

  next = Array.from(new Set(next))

  const same = next.length === cur.length && next.every((v, i) => Number(v) === Number(cur[i]))
  if (!same) {
    suppressAutoFetchAll = true
    try {
      selectedPlatformIds.value = next
    } finally {
      suppressAutoFetchAll = false
    }
  }
}

let suppressAutoFetchAll = false

// 将当前激活项目的品牌 ID 应用到 selectedBrandIds 中，并触发数据刷新
const applyProjectToBrands = async () => {
  const p = activeProject.value
  if (!p) return
  const ids = (p.brand_ids || []).map((x) => Number(x)).filter((x) => Number.isFinite(x))
  const plats = (p.enabled_platform_ids || []).map((x) => Number(x)).filter((x) => Number.isFinite(x))
  suppressAutoFetchAll = true
  try {
    selectedBrandIds.value = ids.slice(0, 4)
    selectedPlatformIds.value = plats
    await fetchAll()
  } finally {
    suppressAutoFetchAll = false
  }
}

// 根据 timeKey 和 customStart/customEnd 计算出用于 API 请求的时间参数
const timeQuery = computed(() => {
  if (timeKey.value === 'custom') {
    const start = toDateOnly(customStart.value)
    const end = toDateOnly(customEnd.value)
    return { start_date: start, end_date: end }
  }
  const days = Number(timeKey.value.replace('last', ''))
  return { days }
})

// 判断自定义日期范围是否准备就绪（即 start 和 end 都是有效日期）
const isCustomRangeReady = computed(() => {
  if (timeKey.value !== 'custom') return true
  const start = toDateOnly(customStart.value)
  const end = toDateOnly(customEnd.value)
  return Boolean(start && end)
})
// 根据 timeKey 计算出显示在输入框中的文本
const timeText = computed(() => {
  if (timeKey.value === 'custom') return '自定义'
  const days = Number(timeKey.value.replace('last', ''))
  return `近 ${days} 天`
})
// 根据 overview 中的 range 或者 timeKey 和 customStart/customEnd 计算出当前选中的时间范围标签
const selectedRangeLabel = computed(() => {
  const r = overview.value?.range
  if (r?.from && r?.to) return `${r.from} ~ ${r.to}`

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

// 从后端获取可选的品牌、项目和平台列表，并更新 options 状态
const fetchOptions = async () => {
  const { data } = await api.get('/api/dashboard/options')
  options.value = data
  clampSelectedBrands()
  clampSelectedPlatforms()
}
// 从后端获取仪表盘的所有数据（概览、趋势、告警、关键词频率），并更新对应的状态
const fetchAll = async () => {
  loading.value = true
  error.value = ''
  try {
    clampSelectedBrands()
    clampSelectedPlatforms()
    if (!isCustomRangeReady.value) return
    // 构建基础的查询参数，包括选中的品牌 ID / 当前项目 / 时间范围
    const baseParams = { brand_ids: selectedBrandIds.value, project_id: activeProjectId.value || undefined, ...timeQuery.value }
    const [o, t, a, k] = await Promise.all([
      api.get('/api/dashboard/overview', { params: buildParams(baseParams) }),
      api.get('/api/dashboard/sentiment_trends', {
        params: buildParams({ ...baseParams, metric: trendMetric.value }),
      }),
      api.get('/api/dashboard/sentiment_alerts', { params: buildParams(baseParams) }),
      api.get('/api/dashboard/keyword_frequencies', { params: buildParams({ ...baseParams, top_n: 12 }) }),
    ])
    overview.value = o.data
    trends.value = t.data
    alerts.value = a.data
    keywordFreq.value = k.data
  } catch (e) {
    error.value = formatApiError(e) || '加载仪表盘数据失败'
  } finally {
    loading.value = false
  }
}
// 触发后端进行一次手动数据刷新，刷新完成后重新获取所有数据
const manualRefresh = async () => {
  loading.value = true
  error.value = ''
  try {
    clampSelectedBrands()
    clampSelectedPlatforms()
    if (!isCustomRangeReady.value) return

    await api.post('/api/dashboard/manual_refresh', {
      brand_ids: selectedBrandIds.value,
      project_id: activeProjectId.value || undefined,
      platform_ids: selectedPlatformIds.value.length ? selectedPlatformIds.value : undefined,
      max_posts_per_run: Number(refreshMaxPostsPerRun.value) || 30,
      sentiment_model: String(refreshSentimentModel.value || 'rule-based'),
      trigger_type: 'manual',
    })

    await fetchAll()
  } catch (e) {
    error.value = formatApiError(e) || '手动刷新失败'
  } finally {
    loading.value = false
  }
}

// 构建情绪趋势图的配置项，根据 trends 数据和当前选中的 trendMetric 计算出适合 ECharts 的配置对象
const buildChartOption = () => {
  const dates = trends.value?.dates || []
  const series = (trends.value?.series || []).map((s) => ({
    name: s.name,
    type: 'line',
    smooth: true,
    showSymbol: false,
    data: s.data || [],
  }))

  const metricLabel =
    trendMetric.value === 'positive_ratio' ? '正面占比' : trendMetric.value === 'spam_ratio' ? '水军占比' : '负面占比'

  return {
    tooltip: { trigger: 'axis', valueFormatter: (v) => `${(Number(v) * 100).toFixed(1)}%` },
    legend: { top: 6 },
    grid: { left: 40, right: 18, top: 46, bottom: 34 },
    xAxis: { type: 'category', data: dates },
    yAxis: { type: 'value', axisLabel: { formatter: (v) => `${Math.round(v * 100)}%` }, min: 0, max: 1 },
    series,
    title: { text: `情绪趋势 - ${metricLabel}`, left: 8, top: 6, textStyle: { fontSize: 14 } },
  }
}
// 构建关键词监控图的配置项，根据 keywordFreq 数据计算出适合 ECharts 的配置对象，展示各品牌在 top 关键词上的分布情况
const buildKeywordChartOption = () => {
  const top = keywordFreq.value?.top_keywords || []
  const categories = top.map((x) => x.keyword).filter((x) => x != null && String(x).trim() !== '')
  const brands = keywordFreq.value?.brands || []
  const series = (brands || []).map((b) => {
    const itemMap = new Map((b.items || []).map((x) => [String(x.keyword), Number(x.count) || 0]))
    return {
      name: b.brand_name || (b.brand_id != null ? `#${b.brand_id}` : '品牌'),
      type: 'bar',
      stack: 'total',
      emphasis: { focus: 'series' },
      data: categories.map((k) => itemMap.get(String(k)) || 0),
    }
  })

  return {
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { top: 6 },
    grid: { left: 40, right: 18, top: 46, bottom: 64 },
    xAxis: { type: 'category', data: categories, axisLabel: { rotate: 25 } },
    yAxis: { type: 'value' },
    series,
    title: { text: '关键词监控（Top 12）', left: 8, top: 6, textStyle: { fontSize: 14 } },
  }
}
// 渲染情绪趋势图，如果 chartEl 已经存在但 chart 实例不存在，则初始化一个新的 ECharts 实例；如果没有数据则显示暂无数据的提示
const renderKeywordChart = () => {
  if (!keywordChartEl.value) return
  if (!keywordChart) keywordChart = echarts.init(keywordChartEl.value)
  const hasData = (keywordFreq.value?.top_keywords || []).length > 0
  if (!hasData) {
    keywordChart.clear()
    keywordChart.setOption({
      title: { text: '关键词监控', left: 8, top: 6, textStyle: { fontSize: 14 } },
      graphic: {
        type: 'text',
        left: 'center',
        top: 'middle',
        style: { text: '暂无关键词数据', fill: '#6b7280', fontSize: 13 },
      },
    })
    return
  }
  keywordChart.setOption(buildKeywordChartOption(), true)
}

const renderChart = () => {
  if (!chartEl.value) return
  if (!chart) chart = echarts.init(chartEl.value)
  chart.setOption(buildChartOption(), true)
}

const resizeChart = () => {
  if (chart) chart.resize()
  if (keywordChart) keywordChart.resize()
}

watch([selectedBrandIds, timeKey, trendMetric], async () => {
  if (suppressAutoFetchAll) return
  await fetchAll()
})

watch([customStart, customEnd], async () => {
  if (timeKey.value !== 'custom') return
  if (!isCustomRangeReady.value) return
  await fetchAll()
})

watch(trends, () => renderChart(), { deep: true })
watch(keywordFreq, () => renderKeywordChart(), { deep: true })

onMounted(async () => {
  await projectsStore.fetch()
  await fetchOptions()
  if (activeProject.value) await applyProjectToBrands()
  else await fetchAll()
  renderChart()
  renderKeywordChart()
  window.addEventListener('resize', resizeChart)
})

watch(
  () => projectsStore.activeProjectId,
  async () => {
    await applyProjectToBrands()
  }
)

onBeforeUnmount(() => {
  stopPolling()
  window.removeEventListener('resize', resizeChart)
  if (chart) {
    chart.dispose()
    chart = null
  }
  if (keywordChart) {
    keywordChart.dispose()
    keywordChart = null
  }
})

const overviewItems = computed(() => overview.value?.items || [])

const primaryItem = computed(() => {
  const items = overviewItems.value
  if (!items.length) return null
  const firstSelected = selectedBrandIds.value[0]
  return items.find((i) => Number(i.brand_id) === Number(firstSelected)) || items[0]
})

const formatPct = (v) => `${Math.round(Number(v || 0) * 1000) / 10}%`

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
  // keep popover open; clear invalid end
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
  // close only after end picked
  timePopoverVisible.value = false
  fetchAll()
}

const goPostsForBrands = (brandIds) => {
  const ids = (brandIds || []).map((x) => Number(x)).filter((x) => Number.isFinite(x))
  if (!ids.length) return
  router.push({
    path: '/posts',
    query: { mode: 'all', project_id: projectsStore.activeProjectId || undefined, brand_ids: ids, ...timeQuery.value },
  })
}

const onOverviewRowClick = (row) => {
  if (row?.brand_id) goPostsForBrands([row.brand_id])
}
</script>

<template>
  <section class="page">
    <PageHeader content="竞品舆情仪表盘" />

    <div class="topbar">
    
      <div class="muted">每天自动刷新</div>
    </div>

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />

    <el-card class="panel" shadow="hover">
      <div class="toolbar">
        <div class="left">
          <div class="field">
            <div class="label">启用项目</div>
            <el-select
              v-model="activeProjectId"
              placeholder="请选择启用项目"
              style="width: 220px; max-width: 100%"
              :disabled="enabledProjects.length === 0"
            >
              <el-option
                v-for="p in enabledProjects"
                :key="p.id"
                :label="p.product_category ? `${p.name}（${p.product_category}）` : p.name"
                :value="p.id"
              />
            </el-select>
          </div>

          <div class="field">
            <div class="label">品牌（最多 4 个；多选自动对比）</div>
            <el-select
              v-model="selectedBrandIds"
              multiple
              filterable
              collapse-tags
              collapse-tags-tooltip
              placeholder="选择品牌"
              style="width: 360px; max-width: 100%"
              :disabled="enabledProjects.length === 0"
            >
              <el-option v-for="b in visibleBrands" :key="b.id" :label="b.name" :value="b.id" />
            </el-select>
          </div>

          <div class="field">
            <div class="label">平台（项目已启用平台）</div>
            <el-select
              v-model="selectedPlatformIds"
              multiple
              filterable
              collapse-tags
              collapse-tags-tooltip
              placeholder="选择平台"
              style="width: 320px; max-width: 100%"
              :disabled="enabledProjects.length === 0"
            >
              <el-option v-for="p in visiblePlatforms" :key="p.id" :label="p.name" :value="p.id" />
            </el-select>
          </div>

          <div class="field">
            <div class="label">时间范围  <div class="range">当前范围：{{ selectedRangeLabel }}</div></div>
            <el-popover v-model:visible="timePopoverVisible" trigger="manual" placement="bottom-start" width="420">
              <template #reference>
                <el-input
                  readonly
                  :model-value="timeText"
                  style="width: 220px; max-width: 100%"
                  placeholder="选择时间范围"
                  @click="timePopoverVisible = !timePopoverVisible"
                />
              </template>
              <div class="time-pop">
                <div class="time-level1">
                  <div class="time-item" :class="{ active: timeKey === 'last7' }" @click="selectTimeKey('last7')">
                    近 7 天
                  </div>
                  <div class="time-item" :class="{ active: timeKey === 'last14' }" @click="selectTimeKey('last14')">
                    近 14 天
                  </div>
                  <div class="time-item" :class="{ active: timeKey === 'last30' }" @click="selectTimeKey('last30')">
                    近 30 天
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
          </div>
        </div>

        <div class="right">
          <div class="refresh-params">
            <div class="param">
              <div class="label">每次抓取</div>
              <el-input-number v-model="refreshMaxPostsPerRun" :min="1" :max="500" :step="5" controls-position="right" />
            </div>
            <div class="param">
              <div class="label">情感模型</div>
              <el-select v-model="refreshSentimentModel" filterable allow-create default-first-option style="width: 160px">
                <el-option label="rule-based" value="rule-based" />
              </el-select>
            </div>
          </div>
          <el-button type="primary" :loading="loading" @click="manualRefresh">
            {{ loading ? '刷新中…' : '手动刷新' }}
          </el-button>
        </div>
      </div>
    </el-card>

    <div class="grid">
      <el-card class="panel" shadow="hover">
        <div class="panel-title">
          数据概况
          <span class="sub" v-if="canCompare">（对比模式）</span>
        </div>

        <div v-if="canCompare" class="table-scroll">
          <el-table :data="overviewItems" size="small" stripe @row-click="onOverviewRowClick">
            <el-table-column type="index" label="#" width="46" />
            <el-table-column prop="brand_name" label="品牌" min-width="160" />
            <el-table-column prop="total_posts" label="帖子量" width="100" />
            <el-table-column label="正面占比" width="110">
              <template #default="{ row }">{{ formatPct(row.positive_ratio) }}</template>
            </el-table-column>
            <el-table-column label="负面占比" width="110">
              <template #default="{ row }">{{ formatPct(row.negative_ratio) }}</template>
            </el-table-column>
            <el-table-column label="水军占比" width="110">
              <template #default="{ row }">{{ formatPct(row.spam_ratio) }}</template>
            </el-table-column>
            <el-table-column prop="intensity" label="情绪强度" width="110" />
          </el-table>
        </div>

        <div v-else class="kpis" @click="primaryItem && goPostsForBrands([primaryItem.brand_id])">
          <template v-if="primaryItem">
            <div class="kpi clickable">
              <div class="kpi-label">品牌</div>
              <div class="kpi-value">{{ primaryItem.brand_name }}</div>
            </div>
            <div class="kpi clickable">
              <div class="kpi-label">帖子量</div>
              <div class="kpi-value">{{ primaryItem.total_posts }}</div>
            </div>
            <div class="kpi clickable">
              <div class="kpi-label">正面占比</div>
              <div class="kpi-value">{{ formatPct(primaryItem.positive_ratio) }}</div>
            </div>
            <div class="kpi clickable">
              <div class="kpi-label">负面占比</div>
              <div class="kpi-value danger">{{ formatPct(primaryItem.negative_ratio) }}</div>
            </div>
            <div class="kpi clickable">
              <div class="kpi-label">水军占比</div>
              <div class="kpi-value">{{ formatPct(primaryItem.spam_ratio) }}</div>
            </div>
            <div class="kpi clickable">
              <div class="kpi-label">情绪强度</div>
              <div class="kpi-value">{{ primaryItem.intensity }}</div>
            </div>
          </template>
          <template v-else>
            <div class="empty">暂无数据</div>
          </template>
        </div>
      </el-card>

      <el-card class="panel" shadow="hover">
        <div class="panel-head">
          <div class="panel-title">情绪趋势图</div>
          <div class="panel-actions">
            <el-radio-group v-model="trendMetric" size="small">
              <el-radio-button :value="'negative_ratio'">负面</el-radio-button>
              <el-radio-button :value="'positive_ratio'">正面</el-radio-button>
              <el-radio-button :value="'spam_ratio'">水军</el-radio-button>
            </el-radio-group>
          </div>
        </div>
        <div ref="chartEl" class="chart" />
      </el-card>

      <el-card class="panel" shadow="hover">
        <div class="panel-title">关键词监控</div>
        <div ref="keywordChartEl" class="chart keyword-chart" />
      </el-card>

      <el-card class="panel" shadow="hover">
        <div class="panel-title">实时告警</div>
        <div v-if="!alerts.length" class="empty">暂无告警</div>
        <div v-else class="alerts">
          <el-alert
            v-for="(a, idx) in alerts"
            :key="`${a.brand_id || a.project_id || 'x'}-${idx}-${a.type || 't'}`"
            :title="`${String(a.level || '').toUpperCase()} | ${a.product}`"
            :description="a.reason"
            :type="a.level === 'high' ? 'error' : 'warning'"
            show-icon
            :closable="false"
            class="alert"
          />
        </div>
      </el-card>
    </div>
  </section>
</template>

<style scoped>
.page {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.topbar {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  flex-wrap: wrap;
  color: #374151;
}

.range {
  font-size: 9px;
  color: #525b6f;
}

.muted {
  font-size: 12px;
  color: #6e7b96;
}

.panel {
  border-radius: 10px;
}

.toolbar {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 12px;
}

.left {
  display: flex;
  gap: 14px;
  align-items: flex-end;
  flex-wrap: wrap;
  min-width: 0;
}

.field {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-width: 220px;
  max-width: 100%;
}

.label {
  font-size: 12px;
  color: #4b5563;
}

.right {
  display: flex;
  align-items: flex-end;
  gap: 12px;
  flex-wrap: wrap;
  justify-content: flex-end;
}

.refresh-params {
  display: flex;
  align-items: flex-end;
  gap: 10px;
  flex-wrap: wrap;
}

.refresh-params .param {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: 12px;
}

@media (min-width: 1100px) {
  .grid {
    grid-template-columns: 1.2fr 1.8fr;
    grid-auto-rows: minmax(240px, auto);
  }
  .grid > :nth-child(1) {
    grid-column: 1;
  }
  .grid > :nth-child(2) {
    grid-column: 2;
    grid-row: 1 / span 2;
  }
  .grid > :nth-child(3) {
    grid-column: 1;
  }
  .grid > :nth-child(4) {
    grid-column: 1 / -1;
  }
}

@media (max-width: 640px) {
  .field {
    min-width: 100%;
  }
  .chart {
    height: 300px;
  }
  .keyword-chart {
    height: 280px;
  }
}

.panel-title {
  font-weight: 600;
  margin-bottom: 10px;
}

.panel-title .sub {
  font-weight: 400;
  color: #6b7280;
  margin-left: 6px;
  font-size: 12px;
}

.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 8px;
}

.chart {
  width: 100%;
  height: 360px;
}

.keyword-chart {
  height: 320px;
}

.table-scroll {
  overflow-x: auto;
  padding-bottom: 4px;
}

.kpis {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 10px;
}

.kpi {
  min-width: 0;
  background: #f9fafb;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 10px 12px;
}

.kpi.clickable {
  cursor: pointer;
}

.kpi-label {
  font-size: 12px;
  color: #6b7280;
}

.kpi-value {
  margin-top: 6px;
  font-size: 18px;
  font-weight: 700;
  color: #111827;
  word-break: break-word;
}

.kpi-value.danger {
  color: #b91c1c;
}

.empty {
  color: #6b7280;
  font-size: 13px;
  padding: 10px 0;
}

.alerts {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.alert {
  border-radius: 10px;
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
