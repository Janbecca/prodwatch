<template>
  <PageSection title="Data Overview">
    <el-alert
      v-if="error"
      type="error"
      :title="error"
      :closable="false"
      show-icon
      style="margin-bottom: 10px"
    />

    <el-skeleton v-if="loading" :rows="3" animated />

    <template v-else>
      <el-empty v-if="!hasFilters" description="Select at least 1 brand and 1 platform" />
      <el-empty v-else-if="!hasData" description="No data" />

      <template v-else>
        <!-- Single brand: card layout -->
        <el-row v-if="viewMode === 'single'" :gutter="12">
          <el-col v-for="c in singleCards" :key="c.key" :span="6">
            <el-card shadow="never" class="card">
              <div class="card__top">
                <el-text type="info">{{ c.label }}</el-text>
              </div>
              <div class="card__val">{{ c.value }}</div>
            </el-card>
          </el-col>
        </el-row>

        <!-- Multi brand: list compare -->
        <el-table v-else :data="tableRows" border>
          <el-table-column prop="brandName" label="Brand" min-width="160" />
          <el-table-column prop="totalPostCount" label="Posts" min-width="120" />
          <el-table-column prop="positiveRatioText" label="Positive %" min-width="120" />
          <el-table-column prop="negativeRatioText" label="Negative %" min-width="120" />
        </el-table>
      </template>
    </template>
  </PageSection>
</template>

<script setup>
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import PageSection from '../common/PageSection.vue'
import { useDashboardStore } from '../../stores/dashboard'
import { resolveDateRange } from '../../utils/date'
import { fetchDashboardOverviewByBrand } from '../../api/dashboard'

const dashboard = useDashboardStore()

const loading = ref(false)
const error = ref('')
const items = ref([])

let ac = null
let timer = null

const brandNameById = computed(() => {
  const map = {}
  for (const b of dashboard.brandOptions || []) {
    const id = Number(b?.id)
    if (!Number.isFinite(id)) continue
    map[id] = b?.name ?? `Brand ${id}`
  }
  return map
})

const viewMode = computed(() => {
  const n = (dashboard.brandIds || []).length
  return n === 1 ? 'single' : 'multi'
})

const hasFilters = computed(() => {
  return (dashboard.brandIds || []).length > 0 && (dashboard.platformIds || []).length > 0
})

const hasData = computed(() => Array.isArray(items.value) && items.value.length > 0)

function pct(n) {
  return `${(Number(n || 0) * 100).toFixed(1)}%`
}

function ratio(num, den) {
  const a = Number(num || 0)
  const b = Number(den || 0)
  if (!b) return 0
  return a / b
}

const singleRow = computed(() => {
  const selected = (dashboard.brandIds || []).map((x) => Number(x)).filter((x) => Number.isFinite(x))
  if (selected.length === 1) {
    const hit = (items.value || []).find((it) => Number(it?.brand_id) === selected[0])
    if (hit) return hit
  }
  return (items.value || [])[0] || null
})

const singleCards = computed(() => {
  const row = singleRow.value
  if (!row) return []
  const bid = Number(row?.brand_id)
  const name = brandNameById.value[bid] || `Brand ${bid}`
  const total = Number(row?.total_post_count || 0)
  const pos = Number(row?.positive_count || 0)
  const neg = Number(row?.negative_count || 0)
  return [
    { key: 'brand', label: 'Brand', value: name },
    { key: 'posts', label: 'Posts', value: total.toLocaleString() },
    { key: 'pos', label: 'Positive %', value: pct(ratio(pos, total)) },
    { key: 'neg', label: 'Negative %', value: pct(ratio(neg, total)) },
  ]
})

const tableRows = computed(() => {
  return (items.value || []).map((row) => {
    const bid = Number(row?.brand_id)
    const total = Number(row?.total_post_count || 0)
    const pos = Number(row?.positive_count || 0)
    const neg = Number(row?.negative_count || 0)
    return {
      brandId: bid,
      brandName: brandNameById.value[bid] || `Brand ${bid}`,
      totalPostCount: total.toLocaleString(),
      positiveRatioText: pct(ratio(pos, total)),
      negativeRatioText: pct(ratio(neg, total)),
    }
  })
})

async function load() {
  const pid = dashboard.enabledProjectId
  if (!pid) {
    items.value = []
    error.value = ''
    loading.value = false
    return
  }

  if (!hasFilters.value) {
    if (ac) ac.abort()
    items.value = []
    error.value = ''
    loading.value = false
    return
  }

  const dr = resolveDateRange(dashboard.timeQuery)
  if (!dr) {
    items.value = []
    error.value = 'Invalid date range (select a custom range, or use last 7/14/30 days).'
    loading.value = false
    return
  }

  if (ac) ac.abort()
  ac = new AbortController()

  loading.value = true
  error.value = ''
  try {
    const res = await fetchDashboardOverviewByBrand(
      {
        projectId: pid,
        startDate: dr.startDate,
        endDate: dr.endDate,
        platformIds: dashboard.platformIds,
        brandIds: dashboard.brandIds,
      },
      { signal: ac.signal }
    )
    items.value = Array.isArray(res?.items) ? res.items : []
  } catch (e) {
    if (e?.name === 'AbortError') return
    error.value = e?.message || String(e)
    items.value = []
  } finally {
    loading.value = false
  }
}

function scheduleLoad() {
  if (timer) clearTimeout(timer)
  timer = setTimeout(() => load(), 60)
}

onBeforeUnmount(() => {
  if (timer) clearTimeout(timer)
  if (ac) ac.abort()
})

watch(
  () => [
    dashboard.enabledProjectId,
    dashboard.reloadSeq,
    (dashboard.brandIds || []).join(','),
    (dashboard.platformIds || []).join(','),
    JSON.stringify(dashboard.timeQuery || {}),
  ],
  () => scheduleLoad(),
  { immediate: true }
)
</script>

<style scoped>
.card {
  border: 1px solid var(--el-border-color-lighter);
}
.card__top {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.card__val {
  margin: 8px 0 6px;
  font-size: 24px;
  font-weight: 700;
  line-height: 1.1;
}
</style>
