<!-- 作用：前端组件：系统功能概览图（中英文两版，可在编辑器中直接修改 SVG 内容） -->

<template>
  <div ref="wrapRef" class="diagram">
    <div class="toolbar">
      <el-space :size="8" wrap>
        <el-text type="info" size="small">{{ lang === 'zh' ? '缩放' : 'Zoom' }}</el-text>
        <el-button-group>
          <el-button size="small" :disabled="scale <= minScale" @click="zoomOut">-</el-button>
          <el-button size="small" @click="resetZoom">100%</el-button>
          <el-button size="small" :disabled="scale >= maxScale" @click="zoomIn">+</el-button>
        </el-button-group>
        <el-button size="small" @click="fitToWidth">{{ lang === 'zh' ? '适配宽度' : 'Fit width' }}</el-button>
        <el-slider v-model="scalePct" :min="minScale * 100" :max="maxScale * 100" :step="5" style="width: 180px" />
        <el-text type="info" size="small">{{ Math.round(scale * 100) }}%</el-text>
      </el-space>
    </div>

    <div class="canvas" :style="{ transform: `scale(${scale})` }">
      <svg
        v-if="lang === 'zh'"
        class="svg"
        viewBox="0 0 1320 760"
        role="img"
        aria-label="ProdWatch 系统功能概览（中文）"
      >
        <defs>
          <marker id="arrowHeadModZh" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto">
            <path d="M0,0 L10,4 L0,8 Z" fill="currentColor"></path>
          </marker>
        </defs>

        <!-- 左侧：用户与外部 -->
        <rect x="40" y="80" rx="14" ry="14" width="190" height="110" class="box section" />
        <text x="60" y="115" class="t title">用户 / 浏览器</text>
        <text x="60" y="140" class="t muted">访问 Web UI</text>
        <text x="60" y="162" class="t muted">触发刷新 / 报告</text>

        <rect x="40" y="220" rx="14" ry="14" width="190" height="170" class="box section" />
        <text x="60" y="252" class="t title">外部依赖</text>
        <text x="60" y="275" class="t muted">平台数据源</text>
        <text x="60" y="295" class="t muted small">backend/services/external_crawlers/*</text>
        <text x="60" y="325" class="t muted">LLM 服务商</text>
        <text x="60" y="345" class="t muted small">backend/llm/providers/*</text>

        <!-- 前端 -->
        <rect x="270" y="50" rx="16" ry="16" width="380" height="340" class="box section blue" />
        <text x="290" y="82" class="t title">前端（frontend/）</text>
        <text x="290" y="105" class="t muted">Vue3 + Vite + Pinia + Element Plus</text>

        <rect x="295" y="130" rx="12" ry="12" width="330" height="190" class="box blue" />
        <text x="315" y="158" class="t title">页面模块（views/）</text>
        <text x="315" y="182" class="t muted">Dashboard / Posts / Reports</text>
        <text x="315" y="204" class="t muted">ReportDetail / ProjectConfig / LLMConfig</text>
        <text x="315" y="230" class="t muted small">路由：frontend/src/router/index.js</text>

        <rect x="295" y="335" rx="12" ry="12" width="330" height="80" class="box blue" />
        <text x="315" y="364" class="t title">API 客户端（src/api/）</text>
        <text x="315" y="387" class="t muted small">fetch 封装 + 业务 API（dashboard/posts/reports/projects…）</text>

        <!-- 后端 API -->
        <rect x="690" y="50" rx="16" ry="16" width="400" height="410" class="box section green" />
        <text x="710" y="82" class="t title">后端 API（backend/api/）</text>
        <text x="710" y="105" class="t muted">FastAPI 入口：backend/api/app.py</text>

        <rect x="715" y="130" rx="12" ry="12" width="350" height="300" class="box green" />
        <text x="735" y="158" class="t title">/api/* 路由组</text>
        <text x="735" y="182" class="t muted">dashboard / posts / reports</text>
        <text x="735" y="204" class="t muted">projects（配置 / 刷新 / 变更）</text>
        <text x="735" y="226" class="t muted">crawl_jobs（进度）/ scheduler（定时）</text>
        <text x="735" y="248" class="t muted">meta / settings / llm_config</text>
        <text x="735" y="274" class="t muted small">DB：backend/api/db.py（SQLite 连接注入）</text>

        <!-- 服务层 -->
        <rect x="690" y="500" rx="16" ry="16" width="400" height="210" class="box section orange" />
        <text x="710" y="532" class="t title">服务层（backend/services/）</text>
        <text x="710" y="555" class="t muted">刷新 / 分析 / 报告 / 调度（编排流水线）</text>

        <rect x="715" y="580" rx="12" ry="12" width="350" height="110" class="box orange" />
        <text x="735" y="608" class="t title">核心服务</text>
        <text x="735" y="632" class="t muted small">RefreshService / AnalyzerService / ReportGenerationService</text>
        <text x="735" y="654" class="t muted small">DailyRefreshScheduler / CrawlerGenerationService</text>

        <!-- LLM 与数据层 -->
        <rect x="1120" y="50" rx="16" ry="16" width="170" height="240" class="box section purple" />
        <text x="1140" y="82" class="t title">LLM 模块</text>
        <text x="1140" y="105" class="t muted small">backend/llm/</text>
        <rect x="1140" y="128" rx="12" ry="12" width="130" height="140" class="box purple" />
        <text x="1156" y="156" class="t title">LLMRouter</text>
        <text x="1156" y="178" class="t muted small">task_type 选模</text>
        <text x="1156" y="200" class="t muted small">prompt 渲染</text>
        <text x="1156" y="222" class="t muted small">缓存 / 日志</text>
        <text x="1156" y="244" class="t muted small">providers/*</text>

        <rect x="1120" y="315" rx="16" ry="16" width="170" height="395" class="box section pink" />
        <text x="1140" y="347" class="t title">数据 / 存储</text>
        <text x="1140" y="370" class="t muted small">SQLite + Stores</text>

        <rect x="1140" y="392" rx="12" ry="12" width="130" height="120" class="box pink" />
        <text x="1156" y="420" class="t title">SQLite</text>
        <text x="1156" y="442" class="t muted small">backend/database/</text>
        <text x="1156" y="464" class="t muted small">database.sqlite</text>
        <text x="1156" y="486" class="t muted small">database..sqlite</text>

        <rect x="1140" y="527" rx="12" ry="12" width="130" height="95" class="box pink" />
        <text x="1156" y="555" class="t title">Store</text>
        <text x="1156" y="578" class="t muted small">analysis_store.py</text>
        <text x="1156" y="600" class="t muted small">crawl_job_progress_store.py</text>

        <rect x="1140" y="636" rx="12" ry="12" width="130" height="55" class="box pink" />
        <text x="1156" y="664" class="t title">配置</text>
        <text x="1156" y="686" class="t muted small">backend/data/users.json</text>

        <!-- 连接箭头（间隙增大后的排布） -->
        <path d="M230 135 C245 135, 255 135, 270 135" class="arrow blue" />
        <path d="M650 250 C670 250, 675 250, 690 250" class="arrow green" />
        <path d="M890 460 C890 475, 890 487, 890 500" class="arrow orange" />
        <path d="M1090 150 C1100 150, 1108 150, 1120 150" class="arrow purple" />
        <path d="M1090 610 C1100 610, 1108 610, 1120 610" class="arrow pink" />
        <path d="M230 305 C420 305, 520 360, 690 585" class="arrow soft" />
        <path d="M1205 290 C1180 330, 1165 360, 1155 392" class="arrow soft purple" />

        <text x="665" y="235" class="t muted">HTTP/JSON</text>
        <text x="910" y="488" class="t muted">调用/编排</text>
        <text x="1110" y="592" class="t muted">读写</text>
        <text x="1110" y="138" class="t muted">推理/抽取</text>
      </svg>

      <svg
        v-else
        class="svg"
        viewBox="0 0 1320 760"
        role="img"
        aria-label="ProdWatch System Module Overview (English)"
      >
        <defs>
          <marker id="arrowHeadModEn" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto">
            <path d="M0,0 L10,4 L0,8 Z" fill="currentColor"></path>
          </marker>
        </defs>

        <!-- Left: user + external -->
        <rect x="40" y="80" rx="14" ry="14" width="190" height="110" class="box section" />
        <text x="60" y="115" class="t title">User / Browser</text>
        <text x="60" y="140" class="t muted">Uses the Web UI</text>
        <text x="60" y="162" class="t muted">Triggers refresh / reports</text>

        <rect x="40" y="220" rx="14" ry="14" width="190" height="170" class="box section" />
        <text x="60" y="252" class="t title">External</text>
        <text x="60" y="275" class="t muted">Platform data sources</text>
        <text x="60" y="295" class="t muted small">backend/services/external_crawlers/*</text>
        <text x="60" y="325" class="t muted">LLM providers</text>
        <text x="60" y="345" class="t muted small">backend/llm/providers/*</text>

        <!-- Frontend -->
        <rect x="270" y="50" rx="16" ry="16" width="380" height="340" class="box section blue" />
        <text x="290" y="82" class="t title">Frontend (frontend/)</text>
        <text x="290" y="105" class="t muted">Vue3 + Vite + Pinia + Element Plus</text>

        <rect x="295" y="130" rx="12" ry="12" width="330" height="190" class="box blue" />
        <text x="315" y="158" class="t title">UI Pages (views/)</text>
        <text x="315" y="182" class="t muted">Dashboard / Posts / Reports</text>
        <text x="315" y="204" class="t muted">ReportDetail / ProjectConfig / LLMConfig</text>
        <text x="315" y="230" class="t muted small">Routes: frontend/src/router/index.js</text>

        <rect x="295" y="335" rx="12" ry="12" width="330" height="80" class="box blue" />
        <text x="315" y="364" class="t title">API Client (src/api/)</text>
        <text x="315" y="387" class="t muted small">fetch wrapper + business APIs (dashboard/posts/reports/projects…)</text>

        <!-- Backend API -->
        <rect x="690" y="50" rx="16" ry="16" width="400" height="410" class="box section green" />
        <text x="710" y="82" class="t title">Backend API (backend/api/)</text>
        <text x="710" y="105" class="t muted">FastAPI entry: backend/api/app.py</text>

        <rect x="715" y="130" rx="12" ry="12" width="350" height="300" class="box green" />
        <text x="735" y="158" class="t title">/api/* groups</text>
        <text x="735" y="182" class="t muted">dashboard / posts / reports</text>
        <text x="735" y="204" class="t muted">projects (config / refresh / mutations)</text>
        <text x="735" y="226" class="t muted">crawl_jobs (progress) / scheduler</text>
        <text x="735" y="248" class="t muted">meta / settings / llm_config</text>
        <text x="735" y="274" class="t muted small">DB: backend/api/db.py (SQLite DI)</text>

        <!-- Services -->
        <rect x="690" y="500" rx="16" ry="16" width="400" height="210" class="box section orange" />
        <text x="710" y="532" class="t title">Service Layer (backend/services/)</text>
        <text x="710" y="555" class="t muted">Refresh / Analyze / Reports / Scheduler</text>

        <rect x="715" y="580" rx="12" ry="12" width="350" height="110" class="box orange" />
        <text x="735" y="608" class="t title">Core services</text>
        <text x="735" y="632" class="t muted small">RefreshService / AnalyzerService / ReportGenerationService</text>
        <text x="735" y="654" class="t muted small">DailyRefreshScheduler / CrawlerGenerationService</text>

        <!-- LLM + data -->
        <rect x="1120" y="50" rx="16" ry="16" width="170" height="240" class="box section purple" />
        <text x="1140" y="82" class="t title">LLM</text>
        <text x="1140" y="105" class="t muted small">backend/llm/</text>
        <rect x="1140" y="128" rx="12" ry="12" width="130" height="140" class="box purple" />
        <text x="1156" y="156" class="t title">LLMRouter</text>
        <text x="1156" y="178" class="t muted small">select by task</text>
        <text x="1156" y="200" class="t muted small">prompt render</text>
        <text x="1156" y="222" class="t muted small">cache / logs</text>
        <text x="1156" y="244" class="t muted small">providers/*</text>

        <rect x="1120" y="315" rx="16" ry="16" width="170" height="395" class="box section pink" />
        <text x="1140" y="347" class="t title">Data / Storage</text>
        <text x="1140" y="370" class="t muted small">SQLite + Stores</text>

        <rect x="1140" y="392" rx="12" ry="12" width="130" height="120" class="box pink" />
        <text x="1156" y="420" class="t title">SQLite</text>
        <text x="1156" y="442" class="t muted small">backend/database/</text>
        <text x="1156" y="464" class="t muted small">database.sqlite</text>
        <text x="1156" y="486" class="t muted small">database..sqlite</text>

        <rect x="1140" y="527" rx="12" ry="12" width="130" height="95" class="box pink" />
        <text x="1156" y="555" class="t title">Stores</text>
        <text x="1156" y="578" class="t muted small">analysis_store.py</text>
        <text x="1156" y="600" class="t muted small">crawl_job_progress_store.py</text>

        <rect x="1140" y="636" rx="12" ry="12" width="130" height="55" class="box pink" />
        <text x="1156" y="664" class="t title">Config</text>
        <text x="1156" y="686" class="t muted small">backend/data/users.json</text>

        <!-- Arrows -->
        <path d="M230 135 C245 135, 255 135, 270 135" class="arrow blue" />
        <path d="M650 250 C670 250, 675 250, 690 250" class="arrow green" />
        <path d="M890 460 C890 475, 890 487, 890 500" class="arrow orange" />
        <path d="M1090 150 C1100 150, 1108 150, 1120 150" class="arrow purple" />
        <path d="M1090 610 C1100 610, 1108 610, 1120 610" class="arrow pink" />
        <path d="M230 305 C420 305, 520 360, 690 585" class="arrow soft" />
        <path d="M1205 290 C1180 330, 1165 360, 1155 392" class="arrow soft purple" />

        <text x="665" y="235" class="t muted">HTTP/JSON</text>
        <text x="910" y="488" class="t muted">orchestrate</text>
        <text x="1110" y="592" class="t muted">read/write</text>
        <text x="1110" y="138" class="t muted">infer/extract</text>
      </svg>
    </div>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'

defineProps({
  lang: { type: String, default: 'zh' }, // 'zh' | 'en'
})

const baseWidth = 1320
const minScale = 0.6
const maxScale = 1.6

const wrapRef = ref(null)
const wrapW = ref(0)
const scale = ref(1)
const autoFitted = ref(false)

let ro = null
onMounted(() => {
  if (!wrapRef.value) return
  ro = new ResizeObserver((entries) => {
    const w = entries?.[0]?.contentRect?.width
    wrapW.value = Number.isFinite(w) ? w : 0
    if (!autoFitted.value && wrapW.value > 0) {
      fitToWidth()
      autoFitted.value = true
    }
  })
  ro.observe(wrapRef.value)
})
onBeforeUnmount(() => {
  try {
    ro?.disconnect()
  } catch {
    // ignore
  }
})

const scalePct = computed({
  get: () => Math.round(scale.value * 100),
  set: (v) => {
    const next = Number(v) / 100
    if (!Number.isFinite(next)) return
    scale.value = Math.min(maxScale, Math.max(minScale, next))
  },
})

function zoomIn() {
  scalePct.value = scalePct.value + 10
}
function zoomOut() {
  scalePct.value = scalePct.value - 10
}
function resetZoom() {
  scale.value = 1
}
function fitToWidth() {
  const w = wrapW.value
  if (!w) return
  const usable = Math.max(0, w - 26)
  const next = usable / baseWidth
  scale.value = Math.min(maxScale, Math.max(minScale, next))
}
</script>

<style scoped>
.diagram {
  width: 100%;
  overflow: auto;
  padding: 10px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 10px;
  background:
    linear-gradient(transparent 23px, rgba(0, 0, 0, 0.04) 24px),
    linear-gradient(90deg, transparent 23px, rgba(0, 0, 0, 0.04) 24px),
    var(--el-bg-color);
  background-size: 24px 24px;
}
.toolbar {
  position: sticky;
  top: 0;
  z-index: 2;
  padding: 8px 6px 10px;
  margin: -6px -6px 10px;
  border-bottom: 1px solid var(--el-border-color-lighter);
  background: var(--el-bg-color);
  background: color-mix(in srgb, var(--el-bg-color) 88%, transparent);
  backdrop-filter: blur(8px);
}
.canvas {
  transform-origin: 0 0;
  display: inline-block;
}
.svg {
  width: 100%;
  height: auto;
  min-width: 1200px;
  display: block;
  color: var(--el-text-color-secondary); /* currentColor for arrows + markers */
}

.t {
  fill: var(--el-text-color-primary);
  font-size: 13px;
  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, PingFang SC, Microsoft YaHei, Arial,
    sans-serif;
}
.t.muted {
  fill: var(--el-text-color-secondary);
  font-size: 12px;
}
.t.small {
  font-size: 11px;
}
.t.title {
  font-weight: 700;
}

.box {
  fill: var(--el-fill-color-lighter);
  stroke: var(--el-border-color);
  stroke-width: 1;
}
.box.section {
  fill: var(--el-fill-color-light);
  stroke: var(--el-border-color);
  filter: drop-shadow(0 10px 22px rgba(0, 0, 0, 0.12));
}

.box.blue {
  stroke: var(--el-color-primary);
  fill: var(--el-bg-color);
}
.box.section.blue {
  fill: var(--el-color-primary-light-9);
}
.box.green {
  stroke: var(--el-color-success);
  fill: var(--el-bg-color);
}
.box.section.green {
  fill: var(--el-color-success-light-9);
}
.box.orange {
  stroke: var(--el-color-warning);
  fill: var(--el-bg-color);
}
.box.section.orange {
  fill: var(--el-color-warning-light-9);
}
.box.pink {
  stroke: var(--el-color-danger);
  fill: var(--el-bg-color);
}
.box.section.pink {
  fill: var(--el-color-danger-light-9);
}
.box.purple {
  stroke: var(--el-color-info);
  fill: var(--el-bg-color);
}
.box.section.purple {
  fill: var(--el-color-info-light-9);
}

.arrow {
  stroke: currentColor;
  stroke-width: 1.6;
  fill: none;
  marker-end: url(#arrowHeadModZh);
}
.arrow.soft {
  opacity: 0.55;
  stroke-dasharray: 4 4;
}
.arrow.blue {
  color: var(--el-color-primary);
}
.arrow.green {
  color: var(--el-color-success);
}
.arrow.orange {
  color: var(--el-color-warning);
}
.arrow.pink {
  color: var(--el-color-danger);
}
.arrow.purple {
  color: var(--el-color-info);
}

/* Ensure EN SVG arrows use its own marker. */
svg[aria-label*='English'] .arrow {
  marker-end: url(#arrowHeadModEn);
}
</style>

