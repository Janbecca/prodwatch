<!-- 作用：前端组件：系统架构图（中英文两版，可在编辑器中直接修改 SVG 内容） -->

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
      <svg v-if="lang === 'zh'" class="svg" viewBox="0 0 1320 820" role="img" aria-label="ProdWatch 系统架构图（中文）">
        <defs>
          <marker id="arrowHeadArchZh" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto">
            <path d="M0,0 L10,4 L0,8 Z" fill="currentColor"></path>
          </marker>
        </defs>

        <!-- 顶部：访问链路 -->
        <rect x="40" y="60" rx="14" ry="14" width="190" height="90" class="box section" />
        <text x="60" y="94" class="t title">浏览器</text>
        <text x="60" y="118" class="t muted">终端用户</text>

        <rect x="270" y="40" rx="16" ry="16" width="420" height="140" class="box section blue" />
        <text x="290" y="72" class="t title">前端：Vue3 + Vite + ElementPlus</text>
        <text x="290" y="96" class="t muted">frontend/（Dashboard / Posts / Reports / ProjectConfig / LLMConfig）</text>
        <text x="290" y="120" class="t muted small">路由：frontend/src/router/index.js</text>

        <rect x="740" y="40" rx="16" ry="16" width="540" height="190" class="box section green" />
        <text x="760" y="72" class="t title">后端：FastAPI</text>
        <text x="760" y="96" class="t muted">入口：backend/api/app.py（注册路由）</text>
        <text x="760" y="120" class="t muted">统一对外：/api/*</text>

        <rect x="760" y="138" rx="12" ry="12" width="500" height="72" class="box green" />
        <text x="780" y="164" class="t title">主要 API 组</text>
        <text x="780" y="186" class="t muted small">
          projects / dashboard / posts / reports / scheduler / crawl_jobs / llm_config & llm_settings / meta
        </text>

        <!-- 中部：长任务与数据层 -->
        <rect x="40" y="260" rx="16" ry="16" width="650" height="240" class="box section orange" />
        <text x="60" y="292" class="t title">刷新流水线编排（backend/pipeline_main.py）</text>
        <text x="60" y="318" class="t muted">
          生成目标 → 生成候选帖子 → 去重 → 写入 post_raw → 分析 → 聚合指标 → 更新任务状态
        </text>

        <rect x="60" y="350" rx="12" ry="12" width="610" height="130" class="box orange" />
        <text x="80" y="378" class="t title">进度 / 状态</text>
        <text x="80" y="402" class="t muted">查询：/api/crawl_jobs/status、/api/crawl_jobs/progress</text>
        <text x="80" y="426" class="t muted">落库：crawl_job_progress（crawl_job_progress_store.py）</text>
        <text x="80" y="450" class="t muted small">路由：backend/api/routes_crawl_jobs.py</text>

        <rect x="740" y="250" rx="16" ry="16" width="260" height="170" class="box section orange" />
        <text x="760" y="282" class="t title">长任务：刷新/采集/分析</text>
        <text x="760" y="306" class="t muted small">触发：/api/projects/{project_id}/refresh</text>
        <text x="760" y="328" class="t muted small">/simulate /analyze（routes_project_refresh.py）</text>
        <text x="760" y="352" class="t muted small">后台线程：RefreshService</text>

        <rect x="1030" y="250" rx="16" ry="16" width="250" height="170" class="box section pink" />
        <text x="1050" y="282" class="t title">SQLite</text>
        <text x="1050" y="306" class="t muted">连接注入：backend/api/db.py</text>
        <text x="1050" y="328" class="t muted small">默认：backend/database/database.sqlite</text>
        <text x="1050" y="350" class="t muted small">回退：backend/database/database..sqlite</text>
        <text x="1050" y="372" class="t muted small">表：项目/平台/品牌/帖子/分析/指标/报告/LLM日志…</text>

        <!-- 底部：调度、LLM、报告 -->
        <rect x="40" y="530" rx="16" ry="16" width="650" height="250" class="box section green" />
        <text x="60" y="562" class="t title">报告系统</text>
        <text x="60" y="586" class="t muted">前端：/api/reports/* 创建/生成/查看报告</text>
        <text x="60" y="610" class="t muted small">后端：ReportGenerationService + report_chain_e.py</text>
        <text x="60" y="634" class="t muted small">证据：report_evidence（写入真实帖子证据）</text>

        <rect x="740" y="440" rx="16" ry="16" width="290" height="120" class="box section purple" />
        <text x="760" y="472" class="t title">DailyRefreshScheduler</text>
        <text x="760" y="496" class="t muted small">FastAPI startup 启动（app.py）</text>
        <text x="760" y="518" class="t muted small">按配置每天触发刷新（daily_refresh_scheduler.py）</text>

        <rect x="1045" y="440" rx="16" ry="16" width="235" height="150" class="box section purple" />
        <text x="1065" y="472" class="t title">LLM 子系统（backend/llm/）</text>
        <text x="1065" y="496" class="t muted small">LLMRouter：选 provider/model</text>
        <text x="1065" y="518" class="t muted small">prompt 渲染 + 缓存 + 调用日志</text>
        <text x="1065" y="540" class="t muted small">providers：DeepSeek / Qwen</text>
        <text x="1065" y="562" class="t muted small">Prompts：prompts/templates/*.json</text>

        <rect x="740" y="585" rx="16" ry="16" width="290" height="110" class="box section pink" />
        <text x="760" y="617" class="t title">llm_call_log（SQLite）</text>
        <text x="760" y="641" class="t muted">缓存 / 可观测</text>
        <text x="760" y="663" class="t muted small">写入：LLMRouter</text>

        <rect x="1045" y="605" rx="16" ry="16" width="235" height="110" class="box section" />
        <text x="1065" y="637" class="t title">外部大模型服务</text>
        <text x="1065" y="661" class="t muted">OpenAI 兼容接口</text>

        <!-- 箭头：排布更松散 -->
        <path d="M230 105 C245 105, 255 105, 270 105" class="arrow blue" />
        <path d="M690 115 C710 115, 725 115, 740 115" class="arrow green" />

        <path d="M870 230 C870 238, 870 244, 870 250" class="arrow orange" />
        <path d="M1155 230 C1155 238, 1155 244, 1155 250" class="arrow pink" />

        <path d="M740 335 C725 335, 710 335, 690 335" class="arrow orange" />
        <path d="M690 365 C820 365, 920 350, 1030 330" class="arrow soft" />

        <path d="M885 440 C885 430, 885 420, 885 410" class="arrow purple" />
        <path d="M1157 590 C1157 597, 1157 600, 1157 605" class="arrow purple" />
        <path d="M1045 520 C990 545, 930 565, 885 585" class="arrow purple" />

        <path d="M690 650 C860 650, 940 520, 1030 390" class="arrow soft" />

        <text x="705" y="98" class="t muted small">HTTP/JSON</text>
        <text x="890" y="244" class="t muted small">启动长任务</text>
        <text x="1135" y="244" class="t muted small">DI 连接</text>
        <text x="910" y="433" class="t muted small">每日定时触发</text>
        <text x="1180" y="600" class="t muted small">调用</text>
      </svg>

      <svg
        v-else
        class="svg"
        viewBox="0 0 1320 820"
        role="img"
        aria-label="ProdWatch System Architecture Diagram (English)"
      >
        <defs>
          <marker id="arrowHeadArchEn" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto">
            <path d="M0,0 L10,4 L0,8 Z" fill="currentColor"></path>
          </marker>
        </defs>

        <!-- Top: request flow -->
        <rect x="40" y="60" rx="14" ry="14" width="190" height="90" class="box section" />
        <text x="60" y="94" class="t title">Browser</text>
        <text x="60" y="118" class="t muted">End user</text>

        <rect x="270" y="40" rx="16" ry="16" width="420" height="140" class="box section blue" />
        <text x="290" y="72" class="t title">Frontend: Vue3 + Vite + ElementPlus</text>
        <text x="290" y="96" class="t muted">frontend/ (Dashboard / Posts / Reports / ProjectConfig / LLMConfig)</text>
        <text x="290" y="120" class="t muted small">Routes: frontend/src/router/index.js</text>

        <rect x="740" y="40" rx="16" ry="16" width="540" height="190" class="box section green" />
        <text x="760" y="72" class="t title">Backend: FastAPI</text>
        <text x="760" y="96" class="t muted">Entry: backend/api/app.py (router registration)</text>
        <text x="760" y="120" class="t muted">Public API: /api/*</text>

        <rect x="760" y="138" rx="12" ry="12" width="500" height="72" class="box green" />
        <text x="780" y="164" class="t title">Main API Groups</text>
        <text x="780" y="186" class="t muted small">
          projects / dashboard / posts / reports / scheduler / crawl_jobs / llm_config & llm_settings / meta
        </text>

        <!-- Middle: long tasks + data -->
        <rect x="40" y="260" rx="16" ry="16" width="650" height="240" class="box section orange" />
        <text x="60" y="292" class="t title">Pipeline Orchestration (backend/pipeline_main.py)</text>
        <text x="60" y="318" class="t muted">
          build targets → candidate posts → dedupe → write post_raw → analyze → aggregate metrics → update status
        </text>

        <rect x="60" y="350" rx="12" ry="12" width="610" height="130" class="box orange" />
        <text x="80" y="378" class="t title">Progress / Status</text>
        <text x="80" y="402" class="t muted">Query: /api/crawl_jobs/status, /api/crawl_jobs/progress</text>
        <text x="80" y="426" class="t muted">Persist: crawl_job_progress (crawl_job_progress_store.py)</text>
        <text x="80" y="450" class="t muted small">Router: backend/api/routes_crawl_jobs.py</text>

        <rect x="740" y="250" rx="16" ry="16" width="260" height="170" class="box section orange" />
        <text x="760" y="282" class="t title">Long Tasks</text>
        <text x="760" y="306" class="t muted small">Trigger: /api/projects/{project_id}/refresh</text>
        <text x="760" y="328" class="t muted small">/simulate /analyze (routes_project_refresh.py)</text>
        <text x="760" y="352" class="t muted small">Thread: RefreshService</text>

        <rect x="1030" y="250" rx="16" ry="16" width="250" height="170" class="box section pink" />
        <text x="1050" y="282" class="t title">SQLite</text>
        <text x="1050" y="306" class="t muted">DI connection: backend/api/db.py</text>
        <text x="1050" y="328" class="t muted small">Default: backend/database/database.sqlite</text>
        <text x="1050" y="350" class="t muted small">Fallback: backend/database/database..sqlite</text>
        <text x="1050" y="372" class="t muted small">Tables: projects/platforms/brands/posts/analysis/metrics/reports/LLM logs…</text>

        <!-- Bottom: scheduler, LLM, reports -->
        <rect x="40" y="530" rx="16" ry="16" width="650" height="250" class="box section green" />
        <text x="60" y="562" class="t title">Report System</text>
        <text x="60" y="586" class="t muted">Frontend: /api/reports/* create / generate / view</text>
        <text x="60" y="610" class="t muted small">Backend: ReportGenerationService + report_chain_e.py</text>
        <text x="60" y="634" class="t muted small">Evidence: report_evidence</text>

        <rect x="740" y="440" rx="16" ry="16" width="290" height="120" class="box section purple" />
        <text x="760" y="472" class="t title">DailyRefreshScheduler</text>
        <text x="760" y="496" class="t muted small">Started on FastAPI startup (app.py)</text>
        <text x="760" y="518" class="t muted small">Runs daily refresh (daily_refresh_scheduler.py)</text>

        <rect x="1045" y="440" rx="16" ry="16" width="235" height="150" class="box section purple" />
        <text x="1065" y="472" class="t title">LLM Subsystem (backend/llm/)</text>
        <text x="1065" y="496" class="t muted small">LLMRouter: select provider/model</text>
        <text x="1065" y="518" class="t muted small">prompt render + cache + call logs</text>
        <text x="1065" y="540" class="t muted small">Providers: DeepSeek / Qwen</text>
        <text x="1065" y="562" class="t muted small">Prompts: prompts/templates/*.json</text>

        <rect x="740" y="585" rx="16" ry="16" width="290" height="110" class="box section pink" />
        <text x="760" y="617" class="t title">llm_call_log (SQLite)</text>
        <text x="760" y="641" class="t muted">cache / observability</text>
        <text x="760" y="663" class="t muted small">written by: LLMRouter</text>

        <rect x="1045" y="605" rx="16" ry="16" width="235" height="110" class="box section" />
        <text x="1065" y="637" class="t title">External LLM Service</text>
        <text x="1065" y="661" class="t muted">OpenAI-compatible API</text>

        <!-- Arrows -->
        <path d="M230 105 C245 105, 255 105, 270 105" class="arrow blue" />
        <path d="M690 115 C710 115, 725 115, 740 115" class="arrow green" />

        <path d="M870 230 C870 238, 870 244, 870 250" class="arrow orange" />
        <path d="M1155 230 C1155 238, 1155 244, 1155 250" class="arrow pink" />

        <path d="M740 335 C725 335, 710 335, 690 335" class="arrow orange" />
        <path d="M690 365 C820 365, 920 350, 1030 330" class="arrow soft" />

        <path d="M885 440 C885 430, 885 420, 885 410" class="arrow purple" />
        <path d="M1157 590 C1157 597, 1157 600, 1157 605" class="arrow purple" />
        <path d="M1045 520 C990 545, 930 565, 885 585" class="arrow purple" />

        <path d="M690 650 C860 650, 940 520, 1030 390" class="arrow soft" />

        <text x="705" y="98" class="t muted small">HTTP/JSON</text>
        <text x="890" y="244" class="t muted small">start long task</text>
        <text x="1135" y="244" class="t muted small">DI connection</text>
        <text x="910" y="433" class="t muted small">scheduled trigger</text>
        <text x="1180" y="600" class="t muted small">call</text>
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
  color: var(--el-text-color-secondary);
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
  marker-end: url(#arrowHeadArchZh);
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

svg[aria-label*='English'] .arrow {
  marker-end: url(#arrowHeadArchEn);
}
</style>

