# ProdWatch Excel 数据库说明（`prodwatch_database.xlsx`）

本项目默认把 `backend/database/prodwatch_database.xlsx` 当作“数据库”使用（参见 `backend/storage/db.py` 的 `ExcelRepository`）。

- 读：`repo.query(sheet, filters)`（filters 仅支持等值过滤 `df[df[k] == v]`）
- 写：`repo.insert(sheet, row)` / `repo.replace(sheet, rows)` / `repo.update_by_id(sheet, id, updates)`

下文按 **每一个 Sheet** 说明：用途、由谁写入、由谁读取、在前端哪个页面展示，以及与其它功能的串联关系。

---

## Sheet 总览（用途 / 读写位置 / 页面映射）

> 说明：如果某个 Sheet 当前未被代码读取/写入，会标注为“未使用/预留”，但仍保留其在数据链路上的合理位置。

### 1) `brand`
- 用途：品牌维表（品牌 id、名称等），仪表盘按品牌聚合的基础维度。
- 写入：
  - Demo/造数：`scripts/seed_fake_dashboard_data.py`
- 读取：
  - 仪表盘选项与聚合：`backend/routers/dashboard.py`
- 前端展示：
  - 仪表盘品牌多选：`frontend/src/views/Dashboard.vue`

### 2) `platform`
- 用途：平台维表（微博/小红书/抖音等），用于导入/爬虫映射、后台设置页展示平台列表、内容审核页展示平台名称。
- 写入：
  - Demo/造数：`scripts/seed_fake_dashboard_data.py`
- 读取：
  - 数据源列表：`backend/routers/sources.py`
  - 设置页：`backend/routers/settings.py`
  - 仪表盘 options：`backend/routers/dashboard.py`
  - 内容审核/反垃圾：`backend/routers/moderation.py`
  - 报告上下文：`backend/agents/reporter.py`
  - Mock 爬虫映射 code→id：`backend/agents/crawler.py`
  - Excel 导入映射 code→id：`backend/agents/importer.py`
- 前端展示：
  - 设置页（数据源频率配置）：`frontend/src/views/Settings.vue`
  - 调试页（sources / report）：`frontend/src/views/DebugConsole.vue`
  - 内容审核页：`frontend/src/views/Moderation.vue`

### 3) `monitor_project`
- 用途：监控项目维表（项目 id、名称、所属 brand_id 等）。本项目“按品牌聚合”的实现是：**brand_id → 多个 project_id → 再聚合**。
- 写入：
  - Demo/造数：`scripts/seed_fake_dashboard_data.py`
- 读取：
  - 仪表盘 options：`backend/routers/dashboard.py`
  - 仪表盘 brand_id → project_id 映射：`backend/routers/dashboard.py`
  - 帖子浏览 brand_ids → project_id 过滤：`backend/routers/posts.py`
  - 设置页 users 列表（实际上返回的是 monitor_project 列表）：`backend/routers/settings.py`
- 前端展示：
  - 仪表盘内部用于品牌筛选（页面只暴露“品牌”下拉）：`frontend/src/views/Dashboard.vue`
  - 设置页 users：`frontend/src/views/Settings.vue`

### 4) `monitor_keyword`
- 用途：监控关键词维表。导入 Excel 时会用它做关键词去重/补录，并把 `keyword_id` 写到 `post_raw`。
- 写入：
  - Excel 导入时：`backend/agents/importer.py`
- 读取：
  - Excel 导入时：`backend/agents/importer.py`
- 前端展示：当前未直接展示（但 `post_raw.keyword_id` 会在帖子详情/展开行中可见）。

### 5) `agent`（未使用/预留）
- 用途：预留的 Agent 定义表（例如 crawler/analyzer 等 agent 元数据）。
- 读写：当前代码未读取/写入。

### 6) `pipeline_run`
- 用途：流水线运行批次表（一次采集/导入/分析处理 = 一个批次 id）。
- 写入：
  - 手动分析触发：`backend/agents/pipeline.py`（`run_manual_pipeline`）
  - Excel 导入：`backend/agents/importer.py`
  - 仅后处理：`backend/agents/pipeline.py`（`process_existing_run` 会确保记录存在）
  - 批次完成：`backend/agents/pipeline.py`（`update_by_id` 写 `status/end_time`）
- 读取：
  - 帖子浏览时间回填：`backend/routers/posts.py`（当 `publish_time` 为空时按 run 时间回填以支持时间过滤）
- 前端展示：
  - 报告摘要里会返回 `pipelineRunId`：`frontend/src/views/Reports.vue`、`frontend/src/views/DebugConsole.vue`

### 7) `agent_job`（未使用/预留）
- 用途：预留的任务队列/执行记录表（例如定时采集任务）。
- 读写：当前代码未读取/写入。

### 8) `author`（未使用/预留）
- 用途：作者维表（平台作者信息等）。
- 读写：当前代码未读取/写入（`post_raw.author_id` 当前写入为 `None`）。

### 9) `post_raw`
- 用途：原始帖子源数据表（“事实数据源”）。帖子浏览页直接展示该表；仪表盘的部分指标也会间接来自该表的派生（通过 pipeline 聚合到 `daily_metric`）。
- 写入：
  - Mock 爬虫：`backend/agents/crawler.py`
  - Excel 导入：`backend/agents/importer.py`（来源 `post_raw_upload`）
- 读取：
  - 帖子浏览 API：`backend/routers/posts.py`
  - 仪表盘 KPI（/kpis）：`backend/routers/dashboard.py`
  - 内容审核（联表用）：`backend/routers/moderation.py`
  - Pipeline 清洗与聚合：`backend/agents/pipeline.py`
  - latest_run 判定：`backend/storage/db.py`（取 `pipeline_run_id` 最大值）
- 前端展示：
  - 帖子浏览：`frontend/src/components/RawPostList.vue`

### 10) `post_clean`
- 用途：清洗后的帖子（对 `post_raw` 的派生），用于过滤无效内容、后续 spam/sentiment 打标及聚合。
- 写入：
  - Pipeline 清洗：`backend/agents/pipeline.py`（`clean_posts_for_run`）
- 读取：
  - 反垃圾列表联表：`backend/routers/moderation.py`
  - 报告样例：`backend/agents/reporter.py`
  - 情绪打标输入：`backend/agents/pipeline.py` → `backend/agents/analyzer.py`
  - 兼容 wrapper：`backend/agents/filterer.py`
- 前端展示：当前不直接展示（通过 moderation/report 间接展示）。

### 11) `spam_score`
- 用途：对 `post_clean` 的垃圾/水军判定结果（规则命中、label 等）。
- 写入：
  - Pipeline spam 打分：`backend/agents/pipeline.py`（`score_spam_for_run`）
- 读取：
  - 反垃圾概览与列表：`backend/routers/moderation.py`
  - 聚合到日指标：`backend/agents/pipeline.py`（`aggregate_daily_metrics_for_run` 会把 spam 统计进 `daily_metric`）
- 前端展示：
  - 内容审核页：`frontend/src/views/Moderation.vue`

### 12) `sentiment_result`
- 用途：对 `post_clean` 的情绪分析结果（positive/neutral/negative + intensity/emotions）。
- 写入：
  - 情绪分析：`backend/agents/analyzer.py`（由 `backend/agents/pipeline.py` 调用）
- 读取：
  - 仪表盘 KPI：`backend/routers/dashboard.py`
  - 仪表盘情绪强度/趋势/告警：`backend/routers/dashboard.py`
  - 报告样例：`backend/agents/reporter.py`
  - 聚合到日指标：`backend/agents/pipeline.py`（将 pos/neu/neg 汇总进 `daily_metric`）
- 前端展示：
  - 仪表盘（趋势/概览/告警来自聚合或其派生）：`frontend/src/views/Dashboard.vue`

### 13) `topic_result`（未使用/预留）
- 用途：主题建模结果（对帖子聚类/话题）。
- 读写：当前代码未读取/写入。

### 14) `entity_result`（未使用/预留）
- 用途：实体抽取结果（品牌/竞品/功能点等实体）。
- 读写：当前代码未读取/写入。

### 15) `daily_metric`
- 用途：日级聚合指标表（按 project_id + platform_id + metric_date 聚合），是仪表盘的核心数据来源。
- 写入：
  - Pipeline 聚合：`backend/agents/pipeline.py`（`aggregate_daily_metrics_for_run`）
  - Demo/造数：`scripts/seed_fake_dashboard_data.py`
- 读取：
  - 仪表盘概览/趋势/排行/告警：`backend/routers/dashboard.py`
  - 报告上下文：`backend/agents/reporter.py`
- 前端展示：
  - 仪表盘：`frontend/src/views/Dashboard.vue`

### 16) `report`
- 用途：报告表（摘要、标题、生成时间、关联 pipeline_run_id）。
- 写入：
  - Pipeline 生成：`backend/agents/pipeline.py`（`generate_report_for_run`）
- 读取：
  - 报告摘要 API：`backend/routers/report.py`
  - 报告 PDF 上下文：`backend/agents/reporter.py`
- 前端展示：
  - 报告页：`frontend/src/views/Reports.vue`
  - 调试页：`frontend/src/views/DebugConsole.vue`

### 17) `report_citation`（未使用/预留）
- 用途：报告引用（引用原帖/来源链接/证据等）。
- 读写：当前代码未读取/写入。

### 18) `post_raw_upload`
- 用途：Excel 导入“临时表/上传表”。`/api/sources/import_excel` 会从这里读取，再写入 `post_raw`。
- 写入：
  - 生成示例 Excel：`scripts/generate_mock_excel.py`（生成/覆盖该 sheet）
  - 人工填充：也可手动在 Excel 内编辑后导入
- 读取：
  - Excel 导入：`backend/agents/importer.py`
- 前端展示：当前无专用页面（通过调试/接口触发导入）。

### 19) `datasource_config`
- 用途：数据源配置（按 platform.code 存储采集频率等）。
- 写入：
  - 设置页保存：`backend/routers/settings.py`（`repo.replace("datasource_config", ...)`）
- 读取：
  - 设置页读取：`backend/routers/settings.py`
- 前端展示：
  - 设置页：`frontend/src/views/Settings.vue`

---

## 功能如何串联（端到端数据链路）

### A) 数据进入系统的两条主路径

1) **手动分析触发**
- API：`POST /api/analysis/run`
- 代码：`backend/routers/analysis.py` → `backend/agents/pipeline.py:run_manual_pipeline`
- 结果：
  - 生成一条 `pipeline_run`（批次 id）
  - `crawler` 写入 `post_raw`（每条带同一个 `pipeline_run_id`）
  - 进入后处理（见下方 B）

2) **Excel 导入**
- API：`POST /api/sources/import_excel`
- 代码：`backend/routers/sources.py` → `backend/agents/importer.py:import_from_excel`
- 结果：
  - 从 `post_raw_upload` 读取
  - 写入 `pipeline_run` + `post_raw`
  - 触发 `process_existing_run(run_id)` 做后处理（见下方 B）

### B) 批次后处理（把“原始帖子”变成“可看板指标”）

入口：`backend/agents/pipeline.py:process_existing_run(pipeline_run_id)`

链路：
1. `post_raw`（原始帖子，按 run_id 过滤）
2. `post_clean`（清洗：无效内容过滤、hash 等）
3. `spam_score`（反垃圾规则打分）
4. `sentiment_result`（情绪分析：rule-based 或 LLM）
5. `daily_metric`（把帖子按 project_id+platform_id+day 聚合，形成仪表盘可用指标）
6. `report`（生成本次摘要报告，并写 `pipeline_run_id`）
7. `pipeline_run` 状态更新为 finished

### C) 前端页面如何读取这些表

1) **仪表盘**：`frontend/src/views/Dashboard.vue`
- API：`/api/dashboard/options`（读取 `brand/platform/monitor_project`）
- 指标：`/api/dashboard/overview`、`/api/dashboard/sentiment_trends`、`/api/dashboard/competitor_ranking`、`/api/dashboard/sentiment_alerts`
  - 主要读 `daily_metric`，并通过 `monitor_project.brand_id` 做品牌聚合
  - 部分强度/告警会读 `sentiment_result`

2) **帖子浏览**：`frontend/src/components/RawPostList.vue`
- API：`/api/posts`
  - 直接读 `post_raw`
  - `brand_ids` 会先映射到 `monitor_project` 的 project_ids 再过滤
  - 时间过滤按 `publish_time`（必要时用 `pipeline_run` 回填）

3) **内容审核（反垃圾）**：`frontend/src/views/Moderation.vue`
- API：`/api/moderation/spam/overview`、`/api/moderation/spam/list`
  - 读 `spam_score`，并联表 `post_clean`→`post_raw`，用 `platform` 映射显示平台名称

4) **报告页**：`frontend/src/views/Reports.vue`
- API：`/api/report`
  - 读 `report`（最新 created_at）

5) **设置页（数据源）**：`frontend/src/views/Settings.vue`
- API：`/api/settings/datasources` / `POST /api/settings/datasources`
  - 读 `platform` + `datasource_config`
  - 保存时覆盖写入 `datasource_config`

