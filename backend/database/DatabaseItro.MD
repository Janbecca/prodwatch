# ProdWatch Excel “数据库”说明（`prodwatch_database.xlsx`）

更新时间：2026-03-22

本项目默认把 `backend/database/prodwatch_database.xlsx` 当作“数据库”使用（参见 `backend/storage/db.py` 的 `ExcelRepository`）。数据以 **Sheet=表** 的方式组织，并通过 `repo.query/insert/replace/update_by_id` 访问。

> 重要限制（ExcelRepository）：
> - 读：`repo.query(sheet, filters)` 仅支持**等值过滤**（类似 `df[df[k] == v]`），不支持范围/模糊/排序；需要更复杂逻辑时由代码在内存里 `pandas` 处理。
> - 写：多数地方用 `repo.replace(sheet, rows)` **整表覆盖**（需要特别注意并发与误覆盖）。
> - 描述行：很多表的第 2 行会有中文“说明行”，`ExcelRepository` 会用启发式自动跳过（见 `ExcelRepository._drop_description_row`）。

---

## 0. 数据访问入口（Repository）

文件：`backend/storage/db.py`

- 环境变量
  - `DB_TYPE`：默认 `excel`（`mysql` 仅占位，未实现）。
  - `EXCEL_PATH`：默认 `backend/database/prodwatch_database.xlsx`。
- 核心接口
  - `ExcelRepository.query(sheet, filters)`：返回 `DataFrame`，并对 `id` 列做“只保留 numeric id 行”的兼容处理。
  - `ExcelRepository.insert/insert_many`：追加写入。
  - `ExcelRepository.replace`：整表覆盖。
  - `ExcelRepository.update_by_id`：按 `id` 更新（少数地方使用）。
- 锁冲突
  - 当 Excel/WPS 打开了工作簿时可能写入失败，抛 `ExcelWriteLockedError`。

---

## 1. Sheet 一览（当前工作簿）

以下是 `backend/database/prodwatch_database.xlsx` 中实际存在的 Sheet（通过代码读取到的列名）：

| Sheet | 列（columns） |
|---|---|
| `brand` | `id,name,industry,created_at` |
| `platform` | `id,code,name,created_at` |
| `monitor_project` | `id,brand_id,name,description,is_active,created_at,updated_at,product_category` |
| `monitor_project_brand` | `id,project_id,brand_id,created_at` |
| `monitor_project_platform` | `id,project_id,platform_id,is_enabled,crawl_mode,cron_expr,timezone,sentiment_model,max_posts_per_run,query_strategy,last_run_at,last_success_at,next_run_at,created_at,updated_at` |
| `monitor_keyword` | `id,project_id,keyword,keyword_type,weight,is_active,created_at` |
| `pipeline_run` | `id,project_id,project_platform_id,run_no,trigger_type,status,start_time,end_time,params,brand_ids_snapshot,keyword_ids_snapshot,max_posts_per_run,sentiment_model,Unnamed: 13,created_at,platform_id` |
| `post_raw` | `id,pipeline_run_id,project_platform_id,project_id,platform_id,brand_id,keyword_id,content_type,platform_post_id,author_id,publish_time,raw_text,like_count,comment_count,share_count,crawl_source,created_at` |
| `post_clean` | `id,post_raw_id,pipeline_run_id,project_id,clean_text,text_hash,is_valid,invalid_reason,project_platform_id,platform_id,brand_id` |
| `spam_score` | `id,post_clean_id,project_id,score_total,label,rule_hits,project_platform_id,platform_id,brand_id` |
| `sentiment_result` | `id,post_clean_id,project_id,polarity,confidence,intensity,emotions,pipeline_run_id,project_platform_id,platform_id,brand_id` |
| `topic_result` | `id,post_clean_id,project_id,topic_id,topic_name,score,pipeline_run_id,project_platform_id,platform_id,brand_id,created_at` |
| `entity_result` | `id,post_clean_id,project_id,entity_type,entity_text,normalized,confidence,pipeline_run_id,project_platform_id,platform_id,brand_id,created_at` |
| `daily_metric` | `id,project_id,brand_id,platform_id,metric_date,total_posts,valid_posts,spam_posts,pos_posts,neu_posts,neg_posts,pipeline_run_id` |
| `report` | `id,pipeline_run_id,project_platform_id,project_id,brand_id,platform_id,title,report_type,summary,created_at,time_start,time_end,range_from,range_to,content_json,status,trigger_type`（初始列；`backend/routers/reports.py:_ensure_report_sheets` 会补列并覆盖写回） |
| `report_config` | （当前为空表；运行时由代码初始化列） |
| `report_citation` | （当前为空表；运行时由代码初始化列） |
| `datasource_config` | `id,platform_id,default_cron_expr,default_max_posts_per_run,is_global_enabled,updated_at`（注意：部分接口会覆盖为另一套列） |
| `meta` | `key,value,updated_at` |
| `agent` | `id,agent_type,name,version,config,created_at`（当前未被业务用到） |
| `author` | `id,platform_id,platform_uid,nickname,meta,created_at`（帖子展示可联表；默认数据为空） |

另：代码还会读取一个**导入用外部 Sheet**：`post_raw_upload`（见 `backend/agents/importer.py`），但它不在当前工作簿内，通常来自“上传/外部 Excel”。

---

## 2. 表之间如何串联（主键/外键/常用联表）

### 2.1 关系图（概念）

- `brand(id)`
  - `<- monitor_project_brand(brand_id)`（项目-品牌多对多）
  - `<- monitor_project(brand_id)`（旧字段：单品牌项目；新逻辑以 join 表为准）
  - `<- post_raw(brand_id)` / `post_clean(brand_id)` / `daily_metric(brand_id)` / `sentiment_result(brand_id)` / `spam_score(brand_id)` …
- `platform(id)`
  - `<- monitor_project_platform(platform_id)`（项目-平台配置）
  - `<- post_raw(platform_id)` / `daily_metric(platform_id)` / `pipeline_run(platform_id)` …
- `monitor_project(id)`
  - `<- monitor_project_brand(project_id)`（绑定品牌）
  - `<- monitor_project_platform(project_id)`（绑定平台+调度配置）
  - `<- monitor_keyword(project_id)`（监控关键词）
  - `<- post_raw(project_id)` / `post_clean(project_id)` / `daily_metric(project_id)` / `report(project_id)` / `pipeline_run(project_id)` …
- `monitor_project_platform(id)`（也叫 `project_platform_id`）
  - `<- pipeline_run(project_platform_id)`
  - `<- post_raw(project_platform_id)` / `post_clean(project_platform_id)` / `sentiment_result(project_platform_id)` …
- `pipeline_run(id)`（批次）
  - `<- post_raw(pipeline_run_id)`
  - `<- post_clean(pipeline_run_id)` / `sentiment_result(pipeline_run_id)` / `daily_metric(pipeline_run_id)` / `report(pipeline_run_id)` …
- `post_raw(id)`（原始帖子）
  - `<- post_clean(post_raw_id)`（清洗后帖子一对一/一对多（保留最新））
  - `<- report_citation(post_raw_id)`（报告引用原文）
- `post_clean(id)`（清洗帖子）
  - `<- spam_score(post_clean_id)` / `sentiment_result(post_clean_id)` / `topic_result(post_clean_id)` / `entity_result(post_clean_id)`
- `report(id)`
  - `<- report_config(report_id)`（生成参数快照）
  - `<- report_citation(report_id)`（引用证据）
- `meta(key)`
  - `next_project_id`：用于防止项目删除后 id 复用（见 `backend/routers/projects.py`）。

### 2.2 代码里的“联表方式”（Excel 下的实现）

Excel 方案没有数据库 JOIN，全部通过 `pandas` 在内存里拼：

- **项目→品牌**：优先用 `monitor_project_brand`，缺失时回退 `monitor_project.brand_id`（兼容老数据）。
  - 代码：`backend/routers/posts.py:_projects_for_brands`、`backend/routers/dashboard.py:_project_brand_map`。
- **帖子（post_raw）→清洗/打标**：`post_raw.id` 映射到 `post_clean.post_raw_id`，再用 `post_clean.id` 去取 `sentiment_result/spam_score/topic_result/entity_result`。
  - 代码：`backend/routers/posts.py:_enrich_posts`。
- **报告→引用**：`report_citation.report_id` + `report_citation.post_raw_id` 联到 `report` 与 `post_raw`。
  - 代码：`backend/routers/reports.py`、`backend/routers/posts.py:linked_reports`。

---

## 3. 功能 → 数据调用明细（用到了哪个 Sheet 的哪个字段）

### 3.1 项目/品牌配置（Projects/Brands）

前端：`frontend/src/views/Projects.vue`

- 品牌管理（`/api/brands`，`backend/routers/brands.py`）
  - `brand`：读 `id,name,industry`；写 `id,name,industry,created_at`；删时会 `replace(brand)`。
  - 约束检查：
    - `monitor_project_brand.brand_id`（品牌是否被项目使用）
    - `monitor_project.brand_id`（旧字段兼容）
- 项目列表（`GET /api/projects`，`backend/routers/projects.py:list_projects`）
  - `monitor_project`：读 `id,name,product_category,is_active`
  - `monitor_project_brand`：读 `project_id,brand_id`（组装 `brand_ids`）
  - `monitor_project_platform`：读 `project_id,platform_id,is_enabled`（统计 enabled 平台）
- 项目保存（`POST /api/projects`，`backend/routers/projects.py:save_projects`）
  - `monitor_project`：写 `id,name,product_category,is_active,created_at,updated_at`（并将 `brand_id` 置为 `None` 作为 legacy）
  - `monitor_project_brand`：写 `id,project_id,brand_id,created_at`
  - `meta`：写 `key=next_project_id,value,updated_at`（防止 id 复用）
- 项目启用/禁用（`POST /api/projects/{id}/activate`）
  - `monitor_project.is_active` / `monitor_project.product_category`
  - `monitor_project_brand(project_id,brand_id)`：启用时要求至少绑定一个品牌
  - `monitor_project_platform`：启用时要求至少有一个 `is_enabled=1`；若没有则用 `datasource_config` + `platform` 播种默认配置
- 项目-平台配置（`GET/PUT /api/projects/{id}/platforms`）
  - `monitor_project_platform`：读写 `id,project_id,platform_id,is_enabled,crawl_mode,cron_expr,timezone,max_posts_per_run,sentiment_model,query_strategy,created_at,updated_at`
  - `platform`：读 `id,code,name`（返回平台元数据给前端）
- 项目删除（`DELETE /api/projects/{id}`）
  - `monitor_project` / `monitor_project_brand` / `monitor_project_platform`：删除对应项目行
  - `monitor_keyword`：按 `project_id` 删除关键词
  - 级联清理（避免 id 复用导致“新项目继承旧数据”）：按 `project_id` 或 `pipeline_run_id` 清理
    - `post_raw(project_id)`、`post_clean(project_id)`、`spam_score(project_id)`、`sentiment_result(project_id)`、`topic_result(project_id)`、`entity_result(project_id)`、`daily_metric(project_id)`、`report(project_id)`、`report_config(project_id)`、`report_citation(report_id/post_raw_id)`、`pipeline_run(project_id)`

### 3.2 仪表盘（Dashboard）

前端：`frontend/src/views/Dashboard.vue`

- 选项数据（`GET /api/dashboard/options`，`backend/routers/dashboard.py:get_options`）
  - `brand`：`id,name`
  - `platform`：`id,code,name`
  - `monitor_project`：`id,brand_id,name,product_category,description,is_active`
  - `monitor_project_brand`：`project_id,brand_id`（拼装项目的 `brand_ids`，并兼容 `monitor_project.brand_id`）
  - `monitor_project_platform`：`id,project_id,platform_id,is_enabled,crawl_mode,cron_expr,timezone,max_posts_per_run,sentiment_model`
- KPI 卡片（`GET /api/dashboard/kpis`）
  - `post_raw`：仅用行数（总帖子量）
  - `sentiment_result`：`polarity,intensity`（计算正负占比与平均强度）
- 概览面板（`GET /api/dashboard/overview`）
  - 主数据源：`daily_metric`
    - 过滤：`metric_date`（按时间窗）+ `project_id`（由品牌映射得到）
    - 聚合字段：`total_posts,valid_posts,spam_posts,pos_posts,neu_posts,neg_posts,brand_id,project_id`
  - 强度补充：`sentiment_result.intensity`（按 `brand_id` 或回退 `project_id→brand` 计算平均强度）
- 总量趋势（`GET /api/dashboard/trends`）
  - `daily_metric.platform_id,metric_date,total_posts`
  - `platform.id,name`（显示平台名称）
- 情绪/垃圾占比趋势（`GET /api/dashboard/sentiment_trends`）
  - `daily_metric.metric_date,project_id,brand_id,total_posts,valid_posts,spam_posts,pos_posts,neu_posts,neg_posts`
  - `brand.id,name`（系列名称）
- 平台排名（`GET /api/dashboard/ranking`）
  - `daily_metric.platform_id,total_posts,spam_posts`
  - `platform.id,name`
- 竞品热度排名（`GET /api/dashboard/competitor_ranking`）
  - `daily_metric` 同上（按品牌聚合并计算 heat）
  - `brand.id,name`
- 告警（`GET /api/dashboard/alerts`）
  - `daily_metric.platform_id,total_posts,neg_posts`（负面占比阈值）
  - `platform.id,name`
- 情绪波动告警（`GET /api/dashboard/sentiment_alerts`）
  - `daily_metric.metric_date,total_posts,pos_posts,neu_posts,neg_posts`（取最近两天的变化）
  - `brand.id,name`
- 关键词频次（`GET /api/dashboard/keyword_frequencies`）
  - `post_raw.publish_time,project_id,platform_id,brand_id,keyword_id`
  - `monitor_keyword.id,keyword`（`keyword_id→keyword`）
  - `brand.id,name`
- 手动刷新（`POST /api/dashboard/manual_refresh`）
  - 读：`monitor_project.id,is_active`（未指定项目时，刷新所有启用项目）
  - 写：由 `backend/agents/simulator.py:run_simulated_crawl` 触发整条流水线写入（见 3.6）

### 3.3 帖子浏览（Posts）

前端：`frontend/src/components/RawPostList.vue`

- 帖子分页/过滤（`GET /api/posts/page` / `GET /api/posts`，`backend/routers/posts.py`）
  - 主表：`post_raw`
    - 过滤字段：`platform_id,project_id,keyword_id,pipeline_run_id,publish_time`
    - 展示字段：`id,raw_text,like_count,comment_count,share_count,platform_post_id,content_type,crawl_source,created_at` 等
  - 品牌筛选（`brand_ids`）
    - 优先：`monitor_project_brand(project_id,brand_id)` 将品牌映射为项目集合
    - 回退：`monitor_project(id,brand_id)`
  - enrich（同一帖子补充更多列，便于 UI 搜索/筛选）
    - `platform(id,name,code)` → `post_raw.platform_id` → `platform_name`
    - `brand(id,name)` → `post_raw.brand_id`（缺失则 `project_id→brand`）→ `brand_name`
    - `monitor_keyword(id,keyword)` → `post_raw.keyword_id` → `keyword`
    - `author(id,nickname)` → `post_raw.author_id` → `author_nickname`
    - `post_clean(id,post_raw_id,clean_text,text_hash,is_valid,invalid_reason)` → `post_raw.id` → `post_clean_id` 与清洗字段
    - `sentiment_result(post_clean_id,polarity,confidence,intensity,emotions)` → 情绪字段
    - `spam_score(post_clean_id,score_total,label,rule_hits)` → 垃圾字段
    - `topic_result(post_clean_id,topic_name,score)` → 主题字段
    - `entity_result(post_clean_id,entity_type,entity_text,normalized,confidence)` → 聚合为 `entities/entity_texts`
- 统计（`GET /api/posts/stats`）
  - 复用分页相同过滤 + enrich 后的列
  - 使用：`clean_is_valid`、`polarity`、`spam_label`、`topic_name`、`entities`
- 关键词下拉（`GET /api/posts/keywords?project_id=...`）
  - `monitor_keyword.project_id,id,keyword,is_active`
- 帖子关联报告（`GET /api/posts/{id}/linked_reports`）
  - `report_citation.post_raw_id,report_id`
  - `report.id,title,report_type,created_at`

### 3.4 报告中心（Reports）

前端：`frontend/src/views/Reports.vue`

- 报告列表/筛选（`GET /api/reports`，`backend/routers/reports.py:list_reports`）
  - `report`：`id,title,report_type,status,created_at,time_start,time_end,project_id,platform_id,brand_id,pipeline_run_id,trigger_type`
  - 补充名称：
    - `monitor_project.id,name`
    - `platform.id,name`
    - `brand.id,name`
- 报告详情（`GET /api/reports/{id}`）
  - `report.content_json`（核心内容，JSON 字符串）
  - `report.summary/title/report_type/time_start/time_end/range_from/range_to/...`
- 引用列表（`GET /api/reports/{id}/citations`）
  - `report_citation.report_id,citation_type,section_code,quote_text,sort_order,post_raw_id,reason,created_at`
  - 关联原文：`post_raw.id,raw_text,platform_id,publish_time,platform_post_id`
  - 平台名：`platform.id,name`
- 删除报告（`DELETE /api/reports/{id}`）
  - `report`：按 `id` 删除
  - `report_config`：按 `report_id` 删除
  - `report_citation`：按 `report_id` 删除
- 导出（`GET /api/reports/{id}/export`）
  - 读：`report`（生成导出文件）
  - 写：导出文件落地到本地目录（见 `backend/routers/reports.py` 的 `FileResponse`）
- 创建自定义报告（`POST /api/reports`）
  - 写入三张表（由 `_ensure_report_sheets` 兜底补列）：
    - `report`：`id,project_id,platform_id,brand_id,title,report_type,summary,content_json,status,trigger_type,created_at,time_start,time_end,range_from,range_to,pipeline_run_id,project_platform_id`
    - `report_config`：`id,report_id,project_id,time_start,time_end,platform_ids,brand_ids,keyword_ids,include_sections,config_json,created_at`
    - `report_citation`：`id,report_id,citation_type,section_code,quote_text,sort_order,post_raw_id,reason,created_at`
  - 生成过程读取的事实表：
    - `post_raw`（按时间/项目/平台/品牌/关键词过滤，取 `id,raw_text,publish_time,platform_id,project_id,brand_id,keyword_id`）
    - `post_clean` / `sentiment_result` / `spam_score`（统计与抽样）
    - `daily_metric`（趋势与汇总）
    - `topic_result` / `entity_result`（热点话题/实体）

### 3.5 数据源/导入（Sources/Import）

前端：`frontend/src/views/DebugConsole.vue`

- 数据源列表（`GET /api/sources`，`backend/routers/sources.py:list_sources`）
  - `platform`：读 `id,code,name,created_at`
- 数据源详情（`GET /api/sources/{code}`）
  - `platform.code` 精确匹配，返回平台整行
- 导入 Excel（`POST /api/sources/import_excel`）
  - 读取外部 Excel 的 `post_raw_upload`（`backend/agents/importer.py`）
    - 典型列：`project_id,platform_code,platform_post_id,keyword,publish_time,raw_text,like_count,comment_count,share_count`
  - 写入 `pipeline_run`（`trigger_type=import_excel`）+ `post_raw`
  - 随后触发 `process_existing_run(run_id)`（清洗/打标/聚合/生成报告）

### 3.6 数据进入系统与后处理（Pipeline）

入口：`backend/agents/pipeline.py:process_existing_run(pipeline_run_id)`

链路与表写入：

1) `pipeline_run`（批次）
- 写：`id,project_id,project_platform_id,platform_id,run_no,trigger_type,status,start_time,end_time,params,brand_ids_snapshot,keyword_ids_snapshot,max_posts_per_run,sentiment_model,created_at`
- 读：部分 API 会按 `pipeline_run_id` 做过滤/展示

2) `post_raw`（原始）
- 写入来源
  - 手动分析：`backend/agents/crawler.py:crawl`（mock）
  - Excel 导入：`backend/agents/importer.py:import_from_excel`（外部表 `post_raw_upload`）
  - 仪表盘手动/定时刷新：`backend/agents/simulator.py:run_simulated_crawl`
- 核心字段：`id,pipeline_run_id,project_id,project_platform_id,platform_id,brand_id,keyword_id,publish_time,raw_text,like_count,comment_count,share_count,crawl_source,created_at`

3) `post_clean`（清洗）
- 写：`id,post_raw_id,pipeline_run_id,project_id,project_platform_id,platform_id,brand_id,clean_text,text_hash,is_valid,invalid_reason`
- 读：帖子 enrich、反垃圾联表、报告统计

4) `spam_score`（反垃圾）
- 写：`id,post_clean_id,project_id,project_platform_id,platform_id,brand_id,score_total,label,rule_hits`
- 读：帖子/报告/（后续可接 moderation UI）

5) `sentiment_result`（情绪）
- 写：`id,post_clean_id,project_id,pipeline_run_id,project_platform_id,platform_id,brand_id,polarity,confidence,intensity,emotions`
- 写入方：`backend/agents/analyzer.py:analyze_sentiment`
- 读：仪表盘、帖子、报告

6) `topic_result` / `entity_result`（主题/实体）
- 写入方：`backend/agents/pipeline.py`（从 `post_clean.clean_text` 推断）
- 读：帖子列表筛选与报告热点模块

7) `daily_metric`（日聚合）
- 写：`id,project_id,brand_id,platform_id,metric_date,total_posts,valid_posts,spam_posts,pos_posts,neu_posts,neg_posts,pipeline_run_id`
- 读：仪表盘与报告趋势/汇总

8) `report` / `report_citation`（自动日报）
- 写：`report`（`content_json` 为 JSON 字符串）
- 写：`report_citation`（抽样负面帖子作为证据）

---

## 4. 各 Sheet 细节说明（用途 / 字段含义 / 读写点）

> 提示：下面“字段含义”以 **代码实际使用** 为准。对于“存在但暂未被代码引用”的列，会标记为“保留/未用”。

### 4.1 `brand`（品牌维表）

- 主键：`id`
- 字段：
  - `id`：品牌 id（int）
  - `name`：品牌名
  - `industry`：行业（可空）
  - `created_at`：创建时间
- 写入：`backend/routers/brands.py`、`scripts/seed_fake_dashboard_data.py`
- 读取：`backend/routers/dashboard.py`、`backend/routers/reports.py`、`backend/routers/posts.py`、`backend/routers/projects.py`

### 4.2 `platform`（平台维表）

- 主键：`id`
- 字段：`id,code,name,created_at`
- 关键用法：`code` 用于导入/模拟/映射（例如 `weibo/xhs/douyin`），`id` 用于事实表外键。
- 读取：`backend/routers/sources.py`、`backend/routers/dashboard.py`、`backend/routers/projects.py`、`backend/routers/posts.py`、`backend/routers/reports.py`

### 4.3 `monitor_project`（监控项目）

- 主键：`id`
- 字段：
  - `brand_id`：**legacy**（单品牌项目旧字段；当前以 `monitor_project_brand` 为准）
  - `product_category`：启用项目的必填项（见 `POST /api/projects/{id}/activate`）
  - `is_active`：是否启用（启用项目会参与仪表盘刷新）
- 写入：`backend/routers/projects.py`
- 读取：`backend/routers/dashboard.py`、`backend/tasks/scheduler.py`（间接）、`backend/agents/simulator.py`

### 4.4 `monitor_project_brand`（项目-品牌关联）

- 主键：`id`
- 外键：`project_id -> monitor_project.id`，`brand_id -> brand.id`
- 写入：`backend/routers/projects.py`
- 读取：品牌筛选、项目展示、报表聚合、帖子过滤等（`dashboard.py/posts.py/projects.py`）。

### 4.5 `monitor_project_platform`（项目-平台配置/调度）

- 主键：`id`（也会被当作 `project_platform_id` 写入 `pipeline_run/post_raw/...`）
- 外键：`project_id -> monitor_project.id`，`platform_id -> platform.id`
- 关键字段：
  - `is_enabled`：是否启用该平台（启用项目要求至少一个 enabled 平台）
  - `crawl_mode`：`schedule|manual`（调度只处理 schedule）
  - `cron_expr`：只支持 `minute hour * * *` 的最小解析（见 `backend/tasks/scheduler.py`）
  - `timezone`：用于“每天执行一次”的去重判断
  - `max_posts_per_run`：每次批次最多生成/抓取帖子数
  - `sentiment_model`：情绪模型选择（`rule-based` 或 dashscope 模型名）
  - `last_run_at/last_success_at`：scheduler 回写
- 写入：`backend/routers/projects.py`、`backend/tasks/scheduler.py`
- 读取：`backend/agents/simulator.py`

### 4.6 `monitor_keyword`（项目关键词）

- 主键：`id`
- 外键：`project_id -> monitor_project.id`
- 关键字段：`keyword,is_active`
- 写入：`backend/agents/importer.py`（导入时去重/补录）
- 读取：`backend/routers/posts.py`（关键词下拉）、`backend/routers/dashboard.py`（关键词频次）、`backend/agents/simulator.py`

### 4.7 `pipeline_run`（批次）

- 主键：`id`
- 外键：`project_id -> monitor_project.id`，`project_platform_id -> monitor_project_platform.id`（可空）
- 关键字段：`trigger_type,status,start_time,end_time,params,brand_ids_snapshot,keyword_ids_snapshot,max_posts_per_run,sentiment_model`
- 注意：`Unnamed: 13` 是 Excel 空表头带来的“脏列”，代码未使用。
- 写入：`backend/agents/pipeline.py`、`backend/agents/importer.py`、`backend/agents/simulator.py`

### 4.8 `post_raw`（原始帖子事实表）

- 主键：`id`
- 外键：`pipeline_run_id -> pipeline_run.id`，`project_id -> monitor_project.id`，`platform_id -> platform.id`，`keyword_id -> monitor_keyword.id`
- 关键字段：`publish_time,raw_text,like_count,comment_count,share_count,platform_post_id`
- 写入：`crawler/importer/simulator`
- 读取：`posts/dashboard/reports/pipeline`

### 4.9 `post_clean`（清洗帖子）

- 主键：`id`
- 外键：`post_raw_id -> post_raw.id`
- 关键字段：`clean_text,text_hash,is_valid,invalid_reason`
- 写入：`backend/agents/pipeline.py`
- 读取：`backend/routers/posts.py`、`backend/routers/reports.py`、`backend/agents/reporter.py`

### 4.10 `spam_score`（反垃圾结果）

- 主键：`id`
- 外键：`post_clean_id -> post_clean.id`
- 关键字段：`score_total,label,rule_hits`
- 写入：`backend/agents/pipeline.py`
- 读取：`backend/routers/posts.py`、`backend/routers/moderation.py`、`backend/routers/reports.py`

### 4.11 `sentiment_result`（情绪结果）

- 主键：`id`
- 外键：`post_clean_id -> post_clean.id`
- 关键字段：`polarity,confidence,intensity,emotions`
- 写入：`backend/agents/analyzer.py`
- 读取：`dashboard/posts/reports/reporter`

### 4.12 `topic_result`（主题结果）

- 主键：`id`
- 外键：`post_clean_id -> post_clean.id`
- 关键字段：`topic_name,score`
- 写入/读取：`backend/agents/pipeline.py` 生成；`backend/routers/posts.py` 与 `backend/routers/reports.py` 使用。

### 4.13 `entity_result`（实体/功能点结果）

- 主键：`id`
- 外键：`post_clean_id -> post_clean.id`
- 关键字段：`entity_type,entity_text,normalized,confidence`
- 写入/读取：同上。

### 4.14 `daily_metric`（日聚合指标）

- 主键：`id`
- 外键：`project_id -> monitor_project.id`，`brand_id -> brand.id`（可空，旧数据会用 project→brand 映射补）
- 关键字段：`metric_date,total_posts,valid_posts,spam_posts,pos_posts,neu_posts,neg_posts`
- 写入：`backend/agents/pipeline.py`
- 读取：`backend/routers/dashboard.py`、`backend/routers/reports.py`、`backend/agents/reporter.py`

### 4.15 `report`（报告）

- 主键：`id`
- 外键：`pipeline_run_id -> pipeline_run.id`（可空；自定义报告可能没有 run）
- 关键字段：`title,report_type,summary,content_json,created_at,time_start,time_end,range_from,range_to,status,trigger_type`
- 写入：`backend/agents/pipeline.py`（自动日报）与 `backend/routers/reports.py`（报告中心）

> 备注：`backend/routers/reports.py:_ensure_report_sheets` 会在运行时保证 `report` 表包含更多可选列（如 `template_code/chart_json/file_pdf_url/file_docx_url/file_pptx_url` 等），并做 `time_*` 与 `range_*` 的互相回填；因此该 Sheet 的列集合可能会随着首次访问 `/api/reports` 而扩展。

### 4.16 `report_config`（报告生成参数快照）

当前工作簿为空表；代码会在首次访问时用 `_ensure_report_sheets` 补齐列并写回：

- `id,report_id,project_id,time_start,time_end,platform_ids,brand_ids,keyword_ids,include_sections,config_json,created_at`

### 4.17 `report_citation`（报告引用/证据）

当前工作簿为空表；代码会补齐列并兼容历史字段别名：

- `id,report_id,citation_type,section_code,quote_text,sort_order,post_raw_id,reason,created_at`
- 兼容列：`section`（旧字段）→ `section_code`；`quote`（旧字段）→ `quote_text`

### 4.18 `datasource_config`（平台默认配置）

- 预期用途（`backend/routers/projects.py:_seed_default_project_platforms`）：按 `platform.code` 提供默认值
  - `id`：平台 code（字符串）
  - `default_cron_expr`：默认 cron
  - `default_max_posts_per_run`：默认每次抓取量
  - `is_global_enabled`：全局开关（当前未严格使用）
  - `updated_at`
- 注意：`backend/routers/settings.py` 目前把它当作“频率配置表”覆盖写为 `id,freq,updated_at`（如果启用设置页，需要先统一该表的 schema）。

### 4.19 `meta`（系统元信息）

- `key,value,updated_at`
- 当前用法：`key=next_project_id`（见 `backend/routers/projects.py`）。

---

## 5. 代码文件职责（按目录/文件）

### 5.1 Backend

- `backend/main.py`：FastAPI 应用装配；挂载所有 router；启动/停止 scheduler。
- `backend/storage/db.py`：ExcelRepository（读写 Excel 工作簿）与 `get_latest_pipeline_run_id`。
- `backend/tasks/scheduler.py`：最小 cron 调度器；扫描 `monitor_project_platform` 到期任务；触发 `run_simulated_crawl` 并回写 `last_run_at/last_success_at`。
- `backend/routers/auth.py`：JWT 登录/注册/鉴权（用户不存 Excel，见 `backend/storage/users.py`）。
- `backend/routers/dashboard.py`：仪表盘 options/KPI/趋势/排名/告警/关键词频次；手动刷新入口。
- `backend/routers/posts.py`：帖子分页、过滤、统计；把 `post_raw` enrich 成带情绪/垃圾/主题/实体等字段的“宽表”。
- `backend/routers/projects.py`：项目管理（含启用限制、默认平台配置播种、删除级联清理）。
- `backend/routers/brands.py`：品牌管理（增删查）；删除前做引用检查。
- `backend/routers/sources.py`：平台（source）列表与导入 Excel 入口（触发后处理）。
- `backend/routers/analysis.py`：手动触发 pipeline（mock crawl → 后处理）。
- `backend/routers/report.py`：报告摘要（取最新一条 report）。
- `backend/routers/reports.py`：报告中心（列表/详情/引用/导出/自定义生成）；负责 `_ensure_report_sheets` 补列与兼容旧字段。
- `backend/routers/moderation.py`：反垃圾概览与列表（当前前端未接入）。
- `backend/agents/crawler.py`：mock crawler（按平台 code 生成 `post_raw`）。
- `backend/agents/importer.py`：从外部 Excel 的 `post_raw_upload` 导入到 `post_raw`；必要时补录 `monitor_keyword`；写入 `pipeline_run`。
- `backend/agents/analyzer.py`：情绪分析（rule-based 或 dashscope LLM）；写 `sentiment_result`。
- `backend/agents/pipeline.py`：核心流水线（clean/spam/sentiment/topic/entity/metrics/report）；贯穿写入事实表与派生表。
- `backend/agents/simulator.py`：模拟抓取（基于项目配置批量生成 `pipeline_run`+`post_raw`，然后调用 `process_existing_run`）。
- `backend/agents/reporter.py`：基于 `report/daily_metric` 构建 HTML/PDF 报告（PDF 需 playwright）。
- `backend/agents/filterer.py`：兼容 wrapper（调用 `clean_posts_for_run`）。

### 5.2 Frontend（与数据表相关的页面）

- `frontend/src/views/Projects.vue`：项目/品牌管理与平台配置；对接 `monitor_project* / platform / brand / datasource_config(meta)` 等。
- `frontend/src/views/Dashboard.vue`：仪表盘；主要对接 `daily_metric` + 若干维表；手动刷新触发写入。
- `frontend/src/components/RawPostList.vue`：帖子列表；读取 `post_raw` 并通过后端 enrich 得到 `post_clean/sentiment/spam/topic/entity` 等派生字段。
- `frontend/src/views/Reports.vue`：报告中心；读取 `report/report_config/report_citation`，并关联 `post_raw` 原文展示证据。
- `frontend/src/views/DebugConsole.vue`：调试入口（sources 列表、导入、report 摘要）。
