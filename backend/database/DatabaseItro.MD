# 数据库设计说明（ProdWatch 舆情监控系统）

## 1. 概述

本系统用于实现竞品舆情监控的完整业务链路，包括：

- 新建项目
- 配置监控对象（品牌、平台）
- 启用项目
- 触发模拟爬虫任务
- 写入舆情数据（帖子）
- 数据统计与展示（Dashboard / Posts / Reports）

数据库围绕 **监控项目（monitor_project）** 作为核心实体设计，采用关系型结构进行建模。

---

## 2. 数据表总览

| 表名 | 说明 |
|------|------|
| `brand` | 品牌表 |
| `platform` | 平台表 |
| `monitor_project` | 监控项目表（核心表） |
| `monitor_project_brand` | 项目-品牌关联表 |
| `monitor_project_platform` | 项目-平台关联表 |
| `claw_job` | 爬虫任务表 |
| `posts` | 帖子数据表（核心事实表） |
| `daily_metric` | 每日统计表 |

---

## 3. 表结构说明

---

### 3.1 brand（品牌表）

用于存储系统中涉及的品牌信息。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键，自增 |
| name | VARCHAR(255) | 品牌名称 |
| created_at | DATETIME | 创建时间 |

---

### 3.2 platform（平台表）

用于存储舆情数据来源平台。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键，自增 |
| name | VARCHAR(255) | 平台名称（如微博、小红书） |
| created_at | DATETIME | 创建时间 |

---

### 3.3 monitor_project（监控项目表）

系统核心表，表示一个舆情监控任务。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键，自增 |
| name | VARCHAR(255) | 项目名称 |
| product_category | VARCHAR(255) | 监控品类（如摄像头） |
| description | VARCHAR(255) | 项目描述 |
| is_active | BOOLEAN | 是否启用 |
| our_brand_name | VARCHAR(255) | 我方品牌（可选） |
| status | VARCHAR(255) | 项目状态 |
| created_at | DATETIME | 创建时间 |
| updated_at | DATETIME | 更新时间 |

---

### 3.4 monitor_project_brand（项目-品牌关联表）

用于实现项目与品牌的多对多关系。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键 |
| project_id | INTEGER | 项目ID |
| brand_id | INTEGER | 品牌ID |
| created_at | DATETIME | 创建时间 |

---

### 3.5 monitor_project_platform（项目-平台关联表）

用于实现项目与平台的多对多关系。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键 |
| project_id | INTEGER | 项目ID |
| platform_id | INTEGER | 平台ID |
| created_at | DATETIME | 创建时间 |

---

### 3.6 claw_job（爬虫任务表）

用于记录每次数据采集任务的执行情况。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键 |
| project_id | INTEGER | 所属项目 |
| trigger_type | VARCHAR(255) | 触发方式（manual / scheduled） |
| job_status | VARCHAR(255) | 状态（pending / running / success / failed） |
| started_at | TIMESTAMP | 开始时间 |
| finished_at | TIMESTAMP | 结束时间 |
| created_post_count | INTEGER | 新增帖子数量 |
| error_message | VARCHAR(255) | 错误信息 |

---

### 3.7 posts（帖子表）

系统核心数据表，存储舆情帖子及分析结果。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键 |
| project_id | INTEGER | 项目ID |
| claw_job_id | INTEGER | 任务ID |
| title | VARCHAR(255) | 标题 |
| content | VARCHAR(255) | 内容 |
| post_url | VARCHAR(255) | 原帖链接 |
| publish_time | TIME | 发布时间 |
| crawled_at | TIMESTAMP | 抓取时间 |
| keyword_hit | VARCHAR(255) | 命中关键词 |
| sentiment | VARCHAR(255) | 情感（positive / neutral / negative） |
| sentiment_score | VARCHAR(255) | 情感评分 |
| like_count | INTEGER | 点赞数 |
| comment_count | INTEGER | 评论数 |
| share_count | INTEGER | 转发数 |
| view_count | INTEGER | 浏览量 |

---

### 3.8 daily_metric（每日统计表）

用于存储聚合后的统计数据。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键 |
| project_id | INTEGER | 项目ID |
| stat_date | DATE | 日期 |
| total_post_count | INTEGER | 总帖子数 |
| positive_count | INTEGER | 正向数 |
| neutral_count | INTEGER | 中性数 |
| negative_count | INTEGER | 负向数 |
| source_breakdown | JSON | 按平台分布 |

---

## 4. 表关系设计

### 4.1 项目与品牌

- 关系：多对多
- 实现表：`monitor_project_brand`

---

### 4.2 项目与平台

- 关系：多对多
- 实现表：`monitor_project_platform`

---

### 4.3 项目与爬虫任务

- 关系：一对多
monitor_project.id → claw_job.project_id


---

### 4.4 项目与帖子

- 关系：一对多


monitor_project.id → posts.project_id


---

### 4.5 爬虫任务与帖子

- 关系：一对多


claw_job.id → posts.claw_job_id


---

### 4.6 项目与每日统计

- 关系：一对多


monitor_project.id → daily_metric.project_id


---

## 5. 数据流说明（核心业务链路）

系统完整数据流如下：


新建项目
↓
配置品牌 & 平台
↓
启用项目
↓
触发爬虫任务（claw_job）
↓
写入帖子数据（posts）
↓
统计分析（daily_metric）
↓
Dashboard / Posts / Reports 展示


---

## 6. 设计特点

### 6.1 以项目为核心

所有数据围绕 `monitor_project` 组织，便于隔离不同监控任务。

---

### 6.2 多对多解耦设计

通过中间表实现：

- 项目 ↔ 品牌
- 项目 ↔ 平台

提升扩展性。

---

### 6.3 原始数据 + 聚合数据分离

- `posts`：原始数据
- `daily_metric`：统计数据

提高查询效率。

---

### 6.4 支持任务追踪

通过 `claw_job`：

- 记录每次采集
- 支持失败追踪
- 支持统计来源

---

## 7. 后续优化方向（论文可用）

- 使用 MySQL 替代 Excel 提升性能
- 引入索引优化查询效率
- 增加用户表实现多用户隔离
- 增加任务调度系统（如定时任务）
- 支持更多平台与数据源扩展

---