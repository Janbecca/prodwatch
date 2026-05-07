# ProdWatch

ProdWatch 是一个“竞品舆情监测”演示系统，前端为 Vue3 + Element Plus，后端为 FastAPI + SQLite。
当前代码支持两种数据来源路径：
- `LLM毕设演示模式`：主要依赖大模型生成/分析，便于快速演示全链路。
- `真实 MediaCrawler模式`：后端通过 HTTP 调用外部 MediaCrawler 服务抓取真实数据。

---

## 1. 项目启动

### 1.1 环境要求
- Python 3.10+
- Node.js 18+
- Windows PowerShell（本文命令按 Windows 写）
### 1.2 后端启动
在项目根目录执行：
```powershell
# 1) 激活虚拟环境（若已创建）
.\.venv\Scripts\Activate.ps1
# 2) 安装依赖
pip install -r backend/requirements.txt
# 3) 启动后端
.\.venv\Scripts\python.exe -m uvicorn backend.api.app:app --reload
```
后端默认地址：`http://127.0.0.1:8000`
Swagger：`http://127.0.0.1:8000/docs`

### 1.3 前端启动
新开一个终端，在项目根目录执行：

```powershell
cd frontend
npm install
npm run dev
```

前端默认地址（Vite）：`http://127.0.0.1:5173`（端口以实际输出为准）


## 2. 两种运行模式：LLM 演示 vs 真实 MediaCrawler

## 2.1 LLM 演示模式
目标：不依赖外部爬虫服务，仅通过LLM作模拟演示，仅供毕设使用，目的是展示项目核心链路。

1. 配置好 LLM API Key（见.env文件）。
2. 不要额外启动MediaCrawler项目
3. 正常启动前后端即可。

说明：
- `mock_llm` 下，帖子生成优先走 LLM 生成逻辑；后续分析/报告同样走 LLM 任务路由。
- 如果某任务 provider/model 没配好，接口会返回明确报错（如 API key not configured）。

## 2.2 真实 MediaCrawler 模式
目标：通过外部 MediaCrawler WebUI API 抓取真实数据。

### 步骤 A：启动 MediaCrawler 服务（外部项目）
在你的 MediaCrawler 项目目录执行（示例端口 `8080`）：
```powershell
uvicorn api.main:app --port 8080 --reload
```

### 步骤 B：配置 ProdWatch 指向 MediaCrawler
在 `.env` 中配置：

```env
PRODWATCH_MEDIACRAWLER_BASE_URL=http://127.0.0.1:8080
PRODWATCH_MEDIACRAWLER_TIMEOUT_S=30
PRODWATCH_MEDIACRAWLER_POLL_TIMEOUT_S=120
PRODWATCH_MEDIACRAWLER_POLL_INTERVAL_MS=1000
PRODWATCH_MEDIACRAWLER_LOGIN_TYPE=qrcode
PRODWATCH_MEDIACRAWLER_COOKIES=
PRODWATCH_MEDIACRAWLER_HEADLESS=true
PRODWATCH_MEDIACRAWLER_ENABLE_COMMENTS=false
PRODWATCH_MEDIACRAWLER_ENABLE_SUB_COMMENTS=false
PRODWATCH_MEDIACRAWLER_SAVE_OPTION=json
```
前端可设置：

```env
VITE_MANUAL_REFRESH_CRAWL_SOURCE=media_crawler
```

### 步骤 C：在系统中打开MediaCrawler
1. Github项目地址：https://github.com/NanmiCoder/MediaCrawler.git
2. 拉取到本地并跑通
3. 确保跑通的外部项目前端地址与PRODWATCH_MEDIACRAWLER_BASE_URL配置一致

---
