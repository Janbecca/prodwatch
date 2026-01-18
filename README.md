# ProdWatch
## 课题要求
本课题要求设计并实现一个基于多智能体协同架构的竞品舆情监控系统，通过爬虫智能体、过滤智能体、分析智能体与报告智能体的协作，完成跨平台数据采集、水军识别、情感解构与战略报告生成等核心功能，为竞品监控全流程（数据获取→清洗→分析→决策）提供自动化、智能化的支持，显著提升企业市场洞察精度、竞争响应速度与商业决策质量。

## 项目启动方式
### 进入根目录

### 激活虚拟环境：
`.\.venv\Scripts\Activate.ps1`
### 启动后端：
1. `pip install -r backend/requirements.txt`
2. `uvicorn backend.main:app --reload`
3. Swagger 文档：`http://127.0.0.1:8000/docs`
### 启动前端：
1. `cd frontend`
2. `npm install`
3. `npm run dev`
### 数据库planA
平台：Supabase
Project name：ProdWatch
Database password:ZM126.com123


## 开发计划

### V0.1-- 最小可运行版本
1. 系统场景：监控萤石摄像头在微博的舆情
2. 数据来源：微博搜索接口 / 爬虫模拟搜索页
3. 核心功能：展示舆情趋势 + 可一键生成日报/周报
4. 技术栈：
后端：Python + FastAPI 或 Node.js + Express
数据库：MySQL / PostgreSQL（二选一就好）
前端：React + Ant Design / Tailwind


