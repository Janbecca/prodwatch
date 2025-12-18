from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from backend.agents.crawler import crawl
from backend.agents.filterer import filter_posts
from backend.agents.analyzer import analyze
from backend.agents.reporter import report
import sqlite3

# 创建 FastAPI 应用实例
app = FastAPI()

# 定义定时任务调度器
def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(crawl, 'interval', minutes=30)
    scheduler.add_job(filter_posts, 'interval', minutes=30)
    scheduler.add_job(analyze, 'interval', minutes=30)
    scheduler.add_job(report, 'interval', hours=1)
    scheduler.start()

# 启动定时任务调度器
start_scheduler()

# 示例路由：获取数据
@app.get("/posts")
def get_posts():
    # 返回从数据库查询到的爬虫数据
    return {"message": "Here are the posts!"}

# 根路由
@app.get("/")
def read_root():
    return {"message": "Hello World"}
