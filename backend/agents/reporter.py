from jinja2 import Environment, FileSystemLoader
from playwright.sync_api import sync_playwright
from backend.models.db import db_session
from backend.models.schema import Analysis
import os

# 渲染HTML模板
def generate_report():
    analysis_results = db_session.query(Analysis).all()

    env = Environment(loader=FileSystemLoader('backend/templates'))
    template = env.get_template('report.html.j2')

    html_content = template.render(analysis_results=analysis_results)

    # 使用Playwright生成PDF
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.set_content(html_content)
        pdf_path = os.path.join('reports', 'daily_report.pdf')
        page.pdf(path=pdf_path, format="A4")
        browser.close()

    print("Report generated!")

# 定义报告智能体
def report():
    generate_report()
