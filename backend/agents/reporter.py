from jinja2 import Environment, FileSystemLoader
from playwright.sync_api import sync_playwright
from backend.storage.db import get_repo
import os


def generate_report():
    repo = get_repo()
    try:
        analysis_results = repo.query("sentiment_result").to_dict(orient="records")
    except Exception:
        analysis_results = []

    env = Environment(loader=FileSystemLoader("backend/templates"))
    template = env.get_template("report.html.j2")

    html_content = template.render(analysis_results=analysis_results)

    os.makedirs("reports", exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.set_content(html_content)
        pdf_path = os.path.join("reports", "daily_report.pdf")
        page.pdf(path=pdf_path, format="A4")
        browser.close()

    return pdf_path


def report():
    return generate_report()
