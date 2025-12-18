import feedparser
from backend.models.db import db_session
from backend.models.schema import RawPost
from datetime import datetime

# 定义爬虫智能体，用于抓取RSS源
def crawl():
    rss_url = "https://example.com/rss"  # 替换为你要抓取的RSS链接
    feed = feedparser.parse(rss_url)
    
    for entry in feed.entries:
        # 只保存标题、链接和发布时间，其他可以根据需要提取
        post = RawPost(
            platform="RSS",
            url=entry.link,
            title=entry.title,
            content_raw=entry.summary,
            author=entry.author if 'author' in entry else 'Unknown',
            post_time=datetime(*entry.published_parsed[:6]),
            fetched_time=datetime.now()
        )
        
        # 将抓取到的文章存入数据库
        db_session.add(post)
        db_session.commit()

    print("Crawl completed!")
