from bs4 import BeautifulSoup
from backend.models.db import db_session
from backend.models.schema import CleanPost
from simhash import Simhash
from backend.models.schema import RawPost


# 定义过滤智能体，去除HTML标签，去重
def filter_posts():
    raw_posts = db_session.query(RawPost).all()

    for post in raw_posts:
        # 使用BeautifulSoup去除HTML标签
        soup = BeautifulSoup(post.content_raw, 'html.parser')
        cleaned_content = soup.get_text()

        # 生成Simhash值进行去重
        simhash_value = Simhash(cleaned_content).value

        # 将清洗后的内容存入数据库
        clean_post = CleanPost(
            raw_id=post.id,
            content=cleaned_content,
            simhash=simhash_value,
            dedup_group_id=simhash_value % 10  # 假设按照 Simhash 生成的值进行分组去重
        )

        db_session.add(clean_post)
        db_session.commit()

    print("Filtering completed!")
