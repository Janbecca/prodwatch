from backend.models.db import db_session
from backend.models.schema import CleanPost, Analysis
from datetime import datetime

# 简单情感分析（示例：正面、负面、无情感）
def sentiment_analysis(content):
    positive_words = ['good', 'excellent', 'happy']
    negative_words = ['bad', 'terrible', 'sad']

    score = 0
    for word in positive_words:
        if word in content:
            score += 1
    for word in negative_words:
        if word in content:
            score -= 1

    if score > 0:
        return 1  # Positive
    elif score < 0:
        return -1  # Negative
    else:
        return 0  # Neutral

# 简单水军检测（示例：频繁发帖、相同内容）
def bot_detection(content, post_time):
    # 检测频繁发帖和相同内容（简单示例）
    # 如果内容相似且发帖频率过高，认为是水军
    # 可以用Simhash对比或者检查短时间内相同内容
    return False  # 示例暂时返回False，后续可以加入逻辑

# 定义分析智能体
def analyze():
    clean_posts = db_session.query(CleanPost).all()

    for post in clean_posts:
        sentiment = sentiment_analysis(post.content)
        is_bot = bot_detection(post.content, post.fetched_time)

        analysis = Analysis(
            clean_id=post.id,
            sentiment=sentiment,
            bot_score=100 if is_bot else 0,
            created_at=datetime.now()
        )
        
        db_session.add(analysis)
        db_session.commit()

    print("Analysis completed!")
