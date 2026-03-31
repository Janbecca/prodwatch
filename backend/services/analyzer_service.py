from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence


@dataclass(frozen=True)
class PostInput:
    """
    Minimal post representation for analysis.

    Note:
    - Keep it decoupled from SQLite row types so it can be reused by future async workers / LLM analyzers.
    """

    post_id: int
    project_id: int
    platform_id: Optional[int]
    brand_id: Optional[int]
    title: str
    content: str

    @property
    def text(self) -> str:
        t = (self.title or "").strip()
        c = (self.content or "").strip()
        return (t + "\n" + c).strip()


@dataclass(frozen=True)
class ProjectKeyword:
    keyword: str
    keyword_type: Optional[str] = None
    weight: Optional[int] = None
    is_enabled: int = 1


@dataclass(frozen=True)
class CleanPostResult:
    clean_text: str
    is_valid: int
    invalid_reason: Optional[str]
    language: str


@dataclass(frozen=True)
class SentimentResult:
    sentiment: str  # positive|neutral|negative
    sentiment_score: float  # [-1,1]
    emotion_intensity: float  # [0,1]
    model_version: str


@dataclass(frozen=True)
class KeywordHit:
    keyword: str
    keyword_type: Optional[str]
    confidence: float
    source: str  # for future: "rule", "llm", "hybrid"


@dataclass(frozen=True)
class FeatureHit:
    feature_name: str
    feature_sentiment: str  # positive|neutral|negative
    confidence: float
    source: str


@dataclass(frozen=True)
class SpamResult:
    spam_label: str  # spam|normal
    spam_score: float  # [0,1]
    model_version: str


class AnalyzerService:
    """
    Analyzer abstraction.

    First version uses mock/rule-based logic. Future versions can wrap real LLM calls
    while keeping the same IO contracts for pipeline writes.
    """

    def clean_post(self, post: PostInput) -> CleanPostResult:  # pragma: no cover - interface
        raise NotImplementedError

    def analyze_sentiment(self, post: PostInput) -> SentimentResult:  # pragma: no cover - interface
        raise NotImplementedError

    def extract_keywords(self, post: PostInput, project_keywords: Sequence[ProjectKeyword]) -> List[KeywordHit]:
        raise NotImplementedError

    def extract_features(self, post: PostInput) -> List[FeatureHit]:  # pragma: no cover - interface
        raise NotImplementedError

    def detect_spam(self, post: PostInput) -> SpamResult:  # pragma: no cover - interface
        raise NotImplementedError


def _detect_language(text: str) -> str:
    if any("\u4e00" <= ch <= "\u9fff" for ch in text):
        return "zh"
    return "en"


def _uniq_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for v in values:
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


class MockRuleAnalyzerService(AnalyzerService):
    """
    Deterministic rule-based analyzer.

    Design goals:
    - Stable output for demo/dev seeds (so aggregation is reproducible)
    - Structured results that map 1:1 to DB tables
    """

    DEFAULT_FEATURE_TERMS = [
        "battery",
        "camera",
        "price",
        "support",
        "performance",
        "design",
        "lag",
        "overheat",
    ]

    def __init__(self, *, feature_terms: Optional[Sequence[str]] = None):
        self._model_version = "mock-v1"
        self._feature_terms = _uniq_keep_order(feature_terms or [])

    @classmethod
    def for_project(cls, project_keywords: Sequence[ProjectKeyword]) -> "MockRuleAnalyzerService":
        feature_terms = []
        for kw in project_keywords:
            if (kw.keyword_type or "").strip().lower() == "feature" and kw.keyword:
                feature_terms.append(str(kw.keyword))
        # Ensure default terms exist (used by aggregation/feature charts).
        feature_terms.extend([t for t in cls.DEFAULT_FEATURE_TERMS if t not in feature_terms])
        return cls(feature_terms=feature_terms)

    def clean_post(self, post: PostInput) -> CleanPostResult:
        text = (post.text or "").strip()
        is_valid = 1 if text else 0
        invalid_reason = None if is_valid else "empty"
        language = _detect_language(text)
        return CleanPostResult(clean_text=text, is_valid=is_valid, invalid_reason=invalid_reason, language=language)

    def analyze_sentiment(self, post: PostInput) -> SentimentResult:
        text = (post.text or "").lower()
        positive_terms = ["good", "love", "recommend", "satisfied", "great", "awesome"]
        negative_terms = ["bad", "disappointed", "trash", "lag", "overheat", "refund"]
        score = 0.0
        for t in positive_terms:
            if t in text:
                score += 0.2
        for t in negative_terms:
            if t in text:
                score -= 0.2
        score = max(-1.0, min(1.0, score))
        if score > 0.1:
            sentiment = "positive"
        elif score < -0.1:
            sentiment = "negative"
        else:
            sentiment = "neutral"
        return SentimentResult(
            sentiment=sentiment,
            sentiment_score=score,
            emotion_intensity=abs(score),
            model_version=self._model_version,
        )

    def extract_keywords(self, post: PostInput, project_keywords: Sequence[ProjectKeyword]) -> List[KeywordHit]:
        text = post.text
        hits: List[KeywordHit] = []
        for kw in project_keywords:
            if not kw.keyword:
                continue
            if kw.keyword in text:
                hits.append(
                    KeywordHit(
                        keyword=str(kw.keyword),
                        keyword_type=kw.keyword_type,
                        confidence=0.9,
                        source="rule",
                    )
                )
        return hits

    def extract_features(self, post: PostInput) -> List[FeatureHit]:
        text = (post.text or "").lower()
        terms = self._feature_terms or self.DEFAULT_FEATURE_TERMS
        hits: List[FeatureHit] = []
        for f in terms:
            if not f:
                continue
            if str(f).lower() not in text:
                continue
            feature_sentiment = "neutral"
            if any(t in text for t in ["bad", "disappointed", "lag", "overheat"]):
                feature_sentiment = "negative"
            elif any(t in text for t in ["good", "recommend", "satisfied", "great"]):
                feature_sentiment = "positive"
            hits.append(
                FeatureHit(
                    feature_name=str(f),
                    feature_sentiment=feature_sentiment,
                    confidence=0.8,
                    source="rule",
                )
            )
        return hits

    def detect_spam(self, post: PostInput) -> SpamResult:
        text = (post.text or "").lower()
        spam_terms = ["free", "discount", "referral", "dm me", "scan qr", "add me"]
        hit = any(t in text for t in spam_terms)
        spam_label = "spam" if hit else "normal"
        spam_score = 0.9 if hit else 0.1
        return SpamResult(spam_label=spam_label, spam_score=spam_score, model_version=self._model_version)
