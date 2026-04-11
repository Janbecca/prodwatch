from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from backend.llm.router import get_llm_router
from backend.services.analyzer_service import _normalize_topic_text


@dataclass(frozen=True)
class KeywordHitResult:
    """
    Stage1 output: deterministic monitoring keyword hits.
    """

    hits: list[dict[str, str]]  # [{keyword, matched_text}]

    @property
    def keywords(self) -> list[str]:
        out: list[str] = []
        for it in self.hits or []:
            if not isinstance(it, dict):
                continue
            k = str(it.get("keyword") or "").strip()
            if k:
                out.append(k)
        return out


@dataclass(frozen=True)
class PostAnalysisResult:
    """
    Stage2 output: open semantic extraction (LLM).
    """

    entities: list[dict[str, Any]]
    features: list[dict[str, Any]]
    issues: list[dict[str, Any]]
    scenarios: list[dict[str, Any]]
    sentiment_targets: list[dict[str, Any]]
    raw_keywords: list[dict[str, Any]]
    topics: list[str]
    sentiment: Optional[str] = None
    sentiment_score: Optional[float] = None
    emotion_intensity: Optional[float] = None
    spam_label: Optional[str] = None
    spam_score: Optional[float] = None
    meta: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "entities": self.entities,
            "features": self.features,
            "issues": self.issues,
            "scenarios": self.scenarios,
            "sentiment_targets": self.sentiment_targets,
            "raw_keywords": self.raw_keywords,
            "topics": self.topics,
            "sentiment": self.sentiment,
            "sentiment_score": self.sentiment_score,
            "emotion_intensity": self.emotion_intensity,
            "spam_label": self.spam_label,
            "spam_score": self.spam_score,
            "meta": self.meta or {},
        }


@dataclass(frozen=True)
class TopicNormalizationResult:
    """
    Stage3 output: normalized topics + optional mapping.
    """

    topics: list[dict[str, Any]]  # [{topic, confidence, aliases?}]
    normalization_map: list[dict[str, Any]]  # [{raw, topic, method}]

    def to_dict(self) -> dict[str, Any]:
        return {"topics": self.topics, "normalization_map": self.normalization_map}


_ASCII_WORD_RE_CACHE: dict[str, re.Pattern] = {}


def _ascii_word_pattern(word: str) -> re.Pattern:
    k = word.lower()
    pat = _ASCII_WORD_RE_CACHE.get(k)
    if pat is not None:
        return pat
    # word-boundary match for latin terms; tolerate punctuation around.
    pat = re.compile(rf"(?i)(?<![a-z0-9]){re.escape(word)}(?![a-z0-9])")
    _ASCII_WORD_RE_CACHE[k] = pat
    return pat


class KeywordHitMatcher:
    """
    High-performance deterministic matcher for monitoring keywords.

    Design:
    - RULE ONLY (no LLM).
    - Exact match (substring for CJK; word-boundary for ASCII-ish).
    - Optional synonym expansion (maps project keyword -> list of synonym phrases).
    - Returns hit records: [{keyword, matched_text}], where keyword is the canonical project keyword.

    Tips for large-scale usage:
    - Instantiate once per project/scope, then call .match(text) for each post.
    """

    def __init__(
        self,
        *,
        project_keywords: Sequence[str],
        synonyms: Optional[dict[str, Sequence[str]]] = None,
        enable_synonyms: bool = True,
    ):
        kws = []
        seen = set()
        for kw in project_keywords or []:
            k = str(kw or "").strip()
            if not k or k in seen:
                continue
            seen.add(k)
            kws.append(k)
        self.project_keywords = kws
        self.synonyms = synonyms or {}
        self.enable_synonyms = bool(enable_synonyms)

        # Expand candidates: list of (canonical_keyword, candidate_text)
        candidates: list[tuple[str, str]] = []
        for k in self.project_keywords:
            candidates.append((k, k))
            if self.enable_synonyms:
                for s in (self.synonyms.get(k) or []):
                    ss = str(s or "").strip()
                    if ss and ss != k:
                        candidates.append((k, ss))

        # Split into ASCII-ish vs others
        self._ascii: list[tuple[str, str, re.Pattern]] = []
        self._cjk: list[tuple[str, str]] = []
        for canon, cand in candidates:
            try:
                ascii_ratio = sum(1 for ch in cand if ord(ch) < 128) / max(1, len(cand))
            except Exception:
                ascii_ratio = 0.0
            if ascii_ratio >= 0.8:
                self._ascii.append((canon, cand, _ascii_word_pattern(cand)))
            else:
                self._cjk.append((canon, cand))

        # Optional: compile a single alternation regex for non-ascii candidates when size is reasonable.
        self._cjk_re: Optional[re.Pattern] = None
        self._cjk_re_map: dict[str, str] = {}
        try:
            uniq_cands = []
            seen_c = set()
            total_len = 0
            for canon, cand in self._cjk:
                if cand in seen_c:
                    continue
                seen_c.add(cand)
                uniq_cands.append(cand)
                self._cjk_re_map[cand] = canon
                total_len += len(cand)
            # Guard: avoid enormous regex
            if uniq_cands and len(uniq_cands) <= 300 and total_len <= 6000:
                # Longer first to prefer more specific phrases.
                uniq_cands.sort(key=len, reverse=True)
                self._cjk_re = re.compile("|".join(re.escape(x) for x in uniq_cands))
        except Exception:
            self._cjk_re = None
            self._cjk_re_map = {}

    def match(self, text: str) -> list[dict[str, str]]:
        t = str(text or "")
        if not t or (not self._ascii and not self._cjk):
            return []

        hits: list[dict[str, str]] = []
        seen_kw = set()

        # 1) Non-ascii candidates: single-pass regex when available, else substring scan.
        if self._cjk_re is not None:
            for m in self._cjk_re.finditer(t):
                matched = m.group(0)
                canon = self._cjk_re_map.get(matched)
                if not canon or canon in seen_kw:
                    continue
                seen_kw.add(canon)
                hits.append({"keyword": canon, "matched_text": matched})
        else:
            # Fallback: check candidates; prioritize longer phrases first.
            cands = sorted(self._cjk, key=lambda x: len(x[1]), reverse=True)
            for canon, cand in cands:
                if canon in seen_kw:
                    continue
                if cand and (cand in t):
                    seen_kw.add(canon)
                    hits.append({"keyword": canon, "matched_text": cand})

        # 2) ASCII-ish: boundary regex
        for canon, cand, pat in self._ascii:
            if canon in seen_kw:
                continue
            try:
                m = pat.search(t)
            except Exception:
                m = None
            if not m:
                continue
            seen_kw.add(canon)
            hits.append({"keyword": canon, "matched_text": m.group(0) if hasattr(m, "group") else cand})

        return hits


def keyword_hit(
    *,
    post_text: str,
    project_keywords: Sequence[str],
    synonyms: Optional[dict[str, Sequence[str]]] = None,
    enable_synonyms: bool = True,
) -> KeywordHitResult:
    """
    Stage1 keyword_hit (RULE ONLY, performance-first).

    Input:
    - post_text: raw text (title+content, etc.)
    - project_keywords: user configured monitoring keywords (can be empty)
    - synonyms: optional mapping {keyword: [synonym1, synonym2]}

    Output:
    - KeywordHitResult.hits: [{keyword, matched_text}]
    """
    matcher = KeywordHitMatcher(
        project_keywords=project_keywords,
        synonyms=synonyms,
        enable_synonyms=enable_synonyms,
    )
    return KeywordHitResult(hits=matcher.match(str(post_text or "")))


def keyword_extraction_llm(
    *,
    text: str,
    project_keywords: Sequence[str],
    con=None,
    crawl_job_id: int | None = None,
) -> list[dict[str, Any]]:
    """
    Semantic keyword hits against user-configured monitoring words (LLM REQUIRED).

    Output items look like:
      {keyword, confidence, evidence?, keyword_type?}
    """
    kws = [str(k or "").strip() for k in (project_keywords or []) if str(k or "").strip() != ""]
    if not kws:
        return []

    payload = {"text": str(text or ""), "project_keywords": kws}
    res = get_llm_router().run(task_type="keyword_extraction", input=payload, con=con, strict=True)
    try:
        if crawl_job_id is not None:
            from backend.llm.schema_log import log_llm_schema

            log_llm_schema(con, crawl_job_id=int(crawl_job_id), task_type="keyword_extraction", res=res)
    except Exception:
        pass
    if not bool(res.ok):
        raise RuntimeError(f"keyword_extraction failed: {res.error or 'unknown error'}")
    out = res.output if isinstance(res.output, dict) else {}
    items = out.get("hits")
    if not isinstance(items, list):
        return []

    allowed = set(kws)
    hits: list[dict[str, Any]] = []
    seen = set()
    for it in items:
        if isinstance(it, str):
            kw = it.strip()
            conf = 0.6
            ev = ""
            kt = ""
        elif isinstance(it, dict):
            kw = str(it.get("keyword") or "").strip()
            conf = float(it.get("confidence") or 0.6)
            ev = str(it.get("evidence") or it.get("matched_text") or "").strip()
            kt = str(it.get("keyword_type") or "").strip()
        else:
            continue

        if not kw or kw not in allowed:
            continue
        key = (kw, kt)
        if key in seen:
            continue
        seen.add(key)
        conf = max(0.0, min(1.0, float(conf)))
        hits.append({"keyword": kw, "confidence": conf, "evidence": ev, "keyword_type": kt})
        if len(hits) >= 80:
            break
    return hits


def post_analysis_llm(*, text: str, con=None, crawl_job_id: int | None = None) -> PostAnalysisResult:
    """
    Stage2: open semantic extraction (LLM REQUIRED).

    Rules:
    - Must NOT depend on project_keywords as candidate inputs.
    - Must extract freely from raw text: entities/features/issues/scenarios.
    """
    payload = {"text": str(text or "")}
    res = get_llm_router().run(task_type="post_analysis", input=payload, con=con, strict=True)
    try:
        if crawl_job_id is not None:
            from backend.llm.schema_log import log_llm_schema

            log_llm_schema(con, crawl_job_id=int(crawl_job_id), task_type="post_analysis", res=res)
    except Exception:
        pass
    if not bool(res.ok):
        raise RuntimeError(f"post_analysis failed: {res.error or 'unknown error'}")
    out = res.output if isinstance(res.output, dict) else {}

    def _list(name: str) -> list[dict[str, Any]]:
        v = out.get(name)
        return v if isinstance(v, list) else []

    def _raw_keywords() -> list[dict[str, Any]]:
        v = out.get("raw_keywords")
        if not isinstance(v, list):
            return []
        items: list[dict[str, Any]] = []
        for it in v:
            if isinstance(it, str):
                s = it.strip()
                if not s:
                    continue
                items.append({"text": s, "confidence": 0.6})
            elif isinstance(it, dict):
                t = str(it.get("text") or it.get("keyword") or "").strip()
                if not t:
                    continue
                items.append({"text": t, "confidence": float(it.get("confidence") or 0.6)})
        return items

    def _topics() -> list[str]:
        v = out.get("topics")
        if not isinstance(v, list):
            return []
        out_topics: list[str] = []
        seen = set()
        for it in v:
            s = str(it or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out_topics.append(s)
            if len(out_topics) >= 8:
                break
        return out_topics

    def _self_normalize_topics(topics: list[str]) -> list[str]:
        """
       确定性后处理以提高主题稳定性。
       
       强制执行的约束：
       2~5 个主题
       - 每个主题 2~6 个汉字（尽力而为；无效项将被删除）
       - 将常见变体映射为规范名称
       - 去重
        """
        canonical_map = {
            # Night / low light
            "夜景差": "夜视表现差",
            "夜拍差": "夜视表现差",
            "暗光差": "夜视表现差",
            "夜间差": "夜视表现差",
            "夜景糊": "夜视表现差",
            "夜视差": "夜视表现差",
            "夜视偏弱": "夜视表现差",
            # Focus
            "对焦差": "对焦不稳",
            "对焦不准": "对焦不稳",
            "跑焦": "对焦不稳",
            "虚焦": "对焦不稳",
            # Image quality
            "画质差": "画质偏糊",
            "很糊": "画质偏糊",
            "模糊": "画质偏糊",
            "不清晰": "画质偏糊",
            # Battery
            "续航差": "续航偏弱",
            "耗电快": "续航偏弱",
            "掉电快": "续航偏弱",
            "不耐用": "续航偏弱",
            # Heat
            "发热大": "发热明显",
            "过热": "发热明显",
            "烫手": "发热明显",
            "温度高": "发热明显",
            # Performance
            "卡顿": "性能卡顿",
            "掉帧": "性能卡顿",
            "不流畅": "性能卡顿",
            "性能差": "性能卡顿",
            # Network
            "信号差": "信号偏弱",
            "断流": "信号偏弱",
            "掉网": "信号偏弱",
            # Stability
            "闪退": "系统不稳",
            "崩溃": "系统不稳",
            "卡死": "系统不稳",
            "死机": "系统不稳",
            # Service / quality
            "品控差": "做工瑕疵",
            "做工差": "做工瑕疵",
            "售后差": "售后不佳",
            "客服差": "售后不佳",
            # Price
            "价格高": "价格偏高",
            "太贵": "价格偏高",
        }

        def extra_canon(s: str) -> str:
            """
            Extra canonical mapping for broader, non-config topics (marketing/scenario/etc.).
            Keep it deterministic and conservative.
            """
            if not s:
                return ""
            s2 = (
                str(s)
                .replace("/", "")
                .replace("／", "")
                .replace("|", "")
                .replace("｜", "")
                .replace("·", "")
                .strip()
            )
            m = {
                "联名": "联名营销",
                "联名营销": "联名营销",
                "营销": "营销热点",
                "营销热点": "营销热点",
                "新品": "新品发布",
                "新品发布": "新品发布",
                "参数": "参数对比",
                "对比": "参数对比",
                "参数对比": "参数对比",
                "性价比": "性价比",
                "系统更新": "系统更新",
                "做工品控": "做工品控",
                "通勤": "通勤体验",
                "通勤体验": "通勤体验",
                "游戏": "游戏性能",
                "游戏性能": "游戏性能",
                "影像": "拍照表现",
                "拍照表现": "拍照表现",
            }
            return m.get(s2, s2)

        def is_cn_phrase(s: str) -> bool:
            if not s:
                return False
            # Keep only CJK chars for length check
            cn = "".join(ch for ch in s if "\u4e00" <= ch <= "\u9fff")
            return 2 <= len(cn) <= 6

        out2: list[str] = []
        seen2 = set()
        for raw in topics or []:
            s = str(raw or "").strip()
            if not s:
                continue
            s = extra_canon(s)
            s = s.strip("，。！？!?,.;；【】[]()（）《》\"'“”‘’ ")
            if not s:
                continue
            s = extra_canon(canonical_map.get(s, s))
            if not is_cn_phrase(s):
                continue
            if s in seen2:
                continue
            seen2.add(s)
            out2.append(s)
            if len(out2) >= 5:
                break

        # If too few topics, use deterministic hints from extracted issues/features.
        if len(out2) < 2:
            hint_text = " ".join(
                [str(x.get("text") or "") for x in (_list("issues") + _list("features")) if isinstance(x, dict)]
            )
            hint_text += " " + str(text or "")
            hint = hint_text
            # Add a couple of canonical defaults based on hints
            if ("夜" in hint) or ("暗光" in hint) or ("夜景" in hint):
                out2.append("夜视表现差")
            if ("对焦" in hint) or ("跑焦" in hint) or ("虚焦" in hint):
                out2.append("对焦不稳")
            if ("发热" in hint) or ("烫" in hint) or ("过热" in hint):
                out2.append("发热明显")
            if ("卡顿" in hint) or ("掉帧" in hint) or ("不流畅" in hint) or ("lag" in hint.lower()):
                out2.append("性能卡顿")
            if ("续航" in hint) or ("耗电" in hint) or ("掉电" in hint):
                out2.append("续航偏弱")
            # final fallback
            if len(out2) < 2:
                out2.extend(["体验一般", "质量担忧"])
            # de-dup while keeping order
            uniq = []
            seen3 = set()
            for s in out2:
                if s in seen3:
                    continue
                seen3.add(s)
                uniq.append(s)
                if len(uniq) >= 5:
                    break
            out2 = uniq

        return out2[:5]

    meta = {
        "ok": bool(res.ok),
        "provider": res.provider,
        "model": res.model,
        "prompt_version": res.prompt_version,
        "error": res.error,
    }

    topics = _topics()

    return PostAnalysisResult(
        entities=_list("entities"),
        features=_list("features"),
        issues=_list("issues"),
        scenarios=_list("scenarios"),
        sentiment_targets=_list("sentiment_targets"),
        raw_keywords=_raw_keywords(),
        topics=topics,
        sentiment=(out.get("sentiment") if out.get("sentiment") is not None else None),
        sentiment_score=(float(out.get("sentiment_score")) if out.get("sentiment_score") is not None else None),
        emotion_intensity=(float(out.get("emotion_intensity")) if out.get("emotion_intensity") is not None else None),
        spam_label=(out.get("spam_label") if out.get("spam_label") is not None else None),
        spam_score=(float(out.get("spam_score")) if out.get("spam_score") is not None else None),
        meta=meta,
    )


_TOPIC_RULE_MAP: dict[str, str] = {
    # Battery
    "续航": "电池续航",
    "电量": "电池续航",
    "耗电": "电池续航",
    "电池": "电池续航",
    # Camera
    "拍照": "相机拍照",
    "摄像": "相机拍照",
    "相机": "相机拍照",
    # Performance
    "卡顿": "性能卡顿",
    "掉帧": "性能卡顿",
    "性能": "性能表现",
    # Heat
    "发热": "发热",
    "过热": "发热",
    # Price
    "价格": "价格",
    "性价比": "价格",
    # Service
    "售后": "售后服务",
    "客服": "售后服务",
    # Stability
    "闪退": "稳定性",
    "崩溃": "稳定性",
    # Network
    "信号": "网络信号",
    "网络": "网络信号",
}


def _rule_normalize_topic(raw: str) -> str:
    s = _normalize_topic_text(raw)
    if not s:
        return ""
    return _TOPIC_RULE_MAP.get(s, s)


def topic_normalization(
    *,
    extraction: PostAnalysisResult,
    con=None,
    use_llm_fallback: bool = True,
    max_topics: int = 20,
) -> TopicNormalizationResult:
    """
    Stage3: topic normalization.

    Priority:
    1) rule mapping (dict)
    2) LLM fallback (no embeddings; optional)
    """
    candidates: list[str] = []

    def add_from(items: list[dict[str, Any]], *keys: str):
        for it in items or []:
            if not isinstance(it, dict):
                continue
            for k in keys:
                v = str(it.get(k) or "").strip()
                if v:
                    candidates.append(v)

    add_from(extraction.features, "normalized", "text")
    add_from(extraction.issues, "normalized", "text")
    add_from(extraction.scenarios, "normalized", "text")
    add_from(extraction.raw_keywords, "text")
    add_from(extraction.sentiment_targets, "target")

    # rule normalize
    norm_map: list[dict[str, Any]] = []
    topics: list[dict[str, Any]] = []
    seen = set()
    unknown: list[str] = []
    for raw in candidates:
        topic = _rule_normalize_topic(raw)
        if not topic:
            continue
        norm_map.append({"raw": raw, "topic": topic, "method": "rule"})
        if topic not in seen:
            seen.add(topic)
            topics.append({"topic": topic, "confidence": 0.7, "aliases": [raw]})
        if len(topics) >= int(max_topics):
            break

    # LLM fallback: only when enabled and we still have too few topics.
    if use_llm_fallback and len(topics) < min(8, int(max_topics)):
        for raw in candidates:
            t = _rule_normalize_topic(raw)
            if not t:
                continue
            # heuristic: keep those that didn't map via explicit dict rule
            if _TOPIC_RULE_MAP.get(_normalize_topic_text(raw)) is None:
                unknown.append(raw)
        unknown = [x for i, x in enumerate(unknown) if x and x not in unknown[:i]]
        unknown = unknown[:30]
        if unknown:
            res = get_llm_router().run(task_type="topic_normalization", input={"candidates": unknown}, con=con)
            data = res.output if isinstance(res.output, dict) else {}
            items = data.get("topics") if isinstance(data.get("topics"), list) else []
            for it in items:
                if not isinstance(it, dict):
                    continue
                topic = str(it.get("topic") or "").strip()
                if not topic:
                    continue
                topic = _rule_normalize_topic(topic) or topic
                if topic in seen:
                    continue
                seen.add(topic)
                topics.append(
                    {
                        "topic": topic,
                        "confidence": float(it.get("confidence") or 0.6),
                        "aliases": (it.get("aliases") if isinstance(it.get("aliases"), list) else []),
                    }
                )
                norm_map.append({"raw": topic, "topic": topic, "method": "llm"})
                if len(topics) >= int(max_topics):
                    break

    # clean aliases
    for it in topics:
        aliases = it.get("aliases")
        if not isinstance(aliases, list):
            it["aliases"] = []
            continue
        uniq = []
        seen_a = set()
        for a in aliases:
            s = str(a or "").strip()
            if not s or s in seen_a:
                continue
            seen_a.add(s)
            uniq.append(s)
        it["aliases"] = uniq[:6]

    return TopicNormalizationResult(topics=topics[: int(max_topics)], normalization_map=norm_map[:200])


def merge_raw_payload(raw_payload: Any, extra: dict[str, Any]) -> str:
    """
    Keep backward compatibility by always returning a string.
    If raw_payload looks like JSON, merge fields; otherwise wrap it in {"raw": "..."}.
    """
    base: dict[str, Any] = {}
    try:
        if isinstance(raw_payload, str) and raw_payload.strip().startswith("{"):
            base = json.loads(raw_payload)
        elif raw_payload:
            base = {"raw": str(raw_payload)}
    except Exception:
        base = {"raw": str(raw_payload or "")}
    base.update(extra or {})
    return json.dumps(base, ensure_ascii=False, default=str)
