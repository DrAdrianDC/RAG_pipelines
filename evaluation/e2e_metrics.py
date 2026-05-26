"""
End-to-end RAG evaluation metrics (retrieval + generation).

Design
------
Two modes:

1. Retrieval-only (default, no API key needed)
   Extractive proxies computed directly from hit dicts:
   ``faithfulness_proxy``      — filtered context recall.
   ``answer_relevance_proxy``  — query/GT token overlap.

2. LLM-as-judge (``use_llm=True``)
   A single LLM call per query:
     a) Generates a grounded answer from the retrieved context.
     b) Self-scores that answer on faithfulness and answer_relevance.
   One API call per query (not two).

   Provider "groq" — FREE (recommended) ← DEFAULT
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   Get key: https://console.groq.com     (no credit card)
   Set env:  GROQ_API_KEY=...            (in .env, auto-loaded)
   Limits:   30 req/min · 14,400 req/day → 100 queries ≈ 3-4 min, no issue
   Model:    llama-3.1-8b-instant        (default, ~0.5s/call)
   Alt:      llama-3.3-70b-versatile     (higher quality, still free)

   Provider "gemini" — FREE but very limited
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   Get key: https://aistudio.google.com  (no credit card)
   Set env:  GEMINI_API_KEY=...          (in .env, auto-loaded)
   Limits:   20 req/day free tier (flash-lite) — NOT viable for 100 queries
   Model:    gemini-2.5-flash-lite       (default)

   Provider "openai" — paid
   ~~~~~~~~~~~~~~~~~~~~~~~~~
   Set env:  OPENAI_API_KEY=...
   Model:    gpt-4o-mini

Fallback
--------
If the LLM call fails for any reason, execution falls back to extractive
proxies and logs a WARNING — the benchmark never crashes mid-run.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Literal

from evaluation.metrics import context_recall

_log = logging.getLogger(__name__)

_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
_GROQ_BASE_URL   = "https://api.groq.com/openai/v1"

LLMProvider = Literal["groq", "gemini", "openai"]

_DEFAULT_MODELS: dict[LLMProvider, str] = {
    "groq":   "llama-3.1-8b-instant",
    "gemini": "gemini-2.5-flash-lite",
    "openai": "gpt-4o-mini",
}

# Minimum seconds between consecutive calls per provider (preventive throttle).
# groq free tier: 30 req/min → 1 call every 2 s; 2.2 s gives ~27 req/min headroom.
# gemini free tier: 15 req/min → 1 call every 4 s.
# openai paid: no enforced floor here.
_PROVIDER_MIN_INTERVAL: dict[str, float] = {
    "groq":   2.2,
    "gemini": 4.0,
    "openai": 0.0,
}
_MAX_RETRIES = 3

# Per-provider last-call timestamp — shared across all evaluate_e2e_single calls.
_last_call_time: dict[str, float] = {}
_throttle_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Result containers
# ---------------------------------------------------------------------------

@dataclass
class E2EQueryResult:
    query: str
    gt_doc_id: str
    gt_content: str
    gt_sections: list[str]
    retrieved_context: str
    generated_answer: str = ""
    faithfulness: float = 0.0
    answer_relevance: float = 0.0
    used_llm_judge: bool = False
    llm_provider: str = ""


@dataclass
class E2EResults:
    query_results: list[E2EQueryResult]

    def summary(self) -> dict[str, float]:
        if not self.query_results:
            return {"faithfulness": 0.0, "answer_relevance": 0.0, "n": 0}
        return {
            "faithfulness": float(
                sum(r.faithfulness for r in self.query_results) / len(self.query_results)
            ),
            "answer_relevance": float(
                sum(r.answer_relevance for r in self.query_results) / len(self.query_results)
            ),
            "n": float(len(self.query_results)),
            "llm_judged_fraction": float(
                sum(1 for r in self.query_results if r.used_llm_judge) / len(self.query_results)
            ),
        }


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------

def build_retrieved_context(
    hits: list[dict[str, Any]],
    gt_doc_id: str | None = None,
) -> str:
    """Concatenate chunk text; optionally filter to the ground-truth document."""
    chunks = hits if gt_doc_id is None else [h for h in hits if h.get("doc_id") == gt_doc_id]
    return "\n\n".join(h.get("content", "") for h in chunks if h.get("content"))


# ---------------------------------------------------------------------------
# Extractive proxies (no API key)
# ---------------------------------------------------------------------------

def faithfulness_proxy(hits: list[dict[str, Any]], gt_content: str, gt_doc_id: str) -> float:
    """Extractive faithfulness: GT token recall in correct-document chunks."""
    return context_recall(hits, gt_content, gt_doc_id)


def answer_relevance_proxy(query: str, context: str) -> float:
    """
    Lightweight relevance proxy: fraction of query content-words found in context.
    Used as fallback when no LLM is available.
    """
    if not context.strip():
        return 0.0
    q_words = {w.lower() for w in re.findall(r"[a-zA-Z]{4,}", query)}
    ctx_lower = context.lower()
    return sum(1 for w in q_words if w in ctx_lower) / max(len(q_words), 1)


# ---------------------------------------------------------------------------
# LLM: generate answer + self-judge (single API call per query)
# ---------------------------------------------------------------------------

def _generate_and_judge_prompt(query: str, context: str) -> str:
    """
    Prompt that asks the LLM to both answer the question and self-evaluate.
    Single call = half the API quota vs separate generate + judge calls.
    """
    return (
        "You are a biomedical RAG evaluation assistant.\n\n"
        "TASK:\n"
        "1. Answer the question using ONLY information from the context below.\n"
        "   If the context does not contain enough information, say so explicitly.\n"
        "2. Score your answer on two criteria (0.0 to 1.0):\n"
        "   - faithfulness: every claim in your answer is supported by the context "
        "(1.0 = fully grounded, 0.0 = hallucinated).\n"
        "   - answer_relevance: your answer addresses the question "
        "(1.0 = fully relevant, 0.0 = off-topic).\n\n"
        "Return ONLY valid JSON — no markdown, no extra text:\n"
        '{"answer": "...", "faithfulness": 0.0, "answer_relevance": 0.0}\n\n'
        f"Question: {query}\n\n"
        f"Context:\n{context[:6000]}"
    )


def _throttle(provider: str) -> None:
    """Block until the minimum inter-request interval for *provider* has elapsed."""
    min_interval = _PROVIDER_MIN_INTERVAL.get(provider, 0.0)
    if min_interval <= 0:
        return
    with _throttle_lock:
        last = _last_call_time.get(provider, 0.0)
        wait = max(0.0, last + min_interval - time.monotonic())
        if wait > 0:
            time.sleep(wait)
        _last_call_time[provider] = time.monotonic()


def _parse_retry_after(exc: Exception) -> float:
    """Extract retry-after seconds from a rate-limit error message, or return 0."""
    match = re.search(r"retry[_\- ]?(?:after|in)[:\s]+(\d+(?:\.\d+)?)", str(exc), re.I)
    return float(match.group(1)) if match else 0.0


def _call_llm(
    prompt: str,
    model: str,
    provider: LLMProvider,
) -> dict[str, Any]:
    """Route to Groq / Gemini / OpenAI and return parsed JSON.

    Applies per-provider preventive throttling before every call and retries
    up to _MAX_RETRIES times on 429 errors, honouring the retry-after delay.
    """
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError("pip install openai") from exc

    if provider == "groq":
        api_key = os.environ.get("GROQ_API_KEY", "")
        if not api_key:
            raise RuntimeError(
                "GROQ_API_KEY not set. Get a free key at https://console.groq.com"
            )
        client = OpenAI(api_key=api_key, base_url=_GROQ_BASE_URL)
    elif provider == "gemini":
        api_key = os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            raise RuntimeError(
                "GEMINI_API_KEY not set. Get a free key at https://aistudio.google.com"
            )
        client = OpenAI(api_key=api_key, base_url=_GEMINI_BASE_URL)
    else:
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY not set.")
        client = OpenAI(api_key=api_key)

    last_exc: Exception | None = None
    for attempt in range(1, _MAX_RETRIES + 1):
        _throttle(provider)
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                response_format={"type": "json_object"},
            )
            raw = response.choices[0].message.content or "{}"
            return json.loads(raw)
        except Exception as exc:
            last_exc = exc
            err_str = str(exc)
            is_rate_limit = "429" in err_str or "rate" in err_str.lower()
            if is_rate_limit and attempt < _MAX_RETRIES:
                retry_after = _parse_retry_after(exc) or (attempt * 5)
                _log.warning(
                    "Rate limit from %s (attempt %d/%d). Sleeping %.0fs …",
                    provider, attempt, _MAX_RETRIES, retry_after,
                )
                time.sleep(retry_after)
                # Reset the throttle clock so the next _throttle() sees a fresh start.
                with _throttle_lock:
                    _last_call_time[provider] = time.monotonic()
            else:
                raise

    raise RuntimeError(f"All {_MAX_RETRIES} LLM attempts failed") from last_exc


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def evaluate_e2e_single(
    query_meta: dict[str, Any],
    hits: list[dict[str, Any]],
    use_llm: bool = False,
    llm_provider: LLMProvider = "gemini",
    llm_model: str | None = None,
) -> E2EQueryResult:
    """
    Score one query end-to-end.

    Parameters
    ----------
    query_meta:
        Benchmark query dict: ``query``, ``gt_doc_id``, ``gt_content``, ``gt_sections``.
    hits:
        Hit dicts returned by the retriever for this query.
    use_llm:
        If True, call the LLM to generate an answer AND score it.
        Falls back to extractive proxies on any error.
    llm_provider:
        ``"gemini"`` (FREE, default) or ``"openai"`` (paid).
    llm_model:
        Override the model name. Defaults to ``gemini-2.5-flash-lite`` / ``gpt-4o-mini``.
    """
    model = llm_model or _DEFAULT_MODELS.get(llm_provider, "llama-3.1-8b-instant")
    gt_doc_id = query_meta["gt_doc_id"]
    gt_content = query_meta.get("gt_content", "")
    context = build_retrieved_context(hits, gt_doc_id)

    result = E2EQueryResult(
        query=query_meta["query"],
        gt_doc_id=gt_doc_id,
        gt_content=gt_content,
        gt_sections=query_meta.get("gt_sections", []),
        retrieved_context=context,
    )

    if use_llm:
        prompt = _generate_and_judge_prompt(query_meta["query"], context)
        try:
            data = _call_llm(prompt, model=model, provider=llm_provider)
            result.generated_answer = str(data.get("answer", ""))
            result.faithfulness = float(data.get("faithfulness", 0.0))
            result.answer_relevance = float(data.get("answer_relevance", 0.0))
            result.used_llm_judge = True
            result.llm_provider = f"{llm_provider}/{model}"
            return result
        except Exception as exc:
            _log.warning(
                "LLM judge (%s/%s) failed, falling back to proxies: %s",
                llm_provider, model, exc,
            )

    # Extractive fallback
    result.faithfulness = faithfulness_proxy(hits, gt_content, gt_doc_id)
    result.answer_relevance = answer_relevance_proxy(query_meta["query"], context)
    return result
