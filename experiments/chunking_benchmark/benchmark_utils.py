"""
Experiment infrastructure for the chunking benchmark.

Responsibilities
----------------
- Enriching JSONL records with brand/generic drug aliases.
- Generating synthetic benchmark queries (named + hard semantic variants).
- Computing adaptive k (equalises context budget across strategies).
- Thin wrappers over vectorstores/chroma.py.
"""

from __future__ import annotations

import random
import re
from pathlib import Path
from typing import Any

import chromadb

from embeddings.sentence_transformer import DEFAULT_MODEL as EMBEDDING_MODEL
from chunking.utils import make_doc_id, normalize_section
from experiments.chunking_benchmark.config import (
    CHROMA_BASE_PATH,
    DEFAULT_SAMPLE_SIZE,
    DEFAULT_SEED,
    TARGET_CONTEXT_TOKENS,
)
from vectorstores.chroma import (
    get_chroma_collection as _get_collection,
    index_chunks,
    query_collection,
)

__all__ = [
    "enrich_record",
    "generate_benchmark_queries",
    "compute_adaptive_k",
    "get_chroma_collection",
    "index_chunks",
    "query_collection",
]

# ---------------------------------------------------------------------------
# Drug name enrichment
# ---------------------------------------------------------------------------

_BRAND_RE = re.compile(r"\b([A-Z][A-Z0-9-]{2,24})®")
_SUFFIX_RE = re.compile(r"\s*\(\d+\)\s*")


def normalize_generic_name(drug_name: str) -> str:
    """Strip FDA suffixes like ``(1)``, ``(2)`` from metadata drug names."""
    return _SUFFIX_RE.sub("", drug_name).strip()


def extract_brand_name(content: str, generic: str = "") -> str:
    """
    Extract brand/trade name from FDA label text.

    Tries, in order:
    1. ``BRAND®`` trademark pattern
    2. ``BRAND (generic)`` parenthetical pattern
    """
    window = content[:4000]
    match = _BRAND_RE.search(window)
    if match:
        return match.group(1)

    if generic:
        gen_word = generic.split()[0]
        if len(gen_word) >= 4:
            paren = re.search(
                rf"\b([A-Z][A-Z0-9-]{{2,24}})\s*\([^)]*{re.escape(gen_word)}",
                window,
                re.IGNORECASE,
            )
            if paren:
                return paren.group(1)

    return ""


def enrich_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Add normalised drug aliases used for query generation.

    Returns a shallow copy with keys:
    - ``_generic`` : normalised generic name from metadata
    - ``_brand``   : trademark extracted from content (may be empty)
    - ``_drug_query`` : best string to use in a named query (brand+generic)
    """
    content = record.get("content", "")
    generic = normalize_generic_name(record.get("drug_name", "unknown"))
    brand = extract_brand_name(content, generic)

    if brand and generic:
        drug_query = f"{brand} ({generic})"
    elif brand:
        drug_query = brand
    else:
        drug_query = generic

    return {
        **record,
        "_generic": generic,
        "_brand": brand,
        "_drug_query": drug_query,
    }


# ---------------------------------------------------------------------------
# Benchmark query generation
# ---------------------------------------------------------------------------

# Each entry: (template, query_type)
# query_type drives breakdown analysis — "hard" queries omit the drug name.
_QUERY_SPECS: list[tuple[str, str]] = [
    (
        "What does the FDA label for {drug_query} state about {biomarker} testing requirements?",
        "named_full",
    ),
    (
        "Are there contraindications for {drug_query} in patients with {biomarker} variants?",
        "named_full",
    ),
    (
        "What dosing adjustments does {generic} require based on {biomarker} status?",
        "named_generic",
    ),
    (
        "What adverse reactions are associated with {brand} in {biomarker}-positive patients?",
        "named_brand",
    ),
    (
        "What clinical evidence supports the use of {generic} in patients with {biomarker}?",
        "named_generic",
    ),
    (
        "Does the prescribing information for {drug_query} recommend screening for "
        "{biomarker} before treatment?",
        "named_full",
    ),
    (
        "What does the {section} section of the {generic} label say about {biomarker}?",
        "named_section",
    ),
    (
        "Which FDA-approved drug label discusses {biomarker} biomarker testing in "
        "{therapeutic_area}?",
        "semantic_hard",
    ),
    (
        "What biomarker-based prescribing recommendations exist for {biomarker} "
        "in {therapeutic_area}?",
        "semantic_hard",
    ),
]


def _format_query(template: str, rec: dict[str, Any]) -> str:
    sections = rec.get("labeling_sections") or ["Warnings and Precautions"]
    return template.format(
        drug_query=rec.get("_drug_query", rec.get("drug_name", "unknown")),
        generic=rec.get("_generic", rec.get("drug_name", "unknown")),
        brand=rec.get("_brand") or rec.get("_generic", "unknown"),
        biomarker=rec.get("biomarker", "unknown"),
        therapeutic_area=rec.get("therapeutic_area", "medicine"),
        section=sections[0],
    )


def generate_benchmark_queries(
    records: list[dict[str, Any]],
    sample_size: int | None = DEFAULT_SAMPLE_SIZE,
    seed: int = DEFAULT_SEED,
) -> list[dict[str, Any]]:
    """
    Generate synthetic retrieval queries from JSONL metadata.

    Queries rotate through named (easy), section-specific, and semantic-hard
    (no drug name) templates.  Records are enriched with brand/generic aliases
    extracted from label text to reduce metadata–content mismatch.
    """
    rng = random.Random(seed)
    enriched = [enrich_record(r) for r in records]
    pool = list(enriched)
    if sample_size is not None:
        pool = rng.sample(pool, min(sample_size, len(pool)))

    queries: list[dict[str, Any]] = []
    for i, rec in enumerate(pool):
        template, query_type = _QUERY_SPECS[i % len(_QUERY_SPECS)]
        queries.append({
            "query": _format_query(template, rec),
            "query_type": query_type,
            "gt_doc_id": make_doc_id(rec),
            "gt_content": rec.get("content", ""),
            "gt_sections": [normalize_section(s) for s in rec.get("labeling_sections", [])],
            "drug_name": rec.get("drug_name", ""),
            "biomarker": rec.get("biomarker", ""),
            "brand_name": rec.get("_brand", ""),
            "generic_name": rec.get("_generic", ""),
        })
    return queries


# ---------------------------------------------------------------------------
# Adaptive k
# ---------------------------------------------------------------------------

def compute_adaptive_k(
    avg_tokens_per_chunk: float,
    target: int = TARGET_CONTEXT_TOKENS,
) -> int:
    """k = round(target / avg_tokens_per_chunk) — equalises context budget."""
    if avg_tokens_per_chunk <= 0:
        return 1
    return max(1, round(target / avg_tokens_per_chunk))


# ---------------------------------------------------------------------------
# ChromaDB wrapper (injects experiment base path)
# ---------------------------------------------------------------------------

def get_chroma_collection(
    strategy_name: str,
    reset: bool = False,
    model_name: str = EMBEDDING_MODEL,
) -> chromadb.Collection:
    """Return a persistent ChromaDB collection under CHROMA_BASE_PATH."""
    return _get_collection(
        name=strategy_name,
        base_path=CHROMA_BASE_PATH,
        reset=reset,
        model_name=model_name,
    )
