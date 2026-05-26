"""
Library utilities for the chunking package.

Responsibilities
----------------
- Token counting (tiktoken, cl100k_base).
- Section name normalisation (resolves "5 WARNINGS AND PRECAUTIONS" vs
  "Warnings and Precautions" mismatches in evaluation).
- Lazy-loaded SentenceTransformer singleton for SemanticChunker.
- JSONL corpus loader.
- Stable document-ID helper.

Out of scope (lives in experiments/chunking_benchmark/benchmark_utils.py)
--------------------------------------------------------------------------
- ChromaDB collection management.
- Benchmark query generation.
- Adaptive-k computation.

Rule: this file must NOT import chromadb, not depend on experiment paths,
and not import anything from experiments/.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import tiktoken

from chunking.config import DATASET_PATH, EMBEDDING_MODEL

# Re-export so semantic_chunking.py can import from one place.
# The canonical implementation lives in embeddings/sentence_transformer.py.
from embeddings.sentence_transformer import get_sentence_transformer  # noqa: F401

# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------
_TOKENIZER = tiktoken.get_encoding("cl100k_base")


def count_tokens(text: str) -> int:
    """Return the number of cl100k_base tokens in *text*."""
    return len(_TOKENIZER.encode(text))


# ---------------------------------------------------------------------------
# Section name normalisation
# ---------------------------------------------------------------------------
# Maps lowercase section text → canonical Title Case form that matches the
# JSONL "labeling_sections" field.  Applied by both the chunkers (when
# tagging chunks) and the evaluator (when comparing retrieved sections).
# Without this, structure_aware produces "5 WARNINGS AND PRECAUTIONS" while
# the ground truth is "Warnings and Precautions" → Section F1 = 0.

_SECTION_CANONICAL: dict[str, str] = {
    "indications and usage": "Indications and Usage",
    "dosage and administration": "Dosage and Administration",
    "dosage and administration - general": "Dosage and Administration",
    "contraindications": "Contraindications",
    "warnings and precautions": "Warnings and Precautions",
    "adverse reactions": "Adverse Reactions",
    "drug interactions": "Drug Interactions",
    "use in specific populations": "Use in Specific Populations",
    "overdosage": "Overdosage",
    "description": "Description",
    "clinical pharmacology": "Clinical Pharmacology",
    "nonclinical toxicology": "Nonclinical Toxicology",
    "clinical studies": "Clinical Studies",
    "references": "References",
    "how supplied": "How Supplied",
    "patient counseling information": "Patient Counseling Information",
    "boxed warning": "Boxed Warning",
    "box warning": "Boxed Warning",
    "preamble": "Boxed Warning",
}


def normalize_section(name: str) -> str:
    """
    Normalise an FDA section name to a canonical Title Case form.

    - ``"5 WARNINGS AND PRECAUTIONS"`` → ``"Warnings and Precautions"``
    - ``"Warnings and Precautions"``   → ``"Warnings and Precautions"``
    - Unknown names are returned in Title Case unchanged.
    """
    stripped = re.sub(r"^\d+(?:\.\d+)*\s+", "", name.strip())
    return _SECTION_CANONICAL.get(stripped.lower(), stripped.title())


# ---------------------------------------------------------------------------
# Corpus loading
# ---------------------------------------------------------------------------

def load_jsonl(path: Path = DATASET_PATH) -> list[dict[str, Any]]:
    """Load all records from the JSONL corpus file."""
    records: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def make_doc_id(record: dict[str, Any]) -> str:
    """
    Stable unique identifier for a drug+biomarker record.

    Combines drug_name and biomarker so chunks can be traced back to
    their parent document without relying on record position.
    """
    drug = record.get("drug_name", "unknown").strip()
    biomarker = record.get("biomarker", "unknown").strip()
    return f"{drug}||{biomarker}"
