"""
chunking — FDA Biomarker chunking strategy benchmark.

Public API
----------
Chunker classes:
    FixedChunker, RecursiveChunker, SemanticChunker,
    StructureAwareChunker, LangChainFixedChunker, LangChainRecursiveChunker

Factory helpers (return a fully-configured chunker from STRATEGY_CONFIGS):
    build_fixed_512, build_fixed_1024,
    build_recursive_512,
    build_semantic,
    build_structure_aware,
    build_lc_fixed_512, build_lc_fixed_1024, build_lc_recursive_512

Config:
    STRATEGY_CONFIGS, DATASET_PATH, EMBEDDING_MODEL

Usage::

    from chunking import build_fixed_512, build_recursive_512, build_semantic
    chunker = build_fixed_512()
    chunks = chunker.chunk_corpus()
"""

from chunking.fixed_chunking import FixedChunker, build_fixed_512, build_fixed_1024
from chunking.recursive_chunking import RecursiveChunker, build_recursive_512
from chunking.semantic_chunking import SemanticChunker, build_semantic
from chunking.structure_aware_chunking import StructureAwareChunker, build_structure_aware
from chunking.langchain_chunking import (
    LangChainFixedChunker,
    LangChainRecursiveChunker,
    build_lc_fixed_512,
    build_lc_fixed_1024,
    build_lc_recursive_512,
)
from chunking.config import STRATEGY_CONFIGS, DATASET_PATH, EMBEDDING_MODEL
from chunking.base_chunker import BaseChunker

__all__ = [
    # Base
    "BaseChunker",
    # Custom chunkers
    "FixedChunker",
    "RecursiveChunker",
    "SemanticChunker",
    "StructureAwareChunker",
    # LangChain cross-validation chunkers
    "LangChainFixedChunker",
    "LangChainRecursiveChunker",
    # Factory helpers
    "build_fixed_512",
    "build_fixed_1024",
    "build_recursive_512",
    "build_semantic",
    "build_structure_aware",
    "build_lc_fixed_512",
    "build_lc_fixed_1024",
    "build_lc_recursive_512",
    # Config
    "STRATEGY_CONFIGS",
    "DATASET_PATH",
    "EMBEDDING_MODEL",
]
