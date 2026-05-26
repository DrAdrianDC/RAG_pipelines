"""
Abstract base class for all retrievers.

Every retriever in this project must implement the ``retrieve`` method so
that experiment runners and evaluators can work with any retrieval backend
through a uniform interface.

Hit dict contract
-----------------
Each dict returned by ``retrieve`` must contain at minimum:

    {
        "doc_id"  : str,   # parent document identifier
        "section" : str,   # labeling section, or "" if not applicable
        "distance": float, # similarity distance (lower = more similar)
    }

Optional fields (add as needed):
    "chunk_id", "content", "score", "rank", "rerank_score"
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseRetriever(ABC):
    """
    Minimal retriever interface.

    All retrieval strategies (dense, sparse, hybrid, reranked) must subclass
    this and implement ``retrieve``.
    """

    @abstractmethod
    def retrieve(self, query: str, k: int) -> list[dict[str, Any]]:
        """
        Return the top-*k* hits for *query*.

        Parameters
        ----------
        query:
            Natural-language query string.
        k:
            Maximum number of results to return.

        Returns
        -------
        list of hit dicts conforming to the contract above.
        """
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
