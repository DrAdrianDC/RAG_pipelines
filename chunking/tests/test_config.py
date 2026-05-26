"""
Tests for chunking/config.py — configuration integrity.

These tests act as a contract between the config and the chunkers:
if a key is renamed or a value goes out of range, CI catches it before
a chunker silently uses a wrong hyperparameter.

Covers:
- All strategy IDs are present
- Required keys exist per strategy type
- overlap_fraction is in the Weaviate-recommended range [0.0, 0.20]
- chunk_size values are positive integers
- similarity_threshold is in (0.0, 1.0)
- min_chunk_tokens >= 50 (FDA-specific minimum, regression guard)
- max_chunk_tokens > min_chunk_tokens
- DATASET_PATH resolves to an existing file
- EMBEDDING_MODEL is a non-empty string
"""

from __future__ import annotations

import pytest

from chunking.config import (
    DATASET_PATH,
    EMBEDDING_MODEL,
    STRATEGY_CONFIGS,
)


# ---------------------------------------------------------------------------
# Strategy IDs
# ---------------------------------------------------------------------------

EXPECTED_STRATEGY_IDS = {
    "fixed_512",
    "fixed_1024",
    "recursive_512",
    "semantic",
    "structure_aware",
    "lc_fixed_512",
    "lc_fixed_1024",
    "lc_recursive_512",
}


class TestStrategyIds:
    def test_all_expected_strategies_present(self):
        missing = EXPECTED_STRATEGY_IDS - set(STRATEGY_CONFIGS.keys())
        assert not missing, f"Missing strategy IDs in STRATEGY_CONFIGS: {missing}"

    def test_no_unexpected_strategy_ids(self):
        extra = set(STRATEGY_CONFIGS.keys()) - EXPECTED_STRATEGY_IDS
        assert not extra, (
            f"Unexpected strategy IDs found: {extra}. "
            "If intentional, add them to EXPECTED_STRATEGY_IDS in this test."
        )

    def test_all_strategies_have_description(self):
        for strategy_id, cfg in STRATEGY_CONFIGS.items():
            assert "description" in cfg, f"'{strategy_id}' is missing a 'description' key"
            assert isinstance(cfg["description"], str) and cfg["description"], (
                f"'{strategy_id}' has an empty description"
            )


# ---------------------------------------------------------------------------
# Fixed-size strategies: chunk_size and overlap_fraction
# ---------------------------------------------------------------------------

FIXED_STRATEGY_IDS = {"fixed_512", "fixed_1024"}


class TestFixedStrategyConfig:
    @pytest.mark.parametrize("strategy_id", sorted(FIXED_STRATEGY_IDS))
    def test_chunk_size_is_positive_integer(self, strategy_id):
        cfg = STRATEGY_CONFIGS[strategy_id]
        assert "chunk_size" in cfg, f"'{strategy_id}' missing 'chunk_size'"
        assert isinstance(cfg["chunk_size"], int) and cfg["chunk_size"] > 0

    @pytest.mark.parametrize("strategy_id", sorted(FIXED_STRATEGY_IDS))
    def test_overlap_fraction_in_weaviate_range(self, strategy_id):
        """Weaviate recommends 10–20 %. We enforce [0.0, 0.25) to allow experimentation."""
        cfg = STRATEGY_CONFIGS[strategy_id]
        assert "overlap_fraction" in cfg, f"'{strategy_id}' missing 'overlap_fraction'"
        frac = cfg["overlap_fraction"]
        assert 0.0 <= frac < 0.25, (
            f"'{strategy_id}' overlap_fraction={frac} is outside [0.0, 0.25). "
            "Weaviate recommends 0.10–0.20."
        )

    def test_fixed_512_chunk_size_is_512(self):
        assert STRATEGY_CONFIGS["fixed_512"]["chunk_size"] == 512

    def test_fixed_1024_chunk_size_is_1024(self):
        assert STRATEGY_CONFIGS["fixed_1024"]["chunk_size"] == 1024

    def test_fixed_512_overlap_fraction_matches_fixed_1024(self):
        """Both fixed strategies should use the same overlap fraction by convention."""
        assert (
            STRATEGY_CONFIGS["fixed_512"]["overlap_fraction"]
            == STRATEGY_CONFIGS["fixed_1024"]["overlap_fraction"]
        )


# ---------------------------------------------------------------------------
# Recursive strategy: chunk_size and overlap_fraction
# ---------------------------------------------------------------------------

class TestRecursiveStrategyConfig:
    def test_chunk_size_is_512(self):
        assert STRATEGY_CONFIGS["recursive_512"]["chunk_size"] == 512

    def test_overlap_fraction_present_and_valid(self):
        cfg = STRATEGY_CONFIGS["recursive_512"]
        assert "overlap_fraction" in cfg
        assert 0.0 <= cfg["overlap_fraction"] < 0.25

    def test_overlap_fraction_matches_fixed_512(self):
        """Recursive and fixed 512 use the same overlap fraction for fair comparison."""
        assert (
            STRATEGY_CONFIGS["recursive_512"]["overlap_fraction"]
            == STRATEGY_CONFIGS["fixed_512"]["overlap_fraction"]
        )


# ---------------------------------------------------------------------------
# Semantic strategy
# ---------------------------------------------------------------------------

class TestSemanticStrategyConfig:
    def test_required_keys_present(self):
        required = {"similarity_threshold", "min_chunk_tokens", "max_chunk_tokens"}
        cfg = STRATEGY_CONFIGS["semantic"]
        missing = required - cfg.keys()
        assert not missing, f"Semantic config missing keys: {missing}"

    def test_similarity_threshold_in_range(self):
        threshold = STRATEGY_CONFIGS["semantic"]["similarity_threshold"]
        assert 0.0 < threshold < 1.0, (
            f"similarity_threshold={threshold} must be in (0.0, 1.0)"
        )

    def test_min_chunk_tokens_at_least_50(self):
        """
        Regression guard: min_chunk_tokens was 20 (too low for FDA text).
        Must never go below 50 again.
        """
        min_tok = STRATEGY_CONFIGS["semantic"]["min_chunk_tokens"]
        assert min_tok >= 50, (
            f"min_chunk_tokens={min_tok} is below 50. "
            "FDA regulatory text needs at least 50 tokens for an informative embedding."
        )

    def test_max_chunk_tokens_greater_than_min(self):
        cfg = STRATEGY_CONFIGS["semantic"]
        assert cfg["max_chunk_tokens"] > cfg["min_chunk_tokens"], (
            "max_chunk_tokens must be strictly greater than min_chunk_tokens"
        )

    def test_max_chunk_tokens_reasonable_upper_bound(self):
        """max_chunk_tokens should not exceed 2× the embedding model's sweet spot."""
        assert STRATEGY_CONFIGS["semantic"]["max_chunk_tokens"] <= 1024


# ---------------------------------------------------------------------------
# Structure-aware strategy
# ---------------------------------------------------------------------------

class TestStructureAwareStrategyConfig:
    def test_max_chunk_tokens_present(self):
        assert "max_chunk_tokens" in STRATEGY_CONFIGS["structure_aware"]

    def test_max_chunk_tokens_is_positive(self):
        assert STRATEGY_CONFIGS["structure_aware"]["max_chunk_tokens"] > 0

    def test_max_chunk_tokens_is_1024(self):
        assert STRATEGY_CONFIGS["structure_aware"]["max_chunk_tokens"] == 1024


# ---------------------------------------------------------------------------
# LangChain cross-validation variants
# ---------------------------------------------------------------------------

LC_STRATEGY_IDS = {"lc_fixed_512", "lc_fixed_1024", "lc_recursive_512"}


class TestLangChainStrategyConfig:
    @pytest.mark.parametrize("strategy_id", sorted(LC_STRATEGY_IDS))
    def test_chunk_size_present_and_positive(self, strategy_id):
        cfg = STRATEGY_CONFIGS[strategy_id]
        assert "chunk_size" in cfg
        assert cfg["chunk_size"] > 0

    @pytest.mark.parametrize("strategy_id", sorted(LC_STRATEGY_IDS))
    def test_overlap_present_and_non_negative(self, strategy_id):
        cfg = STRATEGY_CONFIGS[strategy_id]
        assert "overlap" in cfg, f"'{strategy_id}' missing 'overlap'"
        assert cfg["overlap"] >= 0

    def test_lc_fixed_512_overlap_approximately_10_percent(self):
        cfg = STRATEGY_CONFIGS["lc_fixed_512"]
        ratio = cfg["overlap"] / cfg["chunk_size"]
        assert 0.08 <= ratio <= 0.15, (
            f"lc_fixed_512 overlap ratio={ratio:.2f}, expected ~0.10 (Weaviate: 10–20 %)"
        )


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

class TestModuleConstants:
    def test_dataset_path_is_defined(self):
        assert DATASET_PATH is not None

    def test_dataset_path_exists(self):
        assert DATASET_PATH.exists(), (
            f"DATASET_PATH does not exist: {DATASET_PATH}\n"
            "Run the data ingestion pipeline first, or check datasets/fda_biomarkers/."
        )

    def test_embedding_model_is_non_empty_string(self):
        assert isinstance(EMBEDDING_MODEL, str) and EMBEDDING_MODEL.strip()

    def test_embedding_model_is_known_sentence_transformer(self):
        """Light guard — catches obvious typos in the model name."""
        known_prefixes = (
            "all-MiniLM",
            "all-mpnet",
            "BAAI/bge",
            "NeuML/",
            "sentence-transformers/",
            "paraphrase-",
        )
        assert any(EMBEDDING_MODEL.startswith(p) for p in known_prefixes), (
            f"EMBEDDING_MODEL='{EMBEDDING_MODEL}' does not match any known "
            f"SentenceTransformer prefix: {known_prefixes}"
        )
