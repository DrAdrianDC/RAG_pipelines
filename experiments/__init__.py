"""
experiments — RAG benchmark orchestration.

This package implements a two-stage experimental design that directly addresses
the "one pipeline, one result" limitation identified by Vecta's chunking
benchmark study (February 2026):

  "One pipeline, one result. Everything here runs on text-embedding-3-small,
   ChromaDB, and gemini-2.5-flash-lite. Swap any of those components and the
   rankings could shift. Running the same experiment across multiple embedding
   models would be valuable follow-up work."
   — Vecta Blog, Feb 2026

Our two experiments cover two independent axes of variation:

Experiment 1 — chunking_benchmark/
    Question : Which chunking strategy produces the best retrieval quality?
    Varies   : chunking strategy (fixed_512, fixed_1024, recursive_512,
               semantic, structure_aware)
    Fixed    : embedding model (all-MiniLM-L6-v2), ChromaDB, 100 queries

Experiment 2 — embedding_benchmark/
    Question : Which embedding model produces the best retrieval quality?
    Varies   : embedding model (all-MiniLM-L6-v2, BAAI/bge-small-en-v1.5,
               NeuML/pubmedbert-base-embeddings)
    Fixed    : chunking strategy (recursive_512), ChromaDB, 100 queries

Both experiments share:
- The same FDA biomarker corpus (597 JSONL records)
- The same 100 synthetic benchmark queries (9 template types, seed=42)
- The same adaptive-k formula: k = round(2000 / avg_tokens_per_chunk)
- The same evaluation metrics: Doc Hit@k, MRR, nDCG, Context Recall, Section Hit@k

Intended workflow
-----------------
1. Run chunking_benchmark → identify best chunking strategy.
2. Run embedding_benchmark with that strategy → identify best embedding model.
3. (Optional) Re-run chunking_benchmark with best embedding for confirmation.

Run commands (from the project root)
--------------------------------------
    python -m experiments.chunking_benchmark.run_benchmark
    python -m experiments.embedding_benchmark.run_benchmark
"""
