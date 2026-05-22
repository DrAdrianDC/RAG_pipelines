

# 📚 RAG Pipelines

A modular framework for systematic RAG experimentation and evaluation.

## Overview
RAG_pipelines is an experimental framework designed to study and optimize Retrieval-Augmented Generation (RAG) systems through reproducible offline evaluation workflows.

The project focuses on modular experimentation across:

- Document ingestion
- Chunking strategies
- Embedding models
- Vector retrieval
- Reranking pipelines
- Context construction
- LLM-based generation
- Retrieval and generation evaluation

## Goal 

The goal is to provide an AI Engineering-oriented environment for benchmarking RAG design decisions rather than building a single chatbot application.



The `datasets/` folder contains the ingestion-ready corpora used for RAG experimentation and evaluation workflows. These datasets are directly consumed by the pipeline for document ingestion, chunk generation, embedding computation, vector database indexing, retrieval benchmarking, and chunking strategy analysis.

The repository is designed so the same datasets can be reused consistently across different experiments involving:
- chunking strategies,
- embedding models,
- retrieval methods,
- rerankers,
- and retrieval quality metrics such as Recall@K, MRR, and NDCG.


---

## Research Motivation

Most RAG repositories focus on building end-user applications.

This project focuses instead on understanding how architectural decisions impact retrieval quality, context relevance, and downstream generation performance through systematic experimentation and evaluation.

---

## Pipeline Architecture

```text
Data Ingestion
      ↓
Document Processing
      ↓
Chunking
      ↓
Embeddings
      ↓
Vector Database
      ↓
Retrieval
      ↓
Reranking
      ↓
Context Construction
      ↓
LLM Generation
      ↓
Evaluation
```
---

# 🧩 Current Components

## 📥 Data Ingestion

Modular ingestion pipelines for acquiring, parsing, and preprocessing
document collections used in retrieval experiments.

Current ingestion workflows include:

* PDF acquisition and document collection
* OCR-based document extraction
* Structured text preprocessing
* JSONL corpus generation
* Preparation of benchmark-ready datasets for downstream RAG evaluation

The ingestion layer is designed to support reproducible dataset construction
for chunking, retrieval, and retrieval-quality benchmarking experiments.

# Next steps

#### ✂️ Chunking Strategies
Experiments with different ways of splitting text:
- Fixed-size chunking
- Recursive splitting
- (More strategies in progress)

#### 🔢 Embeddings
Interface for testing different embedding models.

#### 🧱 Vector Storage
Initial integrations with vector databases for similarity search.

#### 🔍 Retrieval
Basic retrieval pipelines using vector similarity search.

---


# ⚙️ Installation

```bash
git clone https://github.com/DrAdrianDC/RAG_pipelines.git
cd RAG_pipelines

python -m venv venv
source venv/bin/activate
```

Install dependencies as needed per module.
