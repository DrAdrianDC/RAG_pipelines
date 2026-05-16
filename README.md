

# 📚 RAG Pipelines

A modular exploration of Retrieval-Augmented Generation (RAG) pipelines focused on experimenting with different components such as ingestion, chunking, embeddings, and retrieval strategies.

This project is built as a **learning and experimentation space for RAG systems design**.

---

# 🎯 Goal

The main objective of this repository is to:

- Understand how different RAG components affect performance
- Experiment with chunking and retrieval strategies
- Compare embeddings and vector database options
- Build a foundation for production-ready RAG systems

---

# 🧩 Current Components

## 📥 Data Ingestion
Basic pipelines for loading and preparing documents.

## ✂️ Chunking Strategies
Experiments with different ways of splitting text:
- Fixed-size chunking
- Recursive splitting
- (More strategies in progress)

## 🔢 Embeddings
Interface for testing different embedding models.

## 🧱 Vector Storage
Initial integrations with vector databases for similarity search.

## 🔍 Retrieval
Basic retrieval pipelines using vector similarity search.

---

# 🏗️ Planned Extensions

This project is actively evolving. Next steps include:

- Advanced chunking strategies (semantic, agentic)
- Hybrid retrieval (BM25 + vector search)
- Reranking models
- Evaluation framework for RAG quality
- Config-driven pipeline execution
- Full end-to-end pipeline orchestration

---

# ⚙️ Installation

```bash
git clone https://github.com/DrAdrianDC/RAG_pipelines.git
cd RAG_pipelines

python -m venv venv
source venv/bin/activate
```

Install dependencies as needed per module.
