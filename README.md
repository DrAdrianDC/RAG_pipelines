# RAG Pipelines

Production-ready data ingestion pipelines for Retrieval-Augmented Generation (RAG) systems.

Each pipeline extracts, cleans, and transforms domain-specific data into vector-database-ready formats (JSONL).

---

## 📂 Pipelines

| Pipeline | Source | Technique | Output |
|----------|--------|-----------|--------|
| [**fda_rag_extraction**](fda_rag_extraction) | FDA Oncology Approvals | Web scraping + Delta updates | JSONL |
| [**pdf_extraction**](pdf_extraction) | Scientific PDF Documents | Marker ML extraction + PubMed enrichment | JSONL |

---

## 🚀 Quick Start

### fda_rag_extraction

Web scraping pipeline for FDA oncology drug approvals:

```bash
cd fda_rag_extraction
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python scripts/fda_watcher.py
```

### pdf_extraction

ML-powered PDF extraction with PubMed metadata enrichment:

```bash
cd pdf_extraction
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Add PDFs to data/raw/
cp /path/to/pdfs/*.pdf data/raw/

# Run pipeline
python pdf_marker_extraction.py      # Phase 1: Extract text
python pubmed_enrichment.py          # Phase 2-3: Enrich metadata
python combine_json_to_jsonl.py -d data/processed -o Output  # Phase 4: Generate JSONL
```

---

## 📋 Pipeline Features

### fda_rag_extraction
- Automated web scraping of FDA oncology approvals
- Delta updates (only new entries)
- Scheduled execution support
- JSONL output for vector databases

### pdf_extraction
- **ML-Powered Extraction**: Uses [Marker](https://github.com/VikParuchuri/marker) for high-quality PDF to Markdown
- **PubMed Integration**: Validates metadata via NCBI E-utilities API
- **Automatic Hardware Detection**: GPU (CUDA) or CPU
- **Checkpointing**: Resume interrupted processes
- **Schema Validation**: Pydantic models for data integrity

---

## 📄 Output Format

All pipelines produce JSONL files with consistent structure:

```jsonl
{"content": "...", "source": "pipeline_name", "url": "...", "date": "...", "title": "...", ...}
```

Ready for ingestion into vector databases (Pinecone, Weaviate, Qdrant, etc.).

---

## 🛠️ Requirements

- Python 3.9+
- See individual pipeline `requirements.txt` for dependencies

---

## 📚 Documentation

Each pipeline has its own detailed README:
- [fda_rag_extraction/README.md](fda_rag_extraction/README.md)
- [pdf_extraction/README.md](pdf_extraction/README.md)

---

## 📄 License

MIT License - See [LICENSE](LICENSE) for details.

---
