# PDF Extraction Pipeline

A modular pipeline for extracting text from scientific PDFs and enriching metadata via PubMed API. Designed for RAG (Retrieval-Augmented Generation) systems.

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

---

## Features

- **ML-Based Extraction**: Uses [Marker](https://github.com/VikParuchuri/marker) for high-quality PDF to Markdown conversion
- **Metadata Enrichment**: Validates and enriches metadata via NCBI PubMed E-utilities API  
- **Hardware Auto-Detection**: Automatically configures for GPU (CUDA) or CPU
- **Checkpointing**: Resume interrupted processes without reprocessing
- **Schema Validation**: Pydantic models ensure data integrity

## Quick Start

```bash
# Setup
cd pdf_extraction
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Add PDFs to process
cp /path/to/papers/*.pdf data/raw/

# Run pipeline
python pdf_marker_extraction.py    # Phase 1: Extract text
python pubmed_enrichment.py        # Phase 2-3: Enrich metadata  
python combine_json_to_jsonl.py    # Phase 4: Generate JSONL
```

## Pipeline Architecture

```
data/raw/*.pdf
    │
    ▼
┌──────────────────────────────────────┐
│  Phase 1: PDF Extraction             │
│  pdf_marker_extraction.py            │
│  • PDF → Markdown via Marker ML      │
│  • Extract DOI, title from PDF       │
│  Output: data/marker_outputs/*.json  │
└──────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────┐
│  Phase 2-3: PubMed Enrichment        │
│  pubmed_enrichment.py                │
│  • Search PubMed by DOI/title        │
│  • Validate document identity        │
│  • Enrich with official citation     │
│  Output: data/processed/*_final.json │
└──────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────┐
│  Phase 4: JSONL Generation           │
│  combine_json_to_jsonl.py            │
│  • Combine JSON files                │
│  • Transform to DB-ready format      │
│  Output: Output/pdf_extraction.jsonl │
└──────────────────────────────────────┘
```

## Project Structure

```
pdf_extraction/
├── pdf_marker_extraction.py   # Phase 1
├── pubmed_enrichment.py       # Phase 2-3
├── combine_json_to_jsonl.py   # Phase 4
├── requirements.txt
├── pyproject.toml
├── README.md
│
├── data/
│   ├── raw/                   # Input PDFs
│   ├── marker_outputs/        # Phase 1 output
│   ├── processed/             # Phase 2-3 output
│   └── failed/                # Failed documents
│
├── logs/                      # Pipeline logs
│
└── Output/                    # Final JSONL
```

## Configuration

### Hardware

The pipeline auto-detects GPU/CPU. No manual configuration needed.

### PubMed API (Optional)

For faster PubMed queries, set an API key:

```bash
export PUBMED_API_KEY="your_api_key"
export PUBMED_EMAIL="your@email.com"
```

Get a free API key: https://www.ncbi.nlm.nih.gov/account/

## Output Format

### Intermediate JSON (Phase 1)

```json
{
  "text": "Markdown content...",
  "metadata": {
    "title": "Document Title",
    "doi": "10.1234/example"
  }
}
```

### Final JSON (Phase 2-3)

```json
{
  "Title": "Official title from PubMed",
  "Citation": "Author et al. (Year). Title. Journal.",
  "Link": "https://doi.org/10.1234/example",
  "Corpus": "Full document text..."
}
```

### JSONL Output (Phase 4)

```jsonl
{"content": "...", "source": "pdf_extraction", "url": "...", "title": "...", "citation": "..."}
```

## Example Data

For testing, use open-access papers from:

- [PubMed Central](https://www.ncbi.nlm.nih.gov/pmc/) - Filter by "Free full text"
- [BMC journals](https://www.biomedcentral.com/) - Open access
- [PLOS](https://plos.org/) - All open access

## Requirements

- Python 3.9+
- ~8GB RAM (16GB recommended)
- ~5GB disk for ML models
- GPU optional (10-20x faster)

## 📄 License

MIT License - See [LICENSE](LICENSE) for details.

---

