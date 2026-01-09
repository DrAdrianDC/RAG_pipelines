# 🧬 FDA RAG Extraction Pipeline

A production-ready data extraction pipeline that monitors FDA oncology drug approvals, extracts full-text content via deep web scraping, and generates clean JSONL datasets optimized for Retrieval-Augmented Generation (RAG) systems.

**Key Features:**
- 🔐 **Fingerprinting** — MD5 hash-based RAG_IDs for document deduplication
- 🔄 **Delta Updates** — Only processes new records (compares against master database)
- 🕷️ **Deep Scraping** — Extracts full-text from individual drug approval pages

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

---

## 🏗️ Architecture & Workflow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           FDA RAG EXTRACTION PIPELINE                           │
└─────────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────┐
    │   FDA Website        │  Source: Oncology/Hematologic Malignancies Approvals
    │   (HTML Table)       │  https://www.fda.gov/drugs/resources-information-approved-drugs/
    └──────────┬───────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  STEP 1: fda_watcher.py                                                          │
│  ────────────────────────────────────────────────────────────────────────────────│
│  • Scrapes FDA approval notifications table                                      │
│  • Detects NEW entries (compares against Master Excel DB)                        │
│  • Deep scraping: visits each drug URL → extracts full text corpus               │
│  • Generates unique RAG_ID (MD5 hash) for deduplication                          │
│                                                                                  │
│  OUTPUT:                                                                         │
│    ├── data/rag_initial_load.json    (first run - all records)                  │
│    ├── data/rag_delta_update.json    (incremental - new records only)           │
│    └── data/FDA_Oncology_Master_DB.xlsx (persistent master database)            │
└──────────────────────────────────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  STEP 2: json_split_and_clean.py                                                 │
│  ────────────────────────────────────────────────────────────────────────────────│
│  • Cleans raw scraped text (removes boilerplate, normalizes Unicode)             │
│  • Applies smart cutoff detection (removes footer content)                       │
│  • Preserves critical content (dosage information, efficacy data)                │
│  • Splits consolidated JSON into individual case files                           │
│                                                                                  │
│  OUTPUT:                                                                         │
│    └── data/processed-json/{RAG_ID}.json  (one file per drug approval)          │
└──────────────────────────────────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  STEP 3: combine_json_to_jsonl.py                                                │
│  ────────────────────────────────────────────────────────────────────────────────│
│  • Combines all JSON files into single JSONL                                     │
│  • Transforms to vector database schema                                          │
│  • Final cleanup (markdown artifacts, image tags)                                │
│                                                                                  │
│  OUTPUT:                                                                         │
│    └── Output/fda_rag.jsonl  (ready for vector database ingestion)              │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

```bash
# 1. Clone and setup
git clone <repository-url>
cd fda_rag_extraction
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Run the pipeline
python scripts/fda_watcher.py              # Step 1: Scrape FDA
python scripts/json_split_and_clean.py     # Step 2: Clean & Split
python scripts/combine_json_to_jsonl.py    # Step 3: Generate JSONL

# 3. Output ready for RAG
ls Output/fda_rag.jsonl
```

---

## 📊 Pipeline Details

### Step 1: FDA Watcher (Deep Scraping)

The intelligence engine that performs web scraping with production-grade reliability.

**Capabilities:**
- Browser simulation with complete headers (avoids bot detection)
- Session management with cookies (handles Drupal-based FDA site)
- Smart rate limiting (0.5s standard, 2.0s for `/node/` URLs)
- Batch processing (10 records/batch with 5s pauses)
- Retry logic with exponential backoff (3 attempts)
- Change detection via Master Excel comparison

**Execution Modes:**

| Mode | Trigger | Output |
|------|---------|--------|
| **Initial Load** | First run (no Excel exists) | `rag_initial_load.json` + all records |
| **Delta Update** | Excel exists, new records found | `rag_delta_update.json` + new records only |
| **Synchronized** | No new records | No files generated |

```bash
python scripts/fda_watcher.py
```

### Step 2: JSON Split & Clean

Text processing module that transforms raw scraped content into clean, structured data.

**Cleaning Rules:**
- Removes FDA boilerplate (social media links, prescribing info references)
- Smart cutoff detection (stops at "Assessment Aid", "Project Orbis", etc.)
- Preserves dosage information and efficacy data
- Unicode normalization (dashes, quotes → ASCII)
- Whitespace cleanup (preserves paragraph structure)

```bash
python scripts/json_split_and_clean.py
# Or with custom paths:
python scripts/json_split_and_clean.py data/rag_delta_update.json data/processed-json
```

### Step 3: Combine to JSONL

Generates the final JSONL file formatted for vector database ingestion.

**JSONL Output Schema (compatible with any vector database):**
```json
{
  "content": "Full cleaned corpus text...",
  "source": "fda_oncology",
  "url": "https://www.fda.gov/...",
  "date": "2024-01-15",
  "version": "1.0",
  "title": "FDA Approves Drug X for Cancer Y",
  "description": "Short description...",
  "rag_id": "8eb3f836a29121fe5f32fd6c4d8a60a2"
}
```

```bash
python scripts/combine_json_to_jsonl.py
```

---

## 📁 Project Structure

```
fda_rag_extraction/
├── scripts/
│   ├── fda_watcher.py                    # Step 1: Web scraping
│   ├── json_split_and_clean.py           # Step 2: Text cleaning
│   ├── combine_json_to_jsonl.py          # Step 3: JSONL generation
│   └── scheduler.py                      # Optional: automated daily runs
│
├── data/                                 # Auto-created
│   ├── processed-json/                   # Individual cleaned JSON files
│   │   ├── {RAG_ID}.json
│   │   └── ...
│   ├── rag_initial_load.json             # Consolidated (initial run)
│   ├── rag_delta_update.json             # Consolidated (delta runs)
│   └── FDA_Oncology_Master_DB.xlsx       # Master database
│
├── Output/                               # Auto-created
│   └── fda_rag.jsonl                     # Final output for RAG
│
├── logs/                                 # Auto-created
│   ├── fda_watcher.log
│   └── json_split_and_clean.log
│
├── requirements.txt
└── README.md
```

---

## 📄 License

MIT License - See [LICENSE](LICENSE) for details.

---

<p align="center">
  <strong>Built for production RAG pipelines</strong> 🚀
</p>
