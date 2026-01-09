#!/usr/bin/env python3
"""
PDF to Markdown Extraction Pipeline - Phase 1

Extracts text from scientific PDFs using ML-based OCR (Marker library).
Outputs structured JSON with text corpus and metadata for downstream processing.

Author: Adrian Dominguez Castro
License: MIT
"""

import json
import re
import logging
import os
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional

# ML/Deep Learning
import torch

# Marker library for PDF extraction
try:
    from marker.converters.pdf import PdfConverter
    from marker.models import create_model_dict
    MARKER_AVAILABLE = True
except ImportError:
    MARKER_AVAILABLE = False

try:
    from marker.convert import convert_single_pdf
    LEGACY_MARKER = True
except ImportError:
    LEGACY_MARKER = False

from marker.settings import settings
import pypdfium2 as pdfium


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class PipelineConfig:
    """Pipeline configuration settings."""
    input_dir: Path = Path("data/raw")
    output_dir: Path = Path("data/marker_outputs")
    log_dir: Path = Path("logs")
    batch_size: int = 1  # Lower = more accurate, higher = faster
    
    def __post_init__(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class ExtractedDocument:
    """Represents an extracted document with text and metadata."""
    text: str
    metadata: dict
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    def save(self, filepath: Path):
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)


# =============================================================================
# Logging Setup
# =============================================================================

def setup_logging(log_dir: Path) -> logging.Logger:
    """Configure logging for the extraction pipeline."""
    logger = logging.getLogger("pdf_extraction")
    logger.setLevel(logging.INFO)
    
    # File handler
    log_file = log_dir / "extraction.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(message)s")
    )
    logger.addHandler(console_handler)
    
    return logger


# =============================================================================
# Hardware Detection
# =============================================================================

def configure_hardware() -> dict:
    """
    Detect available hardware and configure Marker settings.
    Returns configuration info dict.
    """
    has_gpu = torch.cuda.is_available()
    device = "cuda" if has_gpu else "cpu"
    
    settings.TORCH_DEVICE = device
    os.environ["SURYA_BATCH_SIZE"] = "1"  # Max accuracy
    
    config_info = {
        "device": device,
        "gpu_name": torch.cuda.get_device_name(0) if has_gpu else None,
        "precision": "float16" if has_gpu else "float32"
    }
    
    if has_gpu:
        try:
            settings.TORCH_DTYPE = torch.float16
        except AttributeError:
            pass
    
    return config_info


# =============================================================================
# Metadata Extraction
# =============================================================================

def extract_doi(text: str, max_chars: int = 5000) -> Optional[str]:
    """
    Extract DOI from document text.
    Searches early pages to avoid bibliography references.
    """
    if not text:
        return None
    
    # Focus on first N characters (title page area)
    search_text = text[:max_chars]
    
    # DOI pattern: 10.XXXX/suffix
    pattern = r'10\.\d{4,}/[^\s\]\)>",;]+'
    
    matches = re.findall(pattern, search_text, re.IGNORECASE)
    
    for match in matches:
        # Clean trailing punctuation
        cleaned = re.sub(r'[.,;:\]\)>]+$', '', match)
        
        # Validate format
        if len(cleaned) >= 10 and '/' in cleaned:
            # Skip if looks like bibliography reference
            context_start = max(0, search_text.find(match) - 100)
            context = search_text[context_start:context_start + 200].lower()
            
            if not any(word in context for word in ['reference', 'cited', 'bibliography']):
                return cleaned
    
    return None


def extract_metadata_from_pdf(pdf_path: str, corpus: str = None) -> dict:
    """
    Extract metadata (title, DOI) from PDF file.
    Uses PDF metadata and text content.
    """
    metadata = {"title": None, "doi": None}
    
    try:
        doc = pdfium.PdfDocument(pdf_path)
        pdf_meta = doc.get_metadata_dict(skip_empty=True)
        
        # Get title from PDF metadata
        if pdf_meta:
            title = pdf_meta.get("Title", "").strip()
            if title:
                metadata["title"] = title
        
        # Extract DOI from corpus text
        if corpus:
            doi = extract_doi(corpus)
            if doi:
                metadata["doi"] = doi
        
        # Fallback: check PDF metadata for DOI
        if not metadata["doi"] and pdf_meta:
            for field in ["Subject", "Keywords"]:
                if field in pdf_meta:
                    doi = extract_doi(pdf_meta[field])
                    if doi:
                        metadata["doi"] = doi
                        break
        
        doc.close()
        
    except Exception as e:
        print(f"  Warning: Could not extract metadata: {e}")
    
    return metadata


# =============================================================================
# PDF Conversion
# =============================================================================

class PDFConverter:
    """Handles PDF to Markdown conversion using Marker."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.models = None
        self.converter = None
        self._load_models()
    
    def _load_models(self):
        """Load Marker ML models."""
        self.logger.info("Loading Marker models...")
        
        if not MARKER_AVAILABLE:
            raise RuntimeError("Marker library not available. Install with: pip install marker-pdf")
        
        try:
            model_dict = create_model_dict()
            self.converter = PdfConverter(model_dict)
            self.logger.info("Models loaded successfully")
        except Exception as e:
            self.logger.warning(f"Could not initialize PdfConverter: {e}")
            self.converter = None
            self.models = model_dict if 'model_dict' in dir() else None
    
    def convert(self, pdf_path: Path) -> Optional[str]:
        """
        Convert PDF to Markdown text.
        Returns extracted text or None on failure.
        """
        try:
            # Try PdfConverter first
            if self.converter:
                result = self._convert_with_converter(pdf_path)
                if result:
                    return result
            
            # Fallback to legacy API
            if LEGACY_MARKER and self.models:
                result = self._convert_legacy(pdf_path)
                if result:
                    return result
            
            return None
            
        except Exception as e:
            self.logger.error(f"Conversion failed for {pdf_path.name}: {e}")
            return None
    
    def _convert_with_converter(self, pdf_path: Path) -> Optional[str]:
        """Convert using PdfConverter API."""
        try:
            result = self.converter(str(pdf_path))
            
            if isinstance(result, str):
                return result.strip()
            elif hasattr(result, 'text'):
                return result.text.strip()
            elif hasattr(result, 'get_markdown'):
                return result.get_markdown().strip()
            else:
                return str(result).strip()
                
        except Exception:
            return None
    
    def _convert_legacy(self, pdf_path: Path) -> Optional[str]:
        """Convert using legacy convert_single_pdf API."""
        try:
            text, _, _ = convert_single_pdf(str(pdf_path), self.models, batch_multiplier=1)
            return text.strip() if text else None
        except Exception:
            return None


# =============================================================================
# Main Pipeline
# =============================================================================

def process_pdf(pdf_path: Path, converter: PDFConverter, logger: logging.Logger) -> Optional[ExtractedDocument]:
    """
    Process a single PDF file.
    Returns ExtractedDocument or None on failure.
    """
    logger.info(f"Processing: {pdf_path.name}")
    
    # Convert PDF to text
    corpus = converter.convert(pdf_path)
    
    if not corpus:
        logger.warning(f"  Empty extraction for {pdf_path.name}")
        return None
    
    # Extract metadata
    metadata = extract_metadata_from_pdf(str(pdf_path), corpus)
    
    logger.info(f"  Title: {metadata['title'] or 'Not found'}")
    logger.info(f"  DOI: {metadata['doi'] or 'Not found'}")
    logger.info(f"  Text length: {len(corpus):,} chars")
    
    return ExtractedDocument(text=corpus, metadata=metadata)


def run_extraction_pipeline(config: PipelineConfig = None):
    """
    Run the complete PDF extraction pipeline.
    """
    if config is None:
        config = PipelineConfig()
    
    # Setup
    logger = setup_logging(config.log_dir)
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("PDF EXTRACTION PIPELINE")
    logger.info(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    # Configure hardware
    hw_config = configure_hardware()
    logger.info(f"Device: {hw_config['device'].upper()}")
    if hw_config['gpu_name']:
        logger.info(f"GPU: {hw_config['gpu_name']}")
    
    # Find PDFs
    pdf_files = list(config.input_dir.glob("*.pdf"))
    if not pdf_files:
        logger.warning(f"No PDF files found in {config.input_dir}")
        return
    
    logger.info(f"Found {len(pdf_files)} PDF(s)")
    
    # Initialize converter
    try:
        converter = PDFConverter(logger)
    except Exception as e:
        logger.error(f"Failed to initialize converter: {e}")
        return
    
    # Process files
    stats = {"success": 0, "failed": 0, "skipped": 0}
    
    for pdf_path in pdf_files:
        output_file = config.output_dir / f"{pdf_path.stem}.json"
        
        # Skip if already processed
        if output_file.exists():
            logger.info(f"Skipping {pdf_path.name} (already processed)")
            stats["skipped"] += 1
            continue
        
        # Process
        doc = process_pdf(pdf_path, converter, logger)
        
        if doc:
            doc.save(output_file)
            logger.info(f"  Saved: {output_file.name}")
            stats["success"] += 1
        else:
            stats["failed"] += 1
    
    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    
    logger.info("=" * 60)
    logger.info("EXTRACTION COMPLETE")
    logger.info(f"Success: {stats['success']}, Failed: {stats['failed']}, Skipped: {stats['skipped']}")
    logger.info(f"Time: {elapsed:.1f}s")
    logger.info("=" * 60)


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    run_extraction_pipeline()
