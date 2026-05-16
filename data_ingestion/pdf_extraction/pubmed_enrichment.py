#!/usr/bin/env python3
"""
PubMed Metadata Enrichment Pipeline - Phases 2 & 3

Enriches extracted documents with validated metadata from PubMed.
Validates document identity using DOI and title matching.

Author: Adrian Dominguez Castro
License: MIT
"""

import json
import shutil
import time
import logging
import re
import os
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List
from difflib import SequenceMatcher
import xml.etree.ElementTree as ET

import requests
from pydantic import BaseModel, Field


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class EnrichmentConfig:
    """Configuration for PubMed enrichment pipeline."""
    input_dir: Path = Path("data/marker_outputs")
    output_dir: Path = Path("data/processed")
    failed_dir: Path = Path("data/failed")
    log_dir: Path = Path("logs")
    
    # PubMed API settings (use env vars for API key)
    api_key: str = field(default_factory=lambda: os.environ.get("PUBMED_API_KEY", ""))
    email: str = field(default_factory=lambda: os.environ.get("PUBMED_EMAIL", "user@example.com"))
    
    def __post_init__(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.failed_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Rate limit based on API key presence
        self.request_delay = 0.1 if self.api_key else 0.35


# =============================================================================
# Data Models
# =============================================================================

class EnrichedDocument(BaseModel):
    """Schema for enriched scientific document."""
    Title: str = Field(..., description="Document title")
    Citation: str = Field(..., description="Formatted citation")
    Link: str = Field(..., description="URL to document")
    Corpus: str = Field(..., description="Document text content")


@dataclass
class PubMedResult:
    """Result from PubMed search."""
    pmid: str
    title: str
    doi: Optional[str]
    authors: List[str]
    journal: str
    year: str
    citation: str
    link: str


# =============================================================================
# Logging
# =============================================================================

def setup_logger(log_dir: Path) -> logging.Logger:
    """Setup logging for enrichment pipeline."""
    logger = logging.getLogger("pubmed_enrichment")
    logger.setLevel(logging.INFO)
    
    handler = logging.FileHandler(log_dir / "enrichment.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console)
    
    return logger


# =============================================================================
# PubMed API Client
# =============================================================================

class PubMedClient:
    """Client for interacting with PubMed E-utilities API."""
    
    BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    
    def __init__(self, config: EnrichmentConfig):
        self.config = config
        self.session = requests.Session()
    
    def _make_request(self, endpoint: str, params: dict) -> Optional[requests.Response]:
        """Make API request with rate limiting."""
        if self.config.api_key:
            params["api_key"] = self.config.api_key
        params["email"] = self.config.email
        
        try:
            response = self.session.get(
                f"{self.BASE_URL}/{endpoint}",
                params=params,
                timeout=15
            )
            response.raise_for_status()
            time.sleep(self.config.request_delay)
            return response
        except Exception:
            return None
    
    def search_by_doi(self, doi: str) -> Optional[str]:
        """Search PubMed by DOI. Returns PMID if found."""
        params = {
            "db": "pubmed",
            "term": f'"{doi}"[DOI]',
            "retmode": "json",
            "retmax": 1
        }
        
        response = self._make_request("esearch.fcgi", params)
        if response:
            data = response.json()
            ids = data.get("esearchresult", {}).get("idlist", [])
            return ids[0] if ids else None
        return None
    
    def search_by_title(self, title: str, use_field: bool = True) -> Optional[str]:
        """Search PubMed by title. Returns PMID if found."""
        # Clean title for search
        clean_title = re.sub(r'[;:,]', ' ', title.strip())
        
        search_term = f'"{clean_title}"[Title]' if use_field else clean_title
        
        params = {
            "db": "pubmed",
            "term": search_term,
            "retmode": "json",
            "retmax": 1
        }
        
        response = self._make_request("esearch.fcgi", params)
        if response:
            data = response.json()
            ids = data.get("esearchresult", {}).get("idlist", [])
            return ids[0] if ids else None
        return None
    
    def fetch_details(self, pmid: str) -> Optional[PubMedResult]:
        """Fetch article details from PubMed."""
        params = {
            "db": "pubmed",
            "id": pmid,
            "retmode": "xml",
            "rettype": "abstract"
        }
        
        response = self._make_request("efetch.fcgi", params)
        if not response:
            return None
        
        try:
            return self._parse_pubmed_xml(response.content, pmid)
        except Exception:
            return None
    
    def _parse_pubmed_xml(self, xml_content: bytes, pmid: str) -> Optional[PubMedResult]:
        """Parse PubMed XML response."""
        root = ET.fromstring(xml_content)
        article = root.find(".//PubmedArticle")
        
        if article is None:
            return None
        
        # Extract fields
        title_el = article.find(".//ArticleTitle")
        title = title_el.text if title_el is not None else "Unknown"
        
        doi_el = article.find(".//ArticleId[@IdType='doi']")
        doi = doi_el.text if doi_el is not None else None
        
        journal_el = article.find(".//Journal/Title")
        journal = journal_el.text if journal_el is not None else "Unknown"
        
        year_el = article.find(".//PubDate/Year")
        year = year_el.text if year_el is not None else "Unknown"
        
        # Extract authors
        authors = []
        for author in article.findall(".//Author"):
            last = author.find("LastName")
            first = author.find("ForeName")
            if last is not None:
                name = f"{last.text}, {first.text}" if first is not None else last.text
                authors.append(name)
        
        # Build citation
        author_str = self._format_authors(authors)
        citation = f"{author_str}. ({year}). {title}. {journal}"
        if doi:
            citation += f". https://doi.org/{doi}"
        
        # Build link
        link = f"https://doi.org/{doi}" if doi else f"https://pubmed.ncbi.nlm.nih.gov/{pmid}"
        
        return PubMedResult(
            pmid=pmid,
            title=title,
            doi=doi,
            authors=authors,
            journal=journal,
            year=year,
            citation=citation,
            link=link
        )
    
    @staticmethod
    def _format_authors(authors: List[str]) -> str:
        """Format author list for citation."""
        if not authors:
            return "Unknown"
        if len(authors) == 1:
            return authors[0]
        if len(authors) <= 3:
            return ", ".join(authors[:-1]) + f", & {authors[-1]}"
        return f"{authors[0]} et al."


# =============================================================================
# Document Verification
# =============================================================================

class DocumentVerifier:
    """Verifies document identity against PubMed results."""
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """Normalize text for comparison."""
        text = text.lower().strip()
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text
    
    @staticmethod
    def normalize_doi(doi: str) -> str:
        """Normalize DOI for comparison."""
        doi = doi.strip().lower()
        doi = re.sub(r'^https?://doi\.org/', '', doi)
        doi = re.sub(r'^doi:\s*', '', doi)
        return doi
    
    def verify(self, local_title: str, local_doi: str, 
               pubmed_result: PubMedResult) -> tuple[bool, bool]:
        """
        Verify document against PubMed result.
        Returns (is_verified, allow_doi_in_output).
        """
        # DOI match check
        if local_doi and pubmed_result.doi:
            local_norm = self.normalize_doi(local_doi)
            pub_norm = self.normalize_doi(pubmed_result.doi)
            
            if local_norm == pub_norm:
                return True, True  # Verified with DOI match
            else:
                return False, False  # DOI conflict
        
        # Title similarity check
        if local_title and pubmed_result.title:
            local_norm = self.normalize_text(local_title)
            pub_norm = self.normalize_text(pubmed_result.title)
            
            similarity = SequenceMatcher(None, local_norm, pub_norm).ratio()
            
            if similarity >= 0.90:
                return True, False  # Verified by title, no DOI output
        
        return False, False


# =============================================================================
# Enrichment Pipeline
# =============================================================================

class EnrichmentPipeline:
    """Main enrichment pipeline."""
    
    def __init__(self, config: EnrichmentConfig = None):
        self.config = config or EnrichmentConfig()
        self.logger = setup_logger(self.config.log_dir)
        self.client = PubMedClient(self.config)
        self.verifier = DocumentVerifier()
    
    def run(self):
        """Run the enrichment pipeline."""
        start_time = datetime.now()
        
        self.logger.info("=" * 60)
        self.logger.info("PUBMED ENRICHMENT PIPELINE")
        self.logger.info(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 60)
        
        if self.config.api_key:
            self.logger.info("API key detected (10 req/sec)")
        else:
            self.logger.info("No API key (3 req/sec) - set PUBMED_API_KEY for faster processing")
        
        # Find input files
        json_files = list(self.config.input_dir.glob("*.json"))
        if not json_files:
            self.logger.warning(f"No JSON files in {self.config.input_dir}")
            return
        
        self.logger.info(f"Found {len(json_files)} file(s)")
        
        # Process
        stats = {"success": 0, "failed": 0, "skipped": 0}
        
        for json_file in json_files:
            output_file = self.config.output_dir / f"{json_file.stem}_final.json"
            
            if output_file.exists():
                self.logger.info(f"Skipping {json_file.name} (exists)")
                stats["skipped"] += 1
                continue
            
            if self._process_file(json_file, output_file):
                stats["success"] += 1
            else:
                stats["failed"] += 1
        
        # Summary
        elapsed = (datetime.now() - start_time).total_seconds()
        self.logger.info("=" * 60)
        self.logger.info("ENRICHMENT COMPLETE")
        self.logger.info(f"Success: {stats['success']}, Failed: {stats['failed']}, Skipped: {stats['skipped']}")
        self.logger.info(f"Time: {elapsed:.1f}s")
        self.logger.info("=" * 60)
    
    def _process_file(self, input_file: Path, output_file: Path) -> bool:
        """Process a single file."""
        self.logger.info(f"Processing: {input_file.name}")
        
        try:
            # Load input
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            corpus = data.get("text", "")
            metadata = data.get("metadata", {})
            local_title = metadata.get("title")
            local_doi = metadata.get("doi")
            
            if not corpus:
                self._move_to_failed(input_file, "Empty corpus")
                return False
            
            # Search PubMed
            pubmed_result = self._search_pubmed(local_doi, local_title)
            
            # Build output
            doc = self._build_output(corpus, local_title, local_doi, pubmed_result)
            
            # Save
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(doc.model_dump(), f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"  Saved: {output_file.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"  Error: {e}")
            self._move_to_failed(input_file, str(e))
            return False
    
    def _search_pubmed(self, doi: str, title: str) -> Optional[PubMedResult]:
        """Search PubMed by DOI then title."""
        pmid = None
        
        # Try DOI first
        if doi:
            self.logger.info(f"  Searching by DOI: {doi}")
            pmid = self.client.search_by_doi(doi)
            if pmid:
                self.logger.info(f"  Found PMID: {pmid}")
        
        # Try title
        if not pmid and title:
            self.logger.info(f"  Searching by title: {title[:60]}...")
            pmid = self.client.search_by_title(title, use_field=True)
            
            if not pmid:
                pmid = self.client.search_by_title(title, use_field=False)
            
            if pmid:
                self.logger.info(f"  Found PMID: {pmid}")
        
        # Fetch details
        if pmid:
            return self.client.fetch_details(pmid)
        
        return None
    
    def _build_output(self, corpus: str, local_title: str, local_doi: str,
                      pubmed_result: Optional[PubMedResult]) -> EnrichedDocument:
        """Build output document."""
        
        if pubmed_result:
            verified, use_doi = self.verifier.verify(local_title, local_doi, pubmed_result)
            
            if verified:
                link = pubmed_result.link if use_doi else f"https://pubmed.ncbi.nlm.nih.gov/{pubmed_result.pmid}"
                return EnrichedDocument(
                    Title=pubmed_result.title,
                    Citation=pubmed_result.citation,
                    Link=link,
                    Corpus=corpus
                )
        
        # Fallback - unverified
        self.logger.info("  Using fallback (unverified)")
        fallback_title = local_title or "Unknown Title"
        fallback_citation = f"Document. {fallback_title}. (Unverified)"
        
        return EnrichedDocument(
            Title=fallback_title,
            Citation=fallback_citation,
            Link="https://pubmed.ncbi.nlm.nih.gov",
            Corpus=corpus
        )
    
    def _move_to_failed(self, file: Path, reason: str):
        """Move file to failed directory."""
        try:
            shutil.move(str(file), str(self.config.failed_dir / file.name))
            self.logger.warning(f"  Moved to failed: {reason}")
        except Exception as e:
            self.logger.error(f"  Could not move file: {e}")


# =============================================================================
# Entry Point
# =============================================================================

def main():
    """Run enrichment pipeline."""
    pipeline = EnrichmentPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
