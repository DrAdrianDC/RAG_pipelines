#!/usr/bin/env python3
"""
JSON to JSONL Converter - Phase 4

Combines individual JSON files into a single JSONL file for database ingestion.
Transforms fields to standard format for vector databases.

Author: Adrian Dominguez Castro
License: MIT
"""

import json
import re
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Iterator, Dict, Any
import argparse


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class ConverterConfig:
    """Configuration for JSON to JSONL conversion."""
    input_dir: Path = Path("data/processed")
    output_dir: Path = Path("Output")
    source_name: str = "pdf_extraction"
    
    def __post_init__(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)


# =============================================================================
# Field Transformation
# =============================================================================

def extract_year(citation: str) -> str:
    """Extract year from citation string."""
    match = re.search(r'\((\d{4})\)', citation)
    return match.group(1) if match else datetime.now().strftime("%Y")


def clean_content(text: str) -> str:
    """Clean document content for database storage."""
    if not text:
        return ""
    
    # Remove markdown image tags
    text = re.sub(r'!\[.*?\]\(.*?\)', '', text)
    
    # Normalize newlines
    text = text.replace("\\n", "\n")
    
    # Remove excess whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text.strip()


def transform_document(doc: Dict[str, Any], source: str) -> Dict[str, Any]:
    """
    Transform document to output format.
    
    Input format:  {Title, Citation, Link, Corpus}
    Output format: {content, source, url, date, title, citation, ...}
    """
    output = {
        "content": clean_content(doc.get("Corpus", "")),
        "source": source,
        "url": doc.get("Link", ""),
        "date": extract_year(doc.get("Citation", "")),
        "version": "1.0",
        "title": doc.get("Title", ""),
        "citation": doc.get("Citation", "")
    }
    
    return output


# =============================================================================
# File Processing
# =============================================================================

def load_json_file(filepath: Path) -> Iterator[Dict[str, Any]]:
    """Load JSON file and yield document(s)."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            yield from data
        elif isinstance(data, dict):
            yield data
            
    except Exception as e:
        print(f"Error loading {filepath}: {e}")


def find_json_files(directory: Path) -> Iterator[Path]:
    """Find all JSON files in directory."""
    return sorted(directory.glob("*.json"))


# =============================================================================
# Main Converter
# =============================================================================

def convert_to_jsonl(config: ConverterConfig = None, transform: bool = True) -> dict:
    """
    Convert JSON files to JSONL format.
    
    Returns statistics dict.
    """
    if config is None:
        config = ConverterConfig()
    
    print("=" * 60)
    print("JSON TO JSONL CONVERTER")
    print("=" * 60)
    print(f"Input:  {config.input_dir}")
    print(f"Output: {config.output_dir}")
    print(f"Source: {config.source_name}")
    print("=" * 60)
    
    # Find input files
    json_files = list(find_json_files(config.input_dir))
    
    if not json_files:
        print(f"No JSON files found in {config.input_dir}")
        return {"files": 0, "documents": 0, "errors": 0}
    
    print(f"Found {len(json_files)} JSON file(s)")
    
    # Output file
    output_file = config.output_dir / f"{config.source_name}.jsonl"
    
    # Process
    stats = {"files": 0, "documents": 0, "errors": 0}
    
    with open(output_file, 'w', encoding='utf-8') as out:
        for filepath in json_files:
            try:
                for doc in load_json_file(filepath):
                    if transform:
                        doc = transform_document(doc, config.source_name)
                    
                    out.write(json.dumps(doc, ensure_ascii=False) + '\n')
                    stats["documents"] += 1
                
                stats["files"] += 1
                
                if stats["files"] % 10 == 0:
                    print(f"Processed {stats['files']}/{len(json_files)} files...")
                    
            except Exception as e:
                print(f"Error processing {filepath}: {e}")
                stats["errors"] += 1
    
    # Summary
    print("-" * 60)
    print("CONVERSION COMPLETE")
    print(f"Files processed: {stats['files']}")
    print(f"Documents written: {stats['documents']}")
    print(f"Errors: {stats['errors']}")
    print(f"Output: {output_file}")
    
    if output_file.exists():
        size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"Size: {size_mb:.2f} MB")
    
    print("=" * 60)
    
    return stats


# =============================================================================
# CLI Interface
# =============================================================================

def main():
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description="Convert JSON files to JSONL format"
    )
    
    parser.add_argument(
        '-i', '--input',
        default='data/processed',
        help='Input directory with JSON files'
    )
    
    parser.add_argument(
        '-o', '--output',
        default='Output',
        help='Output directory for JSONL file'
    )
    
    parser.add_argument(
        '-s', '--source',
        default='pdf_extraction',
        help='Source name for output'
    )
    
    parser.add_argument(
        '--no-transform',
        action='store_true',
        help='Skip field transformation'
    )
    
    args = parser.parse_args()
    
    config = ConverterConfig(
        input_dir=Path(args.input),
        output_dir=Path(args.output),
        source_name=args.source
    )
    
    convert_to_jsonl(config, transform=not args.no_transform)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) == 1:
        # Default run
        convert_to_jsonl()
    else:
        main()
