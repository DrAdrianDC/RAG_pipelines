#!/usr/bin/env python3
"""
Script to combine JSON files from FDA extraction into JSONL format for vector databases.

This script:
1. Finds all JSON files in the specified directory (FDA oncology format)
2. Reads each JSON file (can be a single object or an array of objects)
3. Transforms each object to the format expected by RAG systems
4. Converts each object to a line in JSONL format
5. Generates a JSONL file with all documents

JSONL Format: Each line is a valid JSON object, separated by newlines.
Expected format: RAG_ID, Title, Webpage, Description, Date, Corpus
"""

import json
import os
import re
from pathlib import Path
from typing import List, Dict, Any, Union
import argparse
from collections import defaultdict
from datetime import datetime


def load_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """
    Loads a JSON file and returns a list of objects.
    
    Handles two cases:
    - If JSON is a single object: converts it to a list with one element
    - If JSON is an array: returns all objects in the array
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        List of dictionaries (JSON objects)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # If it's a single object (dict), convert to list
        if isinstance(data, dict):
            return [data]
        # If it's an array, return as is
        elif isinstance(data, list):
            return data
        else:
            print(f"⚠️  Warning: {file_path} contains unexpected type: {type(data)}")
            return []
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON in {file_path}: {e}")
        return []
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")
        return []


def transform_to_rag_format(obj: Dict[str, Any], source_name: str) -> Dict[str, Any]:
    """
    Transforms a FDA JSON object to the format expected by RAG systems.
    
    Output fields:
    - content: Main document content (Corpus)
    - source: Source name (e.g., "fda_oncology")
    - url: Document URL (Webpage)
    - date: Document date (Date)
    - version: Document version
    
    Args:
        obj: Original JSON object in FDA format
        source_name: Source name (derived from directory)
        
    Returns:
        Transformed object with RAG-compatible fields
    """
    transformed = {}
    
    # FDA format: Title, Webpage, Description, Date, Corpus, RAG_ID
    transformed["content"] = obj.get("Corpus", obj.get("Description", ""))
    transformed["source"] = source_name
    transformed["url"] = obj.get("Webpage", "")
    transformed["date"] = obj.get("Date", "")
    transformed["version"] = "1.0"
    
    # Preserve original fields as additional metadata
    if "Title" in obj:
        transformed["title"] = obj["Title"]
    if "Description" in obj:
        transformed["description"] = obj["Description"]
    if "RAG_ID" in obj:
        transformed["rag_id"] = obj["RAG_ID"]
    
    # Ensure required fields are not empty
    if not transformed.get("content"):
        transformed["content"] = str(obj)  # Fallback: use entire object as content
    
    if not transformed.get("date"):
        transformed["date"] = datetime.now().strftime("%Y-%m-%d")
    
    if not transformed.get("version"):
        transformed["version"] = "1.0"
    
    # Content cleanup: remove artifacts from marker tool
    content = transformed.get("content", "")
    
    # 1. Wrapper cleanup: remove "markdown='" prefix and trailing quote
    if content.startswith("markdown='"):
        content = content[10:]  # Remove first 10 characters
        if content.endswith("'"):
            content = content[:-1]  # Remove trailing single quote
    
    # 2. Image cleanup: remove Markdown image tags (won't be uploaded to cloud)
    content = re.sub(r'!\[.*?\]\(.*?\)', '', content)
    
    # 3. Normalization: convert escaped newlines and clean extra whitespace
    content = content.replace("\\n", "\n").strip()
    
    transformed["content"] = content
    
    return transformed


def find_json_files_in_directory(directory: str, recursive: bool = True) -> List[Path]:
    """
    Finds all JSON files in a specific directory.
    
    Args:
        directory: Directory path to search
        recursive: If True, searches recursively in subdirectories
        
    Returns:
        List of paths to JSON files found
    """
    json_files = []
    dir_path = Path(directory)
    
    if not dir_path.exists():
        print(f"⚠️  Warning: Directory {directory} does not exist")
        return []
    
    if recursive:
        # Search recursively
        json_files.extend(dir_path.rglob("*.json"))
    else:
        # Only in current directory
        json_files.extend(dir_path.glob("*.json"))
    
    return sorted(json_files)


def combine_json_to_jsonl(
    input_directory: str,
    output_file: str,
    recursive: bool = True,
    add_source_info: bool = False,
    transform_for_rag: bool = True
) -> Dict[str, Any]:
    """
    Combines JSON files from a directory into a JSONL file.
    
    Args:
        input_directory: Directory to search for JSON files
        output_file: Output JSONL file path
        recursive: If True, searches recursively in subdirectories
        add_source_info: If True, adds source file information to each object
        transform_for_rag: If True, transforms fields to RAG-compatible format
        
    Returns:
        Dictionary with process statistics
    """
    print(f"\n🔍 Processing directory: {input_directory}")
    json_files = find_json_files_in_directory(input_directory, recursive=recursive)
    
    if not json_files:
        print(f"⚠️  No JSON files found in {input_directory}")
        return {
            "total_files": 0,
            "total_objects": 0,
            "errors": 0,
            "files_processed": 0
        }
    
    print(f"📁 Found {len(json_files)} JSON files")
    
    # Determine source name based on directory
    dir_path = Path(input_directory).resolve()
    
    # If we're in a subdirectory like "processed-json" or "processed", look higher
    if dir_path.name in ["processed", "processed-json", "data"]:
        # Search up the hierarchy for a directory with "fda" or "rag" in the name
        current = dir_path.parent
        source_name = None
        
        # Search up to 3 levels
        for _ in range(3):
            if current and current.name:
                # If name contains "fda" or "rag", use it
                if "fda" in current.name.lower() or "rag" in current.name.lower():
                    source_name = current.name
                    break
                # If not "data" or "processed", it might also be valid
                elif current.name.lower() not in ["data", "processed", "processed-json"]:
                    source_name = current.name
                    break
            current = current.parent
        
        # If nothing found, use "fda_oncology" as default
        if not source_name:
            source_name = "fda_oncology"
    else:
        source_name = dir_path.name
    
    # Normalize source name: always use "fda_oncology"
    if "fda" in source_name.lower():
        source_name = "fda_oncology"
    
    if transform_for_rag:
        print(f"🔄 Transforming fields to RAG format (source: {source_name})")
    
    # Statistics
    stats = {
        "total_files": len(json_files),
        "total_objects": 0,
        "errors": 0,
        "files_processed": 0,
        "objects_per_file": defaultdict(int)
    }
    
    # Create output directory if it doesn't exist
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"📝 Writing to: {output_file}")
    print("-" * 60)
    
    # Process each JSON file
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for idx, json_file in enumerate(json_files, 1):
            try:
                # Load objects from file
                objects = load_json_file(json_file)
                
                if not objects:
                    stats["errors"] += 1
                    continue
                
                # Write each object as a JSONL line
                for obj in objects:
                    # Transform to RAG format if enabled
                    if transform_for_rag:
                        obj = transform_to_rag_format(obj, source_name)
                    
                    # Optionally add source file information
                    if add_source_info:
                        obj["_source_file"] = str(json_file)
                        obj["_source_relative"] = str(json_file.relative_to(Path.cwd()))
                    
                    # Write as a JSON line (no spaces, compact)
                    json_line = json.dumps(obj, ensure_ascii=False)
                    outfile.write(json_line + '\n')
                    
                    stats["total_objects"] += 1
                    stats["objects_per_file"][json_file.name] += 1
                
                stats["files_processed"] += 1
                
                # Show progress every 10 files
                if idx % 10 == 0:
                    print(f"📊 Processed {idx}/{len(json_files)} files... "
                          f"({stats['total_objects']} objects so far)")
                
            except Exception as e:
                print(f"❌ Error processing {json_file}: {e}")
                stats["errors"] += 1
    
    print("-" * 60)
    print(f"✅ Directory {input_directory} completed!")
    print(f"📊 Statistics:")
    print(f"   - Files processed: {stats['files_processed']}/{stats['total_files']}")
    print(f"   - Total objects written: {stats['total_objects']}")
    print(f"   - Errors: {stats['errors']}")
    print(f"   - Output file: {output_file}")
    if output_path.exists():
        print(f"   - File size: {output_path.stat().st_size / (1024*1024):.2f} MB")
    
    return stats


def process_multiple_directories(
    input_directories: List[str],
    output_dir: str = "Output",
    recursive: bool = True,
    add_source_info: bool = False,
    transform_for_rag: bool = True
) -> Dict[str, Any]:
    """
    Processes multiple directories and generates a separate JSONL for each.
    
    Args:
        input_directories: List of directories to process
        output_dir: Directory to save JSONL files
        recursive: If True, searches recursively in subdirectories
        add_source_info: If True, adds source file information to each object
        transform_for_rag: If True, transforms fields to RAG-compatible format
        
    Returns:
        Dictionary with general statistics
    """
    print("=" * 70)
    print("🔧 JSON to JSONL Combiner - FDA Extraction for RAG Systems")
    print("=" * 70)
    print()
    print(f"📂 Directories to process: {len(input_directories)}")
    for dir_path in input_directories:
        print(f"   - {dir_path}")
    print()
    print(f"📁 Output directory: {output_dir}")
    print("=" * 70)
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # General statistics
    all_stats = {
        "directories_processed": 0,
        "total_files": 0,
        "total_objects": 0,
        "total_errors": 0,
        "output_files": []
    }
    
    # Process each directory separately
    for directory in input_directories:
        dir_path = Path(directory)
        
        # Fixed output JSONL filename
        output_filename = "fda_rag.jsonl"
        
        output_file = output_path / output_filename
        
        # Process this directory
        stats = combine_json_to_jsonl(
            input_directory=directory,
            output_file=str(output_file),
            recursive=recursive,
            add_source_info=add_source_info,
            transform_for_rag=transform_for_rag
        )
        
        # Accumulate statistics
        all_stats["directories_processed"] += 1
        all_stats["total_files"] += stats["total_files"]
        all_stats["total_objects"] += stats["total_objects"]
        all_stats["total_errors"] += stats["errors"]
        all_stats["output_files"].append(str(output_file))
    
    # Final summary
    print()
    print("=" * 70)
    print("🎉 Process completed for all directories!")
    print("=" * 70)
    print(f"📊 General summary:")
    print(f"   - Directories processed: {all_stats['directories_processed']}")
    print(f"   - Total JSON files: {all_stats['total_files']}")
    print(f"   - Total objects written: {all_stats['total_objects']}")
    print(f"   - Total errors: {all_stats['total_errors']}")
    print()
    print("📄 Generated JSONL files:")
    for output_file in all_stats["output_files"]:
        file_path = Path(output_file)
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024*1024)
            print(f"   ✅ {output_file} ({size_mb:.2f} MB)")
        else:
            print(f"   ⚠️  {output_file} (not generated)")
    
    return all_stats


def main():
    """Main function with command line arguments"""
    parser = argparse.ArgumentParser(
        description="Combines JSON files from FDA extraction into JSONL format for RAG systems",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage examples:

  # Process FDA extraction directory (generates fda_rag.jsonl)
  python combine_json_to_jsonl.py -d data/processed-json

  # Specify custom output directory
  python combine_json_to_jsonl.py -d data/processed-json -o my_output

  # Include source file information
  python combine_json_to_jsonl.py -d data/processed-json --add-source

  # Only search in main directory (non-recursive)
  python combine_json_to_jsonl.py -d data/processed-json --no-recursive
        """
    )
    
    parser.add_argument(
        '-d', '--directories',
        nargs='+',
        required=True,
        help='Directories to search for JSON files (generates one JSONL per directory)'
    )
    
    parser.add_argument(
        '-o', '--output-dir',
        default='Output',
        help='Directory to save JSONL files (default: Output)'
    )
    
    parser.add_argument(
        '--no-recursive',
        action='store_true',
        help='Do not search recursively in subdirectories'
    )
    
    parser.add_argument(
        '--add-source',
        action='store_true',
        help='Add _source_file and _source_relative fields to each object'
    )
    
    parser.add_argument(
        '--no-transform',
        action='store_true',
        help='Do not transform fields to RAG format (preserve original fields)'
    )
    
    args = parser.parse_args()
    
    # Execute the process
    stats = process_multiple_directories(
        input_directories=args.directories,
        output_dir=args.output_dir,
        recursive=not args.no_recursive,
        add_source_info=args.add_source,
        transform_for_rag=not args.no_transform
    )
    
    return 0 if stats["total_errors"] == 0 else 1


# Direct usage example (without command line arguments)
if __name__ == "__main__":
    import sys
    
    # If executed without arguments, use default configuration
    if len(sys.argv) == 1:
        # Get the directory where the script is located
        # This makes the script portable and works from any location
        script_dir = Path(__file__).parent.absolute()
        
        # Change to script directory to ensure correct relative paths
        os.chdir(script_dir)
        
        # Look for processed-json directory
        # Expected structure:
        #   scripts/
        #   data/
        #     processed-json/
        if script_dir.name == "scripts":
            # We're in fda_rag_extraction/scripts/, look in ../data/processed-json
            processed_json_dir = script_dir.parent / "data" / "processed-json"
            output_dir = script_dir.parent / "Output"
        else:
            # We're at root or elsewhere, look in data/processed-json
            processed_json_dir = script_dir / "data" / "processed-json"
            output_dir = script_dir / "Output"
        
        # Verify directory exists
        if not processed_json_dir.exists():
            print(f"⚠️  Warning: Directory {processed_json_dir} does not exist")
            print(f"   Searching in alternative locations...")
            # Try searching in other common locations
            alt_locations = [
                script_dir.parent / "data" / "processed-json",  # If we're in scripts/
                script_dir / "data" / "processed-json",  # If we're at root
                script_dir.parent / "processed-json",  # Fallback: directly in parent
            ]
            
            found = False
            for alt_dir in alt_locations:
                if alt_dir.exists():
                    processed_json_dir = alt_dir
                    # Output should be at same level as data/, not inside data/
                    if alt_dir.parent.name == "data":
                        output_dir = alt_dir.parent.parent / "Output"
                    else:
                        output_dir = alt_dir.parent / "Output"
                    print(f"   ✅ Found at: {processed_json_dir}")
                    found = True
                    break
            
            if not found:
                print(f"   ❌ processed-json directory not found")
                print(f"   Run script with arguments: python combine_json_to_jsonl.py -d <directory>")
                sys.exit(1)
        
        default_directories = [str(processed_json_dir)]
        default_output_dir = str(output_dir)
        
        stats = process_multiple_directories(
            input_directories=default_directories,
            output_dir=default_output_dir,
            recursive=True,
            add_source_info=False,
            transform_for_rag=True
        )
    else:
        # Execute with command line arguments
        sys.exit(main())

