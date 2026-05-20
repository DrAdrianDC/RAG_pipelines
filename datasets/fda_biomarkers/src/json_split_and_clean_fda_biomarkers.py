"""
JSON SPLIT AND CLEAN - FDA Biomarkers
---------------------------------------
Reads final_output.json, cleans the content field for chunking,
and splits each biomarker record into individual JSON files with cleaned content field.
"""

import json
import os
import sys
import re


def clean_content_for_chunking(content: str) -> str:
    """
    Cleans the content field for optimal chunking and embeddings.
    
    This function:
    - Replaces newlines with spaces (for better chunking)
    - Normalizes multiple spaces to single spaces
    - Removes leading/trailing whitespace
    
    Args:
        content: Raw content text with newlines
        
    Returns:
        Cleaned content text ready for chunking
    """
    if not content:
        return ""
    
    # Replace \n with spaces
    cleaned = content.replace('\n', ' ')
    
    # Normalize multiple spaces to single space
    cleaned = re.sub(r' +', ' ', cleaned)
    
    # Remove leading/trailing whitespace
    cleaned = cleaned.strip()
    
    return cleaned


def generate_filename(drug_name: str, biomarker: str) -> str:
    """
    Generates a sanitized, readable filename based on drug and biomarker.
    
    Creates a filename by combining drug_name and biomarker, sanitizing
    it for filesystem use (removes invalid characters, normalizes spaces).
    
    Args:
        drug_name: Drug name (e.g., "Abacavir" or "Abemaciclib (1)")
        biomarker: Biomarker name (e.g., "HLA-B" or "ERBB2 (HER2)")
        
    Returns:
        Sanitized filename string (lowercase, no invalid characters)
    """
    # Combine drug and biomarker
    combined = f"{drug_name}_{biomarker}"
    
    # Replace invalid filesystem characters with underscore
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', combined)
    
    # Replace spaces, parentheses, and other separators with underscore
    sanitized = re.sub(r'[\s()]+', '_', sanitized)
    
    # Replace multiple underscores with single underscore
    sanitized = re.sub(r'_+', '_', sanitized)
    
    # Convert to lowercase and strip underscores from edges
    sanitized = sanitized.lower().strip('_')
    
    return sanitized


def split_and_clean(input_json_path: str, out_dir: str = "processed_json"):
    """
    Main function to split and clean FDA biomarker JSON data.
    
    Processes final_output.json format:
    - Reads array of biomarker records
    - Cleans content field for chunking (removes newlines, normalizes spaces)
    - Creates individual JSON files with readable filenames
    - Maintains content field name (does not rename to Corpus)
    
    Args:
        input_json_path: Path to final_output.json file
        out_dir: Output directory for individual JSON files (default: "processed_json")
    
    Returns:
        Number of records processed
    """
    # Create output directory if it doesn't exist
    os.makedirs(out_dir, exist_ok=True)
    
    # Read input JSON
    with open(input_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Process each biomarker record
    processed_count = 0
    
    for item in data:
        # Extract required fields
        drug_name = item.get('drug_name', '')
        biomarker = item.get('biomarker', '')
        content = item.get('content', '')
        
        # Skip if essential fields are missing
        if not drug_name or not biomarker:
            print(f"⚠️  Skipping record with missing drug_name or biomarker")
            continue
        
        # Clean the content for chunking
        content_clean = clean_content_for_chunking(content)
        
        # Create output item with all original fields, cleaning the content field
        # Start with url at the beginning
        output_item = {'url': 'https://www.fda.gov/media/124784/download?attachment'}
        
        # Copy all original fields, replacing content with cleaned version
        for key, value in item.items():
            if key == 'content':
                output_item['content'] = content_clean
            else:
                output_item[key] = value
        
        # Generate readable filename
        filename_base = generate_filename(drug_name, biomarker)
        output_file = os.path.join(out_dir, f"{filename_base}.json")
        
        # Save individual JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_item, f, indent=2, ensure_ascii=False)
        
        processed_count += 1
    
    return processed_count


if __name__ == "__main__":
    """
    Standalone execution: Run split and clean.
    
    Usage:
        python json_split_and_clean_fda_biomarkers.py [input_json_path] [out_dir]
        
    If no arguments provided, uses final_output.json as default input
    and processed_json as default output directory.
    """
    print("="*80)
    print("🚀 JSON SPLIT & CLEAN - FDA Biomarkers")
    print("="*80)
    
    try:
        # Default values
        input_file = None
        out_dir = "processed_json"
        
        # Parse arguments
        if len(sys.argv) > 1:
            input_file = sys.argv[1]
        if len(sys.argv) > 2:
            out_dir = sys.argv[2]
        
        # Auto-detect input file if not provided
        if not input_file:
            # Default to final_output.json in script directory
            script_dir = os.path.dirname(os.path.abspath(__file__))
            default_input = os.path.join(script_dir, 'final_output.json')
            
            if os.path.exists(default_input):
                input_file = default_input
                print(f"📂 Auto-detected input file: {input_file}")
            else:
                error_msg = "❌ ERROR: No input JSON file found."
                print(error_msg)
                print(f"   Expected: {default_input}")
                print("   Or specify with: python json_split_and_clean_fda_biomarkers.py <input_file>")
                sys.exit(1)
        
        if not os.path.exists(input_file):
            error_msg = f"❌ ERROR: Input file not found: {input_file}"
            print(error_msg)
            sys.exit(1)
        
        print(f"📂 Input file: {input_file}")
        print(f"📂 Output directory: {out_dir}")
        
        processed_count = split_and_clean(input_file, out_dir)
        
        print(f"\n✅ Processing completed: {processed_count} records processed")
        print(f"   Output directory: {out_dir}/")
        print("="*80)
            
    except Exception as e:
        error_msg = f"❌ CRITICAL ERROR: {str(e)}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        sys.exit(1)
