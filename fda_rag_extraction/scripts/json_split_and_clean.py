"""
JSON SPLIT AND CLEAN
--------------------
Reads a JSON file from the watcher, cleans the text field (raw scraped content),
and splits each case into individual JSON files with cleaned Corpus field.
"""

import json
import os
import sys
import hashlib
import re
import logging


def clean_corpus(corpus_text):
    """
    Cleans the Corpus field according to specified rules:
    - Removes specific boilerplate lines
    - Removes "cutoff points" - lines that indicate the end of useful content
      (everything after these lines is removed)
    - Cleans multiple whitespace
    - Normalizes Unicode characters (dashes, quotes)
    - Removes repeated headers
    
    CUTOFF POINTS: When these patterns are found, that line AND all subsequent
    lines are removed (they mark the end of useful content).
    """
    if not corpus_text:
        return ""
    
    lines = corpus_text.split('\n')
    cleaned_lines = []
    
    # CUTOFF POINTS: Lines that mark the end of useful content
    # When found, remove this line AND everything after it
    cutoff_patterns = [
        # Assessment Aid - marks end of content
        r".*This review.*used.*Assessment Aid.*",
        r".*This review was conducted.*Assessment Aid.*",
        
        # RTOR (Real-Time Oncology Review) - marks end of content
        r".*This review used.*Real-Time Oncology Review.*",
        r".*This review used.*RTOR.*",
        
        # Project Orbis - marks end of content
        r".*This review was conducted under Project Orbis.*",
        
        # Priority review / Breakthrough / Orphan designation - marks end of content
        r".*The application was granted.*priority review.*",
        r".*The application was granted.*breakthrough.*",
        r".*The application was granted.*orphan.*",
        r".*granted.*priority review.*",
        r".*granted.*breakthrough designation.*",
        r".*granted.*orphan drug designation.*",
        r".*received.*orphan drug designation.*",
        r".*received.*breakthrough designation.*",
        r".*received.*priority review.*",
    ]
    
    # Single-line patterns to remove (boilerplate lines)
    # IMPORTANT: These patterns only match lines that START with the pattern
    # This ensures we don't accidentally remove content that contains these phrases
    patterns_to_remove = [
        # Social media / follow lines (multiple variations)
        r"^Follow the Oncology Center of Excellence.*",
        r"^Follow the Oncology Center of Excellence on X.*",
        r"^Follow the Oncology Center of Excellence on X \(formerly Twitter\).*",
        r"^Follow the Oncology Center of Excellence on Twitter.*",
        r"^Follow us on X.*",
        
        # Adverse event reporting
        r"^Healthcare professionals should report all serious adverse events.*",
        
        # Prescribing information - ONLY lines that START with these exact phrases
        # This ensures we don't remove dosage information like "Less than 50 kg: 120 mg..."
        r"^Full prescribing information for\s+.*",
        r"^View full prescribing information for\s+.*",
        r"^See full prescribing information for\s+.*",
        
        # Project Facilitate / IND assistance
        r"^For assistance with single-patient INDs for investigational oncology products.*",
        
        # Expedited programs / Guidance for Industry
        r"^FDA expedited programs are described in the Guidance for Industry.*",
        r"^A description of FDA expedited programs is in the Guidance.*",
        r"^FDA expedited programs are described in the Guidance.*",
        
        # COVID-19 / Coronavirus references
        r"^For information on the COVID-19 pandemic.*",
        r"^FDA: Coronavirus Disease 2019 \(COVID-19\).*",
        r"^CDC: Coronavirus \(COVID-19\).*",
    ]
    
    # Headers to remove if repeated
    headers_to_remove = [
        "Efficacy and Safety",
        "Recommended Dosage",
        "Expedited Programs",
    ]
    
    # Track context to preserve important information
    # Look ahead buffer to check if important content follows
    lookahead_buffer = []
    MAX_LOOKAHEAD = 5  # Check next 5 lines for important content
    
    for idx, line in enumerate(lines):
        original_line = line
        line_stripped = line.strip()
        
        # Skip empty lines (but preserve structure)
        if not line_stripped:
            # Only skip if we're not in the middle of important content
            # If previous line ended with ":" and we have lookahead, might be list formatting
            if cleaned_lines and cleaned_lines[-1].endswith(':'):
                # This might be part of a formatted list, keep the empty line
                cleaned_lines.append('')
            continue
        
        # IMPORTANT: Check if this line contains dosage information BEFORE applying any filters
        # Dosage patterns: lines that contain weight/dose information
        is_dosage_info = False
        dosage_patterns = [
            r'.*\d+\s*(kg|mg|g|mcg).*',  # Contains weight or dose units
            r'.*less than.*\d+.*',  # "Less than X"
            r'.*greater than.*\d+.*',  # "Greater than X"
            r'.*\d+\s*(or|and)\s*(greater|less).*',  # "X or greater"
            r'.*orally.*twice.*daily.*',  # Dosage frequency
            r'.*orally.*once.*daily.*',
            r'.*mg.*orally.*',
        ]
        
        for dosage_pattern in dosage_patterns:
            if re.search(dosage_pattern, line_stripped, re.IGNORECASE):
                is_dosage_info = True
                break
        
        # Check for CUTOFF POINTS FIRST - before adding to cleaned_lines
        # This ensures cutoff lines are not added even if they're not at the end
        # CRITICAL: Always check if important content follows before cutting off
        is_cutoff = False
        for cutoff_pattern in cutoff_patterns:
            if re.search(cutoff_pattern, line_stripped, re.IGNORECASE):
                # CRITICAL: Before cutting off, ALWAYS check if there's important content following
                # Look ahead to see if there's dosage info or other important content coming
                has_important_followup = False
                
                # Check if previous line suggests important content (ends with ":")
                previous_suggests_list = cleaned_lines and cleaned_lines[-1].endswith(':')
                
                # Look ahead in next lines for dosage information
                # IMPORTANT: Increase lookahead range and don't skip empty lines too aggressively
                # Real pages may have many empty lines between content
                extended_lookahead = MAX_LOOKAHEAD * 3  # Look further ahead (15 lines instead of 5)
                
                for lookahead_idx in range(idx + 1, min(idx + extended_lookahead + 1, len(lines))):
                    lookahead_line = lines[lookahead_idx].strip()
                    if not lookahead_line:
                        # Don't skip empty lines - continue checking (they might be between content)
                        continue
                    
                    # Check if lookahead contains dosage info
                    for dosage_pattern in dosage_patterns:
                        if re.search(dosage_pattern, lookahead_line, re.IGNORECASE):
                            has_important_followup = True
                            break
                    if has_important_followup:
                        break
                    
                    # Also check if lookahead line ends with ":" (might introduce a list)
                    if lookahead_line.endswith(':'):
                        # Check further ahead for dosage info (with extended range)
                        for further_idx in range(lookahead_idx + 1, min(lookahead_idx + extended_lookahead + 1, len(lines))):
                            further_line = lines[further_idx].strip()
                            if not further_line:
                                continue
                            for dosage_pattern in dosage_patterns:
                                if re.search(dosage_pattern, further_line, re.IGNORECASE):
                                    has_important_followup = True
                                    break
                            if has_important_followup:
                                break
                        if has_important_followup:
                            break
                
                # If important content follows, don't cut off yet - just skip this cutoff line
                if has_important_followup or previous_suggests_list:
                    # Still remove this cutoff line (don't add it), but continue processing to preserve important content
                    # This ensures we remove the boilerplate but keep the important dosage info that follows
                    continue
                
                is_cutoff = True
                break
        
        if is_cutoff:
            # Stop processing - everything from this line onwards is boilerplate
            break
        
        # Check if line matches single-line patterns to remove
        # CONSERVATIVE: Only remove lines that START with the pattern (not lines that contain it)
        # This ensures we preserve important content like dosage information
        should_skip = False
        for pattern in patterns_to_remove:
            # Only match if line STARTS with pattern (re.match checks start of string)
            if re.match(pattern, line_stripped, re.IGNORECASE):
                should_skip = True
                break
        
        if should_skip:
            continue
        
        # Skip repeated headers (remove if they appear as standalone lines)
        if line_stripped in headers_to_remove:
            # Skip this header entirely if it appears as a standalone line
            continue
        
        cleaned_lines.append(line_stripped)
    
    # Join lines back
    cleaned_text = '\n'.join(cleaned_lines)
    
    # Normalize Unicode characters
    # Normalize dashes
    cleaned_text = cleaned_text.replace('\u2013', '-')  # en dash
    cleaned_text = cleaned_text.replace('\u2014', '-')  # em dash
    cleaned_text = cleaned_text.replace('\u2212', '-')  # minus sign
    
    # Normalize quotes
    cleaned_text = cleaned_text.replace('\u2018', "'")  # left single quote
    cleaned_text = cleaned_text.replace('\u2019', "'")  # right single quote
    cleaned_text = cleaned_text.replace('\u201C', '"')  # left double quote
    cleaned_text = cleaned_text.replace('\u201D', '"')  # right double quote
    
    # Clean multiple whitespace (but preserve single newlines between paragraphs)
    # Replace multiple spaces with single space
    cleaned_text = re.sub(r' +', ' ', cleaned_text)
    # Replace multiple newlines (3+) with double newline
    cleaned_text = re.sub(r'\n{3,}', '\n\n', cleaned_text)
    
    # Final strip
    cleaned_text = cleaned_text.strip()
    
    return cleaned_text


# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Logging Configuration
logging.basicConfig(
    filename='logs/json_split_and_clean.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'  # Append mode
)

def split_and_clean(input_json_path: str, out_dir: str = "data/processed-json"):
    """
    Main function to split and clean JSON data.
    
    Args:
        input_json_path: Path to the input JSON file (from watcher)
        out_dir: Output directory for individual case files (default: "data/processed-json")
    
    Returns:
        List of dictionaries with:
        - RAG_ID: The unique identifier
        - file: Path to the individual JSON file
        - corpus_hash: MD5 hash of the cleaned Corpus field
    """
    logging.info(f"Starting split_and_clean: input={input_json_path}, output={out_dir}")
    
    # Create output directory if it doesn't exist
    os.makedirs(out_dir, exist_ok=True)
    
    # Read input JSON
    with open(input_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    logging.info(f"Loaded {len(data)} records from {input_json_path}")
    
    # Process each case
    results = []
    
    for item in data:
        # Get RAG_ID
        rag_id = item.get('RAG_ID')
        if not rag_id:
            continue
        
        # Get original text (from watcher, field is called "text")
        text = item.get('text', '')
        
        # Clean the text
        corpus_clean = clean_corpus(text)
        
        # Create a clean copy of the item for output
        # Remove Scraped_At and text, add cleaned Corpus
        output_item = item.copy()
        
        # Remove Scraped_At field
        if 'Scraped_At' in output_item:
            del output_item['Scraped_At']
        
        # Remove original text field (raw scraped content)
        if 'text' in output_item:
            del output_item['text']
        
        # Add cleaned Corpus (processed content ready for use)
        output_item['Corpus'] = corpus_clean
        
        # Calculate MD5 hash of cleaned Corpus
        corpus_hash = hashlib.md5(corpus_clean.encode('utf-8')).hexdigest()
        
        # Create output file path
        output_file = os.path.join(out_dir, f"{rag_id}.json")
        
        # Save individual case file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_item, f, indent=2, ensure_ascii=False)
        
        # Add to results
        results.append({
            'RAG_ID': rag_id,
            'file': output_file,
            'corpus_hash': corpus_hash
        })
    
    logging.info(f"Successfully processed {len(results)} cases")
    logging.info(f"Output directory: {out_dir}")
    return results


if __name__ == "__main__":
    """
    Standalone execution: Run split and clean directly with logging.
    
    Usage:
        python scripts/json_split_and_clean.py [input_json_path] [out_dir]
        
    If no arguments provided, automatically finds the most recent JSON file
    and uses default output directory: data/processed-json
    """
    print("="*80)
    print("🚀 JSON SPLIT & CLEAN - Step 2: Processing")
    print("="*80)
    logging.info("="*80)
    logging.info("JSON SPLIT & CLEAN EXECUTION START")
    logging.info("="*80)
    
    try:
        # Default values
        input_file = None
        out_dir = "data/processed-json"
        
        # Parse arguments
        if len(sys.argv) > 1:
            input_file = sys.argv[1]
        if len(sys.argv) > 2:
            out_dir = sys.argv[2]
        
        # Auto-detect input file if not provided
        if not input_file:
            # Look for most recent JSON file
            json_files = []
            for json_file in ['data/rag_delta_update.json', 'data/rag_initial_load.json']:
                if os.path.exists(json_file):
                    json_files.append((os.path.getmtime(json_file), json_file))
            
            if json_files:
                input_file = max(json_files)[1]  # Most recent
                print(f"📂 Auto-detected input file: {input_file}")
                logging.info(f"Auto-detected input file: {input_file}")
            else:
                error_msg = "❌ ERROR: No input JSON file found."
                print(error_msg)
                print("   Expected: data/rag_initial_load.json or data/rag_delta_update.json")
                print("   Or specify with: python scripts/json_split_and_clean.py <input_file>")
                logging.error(error_msg)
                sys.exit(1)
        
        if not os.path.exists(input_file):
            error_msg = f"❌ ERROR: Input file not found: {input_file}"
            print(error_msg)
            logging.error(error_msg)
            sys.exit(1)
        
        print(f"📂 Input file: {input_file}")
        print(f"📂 Output directory: {out_dir}")
        logging.info(f"Input file: {input_file}")
        logging.info(f"Output directory: {out_dir}")
        
        results = split_and_clean(input_file, out_dir)
        
        print(f"\n✅ Processing completed: {len(results)} cases processed")
        print(f"   Output directory: {out_dir}/")
        print("="*80)
        
        logging.info("="*80)
        logging.info("JSON SPLIT & CLEAN EXECUTION END - SUCCESS")
        logging.info("="*80)
            
    except Exception as e:
        error_msg = f"❌ CRITICAL ERROR: {str(e)}"
        print(error_msg)
        logging.error("="*80)
        logging.error("CRITICAL ERROR IN JSON SPLIT & CLEAN")
        logging.error("="*80)
        logging.error(f"Error: {str(e)}", exc_info=True)
        logging.error("="*80)
        sys.exit(1)

