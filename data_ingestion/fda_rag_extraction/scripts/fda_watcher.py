"""
FDA WATCHER (DEEP SCRAPING & RAG ID VERSION)
--------------------------------------------
This is the "Intelligence Engine" of the Pipeline.
Responsibilities:
1. Fetch the master list from the web.
2. Generate unique IDs (Hash) to avoid duplicates in Vectors.
3. Detect new entries (Comparison with Master Excel).
4. DEEP SCRAPING: Visit each new URL and extract full text content (text field).
"""

import requests
import pandas as pd
import os
import sys
import hashlib
import time
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

# --- CONFIGURATION ---
URL_FDA = "https://www.fda.gov/drugs/resources-information-approved-drugs/oncology-cancerhematologic-malignancies-approval-notifications"
BASE_DOMAIN = "https://www.fda.gov"

# Create data and logs directories if they don't exist
os.makedirs("data", exist_ok=True)
os.makedirs("logs", exist_ok=True)

# Logging Configuration
logging.basicConfig(
    filename='logs/fda_watcher.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'  # Append mode
)

MASTER_DB_FILE = "data/FDA_Oncology_Master_DB.xlsx"

# Output files for RAG
FILE_INITIAL = "data/rag_initial_load.json"
FILE_DELTA = "data/rag_delta_update.json"

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # seconds (will increase with backoff)

# Delays to avoid server saturation
DELAY_STANDARD = 0.5  # Standard delay between normal requests
DELAY_NODE_URL = 2.0  # Longer delay for /node/... URLs (avoid saturation)

# Batch processing configuration
BATCH_SIZE = 10  # Process N records before long pause
BATCH_DELAY = 5.0  # Long pause between batches (seconds) to avoid saturation

# ==========================================
# 1. AUXILIARY TOOLS
# ==========================================

def get_browser_headers(referer=None):
    """
    Generates complete headers to mimic a real browser.
    Includes all necessary headers to avoid bot detection.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin" if referer else "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "DNT": "1"
    }
    
    if referer:
        headers["Referer"] = referer
    
    return headers

def generate_rag_id(text_to_hash):
    """
    Generates a unique fingerprint (MD5) based on the URL.
    This ensures the ID is always the same for the same drug.
    """
    if not text_to_hash:
        return None
    return hashlib.md5(text_to_hash.encode('utf-8')).hexdigest()

def initialize_session(session, initial_url):
    """
    Initializes HTTP session by visiting the main URL to establish cookies.
    This is crucial for Drupal to recognize the session as valid.
    """
    headers = get_browser_headers()
    try:
        response = session.get(initial_url, headers=headers, timeout=15)
        response.raise_for_status()
        print("✅ HTTP session initialized successfully (cookies established)")
        return True
    except Exception as e:
        print(f"⚠️ Warning: Could not fully initialize session: {e}")
        return False

def get_full_corpus(url, session, referer=None, max_retries=MAX_RETRIES):
    """
    DEEP SCRAPING: Visits the URL and extracts detailed text.
    
    SIMPLE AND EFFECTIVE STRATEGY:
    - Longer delays for /node/... URLs to avoid saturation
    - allow_redirects=True to automatically follow redirects
    - Multiple selectors to extract content
    - Clear error handling
    
    Args:
        url: URL to extract
        session: requests.Session to reuse connections
        referer: Referer URL for Referer header
        max_retries: Maximum number of retries for connection errors
    
    Returns:
        str: Extracted text or empty string if 403/404 error
    """
    # Basic validations
    if not url or "http" not in url:
        return ""
    if url.lower().endswith('.pdf'):
        return "[PDF CONTENT - REQUIRES OCR]"
    
    # Detect if it's a /node/... URL to apply longer delay
    is_node_url = "/node/" in url
    delay_time = DELAY_NODE_URL if is_node_url else DELAY_STANDARD
    
    headers = get_browser_headers(referer=referer or URL_FDA)
    
    # Retry logic for connection errors (not for 403/404)
    for attempt in range(max_retries):
        try:
            # Pause before request (longer for /node/ URLs)
            time.sleep(delay_time)
            
            # Attempt to access URL with automatic redirects
            resp = session.get(url, headers=headers, timeout=20, allow_redirects=True)
            
            # Check if there was a redirect
            if resp.history and url != resp.url:
                final_url = resp.url
                if is_node_url and "/node/" not in final_url:
                    print(f"      ↳ ✓ Successful redirect: {final_url[:70]}...")
            
            # Explicit status code handling
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'lxml')
                
                # Extraction strategy: try multiple selectors
                content_div = None
                
                # 1. Try div[role="main"]
                content_div = soup.find('div', role='main')
                
                # 2. Try .field--name-body
                if not content_div:
                    content_div = soup.find('div', class_=lambda x: x and 'field--name-body' in x)
                
                # 3. Try article tag
                if not content_div:
                    content_div = soup.find('article')
                
                # 4. Try .node__content (common in Drupal)
                if not content_div:
                    content_div = soup.find('div', class_=lambda x: x and 'node__content' in x)
                
                # 5. Fallback: search for any div with lots of text content
                if not content_div:
                    all_divs = soup.find_all('div')
                    if all_divs:
                        content_div = max(all_divs, key=lambda d: len(d.find_all('p')))
                
                # 6. Last fallback: body
                if not content_div:
                    content_div = soup.body
                
                if content_div:
                    # Extract content preserving document order
                    # Walk through all direct children and descendants in document order
                    text_parts = []
                    
                    # Get all relevant elements in document order using find_all with multiple tags
                    # This preserves the order they appear in the HTML
                    all_elements = content_div.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'ul', 'ol'])
                    
                    # Process elements in order
                    for element in all_elements:
                        tag_name = element.name
                        
                        if tag_name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                            # Heading
                            heading_text = element.get_text(" ", strip=True)
                            if heading_text:
                                text_parts.append(heading_text)
                        
                        elif tag_name == 'p':
                            # Paragraph
                            para_text = element.get_text(" ", strip=True)
                            if para_text:
                                text_parts.append(para_text)
                        
                        elif tag_name in ['ul', 'ol']:
                            # List - extract list items
                            for li in element.find_all('li', recursive=False):  # Only direct children
                                li_text = li.get_text(" ", strip=True)
                                if li_text:  # Only add non-empty items
                                    text_parts.append(li_text)
                    
                    # Join all parts preserving document order
                    full_text = "\n\n".join(text_parts)
                    
                    # Return only if it has substantial content
                    if len(full_text) > 50:
                        return full_text
                    else:
                        return ""
                
                return ""
            
            elif resp.status_code == 403:
                # 403 Forbidden: Server is blocking access
                # For /node/ URLs, try with longer delay if first attempt
                if is_node_url and attempt == 0:
                    print(f"      ↳ [403] Attempt {attempt + 1}/{max_retries}: Waiting longer before retrying...")
                    time.sleep(DELAY_NODE_URL * 2)  # Double delay before retrying
                    continue
                
                # Report error after all attempts
                print(f"   ❌ [403 FORBIDDEN] Could not access after {attempt + 1} attempts: {url[:80]}...")
                if is_node_url:
                    print(f"      💡 Note: This /node/... URL exists but requires more time or special conditions")
                return ""
            
            elif resp.status_code == 404:
                print(f"   ❌ [404 NOT FOUND] URL not found: {url[:80]}...")
                return ""
            
            else:
                print(f"   ⚠️ [HTTP {resp.status_code}] Error accessing: {url[:80]}...")
                return ""
                
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, 
                requests.exceptions.RequestException) as e:
            # Connection errors: retry with backoff
            if attempt < max_retries - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                print(f"   ⚠️ Connection error (attempt {attempt + 1}/{max_retries}): {str(e)[:60]}... Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"   ❌ [CONNECTION ERROR] Failed after {max_retries} attempts: {url[:80]}...")
                return ""
        
        except Exception as e:
            print(f"   ❌ [ERROR] Unexpected exception at {url[:80]}...: {str(e)[:60]}...")
            return ""
    
    return ""

# ==========================================
# 2. MAIN PROCESS
# ==========================================

def fetch_latest_data(session):
    """
    Downloads the surface table (Metadata).
    
    Extracts metadata (title, description, date, URL) from the HTML table.
    Then, in deep scraping, visits each individual URL to obtain the text content.
    
    Args:
        session: requests.Session to reuse connections
    """
    print("📡 Connecting to FDA (Listing)...")
    headers = get_browser_headers()
    
    try:
        response = session.get(URL_FDA, headers=headers, timeout=15, allow_redirects=True)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        table = soup.find('table')
        
        if not table:
            print("⚠️ No table found on main page.")
            return pd.DataFrame()
            
        rows = table.find_all('tr')
        data = []
        
        for row_idx, row in enumerate(rows):
            cols = row.find_all('td')
            if len(cols) >= 3:
                title = cols[0].get_text(" ", strip=True)
                desc = cols[1].get_text(" ", strip=True)
                date = cols[2].get_text(strip=True)
                
                # Robust URL extraction: search links recursively
                link_tag = None
                
                # 1. Search directly in first column
                link_tag = cols[0].find('a', href=True)
                
                # 2. If not found, search recursively
                if not link_tag:
                    all_links = cols[0].find_all('a', href=True, recursive=True)
                    if all_links:
                        link_tag = all_links[0]
                
                # 3. Build complete URL
                if link_tag and link_tag.get('href'):
                    raw_link = link_tag.get('href')
                    raw_link = raw_link.strip()
                    full_link = urljoin(BASE_DOMAIN, raw_link)
                else:
                    full_link = ""
                
                # Safe ID Generation
                base_string = full_link if full_link else f"{title}_{date}"
                unique_hash = generate_rag_id(base_string)
                
                data.append({
                    "RAG_ID": unique_hash,
                    "Title": title,
                    "Webpage": full_link,
                    "Description": desc,
                    "Date": date,
                    "text": "",  # Will be filled later during deep scraping
                    "Scraped_At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
        
        print(f"✅ Found {len(data)} records in main table.")
        
        # Show statistics for /node/ URLs
        node_urls = sum(1 for d in data if "/node/" in d.get("Webpage", ""))
        if node_urls > 0:
            print(f"📊 Statistics: {node_urls} /node/... URLs found (will apply {DELAY_NODE_URL}s delay)")
        
        return pd.DataFrame(data)

    except requests.exceptions.RequestException as e:
        print(f"❌ Critical error in main table scraping: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Unexpected error in scraping: {e}")
        return pd.DataFrame()

def run_pipeline():
    """
    Main pipeline function.
    Manages the complete cycle: download, comparison, and enrichment.
    """
    logging.info("Starting FDA Watcher pipeline")
    
    # Create HTTP session to reuse connections
    session = requests.Session()
    
    # CRITICAL: Initialize session by visiting main page
    print("🔧 Initializing HTTP session...")
    logging.info("Initializing HTTP session")
    initialize_session(session, URL_FDA)
    
    # A. Status Check (First time or update?)
    initial_mode = not os.path.exists(MASTER_DB_FILE)
    
    # B. Get fresh data (surface)
    df_new = fetch_latest_data(session)
    
    if df_new.empty:
        print("❌ No data obtained from web.")
        session.close()
        return False

    # C. Comparison Logic
    if not initial_mode:
        print(f"📖 Loading Master Database from {MASTER_DB_FILE}...")
        logging.info(f"Loading master database from {MASTER_DB_FILE}")
        try:
            df_master = pd.read_excel(MASTER_DB_FILE)
            print(f"✅ Master Database loaded: {len(df_master)} existing records.")
            logging.info(f"Master database loaded: {len(df_master)} existing records")
        except Exception as e:
            print(f"⚠️ Error reading master Excel: {e}. Starting bootstrap mode...")
            logging.warning(f"Error reading master Excel: {e}. Starting bootstrap mode")
            initial_mode = True
            df_master = pd.DataFrame()
        
        if not initial_mode:
            # Filter: Only records whose RAG_ID is NOT in Excel
            existing_ids = set(df_master['RAG_ID'].astype(str))
            records_to_process = df_new[~df_new['RAG_ID'].astype(str).isin(existing_ids)].copy()
            output_file = FILE_DELTA
            process_type = "DELTA UPDATE"
            logging.info(f"Delta update mode: {len(records_to_process)} new records to process")
        else:
            records_to_process = df_new.copy()
            output_file = FILE_INITIAL
            process_type = "INITIAL LOAD"
            logging.info(f"Initial load mode: {len(records_to_process)} records to process")
    else:
        print("🆕 First Execution (Bootstrap) detected.")
        logging.info("First execution (bootstrap) detected")
        df_master = pd.DataFrame()
        records_to_process = df_new.copy()
        output_file = FILE_INITIAL
        process_type = "INITIAL LOAD"
        logging.info(f"Initial load mode: {len(records_to_process)} records to process")

    # D. Enrichment and Saving
    new_count = len(records_to_process)
    
    if new_count > 0:
        print(f"\n🚨 {process_type}: {new_count} new records.")
        print("📥 Starting Deep Scraping to obtain Corpus... (Please wait)\n")
        print(f"⏱️  Note: Will apply {DELAY_STANDARD}s delay for normal URLs and {DELAY_NODE_URL}s for /node/... URLs")
        print(f"📦 Batch strategy: {BATCH_SIZE} records per batch, {BATCH_DELAY}s pause between batches\n")
        logging.info(f"Starting deep scraping: {new_count} records")
        logging.info(f"Batch strategy: {BATCH_SIZE} records per batch, {BATCH_DELAY}s pause")
        
        # Convert DataFrame to index list for batch processing
        indices_list = list(records_to_process.index)
        total = len(indices_list)
        
        # Statistics
        successful_scrapes = 0
        failed_scrapes = 0
        node_url_success = 0
        node_url_failures = 0
        start_time = time.time()
        
        # Track problematic records (for review mode)
        problematic_records = []  # List of dicts with RAG_ID, Title, Webpage, Issue
        
        # Batch processing to avoid server saturation
        num_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE  # Ceiling division
        
        for batch_num in range(num_batches):
            batch_start = batch_num * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, total)
            batch_indices = indices_list[batch_start:batch_end]
            
            print(f"\n📦 Batch {batch_num + 1}/{num_batches} ({len(batch_indices)} records)...")
            logging.info(f"Processing batch {batch_num + 1}/{num_batches} ({len(batch_indices)} records)")
            
            # Process each record in the batch
            for idx_in_batch, df_index in enumerate(batch_indices, 1):
                global_idx = batch_start + idx_in_batch
                row = records_to_process.loc[df_index]
                
                if row['Webpage']:
                    is_node = "/node/" in row['Webpage']
                    print(f"   [{global_idx}/{total}] Extracting: {row['Title'][:50]}...")
                    if is_node:
                        print(f"      ↳ /node/ URL detected - applying extended delay...")
                    
                    corpus_text = get_full_corpus(
                        row['Webpage'], 
                        session, 
                        referer=URL_FDA
                    )
                    records_to_process.at[df_index, 'text'] = corpus_text
                    
                    if corpus_text:
                        successful_scrapes += 1
                        if is_node:
                            node_url_success += 1
                    else:
                        failed_scrapes += 1
                        if is_node:
                            node_url_failures += 1
                        # Track problematic record
                        problematic_records.append({
                            'RAG_ID': row['RAG_ID'],
                            'Title': row['Title'],
                            'Webpage': row['Webpage'],
                            'Issue': 'Empty text field (403/404 or extraction failed)'
                        })
                else:
                    records_to_process.at[df_index, 'text'] = ""
                    failed_scrapes += 1
                    print(f"   [{global_idx}/{total}] ⚠️ No URL available for: {row['Title'][:50]}...")
                    # Track problematic record
                    problematic_records.append({
                        'RAG_ID': row['RAG_ID'],
                        'Title': row['Title'],
                        'Webpage': row.get('Webpage', 'N/A'),
                        'Issue': 'No URL available'
                    })
            
            # Pause between batches (except after last batch)
            if batch_num < num_batches - 1:
                print(f"\n⏸️  {BATCH_DELAY}s pause between batches to avoid server saturation...")
                time.sleep(BATCH_DELAY)
        
        elapsed_time = time.time() - start_time
        
        # Detailed summary
        print(f"\n{'='*60}")
        print(f"📊 SCRAPING SUMMARY")
        print(f"{'='*60}")
        print(f"   ✅ Successful: {successful_scrapes}")
        print(f"   ❌ Failed: {failed_scrapes}")
        print(f"   📈 Success rate: {(successful_scrapes/total*100):.1f}%")
        if node_url_success > 0 or node_url_failures > 0:
            total_node_urls = node_url_success + node_url_failures
            node_success_rate = (node_url_success / total_node_urls * 100) if total_node_urls > 0 else 0
            print(f"   📌 /node/... URLs: {node_url_success} successful, {node_url_failures} failed ({node_success_rate:.1f}% success)")
        print(f"   ⏱️  Total time: {elapsed_time:.1f}s ({elapsed_time/60:.1f} minutes)")
        print(f"   📦 Batches processed: {num_batches}")
        print(f"{'='*60}\n")
        
        # Logging summary
        logging.info("-" * 80)
        logging.info("SCRAPING SUMMARY")
        logging.info("-" * 80)
        logging.info(f"Process type: {process_type}")
        logging.info(f"Total processed: {total}")
        logging.info(f"Successful: {successful_scrapes}")
        logging.info(f"Failed: {failed_scrapes}")
        logging.info(f"Success rate: {(successful_scrapes/total*100):.1f}%")
        if node_url_success > 0 or node_url_failures > 0:
            total_node_urls = node_url_success + node_url_failures
            node_success_rate = (node_url_success / total_node_urls * 100) if total_node_urls > 0 else 0
            logging.info(f"/node/... URLs successful: {node_url_success}")
            logging.info(f"/node/... URLs failed: {node_url_failures}")
            logging.info(f"/node/... URLs success rate: {node_success_rate:.1f}%")
        logging.info(f"Total time: {elapsed_time:.1f}s ({elapsed_time/60:.1f} minutes)")
        logging.info(f"Batches processed: {num_batches}")
        logging.info(f"Generated JSON file: {output_file}")
        if problematic_records:
            logging.warning(f"Problematic records: {len(problematic_records)}")
        logging.info("-" * 80)
        
        # 1. Save JSON for RAG
        records_to_process.to_json(output_file, orient="records", indent=4)
        print(f"📦 JSON Ready for RAG: {output_file}")
        logging.info(f"JSON file saved: {output_file}")
        
        # 2. Update Master Excel
        if initial_mode:
            df_updated = records_to_process
        else:
            # Put new entries at the beginning
            df_updated = pd.concat([records_to_process, df_master], ignore_index=True)
            
        df_updated.to_excel(MASTER_DB_FILE, index=False)
        print(f"💾 Master Database Updated: {MASTER_DB_FILE} ({len(df_updated)} total records)")
        logging.info(f"Master database updated: {MASTER_DB_FILE} ({len(df_updated)} total records)")
        
        # Prepare statistics to return
        scraping_stats = {
            'success': True,
            'process_type': process_type,
            'total_processed': total,
            'successful': successful_scrapes,
            'failed': failed_scrapes,
            'success_rate': (successful_scrapes/total*100) if total > 0 else 0,
            'node_urls_successful': node_url_success,
            'node_urls_failed': node_url_failures,
            'total_time': elapsed_time,
            'batches_processed': num_batches,
            'json_file': output_file,
            'problematic_records': problematic_records  # List of records with issues
        }
        
        session.close()
        return scraping_stats
        
    else:
        print("\n✅ Everything synchronized. No new entries.")
        # IMPORTANT: Only clean temporary files if NOT first execution
        if not initial_mode:
            for f in [FILE_INITIAL, FILE_DELTA]:
                if os.path.exists(f):
                    os.remove(f)
                    print(f"🗑️ Temporary file deleted: {f}")
        
        session.close()
        return {'success': False, 'message': 'No new entries'}


if __name__ == "__main__":
    """
    Standalone execution: Run scraping directly with logging.
    
    Usage:
        python scripts/fda_watcher.py
    """
    print("="*80)
    print("🚀 FDA WATCHER - Step 1: Scraping")
    print("="*80)
    logging.info("="*80)
    logging.info("FDA WATCHER EXECUTION START")
    logging.info("="*80)
    
    try:
        result = run_pipeline()
        
        if isinstance(result, dict) and result.get('success'):
            print(f"\n✅ Scraping completed successfully!")
            print(f"   JSON file: {result.get('json_file', 'N/A')}")
            print(f"   Processed: {result.get('total_processed', 0)} records")
            print(f"   Successful: {result.get('successful', 0)}")
            print(f"   Failed: {result.get('failed', 0)}")
            
            logging.info(f"Scraping completed: {result.get('json_file', 'N/A')}")
            logging.info(f"Processed: {result.get('total_processed', 0)} records")
            logging.info(f"Successful: {result.get('successful', 0)}")
            logging.info(f"Failed: {result.get('failed', 0)}")
            
            problematic = result.get('problematic_records', [])
            if problematic:
                print(f"\n⚠️  {len(problematic)} problematic records detected:")
                for p in problematic[:5]:  # Show first 5
                    print(f"   - {p.get('RAG_ID', 'N/A')}: {p.get('Issue', 'N/A')}")
                if len(problematic) > 5:
                    print(f"   ... and {len(problematic) - 5} more")
                logging.warning(f"{len(problematic)} problematic records detected")
                for p in problematic:
                    logging.warning(f"Problematic: {p.get('RAG_ID', 'N/A')} - {p.get('Issue', 'N/A')}")
            
            print(f"\n📋 Review the JSON file and fix issues if needed:")
            print(f"   {result.get('json_file', 'N/A')}")
            print(f"\nThen run Step 2:")
            print(f"   python scripts/json_split_and_clean.py")
            print("="*80)
            
            logging.info("="*80)
            logging.info("FDA WATCHER EXECUTION END - SUCCESS")
            logging.info("="*80)
            
        elif isinstance(result, dict) and not result.get('success'):
            print(f"\n✅ No new entries found. Everything synchronized.")
            logging.info("No new entries found. Everything synchronized.")
            logging.info("="*80)
            logging.info("FDA WATCHER EXECUTION END - NO CHANGES")
            logging.info("="*80)
        else:
            print(f"\n❌ Scraping failed. Check logs for details.")
            logging.error("Scraping failed")
            logging.error("="*80)
            logging.error("FDA WATCHER EXECUTION END - FAILED")
            logging.error("="*80)
            sys.exit(1)
            
    except Exception as e:
        error_msg = f"❌ CRITICAL ERROR: {str(e)}"
        print(error_msg)
        logging.error("="*80)
        logging.error("CRITICAL ERROR IN FDA WATCHER")
        logging.error("="*80)
        logging.error(f"Error: {str(e)}", exc_info=True)
        logging.error("="*80)
        sys.exit(1)