import os
import pathlib
import zipfile
import tempfile
import json
import time
import io
from datetime import datetime
from typing import Dict, List, Optional, Tuple

try:
    from google import genai
except ImportError:
    print("CRITICAL ERROR: The 'google-genai' library is not installed.")
    print("Please install it by running: pip install google-genai")
    exit(1)

try:
    from PyPDF2 import PdfReader, PdfWriter
except ImportError:
    print("CRITICAL ERROR: The 'PyPDF2' library is not installed.")
    print("Please install it by running: pip install PyPDF2")
    exit(1)

try:
    from supabase import create_client, Client
except ImportError:
    print("CRITICAL ERROR: The 'supabase' library is not installed.")
    print("Please install it by running: pip install supabase")
    exit(1)

#  i dont care about exposing my apis, kaam hone pr delete krna hain 

SUPABASE_URL = "https://hprvnejxmmxmegoocvsr.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhwcnZuZWp4bW14bWVnb29jdnNyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTAwNDc1MjUsImV4cCI6MjA2NTYyMzUyNX0.e6qyvvtx_HfkvR8Vt4QJswh8Oa4lZNYQxEyJI89bBE8"
INPUT_BUCKET_NAME = "pdf-inputs"
OUTPUT_BUCKET_NAME = "ocr-outputs"

API_KEYS = [
    "AIzaSyDIoa0ekNCKEprdLyoFpntBI5dEmbqJN8U",
    "AIzaSyBXHQxyHB4tu09jC4Y82JESWBrNxouZDoE",
]

MODEL_ID = "gemini-2.5-flash-preview-05-20"

MAX_RETRIES = 2 
RETRY_DELAY = 5

DEFAULT_PAGES_PER_SPLIT = 1

OCR_PROMPT = """
Act as an expert OCR extractor, Extract All the data As it is word by word. Do not summaries or reduce the length of content, Your goal is to extract The texts as it is.

Information about the PDF:
- Language: Hindi
- Format: PDF with two tables on a single page, interleaved
- Each table has 6 columns
- each pdf has multiple pages
At the start of each page, there is metadata about the people in the table. This metadata includes:
- District name
- Polling center
- Polling location
- Ward number
- Body number
- Locality name
Output only the Export the extracted data in the following CSV field format (as JSON objects):
{
  "age": 34,
  "bodyNumber": "1-Ghaziabad",
  "district": "023-Ghaziabad",
  "fatherOrHusbandNameHindi": "ओमप्रकाश",
  "gender": "F",
  "houseNo": "610",
  "locality": "",
  "partNumber": "4",
  "pollingCenter": "",
  "roomNumber": "5",
  "sectionNumber": "4",
  "srNo": "1232",
  "voterNameHindi": "सरिता देवी",
  "ward": "3-Babu Krishan Nagar"
}
"""

PROCESSING_STATUS = {
    "session_info": {
        "start_time": None,
        "end_time": None,
        "total_pdfs": 0,
        "successful_pdfs": 0,
        "failed_pdfs": 0,
        "api_usage_stats": {f"api_{i+1}": {"successful_calls": 0, "failed_calls": 0} for i in range(len(API_KEYS))},
        "storage_stats": {
            "files_downloaded": 0,
            "files_uploaded": 0,
            "total_download_size_mb": 0,
            "total_upload_size_mb": 0
        }
    },
    "pdf_results": {},
    "failed_splits": [],
    "global_errors": [],
    "retry_attempts": []
}

current_api_index = 0
gemini_clients = []

supabase: Optional[Client] = None

def initialize_supabase():
    """Initialize Supabase client."""
    global supabase
    
    try:
        if not SUPABASE_URL or not SUPABASE_KEY or SUPABASE_URL == "https://your-project.supabase.co":
            print("ERROR: Supabase URL and KEY must be configured.")
            print("Please update SUPABASE_URL and SUPABASE_KEY in the script.")
            return False
        
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully initialized Supabase client")
        
        try:
            buckets = supabase.storage.list_buckets()
            print(f"Connected to Supabase. Found {len(buckets)} storage buckets.")
            
            bucket_names = [bucket.name for bucket in buckets]
            
            if INPUT_BUCKET_NAME not in bucket_names:
                print(f"WARNING: Input bucket '{INPUT_BUCKET_NAME}' not found. Creating it...")
                try:
                    supabase.storage.create_bucket(INPUT_BUCKET_NAME, options={"public": False})
                    print(f"Successfully created bucket '{INPUT_BUCKET_NAME}'.")
                except Exception as e_create:
                    print(f"ERROR: Could not create bucket '{INPUT_BUCKET_NAME}': {e_create}")
            else:
                print(f"Input bucket '{INPUT_BUCKET_NAME}' found.")
            
            if OUTPUT_BUCKET_NAME not in bucket_names:
                print(f"WARNING: Output bucket '{OUTPUT_BUCKET_NAME}' not found. Creating it...")
                try:
                    supabase.storage.create_bucket(OUTPUT_BUCKET_NAME, options={"public": False})
                    print(f"Successfully created bucket '{OUTPUT_BUCKET_NAME}'.")
                except Exception as e_create:
                    print(f"ERROR: Could not create bucket '{OUTPUT_BUCKET_NAME}': {e_create}")
            else:
                print(f"Output bucket '{OUTPUT_BUCKET_NAME}' found.")
            
        except Exception as e:
            print(f"Warning: Could not verify or create buckets: {e}")
        
        return True
        
    except Exception as e:
        print(f"CRITICAL ERROR: Could not initialize Supabase client: {e}")
        PROCESSING_STATUS["global_errors"].append({
            "type": "supabase_setup_error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        })
        return False

def list_input_pdfs() -> List[str]:
    """List all PDF files in the input bucket."""
    try:
        files = supabase.storage.from_(INPUT_BUCKET_NAME).list()
        pdf_files = [file['name'] for file in files if file['name'].lower().endswith('.pdf')]
        return pdf_files
    except Exception as e:
        error_msg = f"Error listing PDF files from input bucket: {str(e)}"
        print(error_msg)
        PROCESSING_STATUS["global_errors"].append({
            "type": "supabase_list_error",
            "message": error_msg,
            "timestamp": datetime.now().isoformat()
        })
        return []

def download_pdf_from_supabase(file_name: str, local_path: pathlib.Path) -> bool:
    """Download a PDF file from Supabase input bucket to local temporary storage."""
    try:
        response = supabase.storage.from_(INPUT_BUCKET_NAME).download(file_name)
        
        with open(local_path, 'wb') as f:
            f.write(response)
        
        file_size_mb = len(response) / (1024 * 1024)
        PROCESSING_STATUS["session_info"]["storage_stats"]["files_downloaded"] += 1
        PROCESSING_STATUS["session_info"]["storage_stats"]["total_download_size_mb"] += file_size_mb
        
        print(f"  Downloaded {file_name} ({file_size_mb:.2f} MB)")
        return True
        
    except Exception as e:
        error_msg = f"Error downloading {file_name}: {str(e)}"
        print(f"  {error_msg}")
        PROCESSING_STATUS["global_errors"].append({
            "type": "supabase_download_error",
            "file_name": file_name,
            "message": error_msg,
            "timestamp": datetime.now().isoformat()
        })
        return False

def upload_file_to_supabase(local_path: pathlib.Path, remote_name: str, bucket_name: str = OUTPUT_BUCKET_NAME) -> bool:
    """Upload a file to Supabase bucket."""
    try:
        with open(local_path, 'rb') as f:
            file_content = f.read()
        
        response = supabase.storage.from_(bucket_name).upload(remote_name, file_content)
        
        file_size_mb = len(file_content) / (1024 * 1024)
        PROCESSING_STATUS["session_info"]["storage_stats"]["files_uploaded"] += 1
        PROCESSING_STATUS["session_info"]["storage_stats"]["total_upload_size_mb"] += file_size_mb
        
        print(f"  Uploaded {remote_name} to {bucket_name} ({file_size_mb:.2f} MB)")
        return True
        
    except Exception as e:
        error_msg = f"Error uploading {remote_name} to {bucket_name}: {str(e)}"
        print(f"  {error_msg}")
        PROCESSING_STATUS["global_errors"].append({
            "type": "supabase_upload_error",
            "file_name": remote_name,
            "bucket": bucket_name,
            "message": error_msg,
            "timestamp": datetime.now().isoformat()
        })
        return False

def upload_status_to_supabase(status_data: dict) -> bool:
    """Upload processing status JSON to Supabase."""
    try:
        status_json = json.dumps(status_data, indent=2, ensure_ascii=False)
        status_bytes = status_json.encode('utf-8')
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        status_filename = f"processing_status_{timestamp}.json"
        
        response = supabase.storage.from_(OUTPUT_BUCKET_NAME).upload(status_filename, status_bytes)
        
        print(f"  Status JSON uploaded as {status_filename}")
        return True
        
    except Exception as e:
        error_msg = f"Error uploading status JSON: {str(e)}"
        print(f"  {error_msg}")
        return False

def initialize_gemini_clients():
    """Initialize Gemini clients for all API keys."""
    global gemini_clients
    gemini_clients = []
    
    for i, api_key in enumerate(API_KEYS):
        try:
            if not api_key:
                print(f"WARNING: API key {i+1} is a placeholder or empty. Skipping.")
                gemini_clients.append(None)
                continue
                
            client = genai.Client(api_key=api_key)
            gemini_clients.append(client)
            print(f"Successfully initialized Gemini client {i+1}")
            
        except Exception as e:
            print(f"ERROR: Could not initialize Gemini client {i+1}: {e}")
            gemini_clients.append(None)
            PROCESSING_STATUS["global_errors"].append({
                "type": "api_setup_error",
                "api_index": i+1,
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            })
    
    valid_clients = [client for client in gemini_clients if client is not None]
    if not valid_clients:
        print("CRITICAL ERROR: No valid Gemini API clients could be initialized.")
        print("Please ensure at least one API key is valid.")
        exit(1)
    
    print(f"Initialized {len(valid_clients)} out of {len(API_KEYS)} Gemini clients")

def get_next_available_client() -> Tuple[Optional[genai.Client], int]:
    """Get the next available Gemini client, cycling through all available clients."""
    global current_api_index
    
    attempts = 0
    max_attempts = len(gemini_clients)
    
    while attempts < max_attempts:
        client = gemini_clients[current_api_index]
        api_index = current_api_index
        
        current_api_index = (current_api_index + 1) % len(gemini_clients)
        
        if client is not None:
            return client, api_index
        
        attempts += 1
    
    return None, -1

def update_api_stats(api_index: int, success: bool):
    """Update API usage statistics."""
    if 0 <= api_index < len(API_KEYS):
        api_key = f"api_{api_index + 1}"
        if success:
            PROCESSING_STATUS["session_info"]["api_usage_stats"][api_key]["successful_calls"] += 1
        else:
            PROCESSING_STATUS["session_info"]["api_usage_stats"][api_key]["failed_calls"] += 1

def initialize_pdf_status(pdf_name: str, total_splits: int) -> None:
    """Initialize status tracking for a PDF."""
    PROCESSING_STATUS["pdf_results"][pdf_name] = {
        "status": "processing",
        "total_splits": total_splits,
        "successful_splits": 0,
        "failed_splits": 0,
        "splits_details": {},
        "start_time": datetime.now().isoformat(),
        "end_time": None,
        "errors": [],
        "retry_count": 0
    }

def update_split_status(pdf_name: str, split_name: str, success: bool, error_message: str = None, retry_attempt: int = 0) -> None:
    """Update the status of a specific PDF split."""
    if pdf_name not in PROCESSING_STATUS["pdf_results"]:
        return
    
    pdf_status = PROCESSING_STATUS["pdf_results"][pdf_name]
    
    status_key = f"{split_name}_attempt_{retry_attempt}" if retry_attempt > 0 else split_name
    
    pdf_status["splits_details"][status_key] = {
        "status": "success" if success else "failed",
        "timestamp": datetime.now().isoformat(),
        "error": error_message if not success else None,
        "retry_attempt": retry_attempt
    }
    
    if success:
        pdf_status["successful_splits"] += 1
    else:
        if retry_attempt == MAX_RETRIES:
            pdf_status["failed_splits"] += 1

            PROCESSING_STATUS["failed_splits"].append({
                "pdf_name": pdf_name,
                "split_name": split_name,
                "error": error_message,
                "total_retries": retry_attempt,
                "timestamp": datetime.now().isoformat()
            })

def finalize_pdf_status(pdf_name: str, overall_success: bool, final_error: str = None) -> None:
    """Finalize the status of a PDF processing."""
    if pdf_name not in PROCESSING_STATUS["pdf_results"]:
        return
    
    pdf_status = PROCESSING_STATUS["pdf_results"][pdf_name]
    pdf_status["status"] = "completed" if overall_success else "failed"
    pdf_status["end_time"] = datetime.now().isoformat()
    
    if final_error:
        pdf_status["errors"].append({
            "message": final_error,
            "timestamp": datetime.now().isoformat()
        })
    
    if overall_success:
        PROCESSING_STATUS["session_info"]["successful_pdfs"] += 1
    else:
        PROCESSING_STATUS["session_info"]["failed_pdfs"] += 1

def split_pdf_in_memory(pdf_content: bytes, pages_per_split: int, pdf_name: str) -> List[Tuple[bytes, str]]:
    """Split a PDF in memory and return list of (pdf_bytes, split_name) tuples."""
    split_pdfs = []
    
    try:
        pdf_stream = io.BytesIO(pdf_content)
        pdf_reader = PdfReader(pdf_stream)
        total_pages = len(pdf_reader.pages)
        base_name = pathlib.Path(pdf_name).stem
        
        total_splits = (total_pages + pages_per_split - 1) // pages_per_split
        initialize_pdf_status(pdf_name, total_splits)

        for i in range(0, total_pages, pages_per_split):
            pdf_writer = PdfWriter()
            end_page = min(i + pages_per_split, total_pages)
            for page_num in range(i, end_page):
                pdf_writer.add_page(pdf_reader.pages[page_num])

            output_stream = io.BytesIO()
            pdf_writer.write(output_stream)
            split_pdf_bytes = output_stream.getvalue()
            output_stream.close()
            
            split_name = f"{base_name}-split{i//pages_per_split:02d}.pdf"
            split_pdfs.append((split_pdf_bytes, split_name))
            
            print(f"  Created split: {split_name} (pages {i+1}-{end_page})")

        return split_pdfs

    except Exception as e:
        error_msg = f"Error splitting PDF {pdf_name}: {str(e)}"
        print(error_msg)
        PROCESSING_STATUS["global_errors"].append({
            "type": "pdf_split_error",
            "pdf_name": pdf_name,
            "message": error_msg,
            "timestamp": datetime.now().isoformat()
        })
        return []

def gemini_ocr_pdf_with_retry(pdf_bytes: bytes, split_name: str, pdf_name: str) -> Optional[str]:
    """Perform OCR on a PDF chunk with retry logic and backup APIs."""
    try:
        print(f"  Processing OCR for {split_name}...")

        for attempt in range(MAX_RETRIES + 1):
            try:
                client, api_index = get_next_available_client()
                
                if client is None:
                    error_msg = "No available Gemini API clients"
                    print(f"    Attempt {attempt + 1}: {error_msg}")
                    if attempt < MAX_RETRIES:
                        print(f"    Retrying in {RETRY_DELAY} seconds...")
                        time.sleep(RETRY_DELAY)
                        continue
                    else:
                        update_split_status(pdf_name, split_name, False, error_msg, attempt)
                        update_api_stats(api_index, False)
                        return None

                print(f"    Attempt {attempt + 1}: Using API {api_index + 1}")

                with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                    temp_file.write(pdf_bytes)
                    temp_file_path = temp_file.name

                try:

                    file_ref = client.files.upload(file=temp_file_path)
                    
                    response = client.models.generate_content(
                        model=MODEL_ID,
                        contents=[file_ref, OCR_PROMPT]
                    )

                    print(f"  OCR completed for {split_name} (API {api_index + 1}, Attempt {attempt + 1})")
                    update_split_status(pdf_name, split_name, True, None, attempt)
                    update_api_stats(api_index, True)
                    
                    if attempt > 0:
                        PROCESSING_STATUS["retry_attempts"].append({
                            "pdf_name": pdf_name,
                            "split_name": split_name,
                            "successful_attempt": attempt + 1,
                            "api_used": api_index + 1,
                            "timestamp": datetime.now().isoformat()
                        })
                    
                    return response.text

                finally:
                    try:
                        os.unlink(temp_file_path)
                    except:
                        pass

            except Exception as e:
                error_msg = f"API {api_index + 1} error processing {split_name}: {str(e)}"
                print(f"    Attempt {attempt + 1}: {error_msg}")
                update_api_stats(api_index, False)
                
                if attempt < MAX_RETRIES:
                    print(f"    Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    final_error = f"Failed after {MAX_RETRIES + 1} attempts. Last error: {error_msg}"
                    update_split_status(pdf_name, split_name, False, final_error, attempt)
                    return None

    except Exception as e:
        error_msg = f"Critical error processing {split_name}: {str(e)}"
        print(f"  {error_msg}")
        update_split_status(pdf_name, split_name, False, error_msg, 0)
        return None

def create_zip_from_texts(ocr_results: Dict[str, str], pdf_name: str) -> Optional[bytes]:
    """Create a ZIP archive from OCR text results."""
    try:
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for split_name, ocr_text in ocr_results.items():
                text_filename = f"{pathlib.Path(split_name).stem}_ocr.txt"
                zipf.writestr(text_filename, ocr_text.encode('utf-8'))
        
        zip_bytes = zip_buffer.getvalue()
        zip_buffer.close()
        
        print(f"Successfully created ZIP archive for {pdf_name}")
        return zip_bytes

    except Exception as e:
        error_msg = f"Error creating ZIP archive for {pdf_name}: {str(e)}"
        print(error_msg)
        PROCESSING_STATUS["global_errors"].append({
            "type": "zip_creation_error",
            "pdf_name": pdf_name,
            "message": error_msg,
            "timestamp": datetime.now().isoformat()
        })
        return None

def process_single_pdf(pdf_name: str, pages_per_split: int) -> bool:
    """Complete pipeline to process a single PDF from Supabase."""
    print(f"\n--- Starting pipeline for: {pdf_name} ---")
    
    try:
        print("Step 1: Downloading PDF from Supabase...")
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_pdf_path = pathlib.Path(temp_file.name)
        
        if not download_pdf_from_supabase(pdf_name, temp_pdf_path):
            error_msg = f"Failed to download PDF {pdf_name}"
            print(f"{error_msg}. Aborting for this file.")
            finalize_pdf_status(pdf_name, False, error_msg)
            return False
        
        try:

            with open(temp_pdf_path, 'rb') as f:
                pdf_content = f.read()
            
            print("Step 2: Splitting PDF...")
            split_pdfs = split_pdf_in_memory(pdf_content, pages_per_split, pdf_name)

            if not split_pdfs:
                error_msg = f"Failed to split PDF {pdf_name}"
                print(f"{error_msg}. Aborting for this file.")
                finalize_pdf_status(pdf_name, False, error_msg)
                return False
            print(f"  Created {len(split_pdfs)} split files")

            print("Step 3: Processing splits with Gemini OCR (with retry logic)...")
            ocr_results = {}
            for i, (split_bytes, split_name) in enumerate(split_pdfs, 1):
                print(f"  Processing split {i}/{len(split_pdfs)}: {split_name}")
                ocr_text = gemini_ocr_pdf_with_retry(split_bytes, split_name, pdf_name)
                if ocr_text:
                    ocr_results[split_name] = ocr_text
            
            if not ocr_results:
                error_msg = f"No OCR text was generated for {pdf_name}"
                print(f"{error_msg}. Aborting zip creation for this file.")
                finalize_pdf_status(pdf_name, False, error_msg)
                return False
            print(f"  Successfully generated {len(ocr_results)} OCR text results")

            print("Step 4: Creating ZIP archive and uploading to Supabase...")
            zip_bytes = create_zip_from_texts(ocr_results, pdf_name)
            
            if zip_bytes:
                with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
                    temp_zip.write(zip_bytes)
                    temp_zip_path = pathlib.Path(temp_zip.name)
                
                try:
                    pdf_stem = pathlib.Path(pdf_name).stem
                    output_zip_name = f"{pdf_stem}-output.zip"
                    
                    if upload_file_to_supabase(temp_zip_path, output_zip_name):
                        print(f"Pipeline completed successfully for {pdf_name}!")
                        print(f"Output ZIP uploaded as: {output_zip_name}")
                        finalize_pdf_status(pdf_name, True)
                        return True
                    else:
                        error_msg = f"Failed to upload ZIP archive for {pdf_name}"
                        print(error_msg)
                        finalize_pdf_status(pdf_name, False, error_msg)
                        return False
                        
                finally:
                    try:
                        os.unlink(temp_zip_path)
                    except:
                        pass
            else:
                error_msg = f"Failed to create ZIP archive for {pdf_name}"
                print(error_msg)
                finalize_pdf_status(pdf_name, False, error_msg)
                return False

        finally:
            try:
                os.unlink(temp_pdf_path)
            except:
                pass

    except Exception as e:
        error_msg = f"Pipeline error for {pdf_name}: {str(e)}"
        print(error_msg)
        finalize_pdf_status(pdf_name, False, error_msg)
        return False

def main():
    """Main worker function to execute the PDF OCR pipeline for all PDFs in Supabase."""
    print("=======================================")
    print("=== PDF OCR Pipeline with Supabase Storage ===")
    print("=== Features: Retry Logic + Backup APIs ===")
    print("=======================================")
    
    PROCESSING_STATUS["session_info"]["start_time"] = datetime.now().isoformat()
    
    print("Initializing Supabase client...")
    if not initialize_supabase():
        print("Failed to initialize Supabase. Exiting.")
        return
    
    print("Initializing Gemini API clients...")
    initialize_gemini_clients()
    
    print(f"Input Bucket:     {INPUT_BUCKET_NAME}")
    print(f"Output Bucket:    {OUTPUT_BUCKET_NAME}")
    print(f"Pages per Split:  {DEFAULT_PAGES_PER_SPLIT}")
    print(f"Max Retries:      {MAX_RETRIES}")
    print(f"Retry Delay:      {RETRY_DELAY}s")
    print("-" * 50)

    print("Fetching PDF files from Supabase...")
    pdf_files = list_input_pdfs()

    if not pdf_files:
        print(f"No PDF files found in bucket '{INPUT_BUCKET_NAME}'.")
        print(f"Please upload your PDF files to the '{INPUT_BUCKET_NAME}' bucket in Supabase.")
        PROCESSING_STATUS["session_info"]["total_pdfs"] = 0
        upload_status_to_supabase(PROCESSING_STATUS)
        return

    PROCESSING_STATUS["session_info"]["total_pdfs"] = len(pdf_files)
    
    print(f"Found {len(pdf_files)} PDF(s) to process:")
    for pdf_file in pdf_files:
        print(f"  - {pdf_file}")
    
    successful_pipelines = 0
    failed_pipelines = 0

    for pdf_file_name in pdf_files:
        success = process_single_pdf(
            pdf_name=pdf_file_name,
            pages_per_split=DEFAULT_PAGES_PER_SPLIT
        )
        if success:
            successful_pipelines += 1
        else:
            failed_pipelines += 1
        
        upload_status_to_supabase(PROCESSING_STATUS)
        print("-" * 50)

    PROCESSING_STATUS["session_info"]["end_time"] = datetime.now().isoformat()
    upload_status_to_supabase(PROCESSING_STATUS)

    print("\n=======================================")
    print("=== Pipeline Execution Summary ===")
    print("=======================================")
    print(f"Total PDFs processed: {len(pdf_files)}")
    print(f"Successfully processed: {successful_pipelines}")
    print(f"Failed to process:    {failed_pipelines}")
    print(f"Results saved in bucket: {OUTPUT_BUCKET_NAME}")
    
    storage_stats = PROCESSING_STATUS["session_info"]["storage_stats"]
    print(f"\n=== Storage Statistics ===")
    print(f"Files downloaded: {storage_stats['files_downloaded']}")
    print(f"Files uploaded: {storage_stats['files_uploaded']}")
    print(f"Total download size: {storage_stats['total_download_size_mb']:.2f} MB")
    print(f"Total upload size: {storage_stats['total_upload_size_mb']:.2f} MB")
  
 
    print("\n=== API Usage Statistics ===")
    for api_key, stats in PROCESSING_STATUS["session_info"]["api_usage_stats"].items():
        successful = stats["successful_calls"]
        failed = stats["failed_calls"]
        total = successful + failed
        if total > 0:
            success_rate = (successful / total) * 100
            print(f"{api_key.upper()}: {successful} success, {failed} failed (Success rate: {success_rate:.1f}%)")
    
    total_retries = len(PROCESSING_STATUS["retry_attempts"])
    if total_retries > 0:
        print(f"\n=== Retry Statistics ===")
        print(f"Total successful retries: {total_retries}")
    
    print("=======================================")

if __name__ == "__main__":
    main()