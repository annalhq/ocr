import os
from typing import List, Optional, Tuple
from supabase import create_client, Client
from dotenv import load_dotenv
import tempfile
import shutil

load_dotenv()

class SupabaseStorage:
    def __init__(self):
        """Initialize Supabase client with environment variables"""
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")
        
        self.client: Client = create_client(self.supabase_url, self.supabase_key)
        self.input_bucket = "input-pdfs"
        self.output_bucket = "output-pdfs"
        self.temp_dir = tempfile.mkdtemp()

    def _ensure_buckets_exist(self):
        """Ensure required storage buckets exist"""
        try:
            buckets = self.client.storage.list_buckets()
            bucket_names = [bucket.name for bucket in buckets]
            
            if self.input_bucket not in bucket_names:
                self.client.storage.create_bucket(self.input_bucket)
            if self.output_bucket not in bucket_names:
                self.client.storage.create_bucket(self.output_bucket)
        except Exception as e:
            print(f"Error ensuring buckets exist: {str(e)}")
            raise

    def upload_input_pdf(self, file_path: str) -> str:
        """
        Upload a PDF file to the input bucket
        
        Args:
            file_path (str): Local path to the PDF file
            
        Returns:
            str: The path in Supabase storage
        """
        try:
            self._ensure_buckets_exist()
            file_name = os.path.basename(file_path)
            storage_path = f"input/{file_name}"
            
            with open(file_path, 'rb') as f:
                self.client.storage.from_(self.input_bucket).upload(
                    storage_path,
                    f.read(),
                    {"content-type": "application/pdf"}
                )
            
            return storage_path
        except Exception as e:
            print(f"Error uploading input PDF: {str(e)}")
            raise

    def download_pdf(self, storage_path: str, bucket: str) -> str:
        """
        Download a PDF from Supabase storage to a temporary file
        
        Args:
            storage_path (str): Path in Supabase storage
            bucket (str): Bucket name
            
        Returns:
            str: Local path to the downloaded file
        """
        try:
            temp_file = os.path.join(self.temp_dir, os.path.basename(storage_path))
            
            response = self.client.storage.from_(bucket).download(storage_path)
            
            with open(temp_file, 'wb') as f:
                f.write(response)
            
            return temp_file
        except Exception as e:
            print(f"Error downloading PDF: {str(e)}")
            raise

    def upload_output_pdf(self, file_path: str, original_name: str) -> str:
        """
        Upload a processed PDF to the output bucket
        
        Args:
            file_path (str): Local path to the processed PDF
            original_name (str): Original PDF name for reference
            
        Returns:
            str: The path in Supabase storage
        """
        try:
            self._ensure_buckets_exist()
            file_name = os.path.basename(file_path)
            storage_path = f"output/{original_name}/{file_name}"
            
            with open(file_path, 'rb') as f:
                self.client.storage.from_(self.output_bucket).upload(
                    storage_path,
                    f.read(),
                    {"content-type": "application/pdf"}
                )
            
            return storage_path
        except Exception as e:
            print(f"Error uploading output PDF: {str(e)}")
            raise

    def list_input_pdfs(self) -> List[str]:
        """
        List all PDFs in the input bucket
        
        Returns:
            List[str]: List of PDF paths in storage
        """
        try:
            self._ensure_buckets_exist()
            files = self.client.storage.from_(self.input_bucket).list("input")
            return [f["name"] for f in files if f["name"].endswith(".pdf")]
        except Exception as e:
            print(f"Error listing input PDFs: {str(e)}")
            raise

    def cleanup(self):
        """Clean up temporary files"""
        try:
            shutil.rmtree(self.temp_dir)
        except Exception as e:
            print(f"Error cleaning up temporary files: {str(e)}")

    def get_pdf_info(self, storage_path: str, bucket: str) -> Tuple[str, int]:
        """
        Get PDF file information
        
        Args:
            storage_path (str): Path in Supabase storage
            bucket (str): Bucket name
            
        Returns:
            Tuple[str, int]: (file_name, file_size)
        """
        try:
            response = self.client.storage.from_(bucket).get_public_url(storage_path)
            file_name = os.path.basename(storage_path)
            # Note: Getting file size would require an additional API call
            # For now, we'll return 0 as the size
            return file_name, 0
        except Exception as e:
            print(f"Error getting PDF info: {str(e)}")
            raise 