import os
import zipfile
from typing import List, Optional
from supabase_utils import SupabaseStorage

class FileManager:
    def __init__(self):
        """Initialize file manager with Supabase storage"""
        self.storage = SupabaseStorage()

    def upload_file(self, file_path: str) -> str:
        """
        Upload a file to Supabase storage.
        
        Args:
            file_path (str): Local path to the file
            
        Returns:
            str: Storage path in Supabase
        """
        try:
            return self.storage.upload_input_pdf(file_path)
        except Exception as e:
            print(f"Error uploading file: {str(e)}")
            return None

    def create_zip_archive(self, files: List[str], zip_filename: str) -> Optional[str]:
        """
        Create a ZIP archive from a list of files.
        
        Args:
            files (List[str]): List of file paths to include
            zip_filename (str): Name of the output zip file
            
        Returns:
            str: Path to the created zip file or None if failed
        """
        try:
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in files:
                    if os.path.exists(file_path):
                        arcname = os.path.basename(file_path)
                        zipf.write(file_path, arcname)

            print(f"Created ZIP archive: {zip_filename}")
            return zip_filename

        except Exception as e:
            print(f"Error creating ZIP archive: {str(e)}")
            return None

    def cleanup_files(self, file_list: List[str]):
        """
        Clean up temporary files.
        
        Args:
            file_list (List[str]): List of file paths to delete
        """
        for file_path in file_list:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"Cleaned up: {file_path}")
            except Exception as e:
                print(f"Error cleaning up {file_path}: {str(e)}")

    def get_input_pdfs(self) -> List[str]:
        """
        Get list of input PDFs from Supabase storage.
        
        Returns:
            List[str]: List of PDF storage paths
        """
        try:
            return self.storage.list_input_pdfs()
        except Exception as e:
            print(f"Error getting input PDFs: {str(e)}")
            return []

    def download_pdf(self, storage_path: str, bucket: str) -> Optional[str]:
        """
        Download a PDF from Supabase storage.
        
        Args:
            storage_path (str): Path in Supabase storage
            bucket (str): Bucket name
            
        Returns:
            str: Local path to the downloaded file or None if failed
        """
        try:
            return self.storage.download_pdf(storage_path, bucket)
        except Exception as e:
            print(f"Error downloading PDF: {str(e)}")
            return None

    def upload_output(self, file_path: str, original_name: str) -> Optional[str]:
        """
        Upload a processed PDF to the output bucket.
        
        Args:
            file_path (str): Local path to the processed PDF
            original_name (str): Original PDF name for reference
            
        Returns:
            str: Storage path in Supabase or None if failed
        """
        try:
            return self.storage.upload_output_pdf(file_path, original_name)
        except Exception as e:
            print(f"Error uploading output: {str(e)}")
            return None

    def cleanup_storage(self):
        """Clean up temporary files and storage resources"""
        self.storage.cleanup()
