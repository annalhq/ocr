import json
import os
from datetime import datetime
from typing import Dict, List, Optional
from pdf_utils import split_pdf

class PDFProcessingTracker:
    def __init__(self, status_file: str = "pdf_processing_status.json"):
        self.status_file = status_file
        self.status_data = self._load_status()

    def _load_status(self) -> Dict:
        """Load the processing status from JSON file"""
        if os.path.exists(self.status_file):
            try:
                with open(self.status_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {}
        return {}

    def _save_status(self):
        """Save the current status to JSON file"""
        with open(self.status_file, 'w') as f:
            json.dump(self.status_data, f, indent=2)

    def initialize_pdf(self, pdf_path: str, total_pages: int):
        """Initialize tracking for a new PDF"""
        if pdf_path not in self.status_data:
            self.status_data[pdf_path] = {
                "total_pages": total_pages,
                "status": "pending",
                "retry_count": 0,
                "last_updated": datetime.now().isoformat(),
                "splits": {}
            }
            self._save_status()

    def update_split_status(self, pdf_path: str, split_path: str, status: str, 
                          start_page: int, end_page: int, error: Optional[str] = None):
        """Update the status of a PDF split"""
        if pdf_path in self.status_data:
            self.status_data[pdf_path]["splits"][split_path] = {
                "status": status,
                "start_page": start_page,
                "end_page": end_page,
                "error": error,
                "last_updated": datetime.now().isoformat()
            }
            self._save_status()

    def mark_pdf_complete(self, pdf_path: str):
        """Mark a PDF as completely processed"""
        if pdf_path in self.status_data:
            self.status_data[pdf_path]["status"] = "completed"
            self.status_data[pdf_path]["last_updated"] = datetime.now().isoformat()
            self._save_status()

    def increment_retry_count(self, pdf_path: str):
        """Increment the retry count for a PDF"""
        if pdf_path in self.status_data:
            self.status_data[pdf_path]["retry_count"] += 1
            self.status_data[pdf_path]["last_updated"] = datetime.now().isoformat()
            self._save_status()

    def get_failed_splits(self, pdf_path: str) -> List[Dict]:
        """Get all failed splits for a PDF"""
        if pdf_path not in self.status_data:
            return []
        
        failed_splits = []
        for split_path, split_info in self.status_data[pdf_path]["splits"].items():
            if split_info["status"] == "failed":
                failed_splits.append({
                    "split_path": split_path,
                    "start_page": split_info["start_page"],
                    "end_page": split_info["end_page"],
                    "error": split_info.get("error")
                })
        return failed_splits

    def retry_failed_splits(self, pdf_path: str) -> List[str]:
        """Retry failed splits by creating new 2-page splits"""
        failed_splits = self.get_failed_splits(pdf_path)
        if not failed_splits:
            return []

        new_split_paths = []
        for split in failed_splits:
            # Create new 2-page splits for the failed range
            start_page = split["start_page"]
            end_page = split["end_page"]
            
            # Split into 2-page chunks
            new_splits = split_pdf(pdf_path, pages_per_split=2)
            
            # Update status for new splits
            for new_split in new_splits:
                self.update_split_status(
                    pdf_path=pdf_path,
                    split_path=new_split,
                    status="pending",
                    start_page=start_page,
                    end_page=min(start_page + 1, end_page)
                )
                new_split_paths.append(new_split)
                start_page += 2

        self.increment_retry_count(pdf_path)
        return new_split_paths

    def get_pdf_status_summary(self) -> Dict:
        """Get a summary of all PDFs and their processing status"""
        summary = {}
        for pdf_path, data in self.status_data.items():
            summary[pdf_path] = {
                "status": data["status"],
                "retry_count": data["retry_count"],
                "total_pages": data["total_pages"],
                "splits": {
                    split_path: {
                        "status": split_info["status"],
                        "pages": f"{split_info['start_page']}-{split_info['end_page']}"
                    }
                    for split_path, split_info in data["splits"].items()
                }
            }
        return summary

    def log_status_summary(self):
        """Log the current status of all PDFs and their splits"""
        summary = self.get_pdf_status_summary()
        print("\nPDF Processing Status Summary:")
        print("=" * 80)
        
        for pdf_path, data in summary.items():
            print(f"\nPDF: {pdf_path}")
            print(f"Overall Status: {data['status']}")
            print(f"Retry Count: {data['retry_count']}")
            print(f"Total Pages: {data['total_pages']}")
            print("\nSplits:")
            for split_path, split_info in data["splits"].items():
                print(f"  - {split_path}: {split_info['status']} (Pages {split_info['pages']})")
            print("-" * 80) 