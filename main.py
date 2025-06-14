import os
import pathlib
import logging
import shutil
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import from local modules
from .pdf_utils import split_pdf
from .ocr_utils import gemini_ocr_pdf
from .file_utils import upload_file, create_zip_archive, cleanup_files
from .config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_pdf_pipeline(
    pdf_path: str,
    pages_per_split: int = settings.PAGES_PER_SPLIT,
    cleanup_splits: bool = True,
    max_workers: int = 4
) -> Optional[str]:
    """
    Complete pipeline to process a PDF: split, OCR, and zip results.

    Args:
        pdf_path: Path to the input PDF file
        pages_per_split: Number of pages per split PDF
        cleanup_splits: Whether to delete split PDFs after processing
        max_workers: Maximum number of parallel workers for OCR processing

    Returns:
        Optional[str]: Path to the final ZIP file or None if failed
    """
    try:
        # Extract base name for output naming
        base_name = pathlib.Path(pdf_path).stem
        output_dir = settings.OUTPUT_DIR
        zip_filename = f"{base_name}-output.zip"

        logger.info(f"Starting pipeline for: {pdf_path}")
        logger.info(f"Base name: {base_name}")
        logger.info(f"Pages per split: {pages_per_split}")

        # Step 1: Split the PDF
        logger.info("Step 1: Splitting PDF...")
        split_files = split_pdf(pdf_path, pages_per_split)

        if not split_files:
            logger.error("Failed to split PDF. Aborting pipeline.")
            return None

        logger.info(f"Created {len(split_files)} split files")

        # Step 2: Process each split with Gemini OCR in parallel
        logger.info("Step 2: Processing splits with Gemini OCR...")
        processed_files = []
        failed_files = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(gemini_ocr_pdf, split_file, output_dir): split_file
                for split_file in split_files
            }

            for future in as_completed(future_to_file):
                split_file = future_to_file[future]
                try:
                    output_file = future.result()
                    if output_file:
                        processed_files.append(output_file)
                        logger.info(f"✓ Successfully processed: {split_file}")
                    else:
                        failed_files.append(split_file)
                        logger.error(f"✗ Failed to process: {split_file}")
                except Exception as e:
                    failed_files.append(split_file)
                    logger.error(f"✗ Error processing {split_file}: {str(e)}")

        if not processed_files:
            logger.error("No files were processed successfully by OCR. Aborting zip creation.")
            if cleanup_splits:
                logger.info("Cleaning up split files...")
                cleanup_files(split_files)
            return None

        logger.info(f"Processed {len(processed_files)}/{len(split_files)} files successfully")
        if failed_files:
            logger.warning(f"Failed to process {len(failed_files)} files")

        # Step 3: Create ZIP archive
        logger.info("Step 3: Creating ZIP archive...")
        if not os.path.exists(output_dir) or not os.listdir(output_dir):
            logger.error(f"Output directory '{output_dir}' is empty or does not exist. Skipping ZIP creation.")
            if cleanup_splits:
                logger.info("Cleaning up split files...")
                cleanup_files(split_files)
            return None

        zip_path = create_zip_archive(output_dir, zip_filename)

        if zip_path:
            logger.info(f"✓ Pipeline completed successfully!")
            logger.info(f"✓ ZIP archive created: {zip_path}")
        else:
            logger.error("✗ Failed to create ZIP archive")

        # Step 4: Cleanup
        if cleanup_splits:
            logger.info("Step 4: Cleaning up split files...")
            cleanup_files(split_files)
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)
                logger.info(f"Cleaned up output directory: {output_dir}")

        return zip_path

    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        return None

# main worker

# Block 7: Main worker function - Run this to execute the complete pipeline
def main() -> None:
    """
    Main worker function to execute the complete PDF OCR pipeline.
    This function handles file upload, processing, and provides download link.
    """
    logger.info("=== PDF OCR Pipeline with Gemini ===")
    logger.info("This pipeline will:")
    logger.info("1. Upload your PDF file")
    logger.info("2. Split it into smaller PDFs (3 pages each)")
    logger.info("3. Process each split with Gemini OCR")
    logger.info("4. Save results as text files")
    logger.info("5. Create a ZIP archive with all results")
    logger.info("=" * 50)

    try:
        # Step 1: Upload file
        pdf_file = upload_file()
        if not pdf_file:
            logger.error("No file uploaded. Exiting.")
            return

        # Verify the uploaded file exists
        if not os.path.exists(pdf_file):
            logger.error(f"Uploaded file not found: {pdf_file}")
            return

        logger.info(f"File uploaded successfully: {pdf_file}")
        logger.info("=" * 50)

        # Step 2: Process the pipeline
        result_zip = process_pdf_pipeline(
            pdf_path=pdf_file,
            pages_per_split=settings.PAGES_PER_SPLIT,
            cleanup_splits=True
        )

        # Step 3: Provide download link
        if result_zip and os.path.exists(result_zip):
            logger.info("=" * 50)
            logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY! 🎉")
            logger.info(f"📁 Output ZIP file: {result_zip}")
            logger.info("📥 Downloading the results...")

            # Download the ZIP file
            files.download(result_zip) # This is Colab specific

            logger.info("✅ Download initiated!")
            logger.info("💡 Check your downloads folder for the ZIP file.")
            
            # Clean up the uploaded PDF and the final zip file after download
            cleanup_files([pdf_file, result_zip])
            # Clean up the output directory if it still exists and is empty or if you want to remove it
            output_dir = "output"
            if os.path.exists(output_dir) and not os.listdir(output_dir):
                 os.rmdir(output_dir)
                 logger.info(f"Cleaned up empty output directory: {output_dir}")
            # If output_dir might contain files not in zip (e.g. failed OCRs not cleaned),
            # or if you want to remove it regardless, use shutil.rmtree(output_dir)
            # import shutil
            # if os.path.exists(output_dir):
            #     shutil.rmtree(output_dir)
            #     print(f"Cleaned up output directory: {output_dir}")


        else:
            logger.error("❌ Pipeline failed. Please check the error messages above.")
            # Clean up the originally uploaded PDF if pipeline failed
            if os.path.exists(pdf_file):
                cleanup_files([pdf_file])


    except Exception as e:
        logger.error(f"❌ Main function error: {str(e)}")
        # Attempt to clean up uploaded file in case of error during processing
        # Check if pdf_file variable exists and then if the file exists
        if 'pdf_file' in locals() and pdf_file and os.path.exists(pdf_file):
            cleanup_files([pdf_file])


    logger.info("=" * 50)
    logger.info("Pipeline execution completed.")

# Execute the main pipeline
if __name__ == "__main__":
    main()