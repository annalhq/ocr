import os
import pathlib
import logging
import time
from typing import Optional
from google import genai
from google.genai import types
from google.api_core import retry
from .config import settings, OCR_PROMPT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Gemini client setup
client = genai.Client(api_key=settings.GEMINI_API_KEY)

@retry.Retry(
    predicate=retry.if_exception_type(Exception),
    initial=1.0,
    maximum=60.0,
    multiplier=2.0,
    deadline=300.0
)
def _call_gemini_api(filepath: pathlib.Path) -> str:
    """
    Call Gemini API with retry logic.
    
    Args:
        filepath: Path to the PDF file
        
    Returns:
        str: API response text
        
    Raises:
        Exception: If API call fails after retries
    """
    try:
        response = client.models.generate_content(
            model=settings.MODEL_NAME,
            contents=[
                types.Part.from_bytes(
                    data=filepath.read_bytes(),
                    mime_type='application/pdf',
                ),
                OCR_PROMPT
            ]
        )
        return response.text
    except Exception as e:
        logger.error(f"Gemini API call failed: {str(e)}")
        raise

def gemini_ocr_pdf(pdf_path: str, output_dir: str = settings.OUTPUT_DIR) -> Optional[str]:
    """
    Perform OCR on a PDF using Gemini API and save the result.

    Args:
        pdf_path: Path to the PDF file to process
        output_dir: Directory to save the output text file

    Returns:
        Optional[str]: Path to the output text file or None if failed
    """
    try:
        os.makedirs(output_dir, exist_ok=True)

        # Read PDF file
        filepath = pathlib.Path(pdf_path)
        if not filepath.exists():
            logger.error(f"PDF file not found: {pdf_path}")
            return None

        logger.info(f"Processing {pdf_path} with Gemini OCR...")

        # Call Gemini API with retry logic
        response_text = _call_gemini_api(filepath)

        # Create output filename
        base_name = filepath.stem
        output_filename = f"{base_name}.txt"
        output_path = os.path.join(output_dir, output_filename)

        # Save the response
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(response_text)

        logger.info(f"OCR completed for {pdf_path} -> {output_path}")
        return output_path

    except Exception as e:
        logger.error(f"Error processing {pdf_path}: {str(e)}")
        return None
