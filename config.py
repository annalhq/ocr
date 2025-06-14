import os
from typing import Dict, Any
from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    """Application settings."""
    GEMINI_API_KEY: str = Field(..., env='GEMINI_API_KEY')
    MODEL_NAME: str = Field("gemini-2.5-flash-preview-05-20", env='MODEL_NAME')
    OUTPUT_DIR: str = Field("output", env='OUTPUT_DIR')
    PAGES_PER_SPLIT: int = Field(3, env='PAGES_PER_SPLIT')
    MAX_RETRIES: int = Field(3, env='MAX_RETRIES')
    RETRY_DELAY: int = Field(5, env='RETRY_DELAY')
    
    class Config:
        env_file = ".env"
        case_sensitive = True

OCR_PROMPT = """
Act as an OCR extractor, Extract All the data As it is word by word. Do not summaries or reduce the length of content, Your goal is to extract The texts as it is.
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

settings = Settings()
