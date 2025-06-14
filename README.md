# PDF OCR Pipeline with Gemini

PDF OCR pipeline that splits PDFs into chunks and processes them using Google's Gemini API.

## Features

- PDF splitting into manageable chunks
- Parallel OCR processing using Gemini API
- Automatic retries for failed API calls
- Proper error handling and logging
- Docker containerization
- Configurable through environment variables


## Usage

1. Place your PDF file in the `input` directory as `input.pdf`

2. Run the pipeline using Docker Compose:
```bash
docker-compose up --build
```

3. The processed results will be available in the `output` directory

## Configuration

The pipeline can be configured using environment variables in the `docker-compose.yml` file:

- `GEMINI_API_KEY`: Your Gemini API key
- `MODEL_NAME`: Gemini model to use (default: gemini-2.5-flash-preview-05-20)
- `OUTPUT_DIR`: Directory for output files (default: /app/output)
- `PAGES_PER_SPLIT`: Number of pages per split PDF (default: 3)
- `MAX_RETRIES`: Maximum number of retries for API calls (default: 3)
- `RETRY_DELAY`: Delay between retries in seconds (default: 5)
- `INPUT_PDF_PATH`: Path to the input PDF file (default: /app/input/input.pdf)

## Development

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the pipeline:
```bash
python main.py
```

## Project Structure

```
.
├── Dockerfile
├── README.md
├── docker-compose.yml
├── requirements.txt
├── main.py
├── config.py
├── ocr_utils.py
├── pdf_utils.py
├── file_utils.py
├── input/
└── output/
```

## Monitoring

The pipeline logs all operations with timestamps and log levels. You can monitor the logs using:
```bash
docker-compose logs -f
```