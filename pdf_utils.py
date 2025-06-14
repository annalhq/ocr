import pathlib
from PyPDF2 import PdfReader, PdfWriter

# PDF Splitting function
def split_pdf(input_pdf_path, pages_per_split=3):
    """
    Split a PDF into multiple PDFs with specified pages per split.

    Args:
        input_pdf_path (str): Path to the input PDF file
        pages_per_split (int): Number of pages per split PDF

    Returns:
        list: List of paths to the split PDF files
    """
    try:
        # Extract base name without extension
        base_name = pathlib.Path(input_pdf_path).stem

        # Read the input PDF
        pdf_reader = PdfReader(input_pdf_path)
        total_pages = len(pdf_reader.pages)

        split_file_paths = []

        # Create splits
        for i in range(0, total_pages, pages_per_split):
            pdf_writer = PdfWriter()

            # Add pages to the current split
            end_page = min(i + pages_per_split, total_pages)
            for page_num in range(i, end_page):
                pdf_writer.add_page(pdf_reader.pages[page_num])

            # Create output filename
            split_filename = f"{base_name}-split{i//pages_per_split:02d}.pdf"
            split_file_paths.append(split_filename)

            # Write the split PDF
            with open(split_filename, 'wb') as output_file:
                pdf_writer.write(output_file)

            print(f"Created: {split_filename} (pages {i+1}-{end_page})")

        return split_file_paths

    except Exception as e:
        print(f"Error splitting PDF {input_pdf_path}: {str(e)}")
        return []
