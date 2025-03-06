# import pdfplumber
import pytesseract
from pdf2image import convert_from_path

# from rigour.mime.types import PDF
from zavod import Context


def crawl(context: Context):
    images = convert_from_path(
        "data/datasets/bh_mfa_terrorists/source.pdf", first_page=5
    )  # Convert PDF page to image
    text = pytesseract.image_to_string(images[0], lang="ara")  # Arabic OCR
    print(text)


# Previous attempt with pdfplumber, not being able to decode some chars

# pdf_path = context.fetch_resource("source.pdf", context.data_url)
# context.export_resource(pdf_path, PDF)
# full_text = ""
# try:
#     with pdfplumber.open(pdf_path) as pdf:
#         # Start extraction from page 4 onwards
#         for page_num in range(3, len(pdf.pages)):
#             page = pdf.pages[page_num]
#             full_text += page.extract_text() or ""

#     # context.log.info(f"Extracted Text: {full_text}")
#     # text = full_text.encode("utf-8", "ignore").decode("utf-8")

#     for char in full_text[:300]:
#         print(f"{char}: {ord(char)}")

# except Exception as e:
#     context.log.warning(f"Error extracting text from {pdf_path}: {e}")
