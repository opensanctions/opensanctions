import pdfplumber

from rigour.mime.types import PDF
from zavod import Context


def crawl(context: Context):

    pdf_path = context.fetch_resource("source.pdf", context.data_url)
    context.export_resource(pdf_path, PDF)

    full_text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Start extraction from page 4 onwards
            for page_num in range(3, len(pdf.pages)):
                page = pdf.pages[page_num]
                full_text += page.extract_text() or ""

        context.log.info(f"Extracted Text: {full_text}")
        text = full_text.encode("'ISO-8859-1'", "ignore").decode("utf-8")
        print(text)

    except Exception as e:
        context.log.warning(f"Error extracting text from {pdf_path}: {e}")
