from normality import collapse_spaces, slugify
from pathlib import Path
from pdfplumber.page import Page
from tempfile import mkdtemp
from typing import Callable, Dict, Generator, List, Optional, Tuple
import pdfplumber
import subprocess

from zavod.context import Context


def make_pdf_page_images(pdf_path: Path) -> List[Path]:
    """Split a PDF file into PNG images of its pages.

    This requires `pdftoppm` to be installed on the system, which is
    part of the `poppler-utils` package on Debian-based systems.
    """
    output_path = Path(mkdtemp())
    output_prefix = output_path / pdf_path.stem
    command = [
        "pdftoppm",
        "-png",
        "-r",
        "150",
        pdf_path.as_posix(),
        output_prefix.as_posix(),
    ]
    subprocess.run(command, check=True)
    return sorted(output_path.glob("*.png"))


def header_slug(text: str, preserve_newlines: bool) -> str:
    if preserve_newlines:
        rows = text.split("\n")
        return "\n".join(slugify(row, sep="_") for row in rows)
    else:
        return slugify(collapse_spaces(text), sep="_")


def parse_pdf_table(
    context: Context,
    path: Path,
    headers_per_page=False,
    preserve_header_newlines=False,
    start_page=1,
    end_page: Optional[int] = None,
    skiprows=0,
    page_settings: Callable[[Page], Tuple[Page, Dict]] = None,
    save_debug_images=False,
) -> Generator[Dict[str, str], None, None]:
    """
    Parse the largest table on each page of a PDF file and yield their rows as dictionaries.

    Multiline header values are slugified like  "first_row-second_row".

    Arguments:
        path: Path to the PDF file.
        headers_per_page: Set to true if the headers are repeated on each page.
        preserve_header_newlines: Don't slugify newlines in headers -
            e.g. for when the line breaks are meaningful.
        start_page: The first page to process. 1-indexed.
        end_page: The last page to process. 1-indexed.
        skiprows: The number of rows to skip before processing table headers.
        page_settings: A function that takes a `pdfplumber.page.Page` object and returns
            a tuple of a Page that will be used to extract a table, and a dictionary of
            settings for `extract_table`. The page could be e.g. a cropped version of the
            original.
        save_debug_images: Save a PNG image of each page.
    """
    start_page_idx = start_page - 1 if isinstance(start_page, int) else None
    end_page_idx = end_page - 1 if isinstance(end_page, int) else None
    pdf = pdfplumber.open(path)
    headers = None
    for page in pdf.pages[start_page_idx:end_page_idx]:
        if page.page_number % 100 == 0:
            context.log.info(f"Processing page {page.page_number}...")

        if headers_per_page:
            headers = None

        if page_settings is not None:
            page, settings = page_settings(page)
        else:
            settings = {}

        if save_debug_images:
            im = page.to_image()
            im.save(f"page-{page.page_number}.png")

        for row_num, row in enumerate(page.extract_table(settings)):
            if headers is None:
                if row_num < skiprows:
                    continue
                headers = [header_slug(cell, preserve_header_newlines) for cell in row]
                continue
            assert len(headers) == len(row), (headers, row)
            yield dict(zip(headers, row))

        page.close()
    pdf.close()
