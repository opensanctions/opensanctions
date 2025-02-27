from normality import collapse_spaces, slugify
from pathlib import Path
from pdfplumber.page import Page
from tempfile import mkdtemp
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple
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
        return "\n".join(slugify(row, sep="_") or "" for row in rows)
    else:
        return slugify(collapse_spaces(text) or "", sep="_") or ""


def parse_pdf_table(
    context: Context,
    path: Path,
    headers_per_page: bool = False,
    preserve_header_newlines: bool = False,
    start_page: Optional[int] = None,
    end_page: Optional[int] = None,
    skiprows: int = 0,
    page_settings: Optional[Callable[[Page], Tuple[Page, Dict[str, Any]]]] = None,
) -> Generator[Dict[str, Optional[str]], None, None]:
    """
    Parse the largest table on each page of a PDF file and yield their rows as dictionaries.

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

    Pro tip:
        Save debug images in the page settings function to help with debugging.

        - https://github.com/jsvine/pdfplumber?tab=readme-ov-file#drawing-methods
        - https://github.com/jsvine/pdfplumber?tab=readme-ov-file#visually-debugging-the-table-finder

        ```
        def settings_func(page):
            cropped = page.crop((0, 93, page.width, page.height))
            im = cropped.to_image()
            im.save(f"page-{cropped.page_number}.png")
            return (cropped, PAGE_SETTINGS)
        ```
    """
    start_page_idx = start_page - 1 if isinstance(start_page, int) else None
    end_page_idx = end_page if isinstance(end_page, int) else None
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

        rows = page.extract_table(settings)
        if rows is None:
            raise Exception(f"No table found on page {page.page_number} of {path}")
        for row_num, row in enumerate(rows):
            if headers is None:
                if row_num < skiprows:
                    continue
                headers = [
                    header_slug(cell or "", preserve_header_newlines) for cell in row
                ]
                continue
            assert len(headers) == len(row), (headers, row)
            yield dict(zip(headers, row))

        page.close()
    pdf.close()
