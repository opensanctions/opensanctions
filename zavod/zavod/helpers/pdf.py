from normality import collapse_spaces, slugify
from pathlib import Path
from pdfplumber.page import Page
from tempfile import mkdtemp
from typing import Callable, Dict, Generator, List
import pdfplumber
import subprocess


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
    path: Path,
    save_debug_images=False,
    headers_per_page=False,
    preserve_header_newlines=False,
    page_settings: Callable[[Page], Dict] = None,
) -> Generator[Dict[str, str], None, None]:
    """
    Parse the largest table on each page of a PDF file and yield their rows as dictionaries.

    Multiline header values are slugified like  "first_row-second_row".

    Arguments:
        path: Path to the PDF file.
        save_debug_images: Save a PNG image of each page.
        headers_per_page: Set to true if the headers are repeated on each page.
        preserve_header_newlines: Don't slugify newlines in headers -
            e.g. for when the line breaks are meaningful.
        page_settings: A function that takes a `pdfplumber.page.Page` object and returns
            a dictionary of settings for `extract_table`.
    """
    pdf = pdfplumber.open(path)
    headers = None
    for page in pdf.pages:
        if headers_per_page:
            headers = None

        if page_settings is not None:
            settings = page_settings(page)
        else:
            settings = {}

        if save_debug_images:
            im = page.to_image()
            im.save(f"page-{page.page_number}.png")

        for row in page.extract_table(settings):
            if headers is None:
                headers = [header_slug(cell, preserve_header_newlines) for cell in row]
                continue
            assert len(headers) == len(row), (headers, row)
            yield dict(zip(headers, row))
