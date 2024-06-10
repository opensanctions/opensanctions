import subprocess
from pathlib import Path
from typing import List
from tempfile import mkdtemp


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
