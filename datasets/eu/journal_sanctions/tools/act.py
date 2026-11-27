"""Fetch one legal act into a work directory for transcription.

Writes `body.html` (the act body as CELLAR serves it), `meta.json` (CELLAR
metadata, including the publication and entry-into-force dates), and
`text.txt` — a plain rendering with one line per paragraph and one line per
table row, cells separated by ` | ` and paragraphs within a cell by ` ¶ `;
cells of a table nested in a cell are separated by ` ‖ `.
The rendering knows nothing about annexes, entries or labels: reading the
act is the transcriber's job.
"""

import json
import re
import time
from collections.abc import Callable
from pathlib import Path

import click
import requests
from lxml import html
from zavod.shed.ojeu.celex import normalize
from zavod.shed.ojeu.cellar import CellarClient, cli_client, extract_body_html

from .common import DATASET_DIR

OUT_DIR = Path(__file__).resolve().parent / "out"
CORPUS_DIR = DATASET_DIR.parents[2] / "data" / "tmp" / "eu_oj"
BLOCK_TAGS = {"p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "ul", "ol", "hr"}


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def walk(element: html.HtmlElement, nested: bool = False) -> list[str]:
    """Render an element as lines of text, losing none of it.

    Block elements and <br> end a line; inline elements are read through.
    A table yields one line per row, cells joined by ` | ` and the paragraphs
    of a cell by ` ¶ `. A table inside a cell (`nested`) yields one paragraph
    per row, its cells joined by ` ‖ `.
    """
    lines: list[str] = []
    buffer: list[str] = [element.text or ""]

    def flush() -> None:
        text = clean("".join(buffer))
        if text:
            lines.append(text)
        buffer.clear()

    def visit(node: html.HtmlElement) -> None:
        for child in node:
            tag = child.tag if isinstance(child.tag, str) else None
            if tag == "br":
                flush()
            elif tag == "table":
                flush()
                for row in child.xpath("./tr | ./thead/tr | ./tbody/tr | ./tfoot/tr"):
                    cells = [
                        " ¶ ".join(walk(cell, nested=True))
                        for cell in row
                        if cell.tag in ("td", "th")
                    ]
                    if any(cells):
                        lines.append((" ‖ " if nested else " | ").join(cells))
            elif tag in BLOCK_TAGS:
                flush()
                buffer.append(child.text or "")
                visit(child)
                flush()
            elif tag is not None:
                buffer.append(child.text or "")
                visit(child)
            buffer.append(child.tail or "")

    visit(element)
    flush()
    return lines


def ink(text: str) -> str:
    """The visible characters of a text, without whitespace or separators."""
    return re.sub(r"[\s|¶‖]", "", text)


def render_text(directory: Path) -> None:
    """Write `text.txt` from the cached `body.html`, checking nothing is lost."""
    root = html.fromstring((directory / "body.html").read_bytes().decode("utf-8"))
    lines = walk(root)
    if ink("".join(str(t) for t in root.itertext())) != ink("".join(lines)):
        raise ValueError(
            f"{directory.name}: text.txt does not carry all of the body's text"
        )
    (directory / "text.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def retrying[T](call: Callable[[], T], tries: int = 3) -> T:
    """Run a CELLAR call, retrying timeouts and server errors with backoff."""
    for attempt in range(tries):
        try:
            return call()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status < 500 or attempt == tries - 1:
                raise
        except (requests.Timeout, requests.ConnectionError):
            if attempt == tries - 1:
                raise
        time.sleep(10 * (attempt + 1))
    raise AssertionError("unreachable")


def fetch_act(client: CellarClient, celex: str, directory: Path) -> dict[str, object]:
    """Fetch an act's body and metadata into `directory`; return the metadata."""
    directory.mkdir(parents=True, exist_ok=True)
    meta = retrying(lambda: client.query_act(celex).to_dict())
    try:
        expression = retrying(lambda: client.fetch_expression(celex))
    except requests.HTTPError as exc:
        # Older acts have no XHTML expression, only HTML.
        if exc.response is None or exc.response.status_code != 404:
            raise
        expression = retrying(
            lambda: client.fetch_expression(celex, media_type="text/html")
        )
    meta["media_type"] = expression.media_type
    body = extract_body_html(expression)
    (directory / "body.html").write_bytes(body)
    render_text(directory)
    (directory / "meta.json").write_text(json.dumps(meta, indent=2))
    return meta


def obtain_act(celex: str, directory: Path) -> dict[str, object]:
    """Copy an act from the local corpus into `directory`, else fetch it.

    The corpus copy is re-rendered, so `text.txt` always comes from the
    current renderer.
    """
    cached = CORPUS_DIR / celex
    if (cached / "meta.json").exists() and (cached / "body.html").exists():
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "body.html").write_bytes((cached / "body.html").read_bytes())
        render_text(directory)
        meta_text = (cached / "meta.json").read_text()
        (directory / "meta.json").write_text(meta_text)
        meta: dict[str, object] = json.loads(meta_text)
        return meta
    with cli_client() as client:
        return fetch_act(client, celex, directory)


@click.command()
@click.argument("celex")
@click.option(
    "--dir",
    "directory",
    type=click.Path(path_type=Path),
    default=None,
    help="Work directory; default tools/out/<celex>/.",
)
def main(celex: str, directory: Path | None) -> None:
    """Fetch CELEX (from the local corpus when it holds it) and print its
    dates and relations."""
    celex = normalize(celex)
    directory = directory or OUT_DIR / celex
    meta = obtain_act(celex, directory)
    click.echo(f"{celex}  {meta['title']}")
    click.echo(
        f"  adopted {meta['document_date']}  published {meta['publication_date']}"
        f"  in force {meta['entry_into_force']}  amends {meta['amends']}"
    )
    click.echo(f"  -> {directory}")
