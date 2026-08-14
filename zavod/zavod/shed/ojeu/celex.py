"""Normalize CELEX identifiers and inspect EU legal acts from the command line."""

from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import click
import requests

EUR_LEX_URL = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}"

_BARE_CELEX_RE = re.compile(
    r"^(?P<sector>[0-9])(?P<year>[0-9]{4})(?P<type>[A-Z]{1,2})"
    r"(?P<number>[0-9A-Z()]{4,})(?:-(?P<date>[0-9]{8}))?$"
)
_CELEX_IN_TEXT_RE = re.compile(
    r"(?:CELEX\s*(?::|%3A)\s*)([0-9][0-9A-Z()]+(?:-[0-9]{8})?)",
    re.IGNORECASE,
)
_ELI_TYPE_TO_CELEX = {
    "dec": "D",
    "dec_del": "D",
    "dec_impl": "D",
    "dir": "L",
    "dir_del": "L",
    "dir_impl": "L",
    "reg": "R",
    "reg_del": "R",
    "reg_impl": "R",
}


def normalize(value: str) -> str:
    """Return a validated CELEX identifier from an identifier or official URL.

    Use this at crawler and command-line boundaries so every downstream CELLAR
    request is keyed by the same uppercase identifier.
    """
    raw = unquote(value.strip())
    match = _CELEX_IN_TEXT_RE.search(raw)
    if match is not None:
        return _validate(match.group(1))

    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        query = parse_qs(parsed.query)
        for values in query.values():
            for query_value in values:
                match = _CELEX_IN_TEXT_RE.search(query_value)
                if match is not None:
                    return _validate(match.group(1))
        eli_celex = _celex_from_eli_path(parsed.path)
        if eli_celex is not None:
            return _validate(eli_celex)
        raise ValueError(
            f"URL does not contain a supported CELEX or ELI identifier: {value}"
        )

    if raw.upper().startswith("CELEX:"):
        raw = raw.split(":", 1)[1]
    return _validate(raw)


def eur_lex_url(value: str, language: str = "EN") -> str:
    """Build a stable EUR-Lex text URL for links shown to reviewers."""
    celex = normalize(value)
    return EUR_LEX_URL.format(celex=celex).replace("/EN/", f"/{language.upper()}/")


def _validate(value: str) -> str:
    celex = value.strip().upper()
    if _BARE_CELEX_RE.fullmatch(celex) is None:
        raise ValueError(f"Invalid or unsupported CELEX identifier: {value}")
    return celex


def _celex_from_eli_path(path: str) -> str | None:
    parts = [part for part in path.split("/") if part]
    try:
        eli_idx = parts.index("eli")
        eli_type, year, number = parts[eli_idx + 1 : eli_idx + 4]
    except (ValueError, IndexError):
        return None
    celex_type = _ELI_TYPE_TO_CELEX.get(eli_type.lower())
    if (
        celex_type is None
        or not year.isdigit()
        or len(year) != 4
        or not number.isdigit()
    ):
        return None
    celex = f"3{year}{celex_type}{int(number):04d}"
    if len(parts) > eli_idx + 4:
        version = parts[eli_idx + 4]
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", version):
            celex = f"0{celex[1:]}-{version.replace('-', '')}"
    return celex


@click.command(help="Inspect an EU legal act via CELLAR.")
@click.argument("celex")
@click.option(
    "--language", default="ENG", show_default=True, help="CELLAR language code"
)
@click.option(
    "--media-type",
    default="application/xhtml+xml",
    show_default=True,
    help="Preferred expression media type.",
)
@click.option(
    "--metadata-only",
    is_flag=True,
    help="Query metadata without downloading the expression.",
)
@click.option(
    "--save-expression",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Write the fetched expression to this path.",
)
def cli(
    celex: str,
    language: str,
    media_type: str,
    metadata_only: bool,
    save_expression: Path | None,
) -> None:
    """Print stable JSON for an act so agents can inspect it without a crawl."""
    from zavod.shed.ojeu.cellar import cli_client

    try:
        celex = normalize(celex)
        with cli_client() as client:
            act = client.query_act(celex)
            output = act.to_dict()
            if not metadata_only:
                expression = client.fetch_expression(
                    celex, language=language, media_type=media_type
                )
                output["expression"] = expression.to_dict()
                if save_expression is not None:
                    save_expression.write_bytes(expression.content)
                    output["expression"]["saved_to"] = str(save_expression)
            elif save_expression is not None:
                raise ValueError(
                    "--save-expression cannot be used with --metadata-only"
                )
    except (OSError, ValueError, requests.RequestException) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(output, indent=2, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    cli()
