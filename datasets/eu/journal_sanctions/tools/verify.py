"""Check one op-log file against the text of the act it transcribes.

Every transcribed value must be printed in the act: a value missing from
`tools/out/<celex>/text.txt` was mistyped, assembled from pieces, or carried
over from somewhere else. The file's dates must agree with CELLAR's metadata.
Run `python -m tools.act <celex>` first to fetch the act.
"""

import json
import sys

import click

from .act import OUT_DIR
from .common import REGULATIONS_DIR, haystack, missing_values, read_oplog


@click.command()
@click.argument("framework")
@click.argument("celex")
def main(framework: str, celex: str) -> None:
    """Verify data/regulations/FRAMEWORK/CELEX.csv against act CELEX."""
    path = REGULATIONS_DIR / framework / f"{celex}.csv"
    directory = OUT_DIR / celex
    if not path.exists():
        raise click.UsageError(f"no op-log file {path}")
    if not (directory / "text.txt").exists():
        raise click.UsageError(f"run `python -m tools.act {celex}` first")
    meta = json.loads((directory / "meta.json").read_text())
    text = haystack((directory / "text.txt").read_text(encoding="utf-8"))
    errors = warnings = 0
    for line, row in enumerate(read_oplog(path), start=2):
        where = f"{celex}:{line}"
        if row["publishedDate"] != meta["publication_date"]:
            errors += 1
            click.echo(
                f"ERROR    {where}: publishedDate {row['publishedDate']} is not "
                f"CELLAR's {meta['publication_date']}"
            )
        if row["effectiveDate"] not in meta["entry_into_force"]:
            warnings += 1
            click.echo(
                f"WARNING  {where}: effectiveDate {row['effectiveDate']} is not among "
                f"CELLAR's {meta['entry_into_force']}"
            )
        for column, value in missing_values(row, text):
            errors += 1
            click.echo(f"ERROR    {where}: {column} not in the act: {value[:80]!r}")
    click.echo(f"{celex}: {errors} errors, {warnings} warnings")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
