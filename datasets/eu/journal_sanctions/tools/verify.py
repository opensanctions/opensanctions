"""Check one op-log file against the text of the act it transcribes.

Every transcribed value must be printed in the act: a value missing from
`tools/out/<celex>/text.txt` was mistyped, assembled from pieces, or carried
over from somewhere else. The exception is a MOD or DEL, which reproduces its
target's row: values unchanged from the target need not be printed. The
file's dates must agree with CELLAR's metadata. Run `python -m tools.act <celex>` first to fetch the act.
"""

import json
import re
import sys
from datetime import date, timedelta

import click

from .act import OUT_DIR
from .common import REGULATIONS_DIR, haystack, missing_values, read_oplog
from .replay import replay

SAME_DAY_RE = re.compile(
    r"enter into force on the (?:date|day) of its publication", re.IGNORECASE
)
NEXT_DAY_RE = re.compile(
    r"enter into force on the (?:day|date) following (?:that|the date) of its "
    r"publication",
    re.IGNORECASE,
)


def act_in_force(text: str, published: str) -> str | None:
    """The entry-into-force date an act's standard final article states."""
    if SAME_DAY_RE.search(text):
        return published
    if NEXT_DAY_RE.search(text):
        return (date.fromisoformat(published) + timedelta(days=1)).isoformat()
    return None


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
    # A MOD or DEL reproduces its target's row; values carried over from the
    # target need not be printed in this act.
    targets = replay(framework, before=celex).live
    errors = warnings = 0
    for line, row in enumerate(read_oplog(path), start=2):
        key = (row["id"], row["programKey"], row["measure"], row["annex"])
        target = targets.get(key)
        carried = target.row if target is not None and row["op"] != "ADD" else None
        where = f"{celex}:{line}"
        if row["publishedDate"] != meta["publication_date"]:
            errors += 1
            click.echo(
                f"ERROR    {where}: publishedDate {row['publishedDate']} is not "
                f"CELLAR's {meta['publication_date']}"
            )
        # CELLAR records no entry into force for corrigenda; they take
        # effect on publication.
        in_force = meta["entry_into_force"] or [meta["publication_date"]]
        if row["effectiveDate"] not in in_force:
            warnings += 1
            stated = act_in_force(text, meta["publication_date"])
            agrees = "agrees" if stated == row["effectiveDate"] else "disagrees"
            click.echo(
                f"WARNING  {where}: effectiveDate {row['effectiveDate']} is not "
                f"CELLAR's {in_force}; the act's final article says {stated} "
                f"({agrees})"
            )
        for column, value in missing_values(row, text, carried=carried):
            errors += 1
            click.echo(f"ERROR    {where}: {column} not in the act: {value[:80]!r}")
    click.echo(f"{celex}: {errors} errors, {warnings} warnings")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
