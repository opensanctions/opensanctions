"""Replay the US SAM Procurement Exclusions history into observations.

Keys designations on the SAM Number and checksums each row with
`h.record_hash`, as the crawler does. OFAC records are skipped: the crawler
emits no Sanction for them.

Only rows whose checksum differs from the previous version are observed, since
an unchanged row would only move its `last_seen`. That is set once at the end
for the rows of the last version, as the crawler's next run would.

The replay starts on 2026-05-20, when SAM last re-issued record IDs: SAM
Numbers are empty before 2026-04-18, and about 5,300 records got new ones on
2026-05-20. The version of that day is the baseline.
"""

import csv
import io
import logging
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from zipfile import ZipFile

from zavod.helpers import record_hash
from zavod.stateful.model import observation_table

from contrib.backtrack.harness import (
    dump_sql,
    iter_versions,
    observe_designation,
    open_db,
)

log = logging.getLogger("backtrack.us_sam_exclusions")
DATASET = "us_sam_exclusions"
REKEY_DATE = date(2026, 5, 20)


def read_rows(zip_path: Path) -> dict[str, str]:
    """Map each designation's SAM Number to the checksum of its row."""
    rows: dict[str, str] = {}
    with ZipFile(zip_path, "r") as zip:
        for file_name in zip.namelist():
            with zip.open(file_name) as zfh:
                fh = io.TextIOWrapper(zfh)
                reader = csv.DictReader(fh, delimiter=",", quotechar='"')
                for row in reader:
                    checksum = record_hash(row)
                    if row["Excluding Agency"] == "OFAC":
                        continue
                    key = row["SAM Number"]
                    if key in rows:
                        raise RuntimeError(f"{zip_path.name}: duplicate key {key}")
                    rows[key] = checksum
    return rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db = open_db(DATASET)
    previous: dict[str, str] = {}
    current: dict[str, str] = {}
    seen = datetime.min
    for seen, path in iter_versions(DATASET, "source.zip"):
        if seen.date() < REKEY_DATE:
            continue
        current = read_rows(path)
        if len(current) == 0:
            raise RuntimeError(f"{path.name}: no designations found")
        events: Counter[str | None] = Counter()
        for key, checksum in current.items():
            if previous.get(key) == checksum:
                continue
            events[observe_designation(db, DATASET, key, checksum, seen)] += 1
        log.info(
            "%s: %d designations, %d listed, %d changed",
            path.name,
            len(current),
            events["listed"],
            events["changed"],
        )
        previous = current
    if len(previous) == 0:
        raise RuntimeError(f"No version from {REKEY_DATE} on")

    table = observation_table
    for key, checksum in current.items():
        stmt = table.update()
        stmt = stmt.where(table.c.dataset == DATASET)
        stmt = stmt.where(table.c.key == key)
        stmt = stmt.where(table.c.checksum.in_(["", checksum]))
        db.execute(stmt.values(last_seen=seen))
    db.commit()
    log.info("Wrote %s", dump_sql(db, DATASET))


if __name__ == "__main__":
    main()
