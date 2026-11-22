"""Replay the US SAM Procurement Exclusions history into observations.

Keys designations on the SAM Number and checksums each row with
`h.record_hash`, as the crawler does. OFAC records are skipped: the crawler
emits no Sanction for them.

Only rows whose checksum differs from the previous version are observed, since
an unchanged row would only move its `last_seen`. That is set once at the end
for the rows of the last version, as the crawler's next run would.

SAM Numbers are empty in the extracts before 2026-04-18, when SAM introduced
its current record IDs, so the history before then cannot be keyed. The last
version before that day only serves as the baseline. The keys first seen on
the day are recorded as of the baseline, so that `listingDate` stays empty on
the Sanctions without a usable creation date, while their checksums still date
the change as `modifiedAt`.
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
from zavod.stateful.observations import observe

from contrib.backtrack.harness import (
    dump_sql,
    iter_versions,
    observe_designation,
    open_db,
)

log = logging.getLogger("backtrack.us_sam_exclusions")
DATASET = "us_sam_exclusions"
REKEY_DATE = date(2026, 4, 18)


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
    baseline: datetime | None = None
    previous: dict[str, str] = {}
    current: dict[str, str] = {}
    seen = datetime.min
    for seen, path in iter_versions(DATASET, "source.zip"):
        if seen.date() < REKEY_DATE:
            baseline = seen
            continue
        current = read_rows(path)
        if len(current) == 0:
            raise RuntimeError(f"{path.name}: no designations found")
        if seen.date() == REKEY_DATE:
            if baseline is None:
                raise RuntimeError(f"No version before {REKEY_DATE}")
            for key in current:
                observe(db, DATASET, key, "", baseline)
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
