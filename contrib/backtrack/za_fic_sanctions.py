"""Replay the South African FIC targeted financial sanctions list history into observations.

Keys designations on their UN reference number, and checksums each row
without its IndividualID or EntityID row number with `h.record_hash`, as the
crawler does. The baseline is the 2023-02-06 version: earlier versions are
skipped.
"""

import logging
from collections import Counter
from pathlib import Path

from lxml import etree
from zavod.helpers import record_hash

from contrib.backtrack.harness import (
    dump_sql,
    iter_versions,
    observe_designation,
    open_db,
)

log = logging.getLogger("backtrack.za_fic_sanctions")
DATASET = "za_fic_sanctions"

# Versions before this hold only part of the list, some in a legacy format.
BASELINE = "20230206"


def read_rows(path: Path) -> list[dict[str, str]] | None:
    try:
        doc = etree.parse(str(path))
    except etree.XMLSyntaxError:
        return None
    rows: list[dict[str, str]] = []
    for table in [".//Table", ".//Table1"]:
        for row in doc.findall(table):
            data: dict[str, str] = {}
            for field in row.iterchildren():
                value = field.text
                if value is None or value == "NA":
                    continue
                data[field.tag] = value
            rows.append(data)
    return rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db = open_db(DATASET)
    for seen, path in iter_versions(DATASET, "source.xml"):
        if path.name < BASELINE:
            log.info("%s: before baseline, skipped", path.name)
            continue
        rows = read_rows(path)
        if rows is None or len(rows) == 0:
            log.warning("%s: unparsable or empty, skipped", path.name)
            continue
        keys: set[str] = set()
        events: Counter[str | None] = Counter()
        for data in rows:
            key = data.get("ReferenceNumber")
            if key is None:
                raise ValueError(f"{path.name}: row without ReferenceNumber: {data!r}")
            checksum = record_hash(
                {k: v for k, v in data.items() if k not in ("IndividualID", "EntityID")}
            )
            if key in keys:
                log.warning("%s: duplicate key %s", path.name, key)
            keys.add(key)
            events[observe_designation(db, DATASET, key, checksum, seen)] += 1
        log.info(
            "%s: %d designations, %d listed, %d changed",
            path.name,
            len(keys),
            events["listed"],
            events["changed"],
        )
    db.commit()
    log.info("Wrote %s", dump_sql(db, DATASET))


if __name__ == "__main__":
    main()
