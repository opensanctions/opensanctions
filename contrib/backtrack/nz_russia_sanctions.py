"""Replay the NZ Russia sanctions register history into observations.

Keys designations on "Unique Identifier" and checksums each row with
`h.record_hash`, reading the workbook as the crawler does.
"""

import logging
from collections import Counter
from typing import Any

from normality import stringify
from python_calamine import CalamineWorkbook
from zavod.helpers import record_hash

from contrib.backtrack.harness import (
    dump_sql,
    iter_versions,
    observe_designation,
    open_db,
)

log = logging.getLogger("backtrack.nz_russia_sanctions")
DATASET = "nz_russia_sanctions"


def read_rows(path: Any) -> list[dict[str, Any]]:
    workbook = CalamineWorkbook.from_path(path)
    rows: list[dict[str, Any]] = []
    for sheet_name in workbook.sheet_names:
        sheet = workbook.get_sheet_by_name(sheet_name)
        headers: list[str | None] | None = None
        for row in sheet.to_python(skip_empty_area=False):
            cells = [None if value == "" else value for value in row]
            if "Unique Identifier" in cells and "DOB" in cells:
                headers = [stringify(h) for h in cells]
                continue
            if headers is None:
                continue
            data = dict(zip(headers, cells))
            rows.append({k: v for k, v in data.items() if k is not None})
    return rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db = open_db(DATASET)
    for seen, path in iter_versions(DATASET, "source.xlsx"):
        keys: set[str] = set()
        events: Counter[str | None] = Counter()
        for data in read_rows(path):
            entity_type = data.get("Type")
            if entity_type in ("Asset", "Total") or entity_type is None:
                continue
            key = stringify(data.get("Unique Identifier"))
            if key is None:
                log.warning("%s: row without Unique Identifier", path.name)
                continue
            if key in keys:
                log.warning("%s: duplicate Unique Identifier %s", path.name, key)
            keys.add(key)
            checksum = record_hash(data)
            events[observe_designation(db, DATASET, key, checksum, seen)] += 1
        if len(keys) == 0:
            raise RuntimeError(f"{path.name}: no designations found")
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
