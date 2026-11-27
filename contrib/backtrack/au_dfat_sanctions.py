"""Replay the Australian DFAT consolidated list history into observations.

Groups rows into designations by the numeric part of their reference, with a
suffix for each further non-contiguous block of the same reference, and
checksums each designation's rows with `h.record_hash`, as the crawler does.
The list was published as `source.xls` until early 2023, and as `source.xlsx`
since; calamine reads both.
"""

import logging
from collections import Counter, defaultdict
from heapq import merge
from pathlib import Path

from python_calamine import CalamineError, CalamineWorkbook
from zavod.helpers import parse_xlsx_sheet, record_hash

from contrib.backtrack.harness import (
    dump_sql,
    iter_versions,
    observe_designation,
    open_db,
)

log = logging.getLogger("backtrack.au_dfat_sanctions")
DATASET = "au_dfat_sanctions"

# Botched exports, reverted within days: control dates as text (some with day
# and month swapped) or as Excel serial numbers, and Additional Information cut
# to 255 characters. Replaying them would date nearly every record to the fix.
SKIP = {
    "20250902000000-source.xlsx",
    "20250926000000-source.xlsx",
    "20260710020201-source.xlsx",
}


def clean_reference(ref: str) -> str:
    number = ref
    while len(number):
        try:
            return str(int(number))
        except ValueError:
            number = number[:-1]
    raise ValueError(ref)


def read_designations(path: Path) -> dict[str, list[dict[str, str | None]]] | None:
    try:
        workbook = CalamineWorkbook.from_path(str(path))
    except CalamineError:
        return None
    references: defaultdict[str, list[dict[str, str | None]]] = defaultdict(list)
    blocks_seen: dict[str, int] = {}
    last_ref: str | None = None
    block = 0
    for sheet_name in workbook.sheet_names:
        sheet = workbook.get_sheet_by_name(sheet_name)
        # parse_xlsx_sheet doesn't use the context.
        for row in parse_xlsx_sheet(None, sheet):  # type: ignore[arg-type]
            raw_ref = row.get("reference")
            if raw_ref is None:
                continue
            reference = clean_reference(raw_ref)
            if last_ref != reference:
                block = blocks_seen.get(reference, 0) + 1
                blocks_seen[reference] = block
            last_ref = reference
            if block > 1:
                reference = f"{reference}-{block}"
            references[reference].append(row)
    return references


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db = open_db(DATASET)
    versions = merge(
        iter_versions(DATASET, "source.xls"), iter_versions(DATASET, "source.xlsx")
    )
    for seen, path in versions:
        if path.name in SKIP:
            log.info("%s: skipped", path.name)
            continue
        designations = read_designations(path)
        if designations is None or len(designations) == 0:
            log.warning("%s: unparsable or empty, skipped", path.name)
            continue
        events: Counter[str | None] = Counter()
        for reference, rows in designations.items():
            checksum = record_hash(rows)
            events[observe_designation(db, DATASET, reference, checksum, seen)] += 1
        log.info(
            "%s: %d designations, %d listed, %d changed",
            path.name,
            len(designations),
            events["listed"],
            events["changed"],
        )
    db.commit()
    log.info("Wrote %s", dump_sql(db, DATASET))


if __name__ == "__main__":
    main()
