"""Replay the Argentine RePET register history into observations.

Keys designations on the file they're in and their DATAID, like the crawler's
entity IDs, and checksums each record without COMMENTS1 with `h.record_hash`,
as the crawler does. Persons and entities are published as separate files,
replayed together in the order of their versions.
"""

import json
import logging
from collections import Counter
from heapq import merge

from zavod.helpers import record_hash

from contrib.backtrack.harness import (
    dump_sql,
    iter_versions,
    observe_designation,
    open_db,
)

log = logging.getLogger("backtrack.ar_repet")
DATASET = "ar_repet"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db = open_db(DATASET)
    versions = merge(
        iter_versions(DATASET, "personas.json"),
        iter_versions(DATASET, "entidades.json"),
    )
    for seen, path in versions:
        part = path.name.split("-", 1)[1].removesuffix(".json")
        try:
            with open(path, encoding="utf-8-sig") as fh:
                records = json.load(fh)
        except json.JSONDecodeError:
            log.warning("%s: unparsable, skipped", path.name)
            continue
        if not isinstance(records, list) or len(records) == 0:
            log.warning("%s: empty, skipped", path.name)
            continue
        keys: set[str] = set()
        events: Counter[str | None] = Counter()
        for data in records:
            if data.get("DATAID") is None:
                raise ValueError(f"{path.name}: record without DATAID")
            key = f"{part}:{data['DATAID']}"
            if key in keys:
                log.warning("%s: duplicate key %s", path.name, key)
            keys.add(key)
            checksum = record_hash({k: v for k, v in data.items() if k != "COMMENTS1"})
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
