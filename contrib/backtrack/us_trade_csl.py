"""Replay the US Trade Consolidated Screening List history into observations.

Keys designations on the trade.gov record ID and checksums each record with
`h.record_hash`, as the crawler does. OFAC records are skipped: the crawler
emits no Sanction for them.
"""

import json
import logging
from collections import Counter

from zavod.helpers import record_hash

from contrib.backtrack.harness import (
    dump_sql,
    iter_versions,
    observe_designation,
    open_db,
)

log = logging.getLogger("backtrack.us_trade_csl")
DATASET = "us_trade_csl"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db = open_db(DATASET)
    for seen, path in iter_versions(DATASET, "source.json"):
        with open(path) as fh:
            results = json.load(fh)["results"]
        keys: set[str] = set()
        events: Counter[str | None] = Counter()
        for list_entry in results:
            if list_entry.get("entity_number") is not None:
                continue
            name = list_entry.get("name")
            if name is None or len(name.strip()) == 0:
                continue
            key = list_entry["id"]
            if key in keys:
                log.warning("%s: duplicate record ID %s", path.name, key)
            keys.add(key)
            checksum = record_hash(list_entry)
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
