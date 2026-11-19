"""Replay the French Trésor asset freeze register history into observations.

Keys designations on `IdRegistre` and checksums each `PublicationDetail`
record with `h.record_hash`, as the crawler does.
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

log = logging.getLogger("backtrack.fr_tresor_gels_avoir")
DATASET = "fr_tresor_gels_avoir"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db = open_db(DATASET)
    for seen, path in iter_versions(DATASET, "source.json"):
        try:
            with open(path) as fh:
                data = json.load(fh)
            records = data["Publications"]["PublicationDetail"]
        except (json.JSONDecodeError, KeyError, TypeError):
            log.warning("%s: not the register, skipped", path.name)
            continue
        keys: set[str] = set()
        events: Counter[str | None] = Counter()
        for record in records:
            key = str(record["IdRegistre"])
            if key in keys:
                log.warning("%s: duplicate IdRegistre %s", path.name, key)
            keys.add(key)
            checksum = record_hash(record)
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
