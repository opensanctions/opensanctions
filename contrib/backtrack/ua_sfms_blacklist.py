"""Replay the Ukrainian SFMS blacklist history into observations.

Keys designations on the `number-entry` of each `acount-list`, which is unique
in every version and never renumbered, and checksums the whole `acount-list`
node with `h.xml_node_sorted_hash`, as the crawler does. The sorted hash
absorbs a version that reorders the elements of each record.
"""

import logging
from collections import Counter

from lxml import etree
from zavod.helpers import xml_node_sorted_hash

from contrib.backtrack.harness import (
    dump_sql,
    iter_versions,
    observe_designation,
    open_db,
)

log = logging.getLogger("backtrack.ua_sfms_blacklist")
DATASET = "ua_sfms_blacklist"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db = open_db(DATASET)
    for seen, path in iter_versions(DATASET, "source.xml"):
        try:
            doc = etree.parse(str(path))
        except etree.XMLSyntaxError:
            log.warning("%s: unparsable, skipped", path.name)
            continue
        entries = doc.findall(".//acount-list")
        if len(entries) == 0:
            log.warning("%s: empty, skipped", path.name)
            continue
        keys: set[str] = set()
        events: Counter[str | None] = Counter()
        for entry in entries:
            key = entry.findtext("number-entry")
            if key is None:
                raise ValueError(f"{path.name}: record without number-entry")
            if key in keys:
                log.warning("%s: duplicate key %s", path.name, key)
            keys.add(key)
            checksum = xml_node_sorted_hash(entry)
            assert checksum is not None
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
