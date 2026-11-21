"""Replay the OFAC advanced XML history into observations for one dataset.

Both `us_ofac_sdn` and `us_ofac_cons` read the same file and split its
sanctions entries by list. Keys designations on the entry ID and checksums the
party's `DistinctParty` node together with the entry, as the crawler does.
"""

import logging
from collections import Counter, defaultdict

import click
from lxml import etree
from zavod.helpers import record_hash, remove_namespace, xml_node_hash

from contrib.backtrack.harness import (
    dump_sql,
    iter_versions,
    observe_designation,
    open_db,
)

log = logging.getLogger("backtrack.us_ofac")


def keep_entry(dataset: str, list_id: str | None) -> bool:
    if dataset == "us_ofac_sdn":
        return list_id == "SDN List"
    return list_id not in ("Consolidated List", "SDN List")


@click.command()
@click.argument("dataset", type=click.Choice(["us_ofac_sdn", "us_ofac_cons"]))
def main(dataset: str) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db = open_db(dataset)
    for seen, path in iter_versions(dataset, "source.xml"):
        doc = remove_namespace(etree.parse(path)).getroot()
        lists = {
            el.get("ID"): el.text
            for el in doc.findall("ReferenceValueSets/ListValues/List")
        }
        entries: dict[str | None, list[etree._Element]] = defaultdict(list)
        for entry in doc.findall("SanctionsEntries/SanctionsEntry"):
            entries[entry.get("ProfileID")].append(entry)
        keys: set[str] = set()
        events: Counter[str | None] = Counter()
        for party in doc.findall("DistinctParties/DistinctParty"):
            profile_id = party.find("Profile").get("ID")
            party_hash = xml_node_hash(party)
            for entry in entries[profile_id]:
                if not keep_entry(dataset, lists[entry.get("ListID")]):
                    continue
                key = entry.get("ID")
                if key is None:
                    raise RuntimeError(f"{path.name}: entry without ID")
                if key in keys:
                    log.warning("%s: duplicate entry ID %s", path.name, key)
                keys.add(key)
                checksum = record_hash([party_hash, xml_node_hash(entry)])
                events[observe_designation(db, dataset, key, checksum, seen)] += 1
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
    log.info("Wrote %s", dump_sql(db, dataset))


if __name__ == "__main__":
    main()
