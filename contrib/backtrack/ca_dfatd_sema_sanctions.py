"""Replay the Canadian SEMA consolidated list history into observations.

Keys designations on country, schedule and item, and checksums each record
without its listing date with `h.record_hash`, as the crawler does. Older
versions are first brought into the current format: the tags were renamed
over time, and on 2024-07-31 most schedules were renamed without any other
change to the records.
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

log = logging.getLogger("backtrack.ca_dfatd_sema_sanctions")
DATASET = "ca_dfatd_sema_sanctions"

# Older tag names, mapped to the current ones.
TAGS = {
    "Aliases": "Aliases-Alias",
    "Country": "Country-Pays",
    "DateOfBirth": "DateOfBirthOrShipBuildDate-DateDeNaissanceOuDateDeConstructionDuNavire",
    "DateOfBirthOrShipBuildDate": "DateOfBirthOrShipBuildDate-DateDeNaissanceOuDateDeConstructionDuNavire",
    "DateOfListing": "DateOfListing-DateDinscription",
    "Entity": "EntityOrShip-EntiteOuNavire",
    "EntityOrShip": "EntityOrShip-EntiteOuNavire",
    "GivenName": "GivenName-Prenom",
    "Item": "Item-NumeroDarticle",
    "LastName": "LastName-NomDeFamille",
    "Schedule": "Schedule-Annexe",
    "ShipIMONumber": "ShipIMONumber-NumeroOMIDuNavire",
    "Title": "TitleOrShipType-TitreOuTypeDeNavire",
    "TitleOrShip": "TitleOrShipType-TitreOuTypeDeNavire",
}
TAGS.update({tag: tag for tag in list(TAGS.values())})

# Schedules renamed on 2024-07-31, by (country, old schedule). None means the
# schedule was dropped.
RENAMED_BEFORE = "20240731"
SCHEDULES = {
    ("Belarus / Bélarus", "1"): "1, Part 1",
    ("Belarus / Bélarus", "1.1"): "1, Part 1.1",
    ("Belarus / Bélarus", "2"): "1, Part 2",
    ("Belarus / Bélarus", "3"): "1, Part 3",
    ("China / Chine", "1, Part 1"): "Part 1",
    ("China / Chine", "1, Part 2"): "Part 2",
    ("Guatemala", "1"): None,
    ("Haiti / Haïti", "1"): "Part 1",
    ("Haiti / Haïti", "2"): "Part 2",
    ("Haiti / Haïti", "3"): "Part 3",
    ("Iran", "1"): "1, Part 1",
    ("Iran", "1.1"): "1, Part 1.1",
    ("Iran", "2"): "1, Part 2",
    ("Iran", "2.1"): "1, Part 2.1",
    (
        "Justice for Victims of Corrupt Foreign Officials Regulations (JVCFOR) / Règlement relatif à la justice pour les victimes de dirigeants étrangers corrompus (RJVDEC)",
        "1",
    ): None,
    ("Moldova", "1"): "Part 1",
    ("Moldova", "2"): "Part 2",
    ("Nicaragua", "N/A"): None,
    ("Russia / Russie", "1"): "1, Part 1",
    ("South Sudan / Soudan du sud", "N/A"): None,
    ("Sri Lanka", "1"): None,
    ("Syria / Syrie", "1"): "Part 1",
    ("Syria / Syrie", "2"): "Part 2",
    ("Ukraine", "1"): "Part 1",
    ("Ukraine", "1, Part 2"): "Part 2",
    ("Venezuela", "N/A"): None,
    ("Zimbabwe", "N/A"): None,
}

# Truncated, and missing the aliases of 459 records, which were back the next
# day: replaying it would date those records to the day after.
SKIP = {"20220712000000-source.xml"}


def read_rows(path: Path) -> list[dict[str, str]] | None:
    try:
        doc = etree.parse(str(path))
    except etree.XMLSyntaxError:
        return None
    rows: list[dict[str, str]] = []
    for node in doc.findall(".//record"):
        row: dict[str, str] = {}
        for child in node:
            if child.text is not None:
                text = child.text.strip()
                if text:
                    row[TAGS[child.tag]] = text
        if path.name < RENAMED_BEFORE:
            country_schedule = (row.get("Country-Pays"), row.get("Schedule-Annexe"))
            if country_schedule in SCHEDULES:
                schedule = SCHEDULES[country_schedule]
                if schedule is None:
                    row.pop("Schedule-Annexe")
                else:
                    row["Schedule-Annexe"] = schedule
        rows.append(row)
    return rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db = open_db(DATASET)
    for seen, path in iter_versions(DATASET, "source.xml"):
        if path.name in SKIP:
            log.info("%s: skipped", path.name)
            continue
        rows = read_rows(path)
        if rows is None or len(rows) == 0:
            log.warning("%s: unparsable or empty, skipped", path.name)
            continue
        keys: set[str] = set()
        events: Counter[str | None] = Counter()
        for row in rows:
            if "Item-NumeroDarticle" not in row:
                # A few records in older versions have no item number to key on.
                events["no item"] += 1
                continue
            checksum = record_hash(
                {k: v for k, v in row.items() if k != "DateOfListing-DateDinscription"}
            )
            key = "|".join(
                (
                    row.get("Country-Pays", ""),
                    row.get("Schedule-Annexe", ""),
                    row["Item-NumeroDarticle"],
                )
            )
            if key in keys:
                log.warning("%s: duplicate key %s", path.name, key)
            keys.add(key)
            events[observe_designation(db, DATASET, key, checksum, seen)] += 1
        log.info(
            "%s: %d designations, %d listed, %d changed, %d without item",
            path.name,
            len(keys),
            events["listed"],
            events["changed"],
            events["no item"],
        )
    db.commit()
    log.info("Wrote %s", dump_sql(db, DATASET))


if __name__ == "__main__":
    main()
