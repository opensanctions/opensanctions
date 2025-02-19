# A CSV export with the following constraints:
#
# - no newlines within values. Newline only ever denotes a new record.
# - numeric IDs up to 32 digits
#   (the numeric part is from customer experience and not in the Temenos documentation)
# - names are always within the latin-1 character set (but encoded as UTF-8).
#   Apparently non-latin-1 outside of the name field, e.g. aliases, isn't an issue,
#   but the current simplecsv exporter does sometimes pick non-latin script names
#   when they are available, and the customer is operating with latin name inputs.
# - every value is always within double quotes
#
# At initial conception it is intended to support Temenos FCM.

import csv
import hashlib
import re
from typing import List
from followthemoney.types import registry
from normality.scripts import is_latin
from rigour.names import pick_name

from zavod.entity import Entity
from zavod.meta import get_catalog
from zavod.exporters.simplecsv import SimpleCSVExporter


REGEX_CLEAN_NEWLINES = re.compile(r"[\n\r]+")


def hash_id(id_: str) -> str:
    hash = hashlib.sha256()
    hash.update(id_.encode("utf-8"))
    digest = str(int(hash.hexdigest(), 16))
    return digest[-32:]


def prefer_latin_name(entity: Entity) -> str:
    """
    Pick the best name from only the latin script names, falling back to caption.
    """
    name = None
    preferred_candidates = {n for n in entity.get("name") if is_latin(n)}
    name = pick_name(list(preferred_candidates))
    return name or entity.caption


def clean_value(value: str) -> str:
    return REGEX_CLEAN_NEWLINES.sub(" ", value)


class SimpleCSV_02Exporter(SimpleCSVExporter):
    TITLE = "Targets as severely restricted CSV"
    FILE_NAME = "targets.simple-02.csv"
    MIME_TYPE = "text/csv"

    HEADERS = [
        "id",
        "schema",
        "name",
        "aliases",
        "birth_date",
        "countries",
        "addresses",
        "identifiers",
        "topics",
        "sanction",
        "phones",
        "emails",
        "dataset",
        "first_seen",
        "last_seen",
        "last_change",
    ]

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "w")
        self.writer = csv.writer(self.fh, dialect=csv.unix_dialect)
        self.writer.writerow(self.HEADERS)

    def feed(self, entity: Entity) -> None:
        if not entity.target:
            return
        if entity.id is None:
            return
        countries = set(entity.get_type_values(registry.country))
        identifiers = set(entity.get_type_values(registry.identifier))

        name = prefer_latin_name(entity)
        other_names = set(entity.get_type_values(registry.name))
        other_names.discard(name)
        sanctions = set()
        addresses = set(entity.get("address"))

        for _, adjacent in self.view.get_adjacent(entity):
            if adjacent.schema.is_a("Sanction"):
                sanctions.add(self.sanction_text(adjacent))

            if adjacent.schema.is_a("Address"):
                addresses.add(adjacent.caption)

            if adjacent.schema.is_a("Identification"):
                identifiers.update(adjacent.get("number"))
                countries.update(adjacent.get("country"))

        datasets: List[str] = []
        for dataset in entity.datasets:
            ds = get_catalog().require(dataset)
            datasets.append(ds.title)
        row = [
            hash_id(entity.id),
            entity.schema.name,
            clean_value(name),
            clean_value(self.concat_values(other_names)),
            clean_value(self.concat_values(entity.get("birthDate", quiet=True))),
            clean_value(self.concat_values(countries)),
            clean_value(self.concat_values(addresses)),
            clean_value(self.concat_values(identifiers)),
            clean_value(self.concat_values(entity.get("topics"))),
            clean_value(self.concat_values(sanctions)),
            clean_value(self.concat_values(entity.get_type_values(registry.phone))),
            clean_value(self.concat_values(entity.get_type_values(registry.email))),
            clean_value(self.concat_values(datasets)),
            entity.first_seen,
            entity.last_seen,
            entity.last_change,
        ]
        self.writer.writerow(row)
