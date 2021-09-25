import io
import csv
from opensanctions.exporters.common import Exporter
from banal import first
from followthemoney.types import registry

from opensanctions.exporters.common import Exporter
from opensanctions.util import jointext


class SimpleCSVExporter(Exporter):
    TITLE = "Targets as simplified CSV"
    NAME = "targets.simple"
    EXTENSION = "csv"
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
        "sanctions",
        "phones",
        "emails",
        "dataset",
        "last_seen",
        "first_seen",
    ]

    def concat_values(self, values):
        output = io.StringIO()
        writer = csv.writer(
            output,
            dialect=csv.unix_dialect,
            delimiter=";",
            lineterminator="",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writerow(sorted(values))
        return output.getvalue()

    def sanction_text(self, sanction):
        return jointext(
            *sanction.get("program"),
            *sanction.get("reason"),
            *sanction.get("status"),
            *sanction.get("startDate"),
            *sanction.get("endDate"),
            sep=" - ",
        )

    def setup(self):
        self.writer = csv.writer(self.fh, dialect=csv.unix_dialect)
        self.writer.writerow(self.HEADERS)

    def feed(self, entity):
        if not entity.target:
            return
        countries = set(entity.get_type_values(registry.country))
        identifiers = set(entity.get_type_values(registry.identifier))
        names = set(entity.get_type_values(registry.name))
        names.discard(entity.caption)
        sanctions = set()
        addresses = set(entity.get("address"))

        for _, adjacent in self.loader.get_adjacent(entity):
            if adjacent.schema.is_a("Sanction"):
                sanctions.add(self.sanction_text(adjacent))

            if adjacent.schema.is_a("Address"):
                addresses.add(adjacent.caption)

            if adjacent.schema.is_a("Identification"):
                identifiers.update(adjacent.get("number"))
                countries.update(adjacent.get("country"))

        datasets = [ds.title for ds in entity.datasets]
        row = [
            entity.id,
            entity.schema.name,
            entity.caption,
            self.concat_values(names),
            self.concat_values(entity.get("birthDate", quiet=True)),
            self.concat_values(countries),
            self.concat_values(addresses),
            self.concat_values(identifiers),
            self.concat_values(sanctions),
            self.concat_values(entity.get_type_values(registry.phone)),
            self.concat_values(entity.get_type_values(registry.email)),
            self.concat_values(datasets),
            entity.first_seen,
            entity.last_seen,
        ]
        self.writer.writerow(row)
