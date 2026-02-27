import csv
from zavod import Context, helpers as h
from zavod.stateful.positions import categorise
from rigour.mime.types import CSV


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)

    url_el = h.xpath_element(
        doc,
        "//h4[contains(., 'Liens utiles')]/following::a[contains(@href, 'liste.csv')]",
    )
    url = url_el.get("href")
    assert url is not None

    path = context.fetch_resource("source.csv", url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r") as f:
        dict_reader = csv.DictReader(f, delimiter=";")

        for row in dict_reader:
            title = row.pop("civilite")
            first_name = row.pop("prenom")
            last_name = row.pop("nom")
            role = row.pop("qualite")

            person = context.make("Person")
            person.id = context.make_id(first_name, last_name, role)
            h.apply_name(
                person, prefix=title, first_name=first_name, last_name=last_name
            )
            person.add(
                "idNumber", row.pop("id_origine")
            )  # populated for deputies and senators only

            url = "https://www.hatvp.fr" + row.pop("url_dossier")
            person.add("sourceUrl", url)  # url to person's dossier

            person.add("position", role)
            person.add("topics", "role.pep")
            person.add("country", "fr")

            mandate_type = row.pop("type_mandat")
            res = context.lookup("positions", mandate_type)
            topics = res.topics if res else None

            position = h.make_position(
                context,
                name=role,
                topics=topics,
                country="fr",
            )
            position.add(
                "subnationalArea", row.pop("departement")
            )  # int for French departement number
            categorisation = categorise(context, position, is_pep=True)

            occupancy = h.make_occupancy(
                context,
                person,
                position,
                categorisation=categorisation,
            )
            if occupancy is None:
                return
            h.apply_date(occupancy, "declarationDate", row.pop("date_depot"))

            url_declaration_pdf = "http://www.hatvp.fr/livraison/dossiers/" + row.pop(
                "nom_fichier"
            )
            occupancy.add("sourceUrl", url_declaration_pdf)  # url to declaration's PDF

            context.emit(person)
            context.emit(position)
            context.emit(occupancy)

            context.audit_data(
                row,
                ignore=[
                    "classement",
                    "open_data",
                    "date_publication",
                    "url_photo",
                    "statut_publication",
                    "type_document",
                ],
            )
