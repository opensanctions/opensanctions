from zavod import Context
from zavod import helpers as h
from typing import Dict

IGNORE_COLUMNS = [
    "ID",
    "Durum",  # Status
    "GorselURL",  # ImageURL
    "IlkGorselURL",  # FirstImageURL
    "AvatarURL",
    "TOrgutID",  # OrganizationID
    "TKategoriID",  # CategoryID
    "Sil",  # Delete
]
COLOURS = {
    "kırmızı": "red",
    "mavi": "blue",
    "yeşil": "green",
    "turuncu": "orange",
    "gri": "grey",
    "sarı": "yellow",
}


def colour_en(colour: str) -> str:
    return COLOURS[colour]


def crawl_row(context: Context, row: Dict[str, str]):
    person = context.make("Person")

    first_name = row.pop("Adi")
    surname = row.pop("Soyadi")
    place_of_birth = row.pop("DogumYeri")
    date_of_birth = row.pop("DogumTarihi")

    person.id = context.make_id(first_name, surname, place_of_birth, date_of_birth)

    h.apply_name(person, first_name=first_name, last_name=surname)

    person.add("birthPlace", place_of_birth)
    person.add("birthDate", date_of_birth)
    person.add(
        "program",
        f"{context.dataset.model.title} - {colour_en(row.pop('TKategoriAdi'))} List",
    )
    person.add("topics", "sanction.counter")
    # person.add("topics", "wanted")
    person.add("country", "tr")
    context.emit(person)

    organization = context.make("Organization")
    organization_name = row.pop("TOrgutAdi")
    organization.id = context.make_id(organization_name)
    organization.add("name", organization_name)
    context.emit(organization)

    link = context.make("UnknownLink")
    link.id = context.make_id(person.id, organization.id)
    link.add("subject", person)
    link.add("object", organization)
    context.emit(link)

    context.audit_data(row, IGNORE_COLUMNS)


def crawl(context):
    headers = {
        "Content-Length": "0",
        "Content-Type": "application/json",
    }

    res = context.http.post(context.dataset.data.url, headers=headers)
    data = res.json()
    for key in data:
        for row in data[key]:
            crawl_row(context, row)
