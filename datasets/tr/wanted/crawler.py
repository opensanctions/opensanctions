from zavod import Context
from zavod import helpers as h
from typing import Dict
import urllib3
import requests
import datetime

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


# solution from https://github.com/urllib3/urllib3/issues/2653#issuecomment-1733417634
class CustomSslContextHttpAdapter(requests.adapters.HTTPAdapter):
    """ "Transport adapter" that allows us to use a custom ssl context object with the requests."""

    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = urllib3.util.ssl_.create_urllib3_context()
        ctx.load_default_certs()
        ctx.check_hostname = False
        ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT
        self.poolmanager = urllib3.PoolManager(ssl_context=ctx)


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
    person.add("program", f"{context.dataset.title} - {row.pop('TKategoriAdi')} List")
    person.add("topics", "crime")
    person.add("country", "tr")

    context.emit(person, target=True)

    context.audit_data(row, IGNORE_COLUMNS)


def crawl(context):
    if datetime.datetime.now() > datetime.datetime(2024, 9, 15):
        context.log.warn("Check if the SSL renegotiation strategy is still needed")

    headers = {
        "Content-Length": "0",
        "Content-Type": "application/json",
    }
    context.http.mount(context.dataset.data.url, CustomSslContextHttpAdapter())

    res = context.http.post(context.dataset.data.url, headers=headers)
    data = res.json()
    for key in data:
        for row in data[key]:
            crawl_row(context, row)
