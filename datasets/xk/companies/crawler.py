import os
import re
from typing import Any, Dict
from time import sleep
from datetime import datetime, timezone, timedelta
from base64 import b64encode

import requests
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.backends import default_backend
from normality import slugify
from zavod import Context
from zavod import helpers as h

SLEEP_TIME = 0.2
API_KEY = os.environ.get("KOSOVO_ENCRYPTION_KEY", "")
assert (
    API_KEY
), "Please provide the Kosovo API key in the environment variable KOSOVO_ENCRYPTION_KEY"


COMPANY_TYPES = {
    "Shoqëri me përgjegjësi të kufizuara": "Limited Liability Company",
    "Biznes individual": "Individual business",
    "Ortakëri e përgjithshme": "General partnership",
    "Dega e Shoqërisë së Huaj": "Branch of a foreign company",
    # AKM is probably Association of Kosovo Municipalities (AKM)
    "Ndërmarrje tjera nën juridiksion të AKM": "Other enterprises under the jurisdiction of the AKM",
    "Ortakëri e kufizuar": "Limited partnership",
    "Shoqëri aksionare": "Joint stock company",
    "Ndërmarrje publike": "Public enterprise",
    "Kooperativa Bujqësore": "Agricultural cooperative",
    "Zyra e Përfaqësisë në Kosovë": "Representative office in Kosovo",
}

STATUSES = {
    "Regjistruar": "Registered",
    "Shuar": "Closed",
    # No idea what this means
    "Pasiv-09/06/2022": "Passive-09/06/2022",
    "Anuluar nga sistemi": "Canceled by the system",
}


FIELDS_MAPPING = {
    "EmriBiznesit": {"field": "name", "lang": "sqi"},
    "EmriTregtar": {"field": "name", "lang": "sqi"},
    "LlojiBiznesit": {"field": "legalForm", "lang": "sqi"},
    # Status in KBRA
    "StatusiARBK": {"field": "status", "lang": "sqi"},
    # Unique identification number
    "NUI": {"field": "registrationNumber"},
    # Business number
    "NumriBiznesit": {"field": "registrationNumber"},
    # Fiscal number
    "NumriFiskal": {"field": "taxNumber"},
    # "Adresa": {"field": "address"},
    "WebFaqja": {"field": "website"},
    "Telefoni": {"field": "phone"},
    "Email": {"field": "email"},
}

# "Komuna": "Prishtinë",
# "Vendi": "Prishtinë",


def norm_h(string: str) -> str:
    """
    Normalize a string.
    """
    if isinstance(string, str):
        string = string.replace("#", "num")
        string = string.replace("€", "eur")
        string = string.replace("$", "usd")
        string = string.replace("%", "percent")
        string = slugify(string, sep="_")
    return string


def norm_capital(context: Context, string: str) -> Dict[str, str]:
    """
    Normalize a capital string.
    Args:
        context: Context
        string: str
    Returns:
        Capital as a dictionary of value and currency.
    """
    capital = {"value": "", "currency": ""}
    try:
        match = re.search(r"(?P<val>[0-9\.,]*)\s*(?P<cur>[^\d]+)?", string)
        if match:
            capital["value"] = match.groupdict().get("val")
            capital["currency"] = norm_h(match.groupdict().get("cur"))
    except Exception as exc:
        context.log.warning(f"[Unable to parse capital: {exc}]")
    return capital


def parse_date(text: str) -> Any:
    """
    Parse a date from a string.
    """
    return h.parse_date(
        text,
        [
            # 4/29/2014 12:00:00 AM
            "%m/%d/%Y %I:%M:%S %p",
            "%m/%d/%Y %H:%M:%S",
        ],
    )


def get_the_key() -> str:
    # Key and IV must be bytes, here assuming they are the same.
    key = iv = API_KEY.encode("utf-8")  # Key and IV as bytes

    # Message to encrypt
    # Looks like we can safely use timestamp in the future.
    data = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()

    # Padding the data according to PKCS7
    padder = PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(data.encode()) + padder.finalize()

    # Setting up the cipher configuration
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    # Encrypting the data
    encryptor = cipher.encryptor()
    encrypted = encryptor.update(padded_data) + encryptor.finalize()

    return b64encode(encrypted).decode()


def fetch_company(context: Context, company_id: int) -> None:
    """
    Fetch a single company from the Kosovo Registry of Business Organizations and Trade Names.
    """
    try:
        resp = context.fetch_json(
            "https://arbk.rks-gov.net/api/api/Services/TeDhenatBiznesit",
            params={"nRegjistriId": company_id, "Gjuha": "en"},
            headers={"key": get_the_key()},
        )

        if len(resp) != 1 or "teDhenatBiznesit" not in resp[0]:
            context.log.error(f"Something strange with the {company_id})")

        company = resp[0]["teDhenatBiznesit"]
        activities = resp[0].get("aktivitetet", [])
        founders = resp[0].get("pronaret", [])
        representatives = resp[0].get("perfaqesuesit", [])

        company_type = company.get("LlojiBiznesit")
        if company_type not in COMPANY_TYPES:
            context.log.warning("Unknown company type: ", company_type)

        status = company.get("StatusiARBK")
        if status not in STATUSES:
            context.log.warning("Unknown status: ", status)

        entity = context.make("Company")
        entity.id = context.make_id("XKCompany", company_id)
        entity.add("country", "xk")

        for orig_field, field_def in FIELDS_MAPPING.items():
            if company.get(orig_field):
                entity.add(
                    field_def["field"],
                    company.pop(orig_field),
                    lang=field_def.get("lang"),
                )

        if company_type in COMPANY_TYPES:
            entity.add("legalForm", COMPANY_TYPES[company_type], lang="eng")

        if status in STATUSES:
            entity.add("status", STATUSES[status], lang="eng")

        if company.get("DataRegjistrimit"):
            entity.add("incorporationDate", parse_date(company.pop("DataRegjistrimit")))

        if company.get("DataShuarjesBiznesit"):
            entity.add(
                "dissolutionDate", parse_date(company.pop("DataShuarjesBiznesit"))
            )

        if company.get("Kapitali"):
            capital = norm_capital(context, company.pop("Kapitali"))
            if capital["value"]:
                entity.add("capital", capital["value"])

            if capital["currency"]:
                entity.add("currency", capital["currency"])

        if company.get("Adresa") or company.get("Vendi") or company.get("Komuna"):
            address = h.make_address(
                context,
                street=company.pop("Adresa"),
                city=company.pop("Vendi"),
                country_code="xk",
                # Should it be county or region?
                state=company.pop("Komuna"),
            )
            h.apply_address(context, entity, address)

        if company.get("NumriPunetoreve"):
            entity.add(
                "notes",
                f'Number of employees: {company.pop("NumriPunetoreve")}',
                lang="eng",
            )

        if company.get("Shteti") != "1":
            context.log.warning(
                f"Unknown country: {company.get('Shteti')}, company_id: {company_id}"
            )

        if activities:
            for act in activities:
                entity.add("sector", act.get("Pershkrimi", ""), lang="eng")

        for founder_data in founders:
            # Don't know if there might be companies
            founder = context.make("LegalEntity")
            founder.id = context.make_id(
                "XKFounder", entity.id, founder_data["Pronari"]
            )
            founder.add("name", founder_data["Pronari"], lang="sqi")
            context.emit(founder)

            own = context.make("Ownership")
            own.id = context.make_id("XKOwnership", entity.id, founder.id)
            own.add("asset", entity.id)
            own.add("owner", founder.id)

            own.add("ownershipType", "Founder", lang="eng")

            # JD: Not sure where to map shares and capital_contracted
            own.add("sharesValue", founder_data["Kapitali"])
            own.add("percentage", founder_data["KapitaliPerqindje"])
            context.emit(own)

        for rep in representatives:
            director = context.make("Person")
            director.id = context.make_id(
                "XKdirector", entity.id, rep["Emri"], rep["Mbiemri"]
            )
            director.add("name", f'{rep["Emri"]} {rep["Mbiemri"]}', lang="sqi")
            context.emit(director)

            rel = context.make("Directorship")
            rel.id = context.make_id("XKDirectorship", entity.id, director.id)
            rel.add("role", rep["Pozita"], lang="sqi")

            # JD: Should it be description or notes or status or summary?
            rel.add("description", rep["Autorizimet"], lang="sqi")
            rel.add("director", director)
            rel.add("organization", entity)

            context.emit(rel)

        context.audit_data(
            company,
            [
                "nRegjistriID",
                "nLlojiBiznesitID",
                "Shteti",
                "KomunaId",
                "VendiID",
                "Latitude",
                "Longtitude",
            ],
        )
        context.emit(entity, target=True)

    except requests.exceptions.HTTPError as exc:
        context.log.warning(f"Failed to fetch company {company_id}: {type(exc)}, {exc}")


def crawl(context: Context):
    """
    Main function to crawl and process data from the Kosovo Registry of Business
    Organizations and Trade Names.
    """
    for company_id in range(1, 260000):  # 260000):
        if company_id % 100 == 0:
            context.log.info(f"Fetching company {company_id}")
        fetch_company(context, company_id)
        sleep(SLEEP_TIME)
