import os
import re
import requests
from typing import Dict
from normality import slugify
from datetime import timedelta
from base64 import b64encode
from rigour.time import utc_now
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.backends import default_backend

from zavod import Context
from zavod import helpers as h

# On 2024-05-16 we saw 19017.
# It seems to always be the same companies, so they're probably deleted but
# not serving 404, or corrupt or something.
EXPECTED_FAILS = 20000

KOSOVO_REGISTRY_KEY = os.environ.get("OPENSANCTIONS_KOSOVO_REGISTRY_KEY", "")
assert (
    KOSOVO_REGISTRY_KEY
), "Please provide the Kosovo API key in the env var OPENSANCTIONS_KOSOVO_REGISTRY_KEY"

FIELDS_MAPPING = {
    "EmriBiznesit": {"field": "name", "lang": "sqi"},
    "EmriTregtar": {"field": "name", "lang": "sqi"},
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

REGEX_ROUGHLY_VALID_REGNO = re.compile(r"^\d{8,9}$")


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


def roughly_valid_regno(regno: str) -> bool:
    return bool(REGEX_ROUGHLY_VALID_REGNO.match(regno))


def get_the_key() -> str:
    # Key and IV must be bytes, here assuming they are the same.
    key = iv = KOSOVO_REGISTRY_KEY.encode("utf-8")  # Key and IV as bytes

    # Message to encrypt
    # Looks like we can safely use timestamp in the future.
    data = (utc_now() + timedelta(seconds=60)).isoformat()

    # Padding the data according to PKCS7
    padder = PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(data.encode()) + padder.finalize()

    # Setting up the cipher configuration
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    # Encrypting the data
    encryptor = cipher.encryptor()
    encrypted = encryptor.update(padded_data) + encryptor.finalize()

    return b64encode(encrypted).decode()


def fetch_company(context: Context, company_id: int) -> int:
    """
    Fetch a single company from the Kosovo Registry of Business Organizations and Trade Names.

    Returns HTTP status code.
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

        entity = context.make("Company")
        if roughly_valid_regno(company.get("NUI")):
            entity.id = context.make_slug("nui", company.get("NUI"))
        elif roughly_valid_regno(company.get("NumriBiznesit")):
            entity.id = context.make_slug("biznesit", company.get("NumriBiznesit"))
        elif roughly_valid_regno(company.get("NumriFiskal")):
            entity.id = context.make_slug("fiskal", company.get("NumriFiskal"))
        else:
            entity.id = context.make_id("XKCompany", company_id)

        entity.add("country", "xk")

        for orig_field, field_def in FIELDS_MAPPING.items():
            if company.get(orig_field):
                entity.add(
                    field_def["field"],
                    company.pop(orig_field),
                    lang=field_def.get("lang"),
                )

        company_type_sqi = company.pop("LlojiBiznesit")
        company_type_eng = context.lookup_value("company_type", company_type_sqi, None)
        if company_type_eng:
            entity.add("legalForm", company_type_eng, lang="eng")
        elif company_type_sqi:
            context.log.info("Unknown company type: ", type=company_type_sqi)
            entity.add("legalForm", company_type_sqi, lang="sqi")

        status_sqi = company.pop("StatusiARBK")
        status_eng = context.lookup_value("status", status_sqi, None)
        if status_eng:
            entity.add("status", status_eng, lang="eng")
        elif status_sqi:
            context.log.info("Unknown status", status=status_sqi)
            entity.add("status", status_sqi, lang="sqi")

        if company.get("DataRegjistrimit"):
            h.apply_date(entity, "incorporationDate", company.pop("DataRegjistrimit"))

        if company.get("DataShuarjesBiznesit"):
            h.apply_date(entity, "dissolutionDate", company.pop("DataShuarjesBiznesit"))

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
                state=company.pop("Komuna"),
            )
            h.apply_address(context, entity, address)

        if company.get("Shteti") != "1":
            context.log.warning(
                f"Unknown country: {company.get('Shteti')}, company_id: {company_id}"
            )

        if activities:
            for act in activities:
                entity.add("sector", act.get("Pershkrimi", None), lang="eng")

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
                "NumriPunetoreve",
            ],
        )
        context.emit(entity)
        return True

    except requests.exceptions.HTTPError as exc:
        context.log.warning(f"Failed to fetch company {company_id}: {type(exc)}, {exc}")
        return False


def crawl(context: Context):
    """
    Main function to crawl and process data from the Kosovo Registry of Business
    Organizations and Trade Names.
    """
    fails = 0

    for company_id in range(1, 260000):
        if company_id % 100 == 0:
            context.log.info(f"Fetching company {company_id}")
        successful = fetch_company(context, company_id)
        if not successful:
            fails += 1

    assert fails < EXPECTED_FAILS, ("More fails than expected", fails)
    context.log.info(f"Finished with {fails} fails")
