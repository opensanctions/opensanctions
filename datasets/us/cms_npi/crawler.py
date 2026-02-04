import csv
from io import TextIOWrapper
from typing import Any, Optional, TextIO
from urllib.parse import urljoin
import zipfile
from rigour.mime.types import ZIP
from rigour.names.check import is_nullword

from zavod import Context
from zavod import helpers as h


def npi_id(npi: Any) -> Optional[str]:
    """Format NPI as a zero-padded 10-digit string."""
    if npi is None or len(npi) != 10:
        return None
    return f"us-npi-{npi.lower()}"


def crawl_data_file(context: Context) -> str:
    """Fetch the NPI data file index page and extract the ZIP file URL."""
    doc = context.fetch_html(context.data_url, cache_days=1)
    for link in h.xpath_elements(doc, ".//a"):
        href = link.get("href")
        if href is None:
            continue

        if "NPPES_Data_Dissemination" in href and href.endswith("_V2.zip"):
            return urljoin(context.data_url, href)

    raise RuntimeError(
        "Could not find NPPES_Data_Dissemination*_V2.zip file on the download page"
    )


def crawl_npidata(context: Context, fh: TextIO) -> None:
    for row in csv.DictReader(fh):
        # row = {k: v for k, v in raw.items() if len(v) > 0}
        type_code = row.pop("Entity Type Code")
        schema = "LegalEntity"
        if type_code == "1":
            schema = "Person"
        elif type_code == "2":
            schema = "Organization"
        entity = context.make(schema)
        npi = row.pop("NPI")
        entity.id = npi_id(npi)
        if not entity.id:
            context.log.warning(f"Invalid NPI in npidata record: {row!r}")
            continue

        entity.add("npiCode", npi)
        entity.add("name", row.pop("Provider Organization Name (Legal Business Name)"))
        h.apply_name(
            entity,
            first_name=row.pop("Provider First Name"),
            last_name=row.pop("Provider Last Name (Legal Name)"),
            middle_name=row.pop("Provider Middle Name"),
            quiet=True,
        )
        entity.add("title", row.pop("Provider Name Prefix Text"), quiet=True)
        country_code = row.pop(
            "Provider Business Practice Location Address Country Code (If outside U.S.)"
        )

        mailing_addr = h.make_address(
            context,
            street=row.pop("Provider First Line Business Mailing Address"),
            street2=row.pop("Provider Second Line Business Mailing Address"),
            city=row.pop("Provider Business Mailing Address City Name"),
            state=row.pop("Provider Business Mailing Address State Name"),
            postal_code=row.pop("Provider Business Mailing Address Postal Code"),
            country_code=country_code or "us",
        )
        h.copy_address(entity, mailing_addr)

        practice_country = row.pop(
            "Provider Business Mailing Address Country Code (If outside U.S.)"
        )
        practice_addr = h.make_address(
            context,
            street=row.pop("Provider First Line Business Practice Location Address"),
            street2=row.pop("Provider Second Line Business Practice Location Address"),
            city=row.pop("Provider Business Practice Location Address City Name"),
            state=row.pop("Provider Business Practice Location Address State Name"),
            postal_code=row.pop(
                "Provider Business Practice Location Address Postal Code"
            ),
            country_code=practice_country.lower() or "us",
        )
        h.copy_address(entity, practice_addr)

        # context.audit_data(row)
        context.emit(entity)


def crawl_othernames(context: Context, fh: TextIO) -> None:
    for row in csv.DictReader(fh):
        entity = context.make("LegalEntity")
        entity.id = npi_id(row.get("NPI"))
        if not entity.id:
            context.log.warning(f"Invalid NPI in othername record: {row!r}")
            continue
        name = row.get("Provider Other Organization Name")
        if name is None or is_nullword(name):
            continue
        # if name is None or len(name.strip()) == 0:
        #     context.log.warning(f"Missing other organization name in record: {row!r}")
        #     continue
        entity.add("alias", name)
        if not entity.has("alias"):
            # context.log.warning(f"Empty other organization name in record: {name!r}")
            pass
        else:
            context.emit(entity)


def crawl(context: Context) -> None:
    data_url = crawl_data_file(context)

    # Download the ZIP file
    path = context.fetch_resource("source.zip", data_url)
    context.export_resource(path, ZIP, title=context.SOURCE_TITLE)

    with zipfile.ZipFile(path, "r") as zip_ref:
        # List all files in the ZIP archive
        for name in zip_ref.namelist():
            if name.endswith("_fileheader.csv") or name.endswith(".pdf"):
                continue
            with zip_ref.open(name, mode="r") as fbin:
                textfh = TextIOWrapper(fbin, encoding="utf-8")
                if name.startswith("npidata_"):
                    crawl_npidata(context, textfh)
                if name.startswith("othername_"):
                    crawl_othernames(context, textfh)
