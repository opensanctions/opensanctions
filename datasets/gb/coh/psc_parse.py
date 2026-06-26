import csv
import json
import re
import yaml
from typing import Optional, Generator, Dict, Any, cast
from zipfile import ZipFile
from functools import lru_cache
from io import TextIOWrapper
from urllib.parse import urljoin
from rigour.util import MEMO_MEDIUM

from followthemoney.util import PathLike
from followthemoney.types import registry
from followthemoney.util import join_text

from zavod import Context
from zavod import helpers as h

BASE_URL = "http://download.companieshouse.gov.uk/en_output.html"
PSC_URL = "http://download.companieshouse.gov.uk/en_pscdata.html"
PUBLIC_BASE = "https://find-and-update.company-information.service.gov.uk"

# Canonical PSC nature-of-control enumeration, published by Companies House
# in companieshouse/api-enumerations. Fetched live each crawl (cached via
# context.fetch_resource) so the slug taxonomy stays in sync with upstream
# without a vendored snapshot to refresh.
PSC_DESCRIPTIONS_URL = "https://raw.githubusercontent.com/companieshouse/api-enumerations/master/psc_descriptions.yml"

PERCENTAGE_RE = re.compile(r"(\d+)-to-(\d+)-percent")

KINDS = {
    "individual-person-with-significant-control": "Person",
    "individual-beneficial-owner": "Person",
    "corporate-entity-person-with-significant-control": "Company",
    "corporate-entity-beneficial-owner": "Company",
    "legal-person-person-with-significant-control": "Organization",
    "legal-person-beneficial-owner": "Organization",
    "super-secure-person-with-significant-control": "",
    "persons-with-significant-control-statement": "",
    "exemptions": "",
}

IGNORE_BASE_COLUMNS = [
    "Accounts.AccountRefDay",
    "Accounts.AccountRefMonth",
    "Accounts.NextDueDate",
    "Accounts.LastMadeUpDate",
    "Accounts.AccountCategory",
    "Returns.NextDueDate",
    "Returns.LastMadeUpDate",
    "Mortgages.NumMortCharges",
    "Mortgages.NumMortOutstanding",
    "Mortgages.NumMortPartSatisfied",
    "Mortgages.NumMortSatisfied",
    "LimitedPartnerships.NumGenPartners",
    "LimitedPartnerships.NumLimPartners",
    "ConfStmtNextDueDate",
    "ConfStmtLastMadeUpDate",
    "URI",
]


def company_id(company_nr: str) -> str:
    nr = company_nr.lower()
    return f"oc-companies-gb-{nr}"


def fetch_psc_short_descriptions(context: Context) -> dict[str, str]:
    """Fetch CH's PSC nature-of-control short-description map.

    Used to populate the ``role`` text on emitted Ownership links with CH's
    official wording (``"Ownership of shares – More than 25% but not more
    than 50%"``) rather than a slug-derived string. A slug missing from this
    map is logged so new CH taxonomy additions surface in the run.
    """
    path = context.fetch_resource("psc_descriptions.yml", PSC_DESCRIPTIONS_URL)
    with open(path, "r") as fh:
        data = cast(dict[str, Any], yaml.safe_load(fh))
    return cast(dict[str, str], data.get("short_description", {}))


def percentage_range(slug: str) -> Optional[str]:
    """Render the share-range encoded in a PSC slug as ``"25–50%"`` etc."""
    match = PERCENTAGE_RE.search(slug)
    if match is None:
        return None
    return f"{match.group(1)}–{match.group(2)}%"


@lru_cache(maxsize=MEMO_MEDIUM)
def parse_country(name: str, default: Optional[str] = None) -> Optional[str]:
    code = registry.country.clean(name)
    if code is None:
        return default
    return code


@lru_cache(maxsize=MEMO_MEDIUM)
def clean_sector(text: str) -> str:
    sectors = text.split(" - ", 1)
    if len(sectors) > 1:
        return sectors[-1]
    return text


def get_base_data_url(context: Context) -> str:
    doc = context.fetch_html(BASE_URL)
    for link in doc.findall(".//a"):
        url = urljoin(BASE_URL, link.get("href"))
        if "BasicCompanyDataAsOneFile" in url:
            return url
    raise RuntimeError("No base data URL found!")


def read_base_data_csv(path: PathLike) -> Generator[Dict[str, str], None, None]:
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            with zip.open(name, "r") as fh:
                with TextIOWrapper(fh) as fhtext:
                    for row in csv.DictReader(fhtext):
                        yield {k.strip(): v for (k, v) in row.items()}


def parse_base_data(context: Context) -> None:
    base_data_url = get_base_data_url(context)
    if base_data_url is None:
        raise RuntimeError("Base data zip URL not found!")
    data_path = context.fetch_resource("base_data.zip", base_data_url)

    context.log.info("Loading: %s" % data_path)
    for idx, row in enumerate(read_base_data_csv(data_path)):
        if idx > 0 and idx % 100_000 == 0:
            context.log.info("Base data: %d..." % idx)
            context.flush()
        company_nr = row.pop("CompanyNumber")
        entity = context.make("Company")
        entity.id = company_id(company_nr)
        entity.add("name", row.pop("CompanyName"))
        entity.add("registrationNumber", company_nr)
        entity.add("status", row.pop("CompanyStatus"))
        entity.add("legalForm", row.pop("CompanyCategory"))
        entity.add("country", row.pop("CountryOfOrigin"))
        entity.add("jurisdiction", "gb")

        oc_url = f"https://opencorporates.com/companies/gb/{company_nr}"
        entity.add("opencorporatesUrl", oc_url)
        # entity.add("sourceUrl", row.pop("URI"))
        entity.add(
            "sourceUrl",
            f"https://find-and-update.company-information.service.gov.uk/company/{company_nr}",
        )

        for i in range(1, 5):
            sector = row.pop(f"SICCode.SicText_{i}")
            entity.add("sector", clean_sector(sector))
        inc_date = row.pop("IncorporationDate")
        h.apply_date(entity, "incorporationDate", inc_date)
        dis_date = row.pop("DissolutionDate")
        h.apply_date(entity, "dissolutionDate", dis_date)

        for i in range(1, 11):
            row.pop(f"PreviousName_{i}.CONDATE")
            entity.add("previousName", row.pop(f"PreviousName_{i}.CompanyName"))

        addr_country = row.pop("RegAddress.Country")
        street = join_text(
            row.pop("RegAddress.AddressLine1"),
            row.pop("RegAddress.AddressLine2"),
        )
        addr_text = h.format_address(
            summary=row.pop("RegAddress.CareOf"),
            po_box=row.pop("RegAddress.POBox"),
            street=street,
            postal_code=row.pop("RegAddress.PostCode"),
            county=row.pop("RegAddress.County"),
            city=row.pop("RegAddress.PostTown"),
            country=addr_country,
        )
        entity.add("address", addr_text)
        context.audit_data(row, ignore=IGNORE_BASE_COLUMNS)
        context.emit(entity)

    data_path.unlink()


def get_psc_data_url(context: Context) -> str:
    doc = context.fetch_html(PSC_URL)
    for link in doc.findall(".//a"):
        url = urljoin(BASE_URL, link.get("href"))
        if "persons-with-significant-control-snapshot" in url:
            return url
    raise RuntimeError("No PSC data URL found!")


def read_psc_data(path: PathLike) -> Generator[Dict[str, Any], None, None]:
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            with zip.open(name, "r") as fh:
                with TextIOWrapper(fh) as fhtext:
                    for line in fhtext:
                        yield json.loads(line)


def parse_psc_data(context: Context) -> None:
    short_descriptions = fetch_psc_short_descriptions(context)
    psc_data_url = get_psc_data_url(context)
    if psc_data_url is None:
        raise RuntimeError("PSC data zip URL not found!")
    data_path = context.fetch_resource("psc_data.zip", psc_data_url)
    context.log.info("Loading: %s" % data_path)
    for idx, row in enumerate(read_psc_data(data_path)):
        if idx > 0 and idx % 100_000 == 0:
            context.log.info("PSC statements: %d..." % idx)
            context.flush()
        # if idx > 0 and idx % 1000000 == 0:
        #     return
        company_nr = row.pop("company_number", None)
        if company_nr is None:
            context.log.warning("No company number: %r" % row)
            continue
        data = row.pop("data")
        data.pop("etag", None)
        url = data.pop("links").pop("self")
        psc_id = url.rsplit("/", 1)[-1]
        kind = data.pop("kind")
        schema = KINDS.get(kind)
        if schema == "":
            continue
        if schema is None:
            context.log.warn(
                "Unknown kind of PSC",
                kind=kind,
                name=data.get("name"),
            )
            continue
        psc = context.make(schema)
        psc_id_slug = psc_id.replace("_", "-").lower()
        psc.id = f"{context.dataset.prefix}-psc-{company_nr}-{psc_id_slug}"
        nationality = data.pop("nationality", None)
        nationalities = h.multi_split(nationality, [",", "/"])
        if psc.schema.is_a("Person"):
            psc.add("nationality", nationalities, quiet=True, fuzzy=True)
        else:
            psc.add("jurisdiction", nationalities, quiet=True, fuzzy=True)
        psc.add("country", data.pop("country_of_residence", None), fuzzy=True)

        names = data.pop("name_elements", {})
        h.apply_name(
            psc,
            full=data.pop("name"),
            first_name=names.pop("forename", None),
            middle_name=names.pop("middle_name", None),
            last_name=names.pop("surname", None),
            quiet=True,
        )
        psc.add("title", names.pop("title", None), quiet=True)

        dob = data.pop("date_of_birth", {})
        dob_year = dob.pop("year", None)
        dob_month = dob.pop("month", None)
        if dob_year and dob_month:
            psc.add("birthDate", f"{dob_year}-{dob_month:02d}")

        for addr_field in ("address", "principal_office_address"):
            address = data.pop(addr_field, {})
            street = join_text(
                address.pop("address_line_1", None),
                address.pop("address_line_2", None),
            )
            country = address.pop("country", None)
            addr_text = h.format_address(
                summary=address.pop("care_of", None),
                po_box=address.pop("po_box", None),
                street=street,
                house_number=address.pop("premises", None),
                postal_code=address.pop("postal_code", None),
                state=address.pop("region", None),
                city=address.pop("locality", None),
                country=country,
            )
            psc.add("address", addr_text)
            context.audit_data(address)

        ident = data.pop("identification", {})
        reg_nr = ident.pop("registration_number", None)
        psc.add("registrationNumber", reg_nr, quiet=True)
        psc.add("legalForm", ident.pop("legal_form", None), quiet=True)
        psc.add("legalForm", ident.pop("legal_authority", None), quiet=True)
        psc.add(
            "jurisdiction",
            ident.pop("country_registered", None),
            quiet=True,
            fuzzy=True,
        )
        # psc.add("jurisdiction", ident.pop("place_registered", None), quiet=True)
        # if len(ident):
        #     pprint(ident)
        asset_id = company_id(company_nr)

        # Generate at least a stub of a company for dissolved companies which
        # aren't in the base data.
        asset = context.make("Company")
        asset.id = asset_id
        asset.add("registrationNumber", company_nr)
        asset.add("jurisdiction", "gb")

        natures = data.pop("natures_of_control", None) or []
        notified_on = data.pop("notified_on")
        ceased_on = data.pop("ceased_on", None)
        source_url = urljoin(PUBLIC_BASE, url)
        status = "ceased" if ceased_on else "active"

        # Every PSC declaration — Conditions 1–5 of the UK regime — is modelled
        # as a single Ownership link. FTM's Ownership reverse is "Ownership and
        # Control"; the per-nature wording lives in the multi-valued ``role``
        # field (CH's short_description verbatim), and percentage ranges from
        # share/voting/surplus-asset slugs are collected separately.
        roles: list[str] = []
        percentages: list[str] = []
        unlabelled_natures: list[str] = []
        for nature in natures:
            if nature is None:
                continue
            label = short_descriptions.get(nature)
            if label is None:
                label = nature.replace("-", " ").capitalize()
                unlabelled_natures.append(nature)
            roles.append(label)
            pct = percentage_range(nature)
            if pct is not None:
                percentages.append(pct)
        if unlabelled_natures:
            context.log.warn(
                "PSC nature-of-control slug missing from CH enumeration",
                natures=unlabelled_natures,
                psc_id=psc_id,
                company_number=company_nr,
            )

        link = context.make("Ownership")
        link.id = context.make_id("psc", company_nr, psc_id)
        link.add("owner", psc.id)
        link.add("asset", asset_id)
        link.add("recordId", psc_id)
        link.add("role", roles)
        link.add("startDate", notified_on)
        link.add("endDate", ceased_on)
        link.add("status", status)
        link.add("sourceUrl", source_url)
        link.add("ownershipType", "beneficial")
        link.add("percentage", percentages)
        context.emit(link)

        if data.pop("is_sanctioned", False):
            psc.add("topics", "sanction")

        context.audit_data(
            data,
            ignore=[
                "service_address_is_same_as_registered_office_address",
                "identity_verification_details",
            ],
        )
        context.emit(psc)
        context.emit(asset)

    data_path.unlink()


def crawl(context: Context) -> None:
    parse_base_data(context)
    parse_psc_data(context)
