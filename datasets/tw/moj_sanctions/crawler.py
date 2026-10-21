import csv
import re
import shutil
from pathlib import Path
from urllib.parse import unquote

from rigour.mime.types import CSV

from zavod import Context, Entity
from zavod import helpers as h

PROGRAM_KEY = "TW-CFT"
RULING_NUMBER = re.compile(r"^法裁第(\d+)號$")
# Reviewed announcement PDFs (percent-decoded url: sha1), see the runbook in the yml.
PDF_HASHES = {
    "https://www.mjib.gov.tw/userfiles/files/35-洗錢防制處/files/20180112-10704500600(1070424).pdf": "ae4113ac9c86f940949b078a6baedd9cf637b9f2",
    "https://www.mjib.gov.tw/userfiles/files/35-洗錢防制處/files/20191125-10804538000(1).pdf": "5725029e2ad409f89fe4629c9d955e43fb6e85ce",
}


def ruling_id(context: Context, ruling_number: str) -> str:
    match = RULING_NUMBER.fullmatch(ruling_number)
    assert match is not None, ruling_number
    entity_id = context.make_slug(match.group(1))
    assert entity_id is not None
    return entity_id


def crawl_related_party(
    context: Context, company: Entity, related_id: str, relationship: str
) -> None:
    res = context.lookup("relationship.type", relationship)
    assert res is not None  # required lookup
    if res.ownership is True:
        ownership = context.make("Ownership")
        ownership.id = context.make_id("ownership", related_id, company.id)
        ownership.add("owner", related_id)
        ownership.add("asset", company)
        ownership.add("percentage", res.percentage)
        ownership.add("role", relationship, lang="zho")
        context.emit(ownership)
    if res.directorship is True:
        directorship = context.make("Directorship")
        directorship.id = context.make_id("directorship", related_id, company.id)
        directorship.add("director", related_id)
        directorship.add("organization", company)
        directorship.add("role", relationship, lang="zho")
        context.emit(directorship)


def crawl_row(context: Context, row: dict[str, str]) -> None:
    entity = context.make(row.pop("Type"))
    ruling_number = row.pop("Ruling number")
    entity.id = ruling_id(context, ruling_number)
    is_person = entity.schema.is_a("Person")
    entity.add("name", row.pop("Name"), lang="zho" if is_person else "eng")
    entity.add("alias", h.multi_split(row.pop("Alias"), ";"), lang="eng")
    entity.add("jurisdiction", row.pop("Jurisdiction"))
    entity.add("registrationNumber", row.pop("Registration number"))
    h.apply_date(entity, "incorporationDate", row.pop("Incorporation date"))
    if is_person:
        country = row.pop("Country")
        entity.add("country", country)
        entity.add("gender", row.pop("Gender"))
        entity.add("birthPlace", row.pop("Birth place"), lang="zho")
        entity.add("idNumber", row.pop("ID number"))
        entity.add("passportNumber", row.pop("Passport number"))
        h.apply_date(entity, "birthDate", row.pop("Birth date"))
        h.apply_date(entity, "deathDate", row.pop("Death date"))
        for address in h.multi_split(row.pop("Address"), ";"):
            h.copy_address(
                entity,
                h.make_address(context, full=address, country_code=country, lang="zho"),
            )

    related_party = row.pop("Related party")
    relationship = row.pop("Relationship")
    if related_party != "":
        related_id = ruling_id(context, related_party)
        crawl_related_party(context, entity, related_id, relationship)

    sanction = h.make_sanction(
        context,
        entity,
        program_name="Counter-Terrorism Financing Act Article 4 Designations",
        program_key=PROGRAM_KEY,
        source_program_key="資恐防制法第4條第1項",
        start_date=row.pop("Listing date"),
        end_date=row.pop("Delisting date"),
    )
    sanction.add("authorityId", ruling_number)
    sanction.add("sourceUrl", row.pop("Listing URL"))
    sanction.add("sourceUrl", row.pop("Delisting URL"))
    sanction.add("summary", row.pop("Delisting reason"), lang="zho")
    if h.is_active(sanction):
        entity.add("topics", "sanction")
    context.emit(entity)
    context.emit(sanction)
    # Person-only columns left over on a company row land here.
    context.audit_data(row)


def check_announcements(context: Context, cited_urls: set[str]) -> None:
    """Warn about announcement PDFs on the MJIB page that haven't been reviewed.

    The announcements are scanned PDFs, so their records are kept in
    moj_sanctions.csv. A PDF counts as reviewed when its url and sha1 are listed
    in PDF_HASHES (see the runbook comment in the yml). The hash catches a PDF
    replaced under the same url.
    """
    assert cited_urls <= PDF_HASHES.keys(), cited_urls - PDF_HASHES.keys()
    assert context.dataset.url is not None
    doc = context.fetch_html(context.dataset.url, cache_days=1, absolute_links=True)
    links = h.xpath_elements(
        doc,
        "//p[contains(., 'Designations Pursuant to Article 4, CFT Act')]"
        "/following-sibling::ul[1]/li/a",
    )
    assert len(links) > 0, "No Article 4 announcement links"
    listed_urls: set[str] = set()
    for link in links:
        href = link.get("href")
        assert href is not None, h.element_text(link)
        url = unquote(href)
        listed_urls.add(url)
        expected_hash = PDF_HASHES.get(url)
        if expected_hash is None:
            context.log.warning(
                "Unreviewed MOJ announcement PDF listed",
                url=url,
                title=h.element_text(link),
            )
            continue
        h.assert_url_hash(context, href, expected_hash)
    for url in PDF_HASHES.keys() - listed_urls:
        context.log.warning("Reviewed MOJ announcement PDF no longer listed", url=url)


def crawl(context: Context) -> None:
    source_file = Path(__file__).parent / "moj_sanctions.csv"
    resource_path = context.get_resource_path("source.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, context.SOURCE_TITLE)

    with open(source_file, encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    ruling_numbers = {row["Ruling number"] for row in rows}
    assert len(ruling_numbers) == len(rows), "Duplicate ruling numbers"

    cited_urls: set[str] = set()
    for row in rows:
        related_party = row["Related party"]
        # Related parties are emitted from their own rows.
        assert related_party == "" or related_party in ruling_numbers, related_party
        cited_urls.add(row["Listing URL"])
        if row["Delisting URL"] != "":
            cited_urls.add(row["Delisting URL"])
        crawl_row(context, row)

    check_announcements(context, cited_urls)
