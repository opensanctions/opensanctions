import csv
import re
import shutil
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from rigour.mime.types import CSV

from zavod import Context, Entity
from zavod import helpers as h

PROGRAM_KEY = "TW-CFT"
RULING_NUMBER = re.compile(r"^法裁第(\d+)號$")
# An address is qualified as the registered domicile or the residence.
ADDRESS = re.compile(r"^(.+)（(設籍|居所)）$")


def ruling_id(context: Context, ruling_number: str) -> str:
    match = RULING_NUMBER.fullmatch(ruling_number)
    assert match is not None, ruling_number
    entity_id = context.make_slug(match.group(1))
    assert entity_id is not None
    return entity_id


def apply_clean_date(
    entity: Entity, prop: str, row: dict[str, str], column: str
) -> None:
    original = row.pop(f"{column}_original")
    clean = row.pop(f"{column}_clean")
    assert (original == "") == (clean == ""), (original, clean)
    h.apply_date(
        entity, prop, clean, original_value=original if original != clean else None
    )


def crawl_related_party(
    context: Context, company: Entity, related_party: str, other_info: str
) -> None:
    res = context.lookup("other_identifying_information", other_info)
    assert res is not None  # required lookup
    is_related = res.ownership is True or res.directorship is True
    assert is_related == (related_party != ""), (related_party, other_info)
    if not is_related:
        return
    related_id = ruling_id(context, related_party)
    if res.ownership is True:
        ownership = context.make("Ownership")
        ownership.id = context.make_id("ownership", related_id, company.id)
        ownership.add("owner", related_id)
        ownership.add("asset", company)
        ownership.add("percentage", res.percentage)
        ownership.add("role", other_info, lang="zho")
        context.emit(ownership)
    if res.directorship is True:
        directorship = context.make("Directorship")
        directorship.id = context.make_id("directorship", related_id, company.id)
        directorship.add("director", related_id)
        directorship.add("organization", company)
        directorship.add("role", other_info, lang="zho")
        context.emit(directorship)


def crawl_row(context: Context, row: dict[str, str]) -> None:
    entity = context.make(row.pop("type"))
    ruling_number = row.pop("ruling_number")
    entity.id = ruling_id(context, ruling_number)
    is_person = entity.schema.is_a("Person")
    entity.add("name", row.pop("name"), lang="zho" if is_person else "eng")
    entity.add("alias", h.multi_split(row.pop("alias"), ";"), lang="eng")
    entity.add("jurisdiction", row.pop("place_of_registration"), lang="zho")
    entity.add("registrationNumber", row.pop("registration_number"))
    apply_clean_date(entity, "incorporationDate", row, "registration_date")
    if is_person:
        entity.add("gender", row.pop("gender"), lang="zho")
        entity.add("birthPlace", row.pop("birth_place"), lang="zho")
        entity.add("idNumber", row.pop("national_id_number"))
        entity.add("passportNumber", row.pop("passport_number"))
        apply_clean_date(entity, "birthDate", row, "birth_date")
        apply_clean_date(entity, "deathDate", row, "death_date")
        for address in h.multi_split(row.pop("address"), ";"):
            match = ADDRESS.fullmatch(address)
            assert match is not None, address
            h.copy_address(
                entity,
                h.make_address(context, full=match.group(1), lang="zho"),
            )

    crawl_related_party(
        context,
        entity,
        row.pop("related_party"),
        row.pop("other_identifying_information"),
    )

    sanction = h.make_sanction(
        context,
        entity,
        program_name="Counter-Terrorism Financing Act Article 4 Designations",
        program_key=PROGRAM_KEY,
        source_program_key="資恐防制法第4條第1項",
    )
    apply_clean_date(sanction, "startDate", row, "listing_date")
    apply_clean_date(sanction, "endDate", row, "delisting_date")
    if sanction.has("endDate"):
        sanction.add("status", "active" if h.is_active(sanction) else "inactive")
    sanction.add("authorityId", ruling_number)
    sanction.add("authorityId", row.pop("listing_notice_number"))
    sanction.add("authorityId", row.pop("delisting_notice_number"))
    sanction.add("sourceUrl", row.pop("listing_url"))
    sanction.add("sourceUrl", row.pop("delisting_url"))
    sanction.add("summary", row.pop("delisting_reason"), lang="zho")
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
    in the dataset's `discovery.reviewed_urls` config (see the runbook there).
    The hash catches a PDF replaced under the same url.
    """
    discovery: dict[str, Any] = context.dataset.config.get("discovery", {})
    reviewed: dict[str, str] = discovery["reviewed_urls"]
    assert cited_urls <= reviewed.keys(), cited_urls - reviewed.keys()
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
        expected_hash = reviewed.get(url)
        if expected_hash is None:
            context.log.warning(
                "Unreviewed MOJ announcement PDF listed",
                url=url,
                title=h.element_text(link),
            )
            continue
        h.assert_url_hash(context, href, expected_hash)
    for url in reviewed.keys() - listed_urls:
        context.log.warning("Reviewed MOJ announcement PDF no longer listed", url=url)


def crawl(context: Context) -> None:
    source_file = Path(__file__).parent / "moj_sanctions.csv"
    resource_path = context.get_resource_path("source.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, context.SOURCE_TITLE)

    with open(source_file, encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    ruling_numbers = {row["ruling_number"] for row in rows}
    assert len(ruling_numbers) == len(rows), "Duplicate ruling numbers"

    cited_urls: set[str] = set()
    for row in rows:
        related_party = row["related_party"]
        # Related parties are emitted from their own rows.
        assert related_party == "" or related_party in ruling_numbers, related_party
        cited_urls.add(row["listing_url"])
        if row["delisting_url"] != "":
            cited_urls.add(row["delisting_url"])
        crawl_row(context, row)

    check_announcements(context, cited_urls)
