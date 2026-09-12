import re
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Literal
from urllib.parse import unquote, urlparse

from pydantic import BaseModel, Field

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract.llm import DEFAULT_MODEL, run_typed_image_prompt
from zavod.stateful.review import (
    JSONSourceValue,
    assert_all_accepted,
    review_extraction,
)


PDF_FILENAME = re.compile(r"^(\d{8}-\d+)(?:\([^)]*\))?\.pdf$")
RULING_NUMBER = re.compile(r"^法裁第(\d+)號$")

EXTRACT_PROMPT = """Extract the Taiwan Ministry of Justice designation or delisting announcement printed in this scanned PDF page.

Extract ONLY information visible on this page. Do not infer, complete, translate, or guess missing values. Pages containing no designation or delisting records must return empty designations and delistings lists.

General instructions:
- Ignore legal boilerplate and reference-law sections such as 參考法條.
- Preserve Chinese and other original-script text exactly as printed, apart from trimming whitespace.
- Convert dates to ISO 8601 (YYYY-MM-DD, YYYY-MM, or YYYY). Taiwan ROC calendar years (民國) convert by adding 1911; for example, 民國107年 is 2018.
- reference_number is the announcement document reference, such as 法檢字第...號, not an individual designation ruling.
- announcement_date is the 發文日期 of the announcement.

For each designation:
- ruling_number: the full 法裁第...號 value.
- entity_type: person for a natural person, legal_entity for a company or other legal entity.
- name: the name in the original script exactly as printed.
- name_eng: the separately printed English or passport name, otherwise null.
- birth_date, death_date, and registration_date: convert printed dates to ISO 8601 using the ROC conversion rule.
- gender: return male or female only when explicitly printed, otherwise null.
- birth_place, addresses, id_number, passport_number, registration_country, and registration_number: extract only explicitly printed values and remove only their field labels.
- other_identifying_info: preserve the complete raw 其他辨識資訊 text, otherwise null.
- related_parties: extract each owner, shareholder, responsible person, or other related party stated in 其他辨識資訊. Keep relationship in the source's wording, such as 負責人及唯一股東 or 由某人百分之百持股. Extract the party name separately. Set ownership_percentage to a bare numeric percentage such as 100 only when explicit.

For each delisting:
- ruling_number: the full ruling number of the designation being lifted.
- name: the delisted party's name exactly as printed.
- delisting_date: the announcement publication date in ISO 8601 when the notice says measures are lifted from publication, otherwise the explicitly printed effective date; null if neither appears on this page.
- death_date: the printed date of death in ISO 8601, otherwise null.
- reason: preserve the printed reason for delisting, otherwise null.
"""


class RelatedParty(BaseModel):
    name: str = Field(description="Related party name exactly as printed")
    relationship: str = Field(description="Relationship in the source's wording")
    ownership_percentage: str | None = Field(
        default=None,
        description="Bare numeric ownership percentage when explicitly printed",
    )


class Designation(BaseModel):
    ruling_number: str = Field(description="Full designation ruling number")
    entity_type: Literal["person", "legal_entity"]
    name: str = Field(description="Name in the original script")
    name_eng: str | None = None
    birth_date: str | None = None
    gender: str | None = None
    birth_place: str | None = None
    addresses: list[str] = Field(default_factory=list)
    id_number: str | None = None
    passport_number: str | None = None
    death_date: str | None = None
    registration_country: str | None = None
    registration_date: str | None = None
    registration_number: str | None = None
    other_identifying_info: str | None = None
    related_parties: list[RelatedParty] = Field(default_factory=list)


class Delisting(BaseModel):
    ruling_number: str = Field(description="Full ruling number being delisted")
    name: str = Field(description="Delisted party name exactly as printed")
    delisting_date: str | None = None
    death_date: str | None = None
    reason: str | None = None


class Announcement(BaseModel):
    reference_number: str | None = None
    announcement_date: str | None = None
    designations: list[Designation] = Field(default_factory=list)
    delistings: list[Delisting] = Field(default_factory=list)


@dataclass(frozen=True)
class AcceptedAnnouncement:
    data: Announcement
    source_url: str


@dataclass
class DesignationSource:
    designation: Designation
    announcement_date: str
    source_url: str
    delisting: Delisting | None = None
    delisting_date: str | None = None
    delisting_source_url: str | None = None


@dataclass(frozen=True)
class BuiltDesignation:
    designation: Designation
    entity: Entity


def merge_scalar(field: str, values: list[str | None]) -> str | None:
    merged: str | None = None
    for value in values:
        if value is None:
            continue
        if merged is not None and value != merged:
            raise ValueError(
                f"Conflicting {field} values across announcement pages: "
                f"{merged!r} and {value!r}"
            )
        merged = value
    return merged


def merge_pages(pages: list[Announcement]) -> Announcement:
    if len(pages) == 0:
        raise ValueError("Announcement PDF produced no page images")
    return Announcement(
        reference_number=merge_scalar(
            "reference_number", [page.reference_number for page in pages]
        ),
        announcement_date=merge_scalar(
            "announcement_date", [page.announcement_date for page in pages]
        ),
        designations=[
            designation for page in pages for designation in page.designations
        ],
        delistings=[delisting for page in pages for delisting in page.delistings],
    )


def pdf_key(pdf_url: str) -> str:
    filename = Path(unquote(urlparse(pdf_url).path)).name
    match = PDF_FILENAME.fullmatch(filename)
    if match is None:
        raise ValueError(f"Unexpected MJIB announcement PDF filename: {filename!r}")
    return match.group(1)


def extract_announcement(
    context: Context, pdf_url: str, label: str
) -> AcceptedAnnouncement | None:
    key = pdf_key(pdf_url)
    pdf_path = context.fetch_resource(f"{key}.pdf", pdf_url)
    page_results = [
        run_typed_image_prompt(
            context=context,
            prompt=EXTRACT_PROMPT,
            image_path=image_path,
            response_type=Announcement,
            model=DEFAULT_MODEL,
        )
        for image_path in h.make_pdf_page_images(pdf_path)
    ]
    extraction = merge_pages(page_results)
    source_value = JSONSourceValue(
        key_parts=key,
        data={"url": pdf_url, "sha1": sha1(pdf_path.read_bytes()).hexdigest()},
        label=label,
        url=pdf_url,
    )
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=extraction,
        origin=DEFAULT_MODEL,
    )
    if not review.accepted:
        return None
    return AcceptedAnnouncement(review.extracted_data, pdf_url)


def ruling_id(ruling_number: str) -> str:
    match = RULING_NUMBER.fullmatch(ruling_number)
    if match is None:
        raise ValueError(f"Unexpected designation ruling number: {ruling_number!r}")
    return match.group(1)


def collect_designations(
    announcements: list[AcceptedAnnouncement],
) -> dict[str, DesignationSource]:
    designations: dict[str, DesignationSource] = {}
    for announcement in announcements:
        data = announcement.data
        if len(data.designations) > 0 and data.announcement_date is None:
            raise ValueError(
                f"Designation announcement has no announcement date: {announcement.source_url}"
            )
        for designation in data.designations:
            identifier = ruling_id(designation.ruling_number)
            if identifier in designations:
                raise ValueError(
                    f"Duplicate designation ruling number: {designation.ruling_number!r}"
                )
            assert data.announcement_date is not None
            designations[identifier] = DesignationSource(
                designation=designation,
                announcement_date=data.announcement_date,
                source_url=announcement.source_url,
            )

    for announcement in announcements:
        data = announcement.data
        if len(data.delistings) > 0 and data.announcement_date is None:
            raise ValueError(
                f"Delisting announcement has no announcement date: {announcement.source_url}"
            )
        for delisting in data.delistings:
            identifier = ruling_id(delisting.ruling_number)
            source = designations.get(identifier)
            if source is None:
                raise ValueError(
                    "Delisting does not match a designation seen this run: "
                    f"{delisting.ruling_number!r}"
                )
            if source.delisting is not None:
                raise ValueError(
                    f"Duplicate delisting for ruling: {delisting.ruling_number!r}"
                )
            assert data.announcement_date is not None
            if (
                delisting.delisting_date is not None
                and delisting.delisting_date != data.announcement_date
            ):
                raise ValueError(
                    "Delisting effective date conflicts with its announcement date: "
                    f"{delisting.delisting_date!r} and {data.announcement_date!r}"
                )
            source.delisting = delisting
            source.delisting_date = data.announcement_date
            source.delisting_source_url = announcement.source_url
    return designations


def build_person(context: Context, source: DesignationSource) -> Entity:
    designation = source.designation
    unexpected = {
        "registration_country": designation.registration_country,
        "registration_date": designation.registration_date,
        "registration_number": designation.registration_number,
    }
    unexpected = {key: value for key, value in unexpected.items() if value is not None}
    if unexpected:
        raise ValueError(
            f"Legal-entity fields extracted for person {designation.ruling_number!r}: "
            f"{unexpected!r}"
        )
    person = context.make("Person")
    person.id = context.make_slug(ruling_id(designation.ruling_number))
    h.apply_name(person, full=designation.name, lang="zho")
    h.apply_name(person, full=designation.name_eng, alias=True, lang="eng")
    h.apply_date(person, "birthDate", designation.birth_date)

    death_date = designation.death_date
    if source.delisting is not None and source.delisting.death_date is not None:
        if death_date is not None and death_date != source.delisting.death_date:
            raise ValueError(
                f"Conflicting death dates for {designation.ruling_number!r}: "
                f"{death_date!r} and {source.delisting.death_date!r}"
            )
        death_date = source.delisting.death_date
    h.apply_date(person, "deathDate", death_date)
    person.add("gender", designation.gender)
    person.add("birthPlace", designation.birth_place)
    person.add("country", "tw")
    person.add("idNumber", designation.id_number)
    person.add("passportNumber", designation.passport_number)
    person.add("notes", designation.other_identifying_info)
    for address in designation.addresses:
        h.copy_address(
            person,
            h.make_address(context, full=address, country_code="tw", lang="zho"),
        )
    return person


def build_company(context: Context, source: DesignationSource) -> Entity:
    designation = source.designation
    unexpected = {
        "birth_date": designation.birth_date,
        "gender": designation.gender,
        "birth_place": designation.birth_place,
        "addresses": designation.addresses or None,
        "id_number": designation.id_number,
        "passport_number": designation.passport_number,
        "death_date": designation.death_date,
    }
    unexpected = {key: value for key, value in unexpected.items() if value is not None}
    if unexpected:
        raise ValueError(
            "Person fields extracted for legal entity "
            f"{designation.ruling_number!r}: {unexpected!r}"
        )
    company = context.make("Company")
    company.id = context.make_slug(ruling_id(designation.ruling_number))
    company.add("name", designation.name, lang="eng")
    company.add("alias", designation.name_eng, lang="eng")
    company.add("jurisdiction", designation.registration_country)
    company.add("registrationNumber", designation.registration_number)
    company.add("notes", designation.other_identifying_info)
    h.apply_date(company, "incorporationDate", designation.registration_date)
    return company


def build_designation(context: Context, source: DesignationSource) -> BuiltDesignation:
    designation = source.designation
    if designation.entity_type == "person":
        entity = build_person(context, source)
    else:
        entity = build_company(context, source)

    sanction = h.make_sanction(
        context,
        entity,
        program_name="Counter-Terrorism Financing Act Article 4 Designations",
        source_program_key="資恐防制法第4條第1項",
        start_date=source.announcement_date,
        end_date=source.delisting_date,
    )
    sanction.add("authorityId", designation.ruling_number)
    sanction.add("sourceUrl", source.source_url)
    sanction.add("sourceUrl", source.delisting_source_url)
    if source.delisting is not None:
        sanction.add("reason", source.delisting.reason)
    if h.is_active(sanction):
        entity.add("topics", "sanction")
    context.emit(entity)
    context.emit(sanction)
    return BuiltDesignation(designation, entity)


def add_name(
    entities_by_name: dict[str, Entity], name: str | None, entity: Entity
) -> None:
    if name is None:
        return
    key = name.strip().casefold()
    existing = entities_by_name.get(key)
    if existing is not None and existing.id != entity.id:
        raise ValueError(f"Ambiguous designated entity name: {name!r}")
    entities_by_name[key] = entity


def emit_relationships(context: Context, built: list[BuiltDesignation]) -> None:
    entities_by_name: dict[str, Entity] = {}
    for item in built:
        add_name(entities_by_name, item.designation.name, item.entity)
        add_name(entities_by_name, item.designation.name_eng, item.entity)

    for item in built:
        if len(item.designation.related_parties) > 0 and not item.entity.schema.is_a(
            "Organization"
        ):
            raise ValueError(
                "Related parties are only supported for designated organizations: "
                f"{item.designation.ruling_number!r}"
            )
        for party in item.designation.related_parties:
            related = entities_by_name.get(party.name.strip().casefold())
            if related is None:
                raise ValueError(f"Unresolvable related party: {party.name!r}")
            relationship = context.lookup("relationship.type", party.relationship)
            if relationship is None:
                raise ValueError(
                    f"Unhandled relationship wording: {party.relationship!r}"
                )

            percentage = party.ownership_percentage
            lookup_percentage = relationship.percentage
            if lookup_percentage is not None:
                lookup_percentage = str(lookup_percentage)
                if percentage is not None and percentage != lookup_percentage:
                    raise ValueError(
                        f"Conflicting ownership percentages for {party.relationship!r}: "
                        f"{percentage!r} and {lookup_percentage!r}"
                    )
                percentage = lookup_percentage

            if relationship.ownership is True:
                ownership = context.make("Ownership")
                ownership.id = context.make_id("ownership", related.id, item.entity.id)
                ownership.add("owner", related)
                ownership.add("asset", item.entity)
                ownership.add("percentage", percentage)
                ownership.add("role", party.relationship)
                context.emit(ownership)
            if relationship.directorship is True:
                directorship = context.make("Directorship")
                directorship.id = context.make_id(
                    "directorship", related.id, item.entity.id
                )
                directorship.add("director", related)
                directorship.add("organization", item.entity)
                directorship.add("role", party.relationship)
                context.emit(directorship)
            if (
                relationship.ownership is not True
                and relationship.directorship is not True
            ):
                raise ValueError(
                    f"Relationship lookup has no output type: {party.relationship!r}"
                )


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    article_4_links = h.xpath_element(
        doc,
        "//p[normalize-space(.)='●Designations Pursuant to Article 4, CFT Act']"
        "/following-sibling::ul[1]",
    )
    links = h.xpath_elements(article_4_links, "./li/a")
    if len(links) == 0:
        raise ValueError("MJIB Article 4 section contains no announcement links")
    announcements: list[AcceptedAnnouncement] = []
    for link in links:
        pdf_url = link.get("href")
        if pdf_url is None:
            raise ValueError("MJIB announcement link has no href")
        accepted = extract_announcement(context, pdf_url, h.element_text(link))
        if accepted is not None:
            announcements.append(accepted)

    designation_sources = collect_designations(announcements)
    built = [
        build_designation(context, source) for source in designation_sources.values()
    ]
    emit_relationships(context, built)
    assert_all_accepted(context)
