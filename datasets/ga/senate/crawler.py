import html
import re
from typing import Any
from urllib.parse import urlparse

from datapatch import Result
from lxml import html as lxml_html
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.stateful.review import assert_all_accepted

from zavod import Context, Entity
from zavod import helpers as h

TOPICS = ["gov.legislative", "gov.national"]


def emit_holder(
    context: Context,
    person: Entity,
    member: Entity,
    categorisation: PositionCategorisation,
    term: Result,
    office: Result | None,
    constituency: str | None = None,
    group: str | None = None,
) -> None:
    if term.elected:
        # Senators must be Gabonese by origin: Organic Law 001/2025 on the Electoral
        # Code, Art. 222. https://journal-officiel.ga/21554-001-2025-
        person.add("citizenship", "ga")
    else:
        # Appointed, so no eligibility rule applies.
        person.add("country", "ga")
    occupancy = h.make_occupancy(
        context,
        person,
        member,
        categorisation=categorisation,
        period_start=term.period_start,
        period_end=term.period_end,
        no_end_implies_current=term.period_end is None,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)
    occupancy.add("politicalGroup", group)
    context.emit(occupancy)

    if office is not None:
        office_position = h.make_position(
            context,
            name=office.name,
            country="ga",
            topics=TOPICS,
            lang="eng",
        )
        office_categorisation = categorise(context, office_position)
        if office_categorisation.is_pep:
            office_occupancy = h.make_occupancy(
                context,
                person,
                office_position,
                categorisation=office_categorisation,
                # The Transition presidency predates the full chamber.
                period_start=office.period_start or term.period_start,
                period_end=term.period_end,
                no_end_implies_current=term.period_end is None,
            )
            if office_occupancy is not None:
                context.emit(office_occupancy)
                context.emit(office_position)
    context.emit(person)


def crawl_senator(
    context: Context,
    member: Entity,
    categorisation: PositionCategorisation,
    term_slugs: dict[int, str],
    record: dict[str, Any],
) -> None:
    slug = record.pop("slug")
    override = context.lookup("post_term", slug)
    if override is not None:
        if override.value is None:
            context.log.info("Skipping post outside the dated terms", slug=slug)
            return
        term_slug = override.value
    else:
        legislatures = record.pop("legislature")
        if len(legislatures) != 1:
            raise ValueError(f"Post {slug} has legislatures {legislatures}")
        term_slug = term_slugs[legislatures[0]]
    term = context.lookup("term", term_slug)
    assert term is not None

    doc = lxml_html.fromstring(record.pop("content")["rendered"])
    headings = h.xpath_elements(doc, "//h2")
    if len(headings) > 1:
        raise ValueError(f"Post {slug} has several roles")
    office = None
    if headings:
        role = h.element_text(headings[0])
        res = context.lookup("position", role)
        assert res is not None, role
        if res.name is not None:
            office = res

    # e.g. <li><b>Né le :</b> 1959-11-27</li>
    fields: dict[str, str] = {}
    for li in h.xpath_elements(doc, "//li[b]"):
        label = h.element_text(h.xpath_element(li, "./b"))
        text = h.element_text(li)
        assert text.startswith(label), text
        fields[label.rstrip(" :")] = text[len(label) :].strip()

    person = context.make("Person")
    person.id = context.make_slug(str(record.pop("id")))
    # e.g. "N&rsquo;NANG NTSEME Alexis"
    name = html.unescape(record.pop("title")["rendered"])
    h.apply_reviewed_name_string(context, person, string=name, llm_cleaning=True)
    person.add("sourceUrl", record.pop("link"))

    dob = fields.pop("Né le")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", dob) is None:
        raise ValueError(f"Unexpected birth date in post {slug}: {dob}")
    # The form pre-fills the entry date (2025, 2026) and one value reads 1557.
    if 1900 <= int(dob[:4]) <= 2000:
        h.apply_date(person, "birthDate", dob)
    else:
        context.log.info("Dropping implausible birth date", slug=slug, dob=dob)
    person.add("birthPlace", fields.pop("Lieu de naissance"))
    person.add("profession", fields.pop("Profession", None))
    person.add("political", fields.pop("Partie politique", None))
    constituency = fields.pop("Département", None)
    if not term.elected:
        # The département some appointed senators give is not a constituency.
        constituency = None
    group = fields.pop("Groupe parlementaire", None)
    # Province is a slug of the département's province; the seat repeats the
    # département or says "Unique". Substitutes aren't emitted.
    context.audit_data(fields, ignore=["Province", "Siège", "Suppléant"])

    emit_holder(
        context, person, member, categorisation, term, office, constituency, group
    )


def crawl_bureau_president(
    context: Context,
    member: Entity,
    categorisation: PositionCategorisation,
    post_slugs: set[str],
) -> None:
    """The Senate President has no senator post; she appears on the Bureau page only."""
    page = context.fetch_json(f"{context.data_url}pages/199")
    # e.g. https://senat.ga/vos-senateurs-2/sixieme-legislature/le-bureau/
    term = context.lookup("term", urlparse(page["link"]).path.strip("/").split("/")[-2])
    assert term is not None
    if term.period_end is not None:
        raise ValueError(f"The Bureau page is for an ended term: {page['link']}")

    doc = lxml_html.fromstring(page["content"]["rendered"])
    presidents = []
    # Each member is an <h5> name followed by a <p> role.
    for heading in h.xpath_elements(doc, "//h5"):
        role = h.element_text(h.xpath_element(heading, "./following-sibling::p[1]"))
        res = context.lookup("position", role)
        assert res is not None, role
        if res.name is not None:
            presidents.append((heading, res))
    if len(presidents) != 1:
        raise ValueError(f"Expected one President on the Bureau page: {presidents}")
    heading, office = presidents[0]
    # e.g. .../senateurs/nyama-ekoume-huguette-yvonne-epse-awori-onanga/
    href = h.xpath_string(heading, "./a/@href")
    if urlparse(href).path.strip("/").split("/")[-1] in post_slugs:
        raise ValueError(f"The President now has a senator post, use it: {href}")
    name = h.element_text(heading)

    person = context.make("Person")
    person.id = context.make_id(name)
    h.apply_reviewed_name_string(context, person, string=name, llm_cleaning=True)
    person.add("sourceUrl", page["link"])
    emit_holder(context, person, member, categorisation, term, office)


def crawl(context: Context) -> None:
    member = h.make_position(
        context,
        name="Member of the Senate of Gabon",
        wikidata_id="Q21295135",
        country="ga",
        topics=TOPICS,
        lang="eng",
    )
    categorisation = categorise(context, member)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=member.id)
        return
    context.emit(member)

    terms = context.fetch_json(f"{context.data_url}legislature?per_page=100")
    term_slugs: dict[int, str] = {}
    for term in terms:
        # The 1st-5th legislatures exist as empty terms.
        if term["count"] > 0:
            # The required lookup raises on a populated term that has no dates yet.
            res = context.lookup("term", term["slug"])
            assert res is not None
            if not isinstance(res.elected, bool):
                raise ValueError(f"Term {term['slug']} needs an elected flag")
            term_slugs[term["id"]] = term["slug"]

    # Fetch every post rather than per term: some posts carry no legislature tag.
    # 100 is the WordPress REST API's maximum page size.
    post_slugs: set[str] = set()
    page, total_pages = 0, 1
    while page < total_pages:
        page += 1
        response = context.fetch_response(
            f"{context.data_url}senateur?per_page=100&page={page}"
        )
        total_pages = int(response.headers["X-WP-TotalPages"])
        for record in response.json():
            post_slugs.add(record["slug"])
            crawl_senator(context, member, categorisation, term_slugs, record)

    crawl_bureau_president(context, member, categorisation, post_slugs)

    assert_all_accepted(context, raise_on_unaccepted=False)
