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


def emit_senator(
    context: Context,
    *,
    person: Entity,
    member_position: tuple[Entity, PositionCategorisation],
    term: Result,
    presidency_position: tuple[Entity, PositionCategorisation] | None,
    constituency: str | None = None,
    political_group: str | None = None,
) -> None:
    """Emit a senator with a membership occupancy for the term, plus a presidency
    occupancy when `presidency_position` is given, i.e. when they presided over the
    Senate in that term."""
    if term.elected:
        # Senators must be Gabonese by origin: Organic Law 001/2025 on the Electoral
        # Code, Art. 222. https://journal-officiel.ga/21554-001-2025-
        person.add("citizenship", "ga")
    else:
        # Appointed, so no eligibility rule applies.
        person.add("country", "ga")
    member_entity, member_categorisation = member_position
    member_occupancy = h.make_occupancy(
        context,
        person,
        member_entity,
        categorisation=member_categorisation,
        period_start=term.period_start,
        period_end=term.period_end,
        no_end_implies_current=term.period_end is None,
    )
    if member_occupancy is None:
        return
    member_occupancy.add("constituency", constituency)
    member_occupancy.add("politicalGroup", political_group)
    context.emit(member_occupancy)

    if presidency_position is not None:
        presidency_entity, presidency_categorisation = presidency_position
        if presidency_categorisation.is_pep:
            presidency_occupancy = h.make_occupancy(
                context,
                person,
                presidency_entity,
                categorisation=presidency_categorisation,
                # The presidency spans the term, unless the term records that its
                # President took office before the full chamber sat.
                period_start=term.presidency_start or term.period_start,
                period_end=term.period_end,
                no_end_implies_current=term.period_end is None,
            )
            if presidency_occupancy is not None:
                context.emit(presidency_occupancy)
                context.emit(presidency_entity)
    context.emit(person)


def crawl_senator(
    context: Context,
    *,
    member_position: tuple[Entity, PositionCategorisation],
    presidency_position: tuple[Entity, PositionCategorisation],
    term_slugs: dict[int, str],
    post: dict[str, Any],
) -> None:
    slug = post.pop("slug")
    term_override = context.lookup("post_term", slug)
    if term_override is not None:
        if term_override.value is None:
            context.log.info("Skipping post outside the dated terms", slug=slug)
            return
        term_slug = term_override.value
    else:
        legislature_ids = post.pop("legislature")
        if len(legislature_ids) != 1:
            raise ValueError(f"Post {slug} has legislatures {legislature_ids}")
        term_slug = term_slugs[legislature_ids[0]]
    # The term's dates and elected flag, by legislature slug.
    term = context.lookup("term", term_slug)
    assert term is not None

    doc = lxml_html.fromstring(post.pop("content")["rendered"])
    headings = h.xpath_elements(doc, "//h2")
    if len(headings) > 1:
        raise ValueError(f"Post {slug} has several roles")
    # The heading gives a senator's role in the chamber. Of these roles, only the
    # presidency is a position of its own, and the only President with a senator post
    # is the Transition one: the sitting President comes from the Bureau page.
    is_president = False
    if headings:
        role = h.element_text(headings[0])
        is_president = role in (
            "Président du Sénat de la Transition",
            "Présidente du Sénat de la Transition",
        )

    # e.g. <li><b>Né le :</b> 1959-11-27</li>
    fields: dict[str, str] = {}
    for li in h.xpath_elements(doc, "//li[b]"):
        label = h.element_text(h.xpath_element(li, "./b"))
        text = h.element_text(li)
        assert text.startswith(label), text
        fields[label.rstrip(" :")] = text[len(label) :].strip()

    person = context.make("Person")
    person.id = context.make_slug(str(post.pop("id")))
    # e.g. "N&rsquo;NANG NTSEME Alexis"
    name = html.unescape(post.pop("title")["rendered"])
    h.apply_reviewed_name_string(context, person, string=name, llm_cleaning=True)
    person.add("sourceUrl", post.pop("link"))

    birth_date = fields.pop("Né le")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", birth_date) is None:
        raise ValueError(f"Unexpected birth date in post {slug}: {birth_date}")
    # The form pre-fills the entry date (2025, 2026) and one value reads 1557.
    if 1900 <= int(birth_date[:4]) <= 2000:
        h.apply_date(person, "birthDate", birth_date)
    else:
        context.log.info(
            "Dropping implausible birth date", slug=slug, birth_date=birth_date
        )
    person.add("birthPlace", fields.pop("Lieu de naissance"))
    person.add("profession", fields.pop("Profession", None))
    person.add("political", fields.pop("Partie politique", None))
    constituency = fields.pop("Département", None)
    if not term.elected:
        # The département some appointed senators give is not a constituency.
        constituency = None
    political_group = fields.pop("Groupe parlementaire", None)
    # Province is a slug of the département's province; the seat repeats the
    # département or says "Unique". Substitutes aren't emitted.
    context.audit_data(fields, ignore=["Province", "Siège", "Suppléant"])

    emit_senator(
        context,
        person=person,
        member_position=member_position,
        term=term,
        presidency_position=presidency_position if is_president else None,
        constituency=constituency,
        political_group=political_group,
    )


def crawl_bureau_president(
    context: Context,
    *,
    member_position: tuple[Entity, PositionCategorisation],
    presidency_position: tuple[Entity, PositionCategorisation],
    post_slugs: set[str],
) -> None:
    """Emit the sitting Senate President, who appears on the Bureau page only and has
    no senator post, with two occupancies: Member of the Senate of Gabon and President
    of the Senate of Gabon."""
    bureau_page = context.fetch_json(f"{context.data_url}pages/199")
    bureau_url = bureau_page["link"]
    # The term's dates and elected flag, by the legislature slug in the Bureau page URL,
    # e.g. https://senat.ga/vos-senateurs-2/sixieme-legislature/le-bureau/
    term = context.lookup("term", urlparse(bureau_url).path.strip("/").split("/")[-2])
    assert term is not None
    if term.period_end is not None:
        raise ValueError(f"The Bureau page is for an ended term: {bureau_url}")

    doc = lxml_html.fromstring(bureau_page["content"]["rendered"])
    president_headings = []
    # Each member is an <h5> name followed by a <p> role.
    for heading in h.xpath_elements(doc, "//h5"):
        role = h.element_text(h.xpath_element(heading, "./following-sibling::p[1]"))
        if role in ("Président du Sénat", "Présidente du Sénat"):
            president_headings.append(heading)
    if len(president_headings) != 1:
        raise ValueError(
            f"Expected one President on the Bureau page: {president_headings}"
        )
    heading = president_headings[0]
    # e.g. .../senateurs/nyama-ekoume-huguette-yvonne-epse-awori-onanga/
    post_url = h.xpath_string(heading, "./a/@href")
    if urlparse(post_url).path.strip("/").split("/")[-1] in post_slugs:
        raise ValueError(f"The President now has a senator post, use it: {post_url}")
    name = h.element_text(heading)

    person = context.make("Person")
    person.id = context.make_id(name)
    h.apply_reviewed_name_string(context, person, string=name, llm_cleaning=True)
    person.add("sourceUrl", bureau_url)
    emit_senator(
        context,
        person=person,
        member_position=member_position,
        term=term,
        presidency_position=presidency_position,
    )


def crawl(context: Context) -> None:
    member_entity = h.make_position(
        context,
        name="Member of the Senate of Gabon",
        wikidata_id="Q21295135",
        country="ga",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    member_categorisation = categorise(context, member_entity)
    if not member_categorisation.is_pep:
        context.log.warning(
            "Position is not categorised as PEP", position=member_entity.id
        )
        return
    context.emit(member_entity)
    member_position = (member_entity, member_categorisation)

    # Held by two people in the source: the Transition President, found through their
    # senator post, and the sitting President, found on the Bureau page. emit_senator
    # emits it only together with a presidency occupancy.
    presidency_entity = h.make_position(
        context,
        name="President of the Senate of Gabon",
        country="ga",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    presidency_position = (presidency_entity, categorise(context, presidency_entity))

    legislatures = context.fetch_json(f"{context.data_url}legislature?per_page=100")
    term_slugs: dict[int, str] = {}
    for legislature in legislatures:
        # The 1st-5th legislatures exist as empty terms.
        if legislature["count"] > 0:
            # The term's dates and elected flag, by legislature slug. The required
            # lookup raises on a populated term that has no dates yet.
            term = context.lookup("term", legislature["slug"])
            assert term is not None
            if not isinstance(term.elected, bool):
                raise ValueError(f"Term {legislature['slug']} needs an elected flag")
            term_slugs[legislature["id"]] = legislature["slug"]

    # Fetch every post rather than per term: some posts carry no legislature tag.
    # 100 is the WordPress REST API's maximum page size.
    post_slugs: set[str] = set()
    page_number, total_pages = 0, 1
    while page_number < total_pages:
        page_number += 1
        response = context.fetch_response(
            f"{context.data_url}senateur?per_page=100&page={page_number}"
        )
        total_pages = int(response.headers["X-WP-TotalPages"])
        for post in response.json():
            post_slugs.add(post["slug"])
            crawl_senator(
                context,
                member_position=member_position,
                presidency_position=presidency_position,
                term_slugs=term_slugs,
                post=post,
            )

    crawl_bureau_president(
        context,
        member_position=member_position,
        presidency_position=presidency_position,
        post_slugs=post_slugs,
    )

    assert_all_accepted(context, raise_on_unaccepted=False)
