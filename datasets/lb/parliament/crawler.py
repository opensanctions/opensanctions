import re

from pydantic import BaseModel
from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element
from zavod.extract.llm import run_typed_text_prompt, DEFAULT_MODEL
from zavod.stateful.review import (
    TextSourceValue,
    review_extraction,
    assert_all_accepted,
)


# A position this chamber confers, paired with its categorisation.
Post = tuple[Entity, PositionCategorisation]

# Each term in the index is labelled with the weekday of its election, e.g.
# "الأحد 15 أيار 2022"; only the trailing day, month and year are the date.
ELECTION_DATE = re.compile(r"\d{1,2} \S+(?: \S+)? \d{4}$")
MEMBER_ID = re.compile(r"MemberDetails\.aspx\?Id=(\d+)$")

CRAWLER_VERSION = 1


class EntityModel(BaseModel):
    name: str
    alias: str | None
    start_date: str | None
    death_date: str | None


prompt = """
Extract the name and from the parenthetical text the alias, death date,
and occupancy start date if they exist. Death date might be indicated if
died, assassinated, or martyred is in the parenthetical text. If there is
no date in the parenthesis, then the parenthetical text is an alias.

Examples of the raw name are:
الياس جوزف سكاف (توفي في 10/10/2015)   -> {"name": "الياس جوزف سكاف", "alias": None, "start_date": None, death_date: "10/10/2015"}
أنطوان غانم (استشهد في 19/9/2007)   -> {"name": "أنطوان غانم", "alias": None, "start_date": None, death_date: "19/9/2007"}
صولانج الجميل (صولانج لويس توتونجي)  -> {"name": "صولانج الجميل", "alias": "صولانج لويس توتونجي", "start_date": None, death_date: None}
كاظم صالح الخير(انتخب في 13 حزيران2010) -> {"name": "كاظم صالح الخير", "alias": None, "start_date": "13/6/2010", death_date: None}
"""


def field(doc: Element, element_id: str) -> str | None:
    """Read a labelled value off a parliament page, or None if it is published blank.

    Every such value sits in an element whose id carries the ASP.NET placeholder
    prefix the site renders its content under.
    """
    element = h.xpath_element(doc, f'.//*[@id="ContentPlaceHolder1_{element_id}"]')
    return h.element_text(element) or None


def make_post(context: Context, name: str, wikidata_id: str) -> Post | None:
    position = h.make_position(
        context,
        name,
        wikidata_id=wikidata_id,
        country="lb",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return None
    context.emit(position)
    return position, categorisation


def crawl_person(context: Context, url: str) -> tuple[Entity, str | None, str | None]:
    """Build a member's Person entity from their detail page, the only place the
    biographical fields appear, and return their constituency alongside it."""
    doc = context.fetch_html(url, cache_days=7)
    member_id = MEMBER_ID.search(url)
    assert member_id is not None, url
    name = field(doc, "hMember")
    assert name is not None, url

    person = context.make("Person")
    person.id = context.make_slug(member_id.group(1))
    h.apply_date(person, "birthDate", field(doc, "spanDOB"))

    entity_data = None
    if h.is_name_irregular(person, name):
        source_value = TextSourceValue(
            key_parts=name, label="Raw name string with parenthetical", text=name
        )
        prompt_result = run_typed_text_prompt(
            context, prompt, source_value.value_string, response_type=EntityModel
        )
        review = review_extraction(
            context,
            crawler_version=CRAWLER_VERSION,
            source_value=source_value,
            original_extraction=prompt_result,
            origin=DEFAULT_MODEL,
        )
        entity_data = review.extracted_data
    else:
        person.add("name", name, lang="ara")

    if entity_data is not None:
        if entity_data.name is not None:
            h.apply_name(
                entity=person,
                full=entity_data.name,
                lang="ara",
            )
        if entity_data.alias is not None:
            person.add("alias", entity_data.alias, lang="ara")
        h.apply_date(person, "deathDate", entity_data.death_date)

    # Deputies must be Lebanese citizens: Parliamentary Election Law No. 44 of
    # 17 June 2017, Art. 7. https://aceproject.org/ero-en/regions/mideast/LB/
    # lebanon-law-no.44-parliamentary-elections-2017
    person.add("citizenship", "lb")
    religion = field(doc, "spanMazhab")
    if religion is not None:
        person.add("religion", religion, lang="ara")

    # The source publishes these four labels with no value for every member on
    # record. audit_data warns if that ever changes, since the party and the
    # parliamentary bloc in particular would be worth extracting.
    context.audit_data(
        {
            "party": field(doc, "spanHizb"),
            "district": field(doc, "spanCaza"),
            "municipality": field(doc, "spanManu"),
            "bloc": field(doc, "spanBlock"),
            # "biography": field(doc, "pMember") # skip bio
        }
    )
    constituency = field(doc, "spanMohafaza")
    start_date = entity_data.start_date if entity_data is not None else None
    return (
        person,
        constituency,
        start_date,
    )


def crawl_term(
    context: Context,
    url: str,
    election_date: str,
    period_end: str | None,
    member_post: Post,
    speaker_post: Post | None,
    deputy_post: Post | None,
) -> None:
    doc = context.fetch_html(url, absolute_links=True, cache_days=1)
    members: dict[str, str] = {}
    links = h.xpath_elements(
        doc, './/div[@class="members-grid"]//div[@class="desc_wrapper"]/a'
    )
    for link in links:
        href = h.xpath_strings(link, "./@href", expect_exactly=1)[0]
        members[h.element_text(link)] = href
    # The presiding officers are identified by the name they are listed under, so a
    # term with two identically named members could not be resolved unambiguously.
    assert len(members) == len(links), url

    # The Speaker and their deputy are elected from among the members and keep their
    # seat, so for the term they hold two positions rather than one.
    officers: dict[str, Post] = {}
    for element_id, post in (
        ("spanPresident", speaker_post),
        ("spanVice", deputy_post),
    ):
        name = field(doc, element_id)
        assert name in members, (url, element_id, name)
        if post is not None:
            officers[name] = post

    for name, member_url in members.items():
        person, constituency, start_date = crawl_person(context, member_url)
        posts = [member_post]
        officer_post = officers.get(name)
        if officer_post is not None:
            posts.append(officer_post)

        # a start date is somtimes provided in the name string; in these cases
        # we check to see if it is more recent than the period start date and if
        # so use it as the election_date
        if start_date is None:
            edate = election_date
        elif start_date > election_date:
            edate = start_date
        else:
            edate = election_date

        for position, categorisation in posts:
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                period_start=election_date,
                period_end=period_end,
                election_date=edate,
                categorisation=categorisation,
            )
            if occupancy is None:
                continue
            # The governorate is only published for sitting members and describes the
            # seat they hold now, so it isn't carried back onto their earlier terms.
            if period_end is None:
                occupancy.add("constituency", constituency)
            context.emit(occupancy)
            context.emit(person)


def crawl(context: Context) -> None:
    member_post = make_post(context, "Member of the Parliament of Lebanon", "Q21328581")
    if member_post is None:
        return
    speaker_post = make_post(
        context, "Speaker of the Parliament of Lebanon", "Q65038000"
    )
    deputy_post = make_post(
        context, "Deputy Speaker of the Parliament of Lebanon", "Q96376243"
    )

    doc = context.fetch_html(context.data_url, absolute_links=True, cache_days=1)
    terms: list[tuple[str, str]] = []
    for item in h.xpath_elements(doc, './/div[contains(@class, "post-item")]'):
        label = h.element_text(h.xpath_element(item, './/div[@class="date_label"]'))
        match = ELECTION_DATE.search(label)
        assert match is not None, label
        (election_date,) = h.extract_date(
            context.dataset, match.group(), fallback_to_original=False
        )
        urls = set(h.xpath_strings(item, './/a[contains(@href, "HouseDetails")]/@href'))
        assert len(urls) == 1, urls
        terms.append((election_date, urls.pop()))

    # A term runs until its successor is elected, which only yields the right end date
    # if the index is ordered newest first. The newest term has yet to end.
    dates = [date for date, _ in terms]
    assert all(a > b for a, b in zip(dates, dates[1:])), dates

    for offset, (election_date, url) in enumerate(terms):
        crawl_term(
            context,
            url,
            election_date,
            dates[offset - 1] if offset else None,
            member_post,
            speaker_post,
            deputy_post,
        )

    assert_all_accepted(context, raise_on_unaccepted=False)
