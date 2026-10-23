from typing import Any
from urllib.parse import urljoin

from lxml import html
from normality import squash_spaces
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import OccupancyStatus, categorise
from zavod.util import LangText

from zavod import Context
from zavod import helpers as h

# Posts in the member category that are news stories, not member profiles.
NEWS_CATEGORY = 491
# Only the member seat and the Speaker have a Wikidata item.
POSITION_QIDS = {
    "Member of the House of Representatives of Yemen": "Q24810806",
    "Speaker of the House of Representatives of Yemen": "Q63143442",
}


def crawl_person(
    context: Context, post: dict[str, Any], row: dict[str, str | None]
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(post["id"])
    name = row.pop("name")
    assert name is not None, post["id"]
    person.add("name", name)
    apply_translit_full_name(context, person, LangText(name, "ara"))
    # The post title is sometimes a shorter form of the name.
    alias = squash_spaces(post["title"]["rendered"])
    if alias != name:
        person.add("alias", alias)
    person.add("political", row.pop("political_bloc"))
    person.add("education", row.pop("education"))
    person.add("sourceUrl", post["link"])
    person.add("citizenship", "ye")

    # One profile has no constituency number, only the governorate.
    parts = [row.pop("constituency"), row.pop("governorate")]
    constituency = " - ".join(p for p in parts if p is not None)

    res = context.lookup("position", row.pop("committee_role"))
    assert res is not None
    for title in res.values:
        position = h.make_position(
            context,
            name=title,
            country="ye",
            topics=["gov.national", "gov.legislative"],
            wikidata_id=POSITION_QIDS.get(title),
            lang="eng",
        )
        categorisation = categorise(context, position)
        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            # The roster gives no dates and still lists members who have died,
            # so a listing does not prove a member is still in office.
            status=OccupancyStatus.UNKNOWN,
        )
        if occupancy is None:
            continue
        occupancy.add("constituency", constituency)
        context.emit(occupancy)
        context.emit(position)
        context.emit(person)

    context.audit_data(row, ignore=["committee"])


def crawl_post(context: Context, post: dict[str, Any]) -> None:
    if NEWS_CATEGORY in post["categories"]:
        return
    # Each profile is a table of label/value rows; a blank field omits its
    # value cell. Unknown labels stay in Arabic and fail the audit.
    doc = html.fromstring(post["content"]["rendered"])
    table = h.xpath_element(doc, ".//table")
    row: dict[str, str | None] = {}
    for tr in h.xpath_elements(table, ".//tr"):
        label, *value = [h.element_text(td) for td in tr.findall("td")]
        assert len(value) <= 1, (label, value)
        key = context.lookup_value("columns", label, warn_unmatched=True) or label
        row[key] = value[0] if value else None
    crawl_person(context, post, row)


def crawl(context: Context) -> None:
    # WordPress rejects a page past the last one, so stop once we have fetched
    # as many posts as the category holds.
    total = context.fetch_json(urljoin(context.data_url, "categories/468"))["count"]
    assert total is not None
    fetched = 0
    page = 1
    while fetched < total:
        posts = context.fetch_json(context.data_url, params={"page": page})
        for post in posts:
            crawl_post(context, post)
        fetched += len(posts)
        page += 1
