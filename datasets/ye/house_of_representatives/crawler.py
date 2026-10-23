from typing import Any
from urllib.parse import urljoin

from lxml import html
from normality import squash_spaces
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import categorise
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


def crawl_post(context: Context, post: dict[str, Any]) -> None:
    if NEWS_CATEGORY in post["categories"]:
        print(post)
        return
    doc = html.fromstring(post["content"]["rendered"])
    table = h.xpath_element(doc, ".//table")
    row: dict[str, str | None] = {}
    for tr in h.xpath_elements(table, ".//tr"):
        cells = [h.element_text(td) for td in h.xpath_elements(tr, "./td")]
        assert len(cells) in (1, 2), cells
        # Map the Arabic labels to English keys; unknown ones fail the audit.
        key = context.lookup_value("columns", cells[0], cells[0]) or cells[0]
        # A profile with a blank field omits its value cell.
        row[key] = cells[1] if len(cells) == 2 else None

    person = context.make("Person")
    person.id = context.make_slug(post["id"])
    name = row.pop("name")
    assert name is not None, post["id"]
    person.add("name", name)
    apply_translit_full_name(context, person, LangText(name, "ara"))
    # The post title is sometimes a shorter form of the name.
    title = squash_spaces(post["title"]["rendered"])
    if title != name:
        person.add("alias", title)
    person.add("political", row.pop("political_bloc"))
    # The Speaker's profile has no education row.
    person.add("education", row.pop("education"))
    person.add("sourceUrl", post["link"])
    # Representatives must be Yemeni: Constitution of Yemen, Art. 64(2).
    # https://www.constituteproject.org/constitution/Yemen_2015
    person.add("citizenship", "ye")

    number = row.pop("constituency")
    governorate = row.pop("governorate")
    if number is None:
        constituency = governorate
    else:
        constituency = f"الدائرة {number} - {governorate}"

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
            # The 2003 general election; the term has been extended since.
            period_start="2003-04-27",
            # The roster still lists members who have died, so a listing does
            # not prove a member is still in office.
            no_end_implies_current=False,
            categorisation=categorisation,
        )
        if occupancy is None:
            continue
        occupancy.add("constituency", constituency)
        context.emit(occupancy)
        context.emit(position)
        context.emit(person)

    context.audit_data(
        row,
        # The committee seat has no FollowTheMoney property.
        ignore=["committee"],
    )


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
