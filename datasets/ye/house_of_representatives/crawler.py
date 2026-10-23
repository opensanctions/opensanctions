from typing import Any

from lxml import html
from normality import squash_spaces
from zavod.stateful.positions import categorise

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
        return
    doc = html.fromstring(post["content"]["rendered"])
    table = h.xpath_element(doc, ".//table")
    row: dict[str, str | None] = {}
    for tr in h.xpath_elements(table, ".//tr"):
        cells = [h.element_text(td) for td in h.xpath_elements(tr, "./td")]
        assert len(cells) in (1, 2), cells
        # A profile with a blank field omits its value cell.
        row[cells[0]] = cells[1] if len(cells) == 2 else None

    person = context.make("Person")
    person.id = context.make_slug(str(post["id"]))
    name = row.pop("إسم العضو:")
    person.add("name", name)
    # The post title is sometimes a shorter form of the name.
    title = squash_spaces(post["title"]["rendered"])
    if title != name:
        person.add("alias", title)
    person.add("political", row.pop("الكتلة السياسية:"))
    # The Speaker's profile has no education row.
    person.add("education", row.pop("المؤهلات العلمية:", None))
    person.add("sourceUrl", post["link"])
    # Representatives must be Yemeni: Constitution of Yemen, Art. 64(2).
    # https://www.constituteproject.org/constitution/Yemen_2015
    person.add("citizenship", "ye")

    number = row.pop("الدائرة الإنتخابية:")
    governorate = row.pop("المحافظة:")
    if number is None:
        constituency = governorate
    else:
        constituency = f"الدائرة {number} - {governorate}"

    res = context.lookup("position", row.pop("الصفة في اللجنة:"))
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
        ignore=["اللجنة المشارك فيها:"],
    )


def crawl(context: Context) -> None:
    page = 1
    while True:
        # WordPress rejects a page past the last one, so read the page count from
        # the response headers rather than waiting for a short page.
        response = context.fetch_response(f"{context.data_url}&page={page}")
        for post in response.json():
            crawl_post(context, post)
        if page >= int(response.headers["X-WP-TotalPages"]):
            break
        page += 1
