import html
import math
from typing import Any
from urllib.parse import urljoin, urlparse

from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.stateful.review import assert_all_accepted

from zavod import Context, Entity
from zavod import helpers as h


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    record: dict[str, Any],
) -> None:
    # e.g. "N&rsquo;NANG NTSEME Alexis"
    name = html.unescape(record.pop("title")["rendered"])

    person = context.make("Person")
    person.id = context.make_slug(str(record.pop("id")))
    h.apply_reviewed_name_string(context, person, string=name, llm_cleaning=True)
    # The API returns http:// links; rebuild them on the https data_url host.
    link = urlparse(record.pop("link"))
    if link.netloc != urlparse(context.data_url).netloc:
        raise ValueError(f"Unexpected deputy link: {link.geturl()}")
    person.add("sourceUrl", urljoin(context.data_url, link.path))
    # Candidates must be Gabonese by origin: Organic Law 001/2025 on the Electoral
    # Code, Art. 188.
    person.add("citizenship", "ga")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Gabon",
        wikidata_id="Q19798976",
        country="ga",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    terms = zyte_api.fetch_json(
        context,
        f"{context.data_url}legislatures?per_page=100",
        geolocation="ga",
        cache_days=14,
    )
    # The terms carry no dates or order, so the sitting one is the only term with
    # members. Two populated terms (a new one added before the old one is emptied, or
    # the Transition roster filled in) need a maintainer to decide how to date them.
    populated = [term for term in terms if term["count"] > 0]
    if len(populated) != 1:
        raise ValueError(f"Expected one populated legislature: {populated}")
    legislature = populated[0]

    # 100 is the WordPress REST API's maximum page size.
    for page in range(1, math.ceil(legislature["count"] / 100) + 1):
        url = (
            f"{context.data_url}depute?legislatures={legislature['id']}"
            f"&per_page=100&page={page}"
        )
        for record in zyte_api.fetch_json(
            context, url, geolocation="ga", cache_days=14
        ):
            crawl_member(context, position, categorisation, record)

    assert_all_accepted(context, raise_on_unaccepted=False)
