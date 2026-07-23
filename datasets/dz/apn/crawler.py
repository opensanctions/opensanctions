import re

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

# The APN site is a React single-page app behind a WAF; the deputies list only appears
# after client-side rendering, so it is fetched through the Zyte API (browser rendering)
# with an Algerian exit.
GEOLOCATION = "dz"

# Each deputy links to a detail page "/<id>-<slug>", e.g. "/1302-guend-nabil". This is
# the stable anchor we key on; the slug carries the transliterated name.
DEPUTY_HREF_RE = re.compile(r"^/?(\d+)-([a-z][a-z0-9-]+)$")

# Validator: the rendered page must contain at least one deputy-style link.
UNBLOCK_VALIDATOR = './/a[contains(@href, "-")]'


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the People's National Assembly of Algeria",
        country="dz",
        wikidata_id="Q21290886",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=UNBLOCK_VALIDATOR,
        geolocation=GEOLOCATION,
        cache_days=1,
    )

    seen: set[str] = set()
    for link in h.xpath_elements(doc, "//a[@href]"):
        href = link.get("href")
        if href is None:
            continue
        match = DEPUTY_HREF_RE.match(href)
        if match is None:
            continue
        deputy_id, slug = match.group(1), match.group(2)
        if deputy_id in seen:
            continue
        seen.add(deputy_id)

        # Prefer the rendered link text (native spelling); fall back to the slug.
        link_text = h.element_text(link)
        name = link_text if link_text else slug.replace("-", " ").title()

        person = context.make("Person")
        person.id = context.make_slug(deputy_id)
        person.add("name", name)
        person.add("sourceUrl", f"https://www.apn.dz/{deputy_id}-{slug}")
        # A candidate for the APN must be of Algerian nationality (Organic Law 21-01 on
        # the electoral regime, Article 200; 2020 Constitution Article 128).
        # https://cour-constitutionnelle.dz/wp-content/uploads/2023/02/loi%20-electFR.pdf
        person.add("citizenship", "dz")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)

    if not seen:
        raise ValueError("No deputy links found on the APN deputies page")
