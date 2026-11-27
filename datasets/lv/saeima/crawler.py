from datetime import datetime
from urllib.parse import urljoin

from lxml.etree import _Element as Element

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


def parse_mandate(
    context: Context, member_doc: Element, status: str, name: str
) -> tuple[str | None, str | None]:
    """Return the (start, end) dates of the deputy's mandate from its history."""
    starts: list[str] = []
    ends: list[str] = []
    for row in h.xpath_elements(
        member_doc, "//div[contains(@class, 'dep-mandate__history')]/div"
    ):
        date_str = h.element_text(
            h.xpath_element(row, ".//span[contains(@class, 'field--datums')]/span[2]")
        )
        # e.g. "03.11.2026."
        date = datetime.strptime(date_str, "%d.%m.%Y.").date().isoformat()
        event = h.element_text(
            h.xpath_element(row, ".//span[contains(@class, 'field--status')]/span[2]")
        )
        # e.g. "Apstiprināts Saeimas deputāta mandāts" / "Nolicis Saeimas deputāta mandātu"
        if event.startswith("Apstiprināts Saeimas deputāt"):
            starts.append(date)
        elif event.startswith("Nolicis Saeimas deputāt") or event.startswith(
            "Nolikusi Saeimas deputāt"
        ):
            ends.append(date)
        else:
            context.log.warning("Unknown mandate event", name=name, mandate_event=event)

    start_date = min(starts) if len(starts) > 0 else None
    if status == "current":
        return start_date, None
    if status == "previous":
        if len(ends) == 0:
            context.log.warning("Previous deputy without mandate end date", name=name)
            return start_date, None
        return start_date, max(ends)
    raise ValueError(f"Unknown deputy status: {status!r} ({name})")


def crawl_item(context: Context, member_url: str, status: str) -> None:
    member_doc = context.fetch_html(member_url, encoding="utf-8")

    full_name = h.element_text(
        h.xpath_element(member_doc, "//h1[@id='dep-profile-name']")
    )
    assert full_name != "", member_url

    entity = context.make("Person")
    entity.id = context.make_slug(full_name)
    h.apply_name(entity, full_name)
    # citizenship required (Art 9): https://likumi.lv/ta/en/id/57980-the-constitution-of-the-republic-of-latvia
    entity.add("citizenship", "lv")
    entity.add("sourceUrl", member_url)

    # The e-mail address is obfuscated by storing it reversed. Deputies who have
    # laid down their mandate have no contact block. The deputy's assistant may
    # also have a contact block, so we select the one bearing the deputy's name.
    for contact in h.xpath_elements(
        member_doc, "//section[contains(@class, 'strv-contact--person')]"
    ):
        contact_name = h.element_text(
            h.xpath_element(contact, ".//p[@class='strv-contact__name']")
        )
        if contact_name != full_name:
            continue
        for email in h.xpath_strings(contact, ".//a/@data-eo-href"):
            entity.add("email", email[::-1])

    start_date, end_date = parse_mandate(context, member_doc, status, full_name)

    position = h.make_position(
        context,
        "Member of the Saeima of Latvia",
        country="lv",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21191589",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        True,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )

    if occupancy is None:
        return

    context.emit(entity)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    assert context.dataset.url is not None
    overview_page = context.fetch_html(context.dataset.url, encoding="utf-8")
    # Check if the overview page still links to the 15th Saeima deputies.
    if (
        len(
            h.xpath_elements(
                overview_page, "//a[normalize-space(.)='15. Saeimas deputāti']"
            )
        )
        == 0
    ):
        context.log.warning(
            "Hardcoded term is no longer current on the overview page, they probably elected a new Saeima.",
        )

    deputies_list_page = context.fetch_html(context.data_url, encoding="utf-8")
    # The page renders the deputies both as a list and as a gallery; use the list.
    items = h.xpath_elements(
        deputies_list_page, "//ul[contains(@class, 'dep-card-list')]/li"
    )
    for item in items:
        status = item.get("data-status")
        assert status is not None
        href = h.xpath_string(item, ".//a[@class='dep-card__name']/@href")
        crawl_item(context, urljoin(context.data_url, href), status)
