import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

from followthemoney import registry
from lxml.etree import _Element as Element
from normality import squash_spaces
from zavod.entity import Entity

from zavod import Context
from zavod import helpers as h

BASE_URL = "https://matal.mod.gov.il"
# entityType values of the site's search endpoint
ORGANIZATIONS = "1"
OPERATIVES = "0"
SPLITS = [";", "Id Number", "a) ", "b) ", "c) ", " :", "\n", "• "]
HEBREW = re.compile(r"[\u0590-\u05FF]")
ARABIC = re.compile(r"[\u0600-\u06FF]")
# The CMS appends " (1)" etc. to make page names unique
DEDUPE_SUFFIX = re.compile(r"\s*\(\d+\)$")
# e.g. 'the source states only the year ("2010"), so the day shown is a placeholder.'
PARTIAL_DATE = re.compile(
    r'^the source states only the (?:year|month and year) \("([^"]+)"\)'
)
# Shown when the source has no date; a comment then explains the placeholder
PLACEHOLDER_DATE = "9 September 9999"
CELL = (
    "contains(concat(' ', normalize-space(@class), ' '), ' terror-card__table-cell ')"
)
LABEL = "contains(@class, 'terror-card__table-cell--gray')"
VALUE = "contains(@class, 'terror-card__table-cell--black')"
KNOWN_TAGS = {"Terror Organization", "Designation Cancelled"}
ALIAS_LANGS = {"English": "eng", "Hebrew": "heb", "Arabic": "ara"}


@dataclass
class ListItem:
    guid: str
    url: str
    designation_number: str | None


@dataclass
class Page:
    url: str
    name_en: str | None
    name_he: str | None
    name_ar: str | None
    aliases: list[tuple[str, str]]
    # (address text, notes such as "Gaza Strip")
    addresses: list[tuple[str, list[str]]]
    cells: dict[str, list[str]]
    comments: dict[str, list[str]]
    notes: list[str]
    related_urls: list[str]


@dataclass
class Links:
    # Organization page path -> entity ID
    organizations: dict[str, str] = field(default_factory=dict)
    # Organization entity ID -> designation number
    numbers: dict[str, str] = field(default_factory=dict)
    # (entity ID, related organization page path)
    related: list[tuple[str, str]] = field(default_factory=list)


def page_path(url: str) -> str:
    return urlparse(url).path


def clean_name(name: str | None) -> str | None:
    if name is None:
        return None
    name = DEDUPE_SUFFIX.sub("", name).strip()
    return name or None


def crawl_listing(context: Context, entity_type: str) -> list[ListItem]:
    items: list[ListItem] = []
    page = 1
    total: int | None = None
    while total is None or len(items) < total:
        doc = context.fetch_html(
            f"{BASE_URL}/sanctions/search",
            params={"entityType": entity_type, "page": page},
            headers={"culture": "en"},
            absolute_links=True,
        )
        if total is None:
            total = int(h.xpath_string(doc, "//*[@data-results]/@data-results"))
        rows = h.xpath_elements(
            doc, "//div[contains(@class, 'sanctions-table__list-item--counter')]"
        )
        if len(rows) == 0:
            break
        for row in rows:
            columns: dict[str, str] = {}
            for col in h.xpath_elements(
                row, ".//div[./div[contains(@class, 'list-item-col-title')]]"
            ):
                title = h.xpath_element(col, "./div[contains(@class, 'col-title')]")
                value = h.xpath_elements(col, "./div[contains(@class, 'col-value')]")
                columns[h.element_text(title)] = h.element_text(
                    value[0] if value else None
                )
            url = h.xpath_string(
                row, ".//a[contains(@class, 'list-item-col-link')]/@href"
            )
            result_id = h.xpath_string(row, ".//*[starts-with(@id, 'sanction-')]/@id")
            guid = result_id.rsplit("-", 1)[-1]
            number = columns.get("Notification of Designation No.") or None
            items.append(ListItem(guid=guid, url=url, designation_number=number))
        page += 1
    if total != len(items):
        context.log.warning(
            "Listing count mismatch",
            entity_type=entity_type,
            total=total,
            seen=len(items),
        )
    return items


def element_lines(el: Element) -> str:
    """Element text with whitespace squashed per line, keeping line breaks."""
    lines = (
        squash_spaces(line) for line in h.element_text(el, squash=False).splitlines()
    )
    return "\n".join(line for line in lines if line != "")


def parse_value(value: Element) -> list[str]:
    items = h.xpath_elements(value, ".//*[@data-item]")
    if len(items) == 0:
        items = h.xpath_elements(value, "./span")
    if len(items) > 0:
        return [h.element_text(item) for item in items]
    return [element_lines(value)]


def parse_comments(value: Element) -> tuple[dict[str, list[str]], list[str]]:
    comments: dict[str, list[str]] = {}
    notes: list[str] = []
    paragraphs = h.xpath_elements(value, "./p")
    if len(paragraphs) == 0:
        notes.append(h.element_text(value))
    for para in paragraphs:
        labels = h.xpath_elements(para, "./strong")
        text = h.element_text(para)
        if len(labels) == 0:
            notes.append(text)
            continue
        label = h.element_text(labels[0])
        if not text.startswith(label):
            notes.append(text)
            continue
        key = label.rstrip(":").strip()
        comments.setdefault(key, []).append(text[len(label) :].strip())
    return comments, notes


def parse_page(context: Context, item: ListItem) -> Page | None:
    url = item.url
    doc = context.fetch_html(url, cache_days=1, absolute_links=True)
    page_guid = h.xpath_string(
        doc, "//form[@id='exportPopup-form']/input[@name='Id']/@value"
    )
    if page_guid.replace("-", "") != item.guid:
        # The CMS gives pages with the same name the same URL, so a listing entry
        # can link to another entity's page.
        context.log.warning(
            "Listing links to the page of another entity",
            url=url,
            listing_guid=item.guid,
            page_guid=page_guid,
            designation_number=item.designation_number,
        )
        return None
    tags = {
        h.element_text(tag)
        for tag in h.xpath_elements(
            doc, "//div[contains(@class, 'terror-intro__tag-item')]"
        )
    }
    if not tags.issubset(KNOWN_TAGS):
        context.log.warning("Unknown page tags", url=url, tags=tags - KNOWN_TAGS)

    titles = h.xpath_elements(doc, "//*[contains(@class, 'terror-intro__title-text')]")
    if len(titles) != 1:
        # Don't keep e.g. a bot-protection interstitial in the cache
        context.clear_url(url)
        snippet = h.element_text(doc)[:300]
        raise ValueError(f"Expected one title, got {len(titles)}: {url} {snippet!r}")
    title = titles[0]
    intro = h.xpath_element(
        doc, "//div[contains(@class, 'terror-intro__content-text')]"
    )
    other_names = h.element_text(intro).split(" | ")
    if len(other_names) != 2:
        context.log.warning("Unexpected name header", url=url, names=other_names)
        other_names = [other_names[0], ""]
    name_en = clean_name(h.element_text(title))
    name_he = clean_name(other_names[0])
    name_ar = clean_name(other_names[1])
    # The site falls back to the Hebrew name when no other language is given
    if name_en == name_he and HEBREW.search(name_he or "") is not None:
        name_en = None
    if name_ar in (name_en, name_he) and ARABIC.search(name_ar or "") is None:
        name_ar = None

    page = Page(
        url=url,
        name_en=name_en,
        name_he=name_he,
        name_ar=name_ar,
        aliases=[],
        addresses=[],
        cells={},
        comments={},
        notes=[],
        related_urls=[],
    )

    for card in h.xpath_elements(doc, "//div[contains(@class, 'card--bg-white')]"):
        card_title = h.element_text(
            h.xpath_element(
                card,
                "./div[contains(@class, 'card__heading')]"
                "/div[contains(@class, 'card__title')]",
            )
        )
        if card_title == "Alternative Names":
            headings = h.xpath_elements(
                card, ".//div[contains(@class, 'table-row--heading')]/div"
            )
            langs = [ALIAS_LANGS[h.element_text(c)] for c in headings]
            for row in h.xpath_elements(
                card,
                ".//div[contains(@class, 'terror-card__table-row') "
                "and not(contains(@class, 'table-row--heading'))]",
            ):
                cells = h.xpath_elements(row, "./div")
                if len(cells) != len(langs):
                    context.log.warning("Alias row length mismatch", url=url)
                    continue
                for lang, cell in zip(langs, cells):
                    alias = h.element_text(cell)
                    if alias != "":
                        page.aliases.append((lang, alias))
            continue
        if card_title == "Related Organizations":
            for href in h.xpath_strings(
                card, ".//a[contains(@class, 'terror-card__social-item')]/@href"
            ):
                page.related_urls.append(page_path(href))
            continue

        for cell in h.xpath_elements(
            card, f".//div[{CELL}][./div[{LABEL}]][./div[{VALUE}]]"
        ):
            labels = h.xpath_elements(cell, f"./div[{LABEL}]")
            label = h.element_text(labels[0])
            value = h.xpath_element(cell, f"./div[{VALUE}]")
            if label == "Addresses":
                for span in h.xpath_elements(value, "./span"):
                    text = squash_spaces(span.text or "").rstrip(",").strip()
                    notes = [h.element_text(p) for p in h.xpath_elements(span, "./p")]
                    page.addresses.append((text, notes))
                continue
            if label == "Comments":
                comments, notes = parse_comments(value)
                for key, values in comments.items():
                    page.comments.setdefault(key, []).extend(values)
                page.notes.extend(notes)
                continue
            page.cells.setdefault(label, []).extend(parse_value(value))
            # ID documents carry a secondary line such as "Nationality: Russia"
            for extra in labels[1:]:
                text = h.element_text(extra, squash=False).strip()
                key, sep, extra_value = text.partition(":")
                if sep == "":
                    context.log.warning(
                        "Unexpected cell annotation", url=url, text=text
                    )
                    continue
                page.cells.setdefault(key.strip(), []).append(extra_value.strip())
    return page


def is_country(context: Context, value: str) -> bool:
    if context.lookup("type.country", value) is not None:
        return True
    return registry.country.clean(value) is not None


def apply_partial_date(
    context: Context,
    entity: Entity,
    prop: str,
    dates: list[str],
    comments: list[str] | None,
) -> None:
    """Apply dates, or the partial date given in a comment saying the day is a placeholder."""
    if comments is None:
        for date in dates:
            h.apply_date(entity, prop, date)
        return
    for comment in comments:
        match = PARTIAL_DATE.match(comment)
        if match is None:
            context.log.warning("Unexpected date comment", prop=prop, comment=comment)
            continue
        h.apply_date(entity, prop, match.group(1))


def apply_designation(context: Context, page: Page, sanction: Entity) -> None:
    cells = page.cells
    for key in ("Designation date", "Cancellation date"):
        for comment in page.comments.pop(key, []):
            if "9/9/9999 is a placeholder" not in comment:
                context.log.warning("Unexpected date comment", key=key, comment=comment)
    # Cancelled without a date given, e.g. "הכרזה בוטלה" (designation cancelled)
    for comment in page.comments.pop("Cancellation", []):
        if comment != "הכרזה בוטלה":
            context.log.warning("Unexpected cancellation comment", comment=comment)
    for key, prop in (
        ("Temporary Designation Date", "startDate"),
        ("Permanent Designation Date", "startDate"),
        ("Cancel Date", "endDate"),
    ):
        for value in cells.pop(key, []):
            if value != PLACEHOLDER_DATE:
                h.apply_date(sanction, prop, value)
    for value in page.comments.pop("West Bank designation date", []):
        h.apply_date(sanction, "startDate", value)
    sanction.add("authority", cells.pop("Local Designating Authority", None))
    sanction.add("publisher", cells.pop("Official Gazette Details", None))
    sanction.add("program", page.comments.pop("Designation type", None))
    sanction.add("recordId", cells.pop("Foreign Listing Number", None))
    sanction.add("sourceUrl", page.url)


def emit_key_operatives(context: Context, entity: Entity, operatives: str) -> None:
    res = context.lookup("key_operatives", operatives)
    if res is None:
        context.log.warning("Unhandled key_operatives", value=operatives)
        return
    for item in res.operatives:
        item = dict(item)
        operative = context.make(item.pop("schema", "LegalEntity"))
        operative.id = context.make_id(
            entity.id, item["name"], item.get("country", None)
        )
        for key, value in item.items():
            operative.add(key, value)
        rel = context.make("UnknownLink")
        rel.id = context.make_id(entity.id, operative.id)
        rel.add("subject", entity.id)
        rel.add("object", operative.id)
        rel.add("role", "Key operative")
        context.emit(operative)
        context.emit(rel)


def crawl_organization(context: Context, item: ListItem, links: Links) -> None:
    page = parse_page(context, item)
    if page is None:
        return
    entity = context.make("Organization")
    entity.id = context.make_id(page.name_en, page.name_he)
    if entity.id is None:
        context.log.warning("Organization without name", url=item.url)
        return
    links.organizations[page_path(item.url)] = entity.id
    if item.designation_number is not None:
        links.numbers[entity.id] = item.designation_number
    for related in page.related_urls:
        links.related.append((entity.id, related))

    entity.add("name", page.name_en, lang="eng")
    entity.add("name", page.name_he, lang="heb")
    entity.add("alias", h.multi_split(page.name_ar, SPLITS), lang="ara")
    for lang, alias in page.aliases:
        entity.add("alias", h.multi_split(alias, SPLITS), lang=lang)
    entity.add("topics", "crime.terror")
    entity.add("notes", page.notes)
    entity.add("sourceUrl", page.url)

    cells = page.cells
    entity.add("legalForm", cells.pop("Corporation Type", None))
    entity.add("registrationNumber", cells.pop("Corporation ID", None))
    entity.add("jurisdiction", cells.pop("Forming Location", None))
    apply_partial_date(
        context,
        entity,
        "incorporationDate",
        cells.pop("Corporation Date", []),
        page.comments.pop("Date of incorporation", None),
    )
    entity.add("phone", cells.pop("Phone Numbers", None))
    entity.add("email", cells.pop("Emails", None))
    entity.add("website", cells.pop("Websites", None))
    for text, notes in page.addresses:
        # Rendered as "{country}, {city}, {street}" with empty parts left out; the
        # street can contain commas, so only the country is taken from the parts.
        first_part = text.split(",")[0].strip()
        country = first_part if is_country(context, first_part) else None
        for note in notes:
            if not is_country(context, note):
                context.log.warning("Unexpected address note", url=page.url, note=note)
            elif country is None:
                country = note
        full = ", ".join([text, *notes])
        address = h.make_address(context, full=full, country=country)
        h.apply_address(context, entity, address)
        entity.add("country", country)

    sanction = h.make_sanction(context, entity)
    sanction.add("recordId", item.designation_number)
    sanction.add("reason", cells.pop("Justification", None))
    apply_designation(context, page, sanction)

    for operatives in page.comments.pop("Key operatives", []):
        emit_key_operatives(context, entity, operatives)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(cells, ignore=["Foreign Designator", "Foreign Designation Date"])
    context.audit_data(page.comments)


def crawl_operative(context: Context, item: ListItem, links: Links) -> None:
    page = parse_page(context, item)
    if page is None:
        return
    entity = context.make("Person")
    entity.id = context.make_id(page.name_en, page.name_he, page.name_ar)
    if entity.id is None:
        context.log.warning("Operative without name", url=item.url)
        return
    for related in page.related_urls:
        links.related.append((entity.id, related))

    entity.add("name", page.name_en, lang="eng")
    entity.add("name", page.name_he, lang="heb")
    entity.add("name", page.name_ar, lang="ara")
    for lang, alias in page.aliases:
        entity.add("alias", h.multi_split(alias, SPLITS), lang=lang)
    entity.add("topics", "crime.terror")
    entity.add("notes", page.notes)
    entity.add("sourceUrl", page.url)

    cells = page.cells
    apply_partial_date(
        context,
        entity,
        "birthDate",
        cells.pop("Dates Of Birth", []),
        page.comments.pop("Date of birth", None),
    )
    for nationality in cells.pop("Nationality", []):
        entity.add("nationality", h.multi_split(nationality, ["\n"]))
    for label in ("Passport", "ID", "Other"):
        for value in cells.pop(label, []):
            if value.lower() == "unknown":
                continue
            entity.add("idNumber", h.multi_split(value, SPLITS))
    entity.add("phone", cells.pop("Phone Numbers", None))
    entity.add("email", cells.pop("Emails", None))
    entity.add("notes", page.comments.pop("Additional information", None))

    sanction = h.make_sanction(context, entity)
    sanction.add("program", cells.pop("Justification", None))
    sanction.add("program", cells.pop("Foreign Designator", None))
    apply_designation(context, page, sanction)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(cells, ignore=["Foreign Designation Date"])
    context.audit_data(page.comments)


def emit_links(context: Context, links: Links) -> None:
    seen: set[tuple[str, str]] = set()
    for entity_id, related_path in links.related:
        related_id = links.organizations.get(related_path)
        if related_id is None:
            context.log.info("Related organization not listed", url=related_path)
            continue
        if entity_id == related_id:
            continue
        own_number = links.numbers.get(entity_id)
        related_number = links.numbers.get(related_id)
        if own_number is not None and related_number is not None:
            # Keep the subject/object order of the old spreadsheet-based crawler,
            # which ordered by (string) designation number, so link IDs stay stable.
            if max(own_number, related_number) == own_number:
                subject_id, object_id = entity_id, related_id
            else:
                subject_id, object_id = related_id, entity_id
        else:
            subject_id, object_id = entity_id, related_id
        if (subject_id, object_id) in seen:
            continue
        seen.add((subject_id, object_id))
        link = context.make("UnknownLink")
        link.id = context.make_id(subject_id, object_id)
        link.add("subject", subject_id)
        link.add("object", object_id)
        context.emit(link)


def crawl(context: Context) -> None:
    links = Links()
    for item in crawl_listing(context, ORGANIZATIONS):
        crawl_organization(context, item, links)
    for item in crawl_listing(context, OPERATIVES):
        crawl_operative(context, item, links)
    emit_links(context, links)
