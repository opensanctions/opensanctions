import re
from typing import cast
from urllib.parse import urlencode
from uuid import uuid4

from lxml import html
from zavod.extract import zyte_api
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)
from zavod.util import Element, LangText

from zavod import Context, Entity
from zavod import helpers as h

# d/m/Y (also with backslashes), Y/m/d, "d <month> Y", or only the year.
BIRTH_DATE = re.compile(
    r"\d{1,2}[/\\]\d{1,2}[/\\]\d{4}|\d{4}/\d{1,2}/\d{1,2}|\d{1,2} \D+? \d{4}|\d{4}"
)


def fetch_pages(context: Context) -> list[Element]:
    """Page through the ASP.NET listing by replaying the pager's postbacks."""
    # Geo-fenced to Jordan. The postbacks need the session cookie set on the first
    # page, so all pages go through one Zyte session.
    session_id = str(uuid4())
    request = zyte_api.ZyteAPIRequest(
        url=context.data_url, geolocation="JO", session_id=session_id
    )
    docs: list[Element] = []
    while True:
        result = zyte_api.fetch(context, request)
        assert result.status_code == 200, result.status_code
        doc = html.fromstring(result.response_text)
        cast(html.HtmlElement, doc).make_links_absolute(context.data_url)
        docs.append(doc)
        pager = h.xpath_element(doc, ".//span[@id='pagerList']")
        # "صفحة 3 من 8": page 3 of 8
        nav = h.element_text(h.xpath_element(pager, "./ancestor::nav[1]"))
        match = re.search(r"صفحة (\d+) من (\d+)", nav)
        assert match is not None and int(match.group(1)) == len(docs), nav
        if len(docs) == int(match.group(2)):
            return docs
        # The next page's number, or "..." for the next block of pages.
        href = h.xpath_string(
            pager, "./span[@class='CurrentPageClass']/following-sibling::a[1]/@href"
        )
        target = re.fullmatch(r"javascript:__doPostBack\('([^']+)',''\)", href)
        assert target is not None, href
        form = {
            el.get("name", ""): el.get("value", "")
            for el in h.xpath_elements(doc, ".//form//input[@type='hidden']")
        }
        form["__EVENTTARGET"] = target.group(1)
        request = zyte_api.ZyteAPIRequest(
            url=context.data_url,
            method="POST",
            body=urlencode(form).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            geolocation="JO",
            session_id=session_id,
        )


def clean_name(context: Context, raw_name: str) -> str:
    # Quotes only group the parts of a compound name; "باشا" can follow the given name.
    name = h.strip_name_titles(context, re.sub(r'["“”]|\bباشا\b', " ", raw_name))
    assert name is not None, raw_name
    return name


def name_key(name: str) -> str:
    # "عبد الكريم" is also written "عبدالكريم".
    return name.replace(" ", "")


def note_key(context: Context, raw_name: str) -> str:
    name = clean_name(context, raw_name)
    # The note can spell a name differently from the member's card.
    return name_key(context.lookup_value("note_names", name) or name)


def parse_note(
    context: Context, doc: Element
) -> tuple[dict[str, tuple[str, str]], dict[str, str]]:
    """Read the note on the decrees since the one the listing publishes. Returns the
    appointments, as (name, start date), and the resignation dates, by name key."""
    note = h.xpath_element(doc, ".//p[contains(., 'التعديلات التي طرأت على المجلس')]")
    appointments: dict[str, tuple[str, str]] = {}
    resignations: dict[str, str] = {}
    headings = decrees = 0
    for el in [note, *h.xpath_elements(note, "following-sibling::*")]:
        text = h.element_text(el).removeprefix("* التعديلات التي طرأت على المجلس: ")
        # Each decree is a numbered heading, a preamble and the decree's text.
        if text == "" or re.match(r"نحن عبد ?الله الثاني ", text):
            continue
        if re.match(r"\d+\. إرادة ملكية ب", text):
            headings += 1
            continue
        decrees += 1
        appointment = re.fullmatch(
            r"[يت]عين (.+) عضو(?:اً?|ين) في مجلس الأعيان،? اعتباراً? من تاريخ"
            r" (\d{4}/\d{1,2}/\d{1,2})\.",
            text,
        )
        resignation = re.fullmatch(
            r"تقبل استقالة (.+) من عضوية مجلس الأعيان،? اعتباراً? من تاريخ"
            r" (\d{4}/\d{1,2}/\d{1,2})\.",
            text,
        )
        if appointment is not None:
            # "يعين دولة … وسعادة …" appoints two members.
            for name in re.split(r" و(?=سعادة |معالي |دولة )", appointment.group(1)):
                appointments[note_key(context, name)] = (name, appointment.group(2))
        elif resignation is not None:
            resignations[note_key(context, resignation.group(1))] = resignation.group(2)
        else:
            raise ValueError(f"Unexpected paragraph in the note: {text!r}")
    assert headings == decrees, (headings, decrees)
    return appointments, resignations


def crawl_cv(context: Context, person: Entity, name: str, url: str) -> None:
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=".//div[@id='MainContent_DivContent']",
        html_source="httpResponseBody",
        geolocation="JO",
        cache_days=7,
    )
    person.add("sourceUrl", url)
    content = h.xpath_element(doc, ".//div[@id='MainContent_DivContent']")
    # A CV heading such as "مكان وتاريخ الولادة:" is followed by its value in the
    # next element with text.
    fields: dict[str, str] = {}
    for el in h.xpath_elements(content, ".//*"):
        label = h.element_text(el)
        field = context.lookup_value("cv_labels", label)
        if field is None or h.element_text(el.getparent()) == label:
            continue
        values = [h.element_text(v) for v in h.xpath_elements(el, "following::*")]
        assert field not in fields, (url, label)
        fields[field] = next(v for v in values if v != "")

    # "الاسم والشهرة": the name the member is commonly known by.
    if "alias" in fields:
        person.add("gender", context.lookup_value("gender", fields["alias"]))
        alias = clean_name(context, fields["alias"])
        if alias != name:
            person.add("alias", alias)
    person.add("birthPlace", fields.get("birthPlace"))
    birth = fields.get("birth")
    if birth is None and "birthPlace" not in fields:
        if re.search("الولادة|الميلاد", h.element_text(content)):
            context.log.warning("Birth details under an unknown heading", url=url)
    res = context.lookup("birth", birth)
    if res is not None:
        birth = res.value
    if birth is not None:
        # "عمان – 1/7/1966", "20 كانون الثاني 1939 - السلط", "السلط 1953"
        dates = BIRTH_DATE.findall(birth)
        if len(dates) != 1:
            context.log.warning("Cannot parse place and date of birth", value=birth)
        else:
            h.apply_date(person, "birthDate", dates[0].replace("\\", "/"))
            place = birth.replace(dates[0], "").strip(" ,،.-–/")
            person.add("birthPlace", place or None)

    for email in h.xpath_strings(content, ".//a[starts-with(@href, 'mailto:')]/@href"):
        person.add("email", email.removeprefix("mailto:"))
    # Some give the address only as text: "البريد الإلكتروني: x@senate.jo".
    for para in h.xpath_elements(content, ".//p"):
        text = h.element_text(para)
        match = re.fullmatch(r"البريد ال[إا]لكتروني ?: ?(\S+@\S+)", text)
        if match is not None:
            person.add("email", match.group(1))


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    person_id: str | None,
    raw_name: str,
    url: str | None,
    start_date: str | None,
    end_date: str | None,
    status: OccupancyStatus | None,
) -> None:
    person = context.make("Person")
    person.id = person_id
    name = clean_name(context, raw_name)
    person.add("name", name, original_value=raw_name)
    apply_translit_full_name(context, person, LangText(name, "ara"))
    person.add("gender", context.lookup_value("gender", raw_name))
    # Senators must be Jordanian: Constitution of Jordan, Art. 75(1)(a).
    # https://www.constituteproject.org/constitution/Jordan_2016
    person.add("citizenship", "jo")
    if url is not None:
        crawl_cv(context, person, name, url)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=end_date,
        status=status,
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Jordan",
        wikidata_id="Q20081407",
        country="jo",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # The whole listing first: slow member pages in between would end the session.
    docs = fetch_pages(context)
    appointments, resignations = parse_note(context, docs[0])
    for doc in docs:
        cards = h.xpath_elements(doc, ".//div[@id='divHeight']")
        assert len(cards) > 0, "No member cards on a listing page"
        for card in cards:
            url = h.xpath_string(card, ".//a[@id='lnkMore']/@href")
            label = h.element_text(h.xpath_element(card, ".//h6"))
            raw_name = label.lstrip("* ")
            key = name_key(clean_name(context, raw_name))
            # Members who have left are starred. The note dates resignations, but
            # not deaths.
            starred = label.startswith("*")
            end_date = resignations.pop(key, None)
            assert end_date is None or starred, (url, end_date)
            appointment = appointments.pop(key, None)
            crawl_member(
                context,
                position,
                categorisation,
                # The item number in /ListDetails/<slug>/<list>/<item> is only
                # unique within the list.
                context.make_id(*url.rsplit("/", 2)[1:]),
                raw_name,
                url,
                start_date=None if appointment is None else appointment[1],
                end_date=end_date,
                status=OccupancyStatus.ENDED if starred and end_date is None else None,
            )
    # Members appointed by a later decree who have no card yet.
    for key, (raw_name, start_date) in appointments.items():
        crawl_member(
            context,
            position,
            categorisation,
            context.make_id(raw_name),
            raw_name,
            None,
            start_date=start_date,
            end_date=resignations.pop(key, None),
            status=None,
        )
    # Every resignation ends a starred member's or an appointee's occupancy.
    assert len(resignations) == 0, resignations
