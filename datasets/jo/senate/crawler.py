import re
from typing import cast
from uuid import uuid4
from collections.abc import Iterator
from urllib.parse import urlencode

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

# The date within the free-text place and date of birth: d/m/Y (also written with
# backslashes), Y/m/d, "d <Levantine month> Y", or only the year.
BIRTH_DATE = re.compile(
    r"\d{1,2}[/\\]\d{1,2}[/\\]\d{4}|\d{4}/\d{1,2}/\d{1,2}|\d{1,2} \D+? \d{4}|\d{4}"
)


def fetch_pages(context: Context, url: str) -> Iterator[Element]:
    """Page through an ASP.NET listing by replaying the pager's postback for the
    link after the current page, until the pager reports the last page."""
    # The government network only serves Jordanian IPs. The postbacks are checked
    # against the session cookie, which the site only sets on the first page, so the
    # whole listing goes through one Zyte session (same IP, cookies kept).
    zyte_session = str(uuid4())
    request = zyte_api.ZyteAPIRequest(
        url=url, geolocation="JO", session_id=zyte_session
    )
    page = 1
    while True:
        result = zyte_api.fetch(context, request)
        assert result.status_code == 200, (url, result.status_code)
        doc = html.fromstring(result.response_text)
        cast(html.HtmlElement, doc).make_links_absolute(context.data_url)
        pager = h.xpath_element(doc, ".//span[@id='pagerList']")
        yield doc
        # "صفحة 3 من 8": page 3 of 8
        nav = h.element_text(h.xpath_element(pager, "./ancestor::nav[1]"))
        match = re.search(r"صفحة (\d+) من (\d+)", nav)
        assert match is not None and int(match.group(1)) == page, (page, nav)
        if page == int(match.group(2)):
            return
        # The link after the current page is the next page's number, or "..." for
        # the next block of page numbers.
        link = h.xpath_element(
            pager, "./span[@class='CurrentPageClass']/following-sibling::a[1]"
        )
        assert h.element_text(link) in (str(page + 1), "..."), h.element_text(link)
        target = re.fullmatch(
            r"javascript:__doPostBack\('([^']+)',''\)", link.get("href", "")
        )
        assert target is not None, link.get("href")
        data = {
            el.get("name", ""): el.get("value", "")
            for el in h.xpath_elements(doc, ".//form//input[@type='hidden']")
        }
        data["__EVENTTARGET"] = target.group(1)
        data["__EVENTARGUMENT"] = ""
        request = zyte_api.ZyteAPIRequest(
            url=url,
            method="POST",
            body=urlencode(data).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            geolocation="JO",
            session_id=zyte_session,
        )
        page += 1


def clean_name(context: Context, full_name: str, person: Entity | None = None) -> str:
    """Drop the ranks and titles from a name, which give the person's gender, and
    the quotes that only group the parts of a compound name."""
    tokens = []
    for token in re.sub(r'["“”]', " ", full_name).split():
        res = context.lookup("honorifics", token)
        if res is None:
            tokens.append(token)
        elif person is not None:
            person.add("gender", res.gender)
    return " ".join(tokens)


def name_key(name: str) -> str:
    """Compare names without spaces: "عبد الكريم" is also written "عبدالكريم"."""
    return name.replace(" ", "")


def note_key(context: Context, raw_name: str) -> str:
    """Key a name from the note for matching to a member's card."""
    name = clean_name(context, raw_name)
    # The note can spell a name differently from the member's card.
    res = context.lookup("note_names", name)
    if res is not None:
        name = str(res.value)
    return name_key(name)


def make_person(context: Context, person_id: str, raw_name: str) -> tuple[Entity, str]:
    person = context.make("Person")
    person.id = person_id
    name = clean_name(context, raw_name, person)
    person.add("name", name)
    apply_translit_full_name(context, person, LangText(name, "ara"))
    # Senators must be Jordanian: Constitution of Jordan, Art. 75(1)(a).
    # https://www.constituteproject.org/constitution/Jordan_2016
    person.add("citizenship", "jo")
    return person, name


def parse_note(
    context: Context, doc: Element
) -> tuple[dict[str, tuple[str, str]], dict[str, str]]:
    """Read the listing's note on the changes to the council.

    The listing is the royal decree that appointed the sitting Senate. The note
    after it gives each later decree as a numbered heading, the decree's preamble
    and its text, which appoints members or accepts a resignation from a date.
    Returns the appointments by name key (with the full name and start date) and
    the resignation dates by name key. The original decree itself is not dated."""
    note = h.xpath_element(doc, ".//p[contains(., 'التعديلات التي طرأت على المجلس')]")
    appointments: dict[str, tuple[str, str]] = {}
    resignations: dict[str, str] = {}
    headings = decrees = 0
    # The note's own paragraph starts with the first heading.
    for el in [note, *h.xpath_elements(note, "following-sibling::*")]:
        text = h.element_text(el).removeprefix("* التعديلات التي طرأت على المجلس: ")
        if text == "" or re.match(r"نحن عبد ?الله الثاني ", text):
            continue
        if re.match(r"\d+\. إرادة ملكية ب", text):
            headings += 1
            continue
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
        decrees += 1
        if appointment is not None:
            # One decree can appoint two members: "يعين دولة … وسعادة …".
            for name in re.split(r" و(?=سعادة |معالي |دولة )", appointment.group(1)):
                appointments[note_key(context, name)] = (name, appointment.group(2))
        elif resignation is not None:
            resignations[note_key(context, resignation.group(1))] = resignation.group(2)
        else:
            raise ValueError(f"Unexpected paragraph in the note: {text!r}")
    assert headings == decrees, (headings, decrees)
    return appointments, resignations


def cv_fields(context: Context, doc: Element) -> dict[str, str]:
    """Return the text after each CV heading, such as "مكان وتاريخ الولادة:", by the
    field the cv_labels lookup gives for it."""
    fields: dict[str, str] = {}
    content = h.xpath_element(doc, ".//div[@id='MainContent_DivContent']")
    # A heading is a <h5>, <p> or <div>, sometimes wrapped in <strong> or <span>,
    # and its value is the next element with text.
    for el in h.xpath_elements(content, ".//*"):
        label = h.element_text(el)
        res = context.lookup("cv_labels", label)
        if res is None or h.element_text(el.getparent()) == label:
            continue
        values = h.xpath_elements(el, "following::*")
        value = next(
            (h.element_text(v) for v in values if h.element_text(v) != ""), None
        )
        assert value is not None, label
        assert res.value not in fields, label
        fields[str(res.value)] = value
    return fields


def crawl_member(
    context: Context,
    url: str,
    label: str,
    position: Entity,
    categorisation: PositionCategorisation,
    appointments: dict[str, tuple[str, str]],
    resignations: dict[str, str],
) -> None:
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=".//div[@id='MainContent_DivContent']",
        html_source="httpResponseBody",
        geolocation="JO",
        cache_days=7,
    )

    # The item number in /ListDetails/<slug>/<list id>/<item> is only unique within
    # the list.
    person_id = context.make_id(*url.rsplit("/", 2)[1:])
    assert person_id is not None, url
    person, name = make_person(context, person_id, label.lstrip("* "))
    person.add("sourceUrl", url)
    fields = cv_fields(context, doc)
    # "الاسم والشهرة": the name the senator is commonly known by.
    if "alias" in fields:
        alias = clean_name(context, fields["alias"], person)
        if alias != name:
            person.add("alias", alias)
    birth = fields.get("birth")
    if birth is not None:
        res = context.lookup("birth", birth)
        birth = birth if res is None else res.value
    if birth is not None:
        # "عمان – 1/7/1966", "20 كانون الثاني 1939 - السلط", "السلط 1953": the place
        # and the date come in either order, separated by spaces, commas or dashes.
        dates = BIRTH_DATE.findall(birth)
        if len(dates) != 1 or int(re.findall(r"\d{4}", dates[0])[0]) < 1900:
            context.log.warning(
                "Cannot parse place and date of birth", url=url, value=birth
            )
        else:
            h.apply_date(person, "birthDate", dates[0].replace("\\", "/"))
            place = birth.replace(dates[0], "").strip(" ,،.-–/")
            if place != "":
                person.add("birthPlace", place)
    person.add("birthPlace", fields.get("birthPlace"))
    if "birth" not in fields and "birthPlace" not in fields:
        text = h.element_text(
            h.xpath_element(doc, ".//div[@id='MainContent_DivContent']")
        )
        if "الولادة" in text or "الميلاد" in text:
            context.log.warning("Birth details under an unknown heading", url=url)
    for email in h.xpath_strings(
        doc,
        ".//div[@id='MainContent_DivContent']//a[starts-with(@href, 'mailto:')]/@href",
    ):
        person.add("email", email.removeprefix("mailto:"))
    # Some pages give the address only as text: "البريد الإلكتروني: x@senate.jo".
    for para in h.xpath_elements(doc, ".//div[@id='MainContent_DivContent']//p"):
        match = re.fullmatch(
            r"البريد ال[إا]لكتروني ?: ?(\S+@\S+)", h.element_text(para)
        )
        if match is not None:
            person.add("email", match.group(1))

    # The listing is the decree that appointed the sitting Senate. Members who have
    # since left are marked with an asterisk; the note dates their resignation, but
    # not a death. Members appointed later are named in the note.
    starred = label.startswith("*")
    end_date = resignations.pop(name_key(name), None)
    assert end_date is None or starred, (url, end_date)
    appointment = appointments.pop(name_key(name), None)
    emit_occupancy(
        context,
        person,
        position,
        categorisation,
        start_date=None if appointment is None else appointment[1],
        end_date=end_date,
        status=OccupancyStatus.ENDED if starred and end_date is None else None,
    )


def emit_occupancy(
    context: Context,
    person: Entity,
    position: Entity,
    categorisation: PositionCategorisation,
    start_date: str | None,
    end_date: str | None,
    status: OccupancyStatus | None,
) -> None:
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
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    # Fetch the whole listing before the slow member pages, so the pager's
    # postbacks follow each other closely enough to stay within the session.
    docs = list(fetch_pages(context, context.data_url))
    appointments, resignations = parse_note(context, docs[0])
    for doc in docs:
        cards = h.xpath_elements(doc, ".//div[@id='divHeight']")
        assert len(cards) > 0, "No member cards on a listing page"
        for card in cards:
            crawl_member(
                context,
                h.xpath_string(card, ".//a[@id='lnkMore']/@href"),
                h.element_text(h.xpath_element(card, ".//h6")),
                position,
                categorisation,
                appointments,
                resignations,
            )
    # Appointees who have a card were taken off the list with their card, so the
    # rest only appear in the note.
    for key, (raw_name, start_date) in appointments.items():
        person_id = context.make_id(raw_name)
        assert person_id is not None, raw_name
        person, _ = make_person(context, person_id, raw_name)
        emit_occupancy(
            context,
            person,
            position,
            categorisation,
            start_date=start_date,
            end_date=resignations.pop(key, None),
            status=None,
        )
    # A resignation must end the occupancy of a member marked as having left, or
    # of an appointee.
    assert len(resignations) == 0, resignations
