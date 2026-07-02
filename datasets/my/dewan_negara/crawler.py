import re
import urllib3

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import categorise

# The parliament portal serves an incomplete TLS certificate chain, which makes
# the default `requests` verification fail. Disabling verification is acceptable
# here: the source is a public government site and there is no login or secret.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def clean_senator_name(raw: str) -> str:
    """Return a senator's name without its trailing honorifics.

    With `lang=en` the source formats names as "<name>, YB Senator <titles>",
    so everything from the first comma onward is honorific decoration. Splitting
    on the comma keeps father-name patronymics that themselves contain titles
    (e.g. "... bin Tan Sri Mohamed", "... a/l Tun Samy Vellu"), which sit before
    the comma.
    """
    return raw.split(",")[0].strip()


def parse_term(value: str) -> tuple[str | None, str | None]:
    """Split a "DD.MM.YYYY - DD.MM.YYYY" term string into (start, end)."""
    parts = value.split(" - ")
    if len(parts) != 2:
        return None, None
    return parts[0].strip(), parts[1].strip()


def parse_detail(context: Context, url: str) -> dict[str, str]:
    """Return the label -> value pairs of a senator's `MAKLUMAT` info table.

    `lang=en` yields English field labels ("Name", "Term of Office", ...); the
    detail links on the roster page omit it, so it is requested explicitly.
    """
    doc = context.fetch_html(url, params={"lang": "en"}, cache_days=7)
    data: dict[str, str] = {}
    for row in h.xpath_elements(doc, ".//tr[td/strong]"):
        cells = h.xpath_elements(row, "./td")
        if len(cells) != 2:
            continue
        label = h.element_text(cells[0])
        data[label] = h.element_text(cells[1])
    return data


def emit_occupancy(
    context: Context,
    person: Entity,
    position: Entity,
    start_date: str | None = None,
    end_date: str | None = None,
) -> bool:
    """Categorise a position and emit an occupancy for it. Returns True on emit."""
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return False
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return False
    context.emit(position)
    context.emit(occupancy)
    return True


def crawl_senator(context: Context, senator_id: str, url: str) -> None:
    data = parse_detail(context, url)
    name = data.pop("Name", None)
    if name is None:
        context.log.warning("Senator without a name", url=url)
        return
    role = data.pop("Position in the Parliament", "")
    cabinet = data.pop("Position in Cabinet", "")
    party = data.pop("Party", "")
    term = data.pop("Term of Office", "")
    reappointment = data.pop("Reappointment", "")
    email = data.pop("Email", "")

    person = context.make("Person")
    person.id = context.make_slug(senator_id)
    person.add("name", clean_senator_name(name))
    person.add("sourceUrl", url)
    # Senate membership requires Malaysian citizenship under Article 47(a) of the
    # Federal Constitution ("Every citizen ... is qualified to be a member of the
    # Senate, if he is not less than thirty years old"):
    # https://lom.agc.gov.my/ (Laws of Malaysia — Federal Constitution)
    person.add("citizenship", "my")
    if party and party != "BEBAS":
        person.add("political", party)
    # Some senators list several contact addresses in one field, separated by
    # slashes or commas. The "-" placeholder is dropped via a type.email lookup.
    person.add("email", h.multi_split(email, ["/", ",", ";"]))

    emitted = False

    # Every listed person is a senator. The senate term (and any reappointment
    # for a second term) give the occupancy dates.
    member = h.make_position(
        context,
        name="Member of the Dewan Negara",
        country="my",
        wikidata_id="Q21328606",
        lang="eng",
    )
    for period in (term, reappointment):
        if not period or period == "-":
            continue
        start_date, end_date = parse_term(period)
        if emit_occupancy(context, person, member, start_date, end_date):
            emitted = True

    # Presiding officers, identified by the (Malay) role label.
    if role == "Yang di-Pertua Dewan Negara":
        president = h.make_position(
            context,
            name="President of the Dewan Negara",
            country="my",
            wikidata_id="Q7241319",
            lang="eng",
        )
        emitted = emit_occupancy(context, person, president) or emitted
    elif role == "Timbalan Yang di-Pertua Dewan Negara":
        deputy = h.make_position(
            context,
            name="Deputy President of the Dewan Negara",
            country="my",
            wikidata_id="Q134572656",
            lang="eng",
        )
        emitted = emit_occupancy(context, person, deputy) or emitted

    # Senators who also hold an executive office. Despite `lang=en`, the
    # `Position in Cabinet` field is inconsistently populated in English or Malay,
    # so it is translated (English input passes through unchanged); the id stays
    # keyed on the untranslated name. "&" is normalised to "dan" so the two
    # spellings of a ministry collapse to one position. (The `Position in the
    # Parliament` field duplicates the cabinet title for ministers; we use the
    # dedicated cabinet field instead.)
    if cabinet and cabinet != "-":
        minister = h.make_position(
            context,
            name=cabinet.replace("&", "dan"),
            country="my",
            lang="msa",
            translate_name=True,
        )
        emitted = emit_occupancy(context, person, minister) or emitted

    context.audit_data(
        data,
        ignore=[
            "Appointment",
            "State",
            "Phone Number",
            "Fax No.",
            "Social Media",
            "Mailing Address",
        ],
    )

    if emitted:
        context.emit(person)


def crawl(context: Context) -> None:
    context.http.verify = False
    doc = context.fetch_html(context.data_url, absolute_links=True, cache_days=1)
    links = h.xpath_elements(
        doc,
        ".//ul[contains(@class,'member-of-parliament')]/li//a[contains(@href,'id=')]",
    )
    if len(links) < 40:
        raise ValueError("Unexpectedly few senators: %d" % len(links))

    seen: set[str] = set()
    for link in links:
        href = link.get("href")
        if href is None:
            continue
        match = re.search(r"id=(\d+)", href)
        if match is None:
            context.log.warning("Senator link without id", href=href)
            continue
        senator_id = match.group(1)
        if senator_id in seen:
            continue
        seen.add(senator_id)
        crawl_senator(context, senator_id, href)
