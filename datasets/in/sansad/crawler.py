from itertools import count
from typing import Any, Dict, List, Set, Tuple

from lxml.html import fromstring
from prefixdate import parse_formats
from requests.exceptions import RequestException

from zavod import Context, Entity
from zavod import helpers as h

CACHE = 7


def decode_email(encoded: str | None) -> List[str]:
    if not encoded or not isinstance(encoded, str):
        return []
    # Decode the email address
    email = encoded.replace("[at]", "@").replace("[dot]", ".")
    email = email.replace("nic. in", "nic.in")  # fix space
    parsed = []
    for part in h.multi_split(email, [";", ","]):
        part = part.strip()
        if len(part) and "@" in part:
            parsed.append(part)
    return parsed


def clean_text(text: str | None) -> str | None:
    if not text or not isinstance(text, str):
        return None
    if not len(text.strip()):
        return None
    text = h.element_text(fromstring(text))
    return text


def emit_rca(context: Context, person: Entity, raw_name: str | None, role: str) -> None:
    if not raw_name or not raw_name.strip():
        return
    if raw_name.startswith("Late "):
        return
    rca = context.make("Person")
    rca.id = context.make_id(person.id, role, raw_name)
    rca.add("name", clean_text(raw_name))
    rca.add("country", "in")
    rca.add("topics", "role.rca")
    context.emit(rca)

    link = context.make("Family")
    link.id = context.make_id(person.id, "family", raw_name)
    link.add("person", person)
    link.add("relative", rca)
    link.add("relationship", role)
    context.emit(link)


def crawl_ls_member(
    context: Context,
    position: Entity,
    periods: Dict[str, Tuple[str, str]],
    member: Dict[str, Any],
) -> None:
    person = context.make("Person")
    mpsno = member.pop("mpsno", None)
    person.id = context.make_slug("ls", mpsno)
    person.add("name", member.pop("name", None))
    person.add("name", member.pop("hname", None), lang="hin")
    h.apply_name(
        person,
        first_name=member.pop("firstName", None),
        last_name=member.pop("lastName", None),
    )
    person.add("title", member.pop("initial", None))
    person.add("gender", member.pop("gender", None))
    person.add("status", member.pop("status", None))
    person.add("political", clean_text(member.pop("partyFname", None)))
    person.add("email", decode_email(member.pop("email", None)))
    h.apply_date(person, "birthDate", member.pop("dob", None))

    # pprint.pprint(member)

    for period in member.pop("lsExpr", "").split(","):
        if period not in periods:
            continue
        start, end = periods[period]
        if period == max(periods.keys()):
            end = None
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=start,
            end_date=end,
            propagate_country=False,
        )
        if occupancy is not None:
            context.emit(occupancy)

    if "role.pep" not in person.get("topics"):
        # context.log.info(f"Skipping non-PEP {person.id}")
        return

    try:
        biodata = context.fetch_json(
            f"https://sansad.in/api_ls/member/{mpsno}?locale=en",
            cache_days=CACHE,
        )

        emit_rca(context, person, biodata.get("spouseName"), "spouse")
        emit_rca(context, person, biodata.get("fatherName"), "father")
        emit_rca(context, person, biodata.get("motherName"), "mother")
        person.add("name", biodata.pop("fullname", None))
        h.apply_date(person, "birthDate", biodata.pop("dateOfBirth", None))
        person.add("education", clean_text(biodata.pop("qualification", None)))
        person.add("education", clean_text(biodata.pop("education", None)))
        person.add("notes", clean_text(biodata.pop("otherInfo", None)))
        person.add("political", clean_text(biodata.pop("partyFname", None)))
        person.add("address", clean_text(biodata.pop("permanentFaddr", None)))
        person.add("address", clean_text(biodata.pop("presentFaddr", None)))
        person.add("birthPlace", clean_text(biodata.pop("birthPlace", None)))
    except RequestException as exc:
        context.log.info(f"Broken MP biodata for {person.id}", exc=str(exc))

    context.emit(person)


def crawl_ls(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Lok Sabha",
        wikidata_id="Q16556694",
        country="in",
        topics=["gov.legislative", "gov.national"],
    )
    context.emit(position)

    periods: Dict[str, Tuple[str, str]] = {}
    dates_resp = context.fetch_json(
        "https://sansad.in/api_ls/business/AllLoksabhaAndSessionDates", cache_days=1
    )
    for ls in dates_resp:
        num = str(ls.get("loksabha", ""))
        dates: Set[str] = set()
        for session in ls.get("sessions", []):
            for date in session.get("dates", []):
                parsed = parse_formats(date, ["%d/%m/%Y"])
                if parsed.text:
                    dates.add(parsed.text)
        if len(dates) > 0:
            periods[num] = (min(dates), max(dates))

    index = context.fetch_json("https://sansad.in/api_ls/member", cache_days=1)
    for member in index["membersDtoList"]:
        crawl_ls_member(context, position, periods, member)


def crawl_rs_member(context: Context, position: Entity, member: Dict[str, Any]) -> None:
    person = context.make("Person")
    mpsno = member.pop("mpsno", None)
    person.id = context.make_slug("rs", mpsno)
    person.add("name", member.pop("name", ""))
    person.add("name", member.pop("hname", ""), lang="hin")
    h.apply_name(
        person,
        first_name=member.pop("firstName", None),
        last_name=member.pop("lastName", None),
    )
    person.add("gender", member.pop("gender", None))
    person.add("status", member.pop("status", None))
    person.add("email", decode_email(member.pop("emailID", None)))
    person.add("political", clean_text(member.pop("party", None)))
    h.apply_date(person, "birthDate", member.pop("dob", None))
    # NOTE: deathDate is not provided in the API, this looks like end of term:
    # h.apply_date(person, "deathDate", member.pop("expirationDate", None))
    # person.add("birthPlace", member["birthPlace"])
    person.add("address", clean_text(member.pop("localAdd", None)))
    person.add("address", clean_text(member.pop("permanentAdd", None)))
    person.add("address", clean_text(member.pop("otherPermanentAdd", None)))
    person.add("citizenship", "in")

    try:
        terms = context.fetch_json(
            "https://sansad.in/api_rs/member/term-years",
            params={"mpCode": mpsno},
            cache_days=CACHE,
        )
        for term in terms.get("records", []):
            period = term.get("termPeriod", "")
            start, end = period.split("-")
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                start_date=start,
                end_date=end,
                propagate_country=False,
            )
            if occupancy is not None:
                context.emit(occupancy)
    except RequestException as exc:
        context.log.info(f"Broken MP term data for {person.id}", exc=str(exc))

    if person.has("deathDate"):
        return
    if "role.pep" not in person.get("topics") and member["mpFlag"] == 0:
        return

    try:
        biodata = context.fetch_json(
            "https://sansad.in/api_rs/member/bio-data",
            params={"mpCode": mpsno, "locale": "en"},
            cache_days=CACHE,
        )

        emit_rca(context, person, biodata.get("spouseName"), "spouse")
        emit_rca(context, person, biodata.get("fatherName"), "father")
        emit_rca(context, person, biodata.get("motherName"), "mother")
        person.add("education", clean_text(biodata.pop("qualification", None)))
        person.add("notes", clean_text(biodata.pop("essentialInformation", None)))
    except RequestException as exc:
        context.log.info(f"Broken MP biodata for {person.id}", exc=str(exc))

    context.emit(person)


def crawl_rs(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Rajya Sabha",
        wikidata_id="Q17324844",
        country="in",
        topics=["gov.legislative", "gov.national"],
    )
    context.emit(position)

    for page in count(1):
        params = {
            "page": page,
            "size": 50,
        }
        index = context.fetch_json(
            "https://sansad.in/api_rs/member/sitting-members",
            params=params,
            cache_days=1,
        )
        for member in index["records"]:
            crawl_rs_member(context, position, member)

        if page >= index["_metadata"]["totalPages"]:
            break


def crawl(context: Context) -> None:
    crawl_ls(context)
    crawl_rs(context)
