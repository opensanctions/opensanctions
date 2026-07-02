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


def parse_detail(context: Context, url: str) -> dict[str, str]:
    """Return the label -> value pairs of a member's `MAKLUMAT` info table."""
    doc = context.fetch_html(url, cache_days=7)
    data: dict[str, str] = {}
    for row in h.xpath_elements(doc, ".//tr[td/strong]"):
        cells = h.xpath_elements(row, "./td")
        if len(cells) != 2:
            continue
        label = h.element_text(cells[0])
        data[label] = h.element_text(cells[1])
    return data


def crawl_member(context: Context, member_id: str, url: str) -> None:
    data = parse_detail(context, url)
    name = data.pop("Nama", None)
    if name is None:
        context.log.warning("Member without a name", url=url)
        return
    jawatan = data.pop("Jawatan dalam Parlimen", "")
    kabinet = data.pop("Jawatan dalam Kabinet", "")
    party = data.pop("Parti", "")
    constituency = data.pop("Parlimen", "")
    email = data.pop("Email", "")

    person = context.make("Person")
    person.id = context.make_slug(member_id)
    person.add("name", h.strip_name_titles(context, name))
    person.add("sourceUrl", url)
    # Membership of the House of Representatives requires Malaysian citizenship
    # under Article 47 of the Federal Constitution of Malaysia (the Speaker and
    # Secretary are likewise Malaysian office holders):
    # https://lom.agc.gov.my/ (Laws of Malaysia — Federal Constitution)
    person.add("citizenship", "my")
    # "BEBAS" means the member is an independent, i.e. holds no party affiliation.
    if party and party != "BEBAS":
        person.add("political", party)
    # Some members list several contact addresses in one field, separated by
    # slashes or commas. The "-" placeholder is dropped via a type.email lookup.
    person.add("email", h.multi_split(email, ["/", ",", ";"]))

    # A member with a constituency (P-code) is an elected MP; the Speaker and the
    # Secretary hold no constituency and are identified by their `Jawatan` role.
    positions: list[Entity] = []
    if constituency:
        positions.append(
            h.make_position(
                context,
                name="Member of the Dewan Rakyat",
                country="my",
                wikidata_id="Q21290861",
                lang="eng",
            )
        )
        if jawatan == "Timbalan Yang di-Pertua Dewan Rakyat":
            positions.append(
                h.make_position(
                    context,
                    name="Deputy Speaker of the Dewan Rakyat",
                    country="my",
                    wikidata_id="Q126361900",
                    lang="eng",
                )
            )
    elif jawatan == "Yang di-Pertua Dewan Rakyat":
        positions.append(
            h.make_position(
                context,
                name="Speaker of the Dewan Rakyat",
                country="my",
                wikidata_id="Q7574262",
                lang="eng",
            )
        )
    elif jawatan == "Setiausaha Dewan Rakyat":
        positions.append(
            h.make_position(
                context,
                name="Secretary of the Dewan Rakyat",
                country="my",
                lang="eng",
            )
        )
    else:
        context.log.warning(
            "Member without constituency or known role",
            url=url,
            jawatan=jawatan,
        )
        return

    # Many members also hold an executive (cabinet) office, e.g. "Menteri
    # Ekonomi" (Minister of Economy) or "Timbalan Menteri Pertahanan" (Deputy
    # Minister of Defence). The Malay title is translated to English; the
    # position id stays keyed on the untranslated name. Normalise "&" to "dan"
    # so the two spellings of a ministry collapse to one position.
    if kabinet and kabinet != "-":
        positions.append(
            h.make_position(
                context,
                name=kabinet.replace("&", "dan"),
                country="my",
                lang="msa",
                translate_name=True,
            )
        )

    context.audit_data(
        data,
        ignore=[
            "Tempat Duduk",
            "Kawasan",
            "Negeri",
            "No. Telefon",
            "No. Faks",
            "Media Sosial",
            "Alamat Surat-menyurat",
        ],
    )

    emitted = False
    for position in positions:
        categorisation = categorise(context, position, default_is_pep=True)
        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(position)
        context.emit(occupancy)
        emitted = True

    if emitted:
        context.emit(person)


def crawl(context: Context) -> None:
    context.http.verify = False
    doc = context.fetch_html(context.data_url, absolute_links=True, cache_days=1)
    links = h.xpath_elements(
        doc,
        ".//ul[contains(@class,'member-of-parliament')]/li//a[contains(@href,'id=')]",
    )
    if len(links) < 180:
        raise ValueError("Unexpectedly few members: %d" % len(links))

    seen: set[str] = set()
    for link in links:
        href = link.get("href")
        if href is None:
            continue
        match = re.search(r"[?&]id=(\d+)", href)
        if match is None:
            context.log.warning("Member link without id", href=href)
            continue
        member_id = match.group(1)
        if member_id in seen:
            continue
        seen.add(member_id)
        crawl_member(context, member_id, href)
