from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


IGNORE = [
    "DepCadId",  # position ID
    "LegDes",  # legislature name, e.g. "X Legislatura"
    "Videos",
]


def roman_to_int(s: str) -> int:
    roman = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    sum = 0
    prevValue = roman[s[0]]

    for i in range(1, len(s)):
        currentValue = roman[s[i]]
        sum += -prevValue if (currentValue > prevValue) else prevValue
        prevValue = currentValue
    sum += prevValue
    return sum


def crawl_parliament(context: Context, url: str) -> None:
    plenary_chamber = context.fetch_json(url)["Plenario"]["Composicao"]

    for member in plenary_chamber:
        id = member.pop("DepId")
        name = member.pop("DepNomeCompleto")

        person = context.make("Person")
        person.id = context.make_id(name, id)
        person.add("name", name)
        # unique MP identifier
        person.add("idNumber", id)
        # shorter parliamentary name
        person.add("alias", member.pop("DepNomeParlamentar"))
        person.add("country", "pt")

        for party_history in member.pop("DepGP") or []:
            person.add("political", party_history.pop("gpSigla"))  # party abbreviation
        ### each member also has:
        # gpId -- party group ID
        # gpDtFim and gpDtInicio -- dates of party affiliation

        position = h.make_position(
            context,
            name="Member of the Portuguese Parliament",
            topics=["gov.national", "gov.legislative"],
            country=["pt"],
        )
        position.add("subnationalArea", member.pop("DepCPDes"))  # constituency name
        position.add("subnationalArea", member.pop("DepCPId"))  # constituency ID
        categorisation = categorise(context, position, is_pep=True)

        if not categorisation.is_pep:
            continue

        # --- fetch non-leadership seat occupancy in the plenary ---
        for seat_occupancy in member.pop("DepSituacao") or []:
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                start_date=seat_occupancy.pop("sioDtInicio"),
                end_date=seat_occupancy.pop("sioDtFim"),
            )

            # MP seat occupancy status, e.g. Effective, Retired, Suspended, Disqulified, etc:
            seatstatus = seat_occupancy.pop("sioDes")
            position.add("notes", seatstatus)

            if occupancy is not None:
                context.emit(person)
                context.emit(occupancy)
                context.emit(position)

        # --- fetch leadership roles in the plenary ---
        for leadership_role in member.pop("DepCargo") or []:
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                start_date=leadership_role.pop("carDtInicio"),
                end_date=leadership_role.pop("carDtFim"),
            )
            if occupancy is not None:
                occupancy.add("recordId", leadership_role.pop("carId"))
                occupancy.add("description", leadership_role.pop("carDes"))
                context.emit(person)
                context.emit(occupancy)
                context.emit(position)

        context.audit_data(member, IGNORE)


def crawl(context: Context) -> None:
    # locate the link to the list of Portugese parliaments
    doc_landing = context.fetch_html(context.data_url, absolute_links=True)
    url_parliament_list = h.xpath_string(
        doc_landing,
        "//a[@title='Recursos' and contains(@href,'DAComposicaoOrgaos')]/@href",
    )

    ### --- iterate over parliaments ---
    doc_parliament_list = context.fetch_html(url_parliament_list, absolute_links=True)
    parliament_urls = h.xpath_strings(
        doc_parliament_list,
        "//div[@id='ctl00_ctl51_g_48ce9bb1_53ac_4c68_b897_c5870f269772_ctl00_pnlPastas']//a/@href",
    )
    parliament_names = h.xpath_strings(
        doc_parliament_list,
        "//div[@id='ctl00_ctl51_g_48ce9bb1_53ac_4c68_b897_c5870f269772_ctl00_pnlPastas']//a/text()",
    )

    for name, u in zip(parliament_names, parliament_urls):
        # skip 'Constituinte' and parliaments older than the 10th (ran from 10 March 2005 to 14 October 2009)
        if name == "Constituinte":
            continue
        roman = name.split()[0]
        if roman_to_int(roman) < 10:
            continue

        # retrieve JSON for the parliament
        doc = context.fetch_html(u, absolute_links=True)
        json_url = h.xpath_string(
            doc,
            "//a[starts-with(@title,'OrgaoComposicao') and contains(@title,'_json.txt')]/@href",
        )
        crawl_parliament(context, json_url)
