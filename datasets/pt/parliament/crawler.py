from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


IGNORE = [
    "DepCadId",  # position ID
    "DepCPId",  # constituency ID
    "LegDes",  # legislature name, e.g. "X Legislatura"
    "Videos",
]


def crawl_parliament(context: Context, url: str) -> None:
    plenary_chamber = context.fetch_json(url)["Plenario"]["Composicao"]

    for member in plenary_chamber:
        id = member.pop("DepId")
        name = member.pop("DepNomeCompleto")

        person = context.make("Person")
        person.id = context.make_id(name, id)
        person.add("name", name)

        # shorter parliamentary name
        person.add("alias", member.pop("DepNomeParlamentar"))
        person.add("citizenship", "pt")

        constituency_name = member.pop("DepCPDes")

        party_history = member.pop("DepGP") or []
        if party_history:
            # record only the most recent party affiliation
            party_affiliations = filter(
                lambda party: party["gpDtFim"] is None, party_history
            )
            person.add("political", [aff["gpSigla"] for aff in party_affiliations])
        ### each member also has:
        # gpId -- party group ID
        # gpDtFim and gpDtInicio -- dates of party affiliation

        position = h.make_position(
            context,
            name="Member of the Assembly of the Portuguese Republic",
            topics=["gov.national", "gov.legislative"],
            country=["pt"],
            wikidata_id="Q19953703",
        )
        categorisation = categorise(context, position, is_pep=True)

        if not categorisation.is_pep:
            continue

        # --- fetch non-leadership seat occupancy in the plenary ---
        for seat_occupancy in member.pop("DepSituacao") or []:
            # sioDes describes the status change type; only proceed for active periods.
            # sioDtInicio/sioDtFim are dates of the status change, not the MP seat itself.
            # There are periods in the data for durations when the member has suspended/had
            # resigned early. We don't want occupancies for those.
            period_status = seat_occupancy.pop("sioDes")
            is_active = (
                context.lookup_value(
                    "seat_status",
                    period_status,
                    warn_unmatched=True,
                )
                == "true"
            )

            if not is_active:
                continue

            occupancy = h.make_occupancy(
                context,
                person,
                position,
                start_date=seat_occupancy.pop("sioDtInicio"),
                end_date=seat_occupancy.pop("sioDtFim"),
                categorisation=categorisation,
            )

            if occupancy is not None:
                occupancy.add("constituency", constituency_name)
                occupancy.add("summary", period_status)
                context.emit(person)
                context.emit(occupancy)
                context.emit(position)

        # --- fetch leadership roles in the plenary ---
        for leadership_role in member.pop("DepCargo") or []:
            position_leadership = h.make_position(
                context,
                name=f"{leadership_role['carDes']}, Assembleia da República",
                topics=["gov.national", "gov.legislative"],
                country=["pt"],
            )
            categorisation_leadership = categorise(
                context, position_leadership, is_pep=None
            )
            if not categorisation_leadership.is_pep:
                continue

            occupancy_leadership = h.make_occupancy(
                context,
                person,
                position_leadership,
                start_date=leadership_role.pop("carDtInicio"),
                end_date=leadership_role.pop("carDtFim"),
            )
            if occupancy_leadership is not None:
                context.emit(person)
                context.emit(occupancy_leadership)
                context.emit(position_leadership)

        context.audit_data(member, IGNORE)


def crawl(context: Context) -> None:
    # locate the link to the list of Portugese parliaments
    doc_landing = context.fetch_html(context.data_url, absolute_links=True)
    url_parliament = h.xpath_string(
        doc_landing,
        "//a[@title='Recursos' and contains(@href,'DAComposicaoOrgaos')]/@href",
    )

    ### --- iterate over parliaments ---
    doc_parliament_list = context.fetch_html(url_parliament, absolute_links=True)
    parliament_urls = h.xpath_strings(
        doc_parliament_list, "//div[@class='archive-item']//a/@href"
    )
    parliament_names = h.xpath_strings(
        doc_parliament_list,
        "//div[@class='archive-item']//a/text()",
    )

    for name, u in zip(parliament_names, parliament_urls, strict=True):
        # skip 'Constituinte' and parliaments older than the 10th (ran from 10 March 2005 to 14 October 2009)
        if name == "IX Legislatura":
            break

        # retrieve JSON for the parliament
        doc = context.fetch_html(u, absolute_links=True)
        json_url = h.xpath_string(
            doc,
            "//a[starts-with(@title,'OrgaoComposicao') and contains(@title,'_json.txt')]/@href",
        )
        crawl_parliament(context, json_url)
