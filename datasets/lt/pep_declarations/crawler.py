from datetime import datetime
import random
from time import sleep
from typing import Optional

from requests import HTTPError

from zavod import Context, Entity
from zavod import helpers as h
from zavod.logic.pep import OccupancyStatus, categorise

DEKLARACIJA_ID_RANGE = range(301_730, 637_217)
# sample 50 for dev purposes
#DEKLARACIJA_ID_RANGE = random.sample(DEKLARACIJA_ID_RANGE, 5000)
GUEST_ID = 9179496


class PinregSession:
    """object for interactacting with PINREG portal"""

    def __init__(self, context: Context):
        self.context = context
        self._guest_token = f"c{GUEST_ID}"

    def get_deklaracija_by_id(self, id: int) -> Optional[dict[any]]:
        id_str = f"{id:06d}"
        try:
            return self.context.fetch_json(
                url=f"https://pinreg.vtek.lt/external/deklaracijos/{id_str}/perziura/viesa",
                params={"v": self._guest_token},
                headers={
                    "Accept": "application/json",
                    "Referer": f"https://pinreg.vtek.lt/app/pid-perziura/{id_str}",
                },
                cache_days=30,
            )
        except HTTPError as ex:
            response = ex.response.json()
            if status_code := response.pop("status") == 404:
                self.context.log.debug(f"deklaracija {id_str} does not exist")
            else:
                raise


def make_person(context: Context, declaration_id: int, data: dict) -> Entity:
    person = context.make("Person")
    first_name = data.pop("vardas")
    last_name = data.pop("pavarde")
    person_id = data.pop("asmensKodas", None)  # this identifier is often missing
    birth_date = data.pop("gimimoData", None)  # often missing
    person.id = context.make_id(
        person_id, first_name, last_name, birth_date or declaration_id
    )
    person.add("registrationNumber", person_id)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    person.add("birthDate", birth_date)
    person.add("legalForm", data.pop("asmensTipas", None))
    # context.audit_data(data, ignore=[
    #    "yraJuridinisAsmuo",  # is legal person
    # ])
    return person


def make_spouse(context: Context, person: Entity, spouse: Entity) -> Entity:
    relationship = context.make("Family")
    relationship.id = context.make_id(person.id, spouse.id)
    relationship.add(
        "relationship", "Sutuoktinis, sugyventinis ar partneris", lang="lit"
    )
    relationship.add("person", person)
    relationship.add("relative", spouse)
    return relationship


def parse_affiliations(
    context: Context, person: Entity, affiliations: list[dict]
) -> list[tuple[Entity]]:
    """
    Args:
        context (Context)
        affiliations (list[dict]): A list of dicts, where each dict refers to a place
         of work and a sub-list of positions.
        is_pep (bool, optional): Defaults to True.

    Returns:
        list[tuple[Entity]]: a flattened list of tuples, where each is
        a position and occupancy. Occupancies that do not meet OpenSanctions criteria
        are skipped.
    """
    entities = []
    for affiliation in affiliations:
        entity_is_lithuanian: bool = affiliation.pop("registruotaLietuvoje")
        entity_name = affiliation.pop("pavadinimas")
        affiliation_start_date: str = affiliation.pop("rysioPradzia")

        for role in affiliation.pop("pareigos"):
            if not role.pop("privaluDeklaruoti"):  # role must be declared
                continue

            position_name: Optional[str] = role.pop("pareigos")
            assert position_name, (person, position_name)
            if not entity_is_lithuanian:
                context.log.warning(
                    "Foreign declared role", name=person.get("name"), entity=entity_name
                )
                continue
            position = h.make_position(
                context,
                name=", ".join([position_name, entity_name]),
                topics=None,
                country="LT",
            )
            categorisation = categorise(
                context,
                position,
                is_pep=True,
            )
            if affiliation_start_date > str(datetime.now().year):
                status = OccupancyStatus.CURRENT
            else:
                status = OccupancyStatus.UNKNOWN
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                no_end_implies_current=False,
                categorisation=categorisation,
                status=status,
            )
            context.audit_data(
                role,
                ignore=[
                    "teisejoKodas",  # code
                    "pareiguTipasPavadinimas",  # nature of duties
                ],
            )
            if occupancy:
                entities.append((position, occupancy))
        context.audit_data(affiliation, [
            "privaluDeklaruoti",  # must declare
            "yraJuridinisAsmuo",  # is legal person
            "jaKodas",  # code
            "darbovietesTipas",  # workplace type
            "duomenuSaltiniai",  # data sources
            "uzpildytaAutomatiskai",  # filled automatically
            "jaTeisinesFormosPavadinimas",  # legal form
        ])
    return entities


def crawl(context: Context) -> None:
    """exhaustively scans PINREG portal and emits all deklaracijos"""

    pinreg = PinregSession(context)
    for deklaracija_id in DEKLARACIJA_ID_RANGE:
        sleep(0.3)
        if not (record := pinreg.get_deklaracija_by_id(deklaracija_id)):
            continue

        assert record.pop("id") == deklaracija_id

        # declarant data
        declarant = make_person(context, deklaracija_id, record.pop("teikejas"))
        declarant_affiliations: list[dict] = record.pop("darbovietes")
        declarant_offices = parse_affiliations(
            context, declarant, declarant_affiliations
        )
        for position, occupancy in declarant_offices:
            context.emit(position, target=False)
            context.emit(occupancy, target=False)
        if not declarant_offices:
            context.log.debug(
                f"deklaracija {deklaracija_id} has no relevant occupancies."
            )
            continue
        context.log.debug(f"deklaracija {deklaracija_id} processing.")

        if spouse_data := record.pop("sutuoktinis", None):
            spouse = (
                make_person(context, deklaracija_id, spouse_data)
                if spouse_data
                else None
            )
            spouse.add("topics", "role.rca")
            marriage = make_spouse(context, declarant, spouse)
            context.emit(spouse)
            context.emit(marriage)

        context.emit(declarant, target=True)
        context.audit_data(
            record,
            ignore=[
                "rysiaiSuJa",  # relationships with declarant
                "sutuoktinioDarbovietes",  # spouse's affiliations
                "pateikimoData",  # date of submission
                "viesumoNutraukimoData",  # date of last modification
                "teikimoPriezastysPavadinimai",  # reasons for submission
                "viesumoStatusas",  # status of submission
                "neviesinimoPriezastis",  # reason for non-disclosure
                "deklaracijosTipas",  # type of declaration
                "rysiaiDelSandoriu",  # relationships due to transactions
                "kitiDuomenys",  # other data
                "kitiDuomenysFa",  # other data
                "teikejasYraIstaigosDarbuotojas",  # declarant is an employee of the institution
                #"deklaracijosAtsiradesGalimasRysys",  # possible relationship
                "senosPidId",  # old PID ID
                "teikimoPriezastys",  # reasons for submission
                #"deklaracijosFaRysiai",  # relationships
                "yraNaujausiaVersija",  # newest version
                "deklaracijosIndividualiosVeiklos",  # individual activities
            ],
        )
