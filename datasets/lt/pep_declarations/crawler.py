import random
from time import sleep
from typing import Optional

from requests import HTTPError

from zavod import Context, Entity
from zavod import helpers as h
from zavod.logic.pep import categorise

DEKLARACIJA_ID_RANGE = range(301_730, 637_217)
# sample 50 for dev purposes
DEKLARACIJA_ID_RANGE = random.sample(DEKLARACIJA_ID_RANGE, 50)


class PinregSession:
    """object for interactacting with PINREG portal"""

    def __init__(self, context: Context):
        self.context = context
        guest_id = random.randint(0, 9_999_999)  # random guest token
        self._guest_token = f"c{guest_id:07d}"

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
                self.context.log.info(f"deklaracija {id_str} does not exist")
            else:
                self.context.log.error(
                    f"deklaracija {id_str} skipped due to {status_code} error."
                )


def make_person(context: Context, data: dict) -> Entity:
    person = context.make("Person")
    first_name = data.pop("vardas")
    last_name = data.pop("pavarde")
    person_id: Optional[str] = data.pop("asmensKodas", None)  # this identifier is often missing
    birth_date: Optional[str] = data.pop("gimimoData", None)
    person.id = context.make_id(person_id, first_name, last_name)
    person.add("firstName", first_name)
    person.add("registrationNumber", person_id)
    person.add("lastName", last_name)
    person.add("birthDate", birth_date)
    person.add("legalForm", data.pop("asmensTipas", None))
    context.audit_data(data)
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
    context: Context, person: Entity, affiliations: list[dict], is_pep: bool = True
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
    for data in affiliations:
        entity_is_legal_entity: bool = data.pop("yraJuridinisAsmuo")
        entity_is_lithuanian: bool = data.pop("registruotaLietuvoje")
        legal_entity_code: Optional[str] = data.pop("jaKodas")
        entity_name = data.pop("pavadinimas")
        affiliation_start_date: str = data.pop("rysioPradzia")
        affiliation_type = data.pop("darbovietesTipas")
        data_sources = data.pop("duomenuSaltiniai")
        filed_automatically: bool = data.pop("uzpildytaAutomatiskai")
        entity_must_be_declared: bool = data.pop("privaluDeklaruoti")
        entity_legal_form: str = data.pop("jaTeisinesFormosPavadinimas")

        for role in data.pop("pareigos"):
            position_name:str = role.pop("pareigos") or "Unknown position"
            legal_code: Optional[str] = role.pop("teisejoKodas")
            role_must_be_declared: Optional[bool] = role.pop("privaluDeklaruoti")
            nature_of_duties: str = role.pop("pareiguTipasPavadinimas")
            position = h.make_position(
                context,
                name=', '.join([position_name, entity_name]),
                topics=None,
                country="LT" if entity_is_lithuanian else None,
            )

            categorisation = categorise(
                context,
                position,
                is_pep=is_pep,
            )

            occupancy = h.make_occupancy(
                context,
                person,
                position,
                no_end_implies_current=False,
                start_date=affiliation_start_date,
                categorisation=categorisation,
                propagate_country=True,
            )
            context.audit_data(role)
            if occupancy:
                entities.append((position, occupancy))
        context.audit_data(data)
    return entities


def crawl(context: Context) -> None:
    """exhaustively scans PINREG portal and emits all deklaracijos"""

    pinreg = PinregSession(context)
    for deklaracija_id in DEKLARACIJA_ID_RANGE:
        sleep(0.3)
        if not (record := pinreg.get_deklaracija_by_id(deklaracija_id)):
            continue

        assert record.pop("id") == deklaracija_id

        submission_date = record.pop("pateikimoData")
        reasons_for_submission = record.pop("teikimoPriezastysPavadinimai")
        submission_status = record.pop("viesumoStatusas")
        nondisclosure_reason = record.pop("neviesinimoPriezastis")
        declaration_type = record.pop("deklaracijosTipas")

        # declarant data
        declarant = make_person(context, record.pop("teikejas"))
        declarant_affiliations: list[dict] = record.pop("darbovietes")
        declarant_offices = parse_affiliations(
            context, declarant, declarant_affiliations
        )
        for position, occupancy in declarant_offices:
            context.emit(position, target=False)
            context.emit(occupancy, target=False)
        if not declarant_offices:
            context.log.info(
                f"deklaracija {deklaracija_id} has no relevant occupancies."
            )
            continue
        context.log.info(f"deklaracija {deklaracija_id} processing.")

        """If the spouse is included in the declaration, and the spouse's occupancies
        meet OpenSanctions criteria, emit the spouse, the marriage, and the occupancies/positions."""
        spouse_affiliations: list[dict] = record.pop("sutuoktinioDarbovietes", [])
        if spouse_data := record.pop("sutuoktinis", None):
            spouse = make_person(context, spouse_data) if spouse_data else None
            marriage = make_spouse(context, declarant, spouse)
            spouse_offices = parse_affiliations(context, spouse, spouse_affiliations)
            if spouse_offices:
                for position, occupancy in spouse_offices:
                    context.emit(position, target=False)
                    context.emit(occupancy, target=False)
            context.emit(spouse, target=False)
            context.emit(marriage)

        # uncertain values
        related_transactions: Optional[list[dict]] = record.pop(
            "rysiaiDelSandoriu", None
        )
        related_legal_entities: Optional[list[dict]] = record.pop("rysiaiSuJa", None)
        context.emit(declarant, target=True)
        submission_session_end_date = record.pop("viesumoNutraukimoData")
        other_data = record.pop("kitiDuomenys")
        other_data_fa = record.pop("kitiDuomenysFa")
        is_employee_of_institution = record.pop("teikejasYraIstaigosDarbuotojas")
        potential_related_declarations = record.pop("deklaracijosAtsiradesGalimasRysys")
        individual_activities_declarations = record.pop(
            "deklaracijosIndividualiosVeiklos"
        )
        declaration_fa = record.pop("deklaracijosFaRysiai")
        is_latest_version = record.pop("yraNaujausiaVersija")
        reasons_for_submission_code = record.pop("teikimoPriezastys")
        old_declaration_id: Optional[str] = record.pop("senosPidId", None)
        context.audit_data(record)
