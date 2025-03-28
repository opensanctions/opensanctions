from datetime import datetime
from typing import Optional

from zavod import Context, Entity
from zavod import helpers as h
from zavod.logic.pep import OccupancyStatus, categorise

DEKLARACIJA_ID_RANGE = range(301_730, 1_000_000)
# We'll stop after MAX_GAP consecutive 404s
MAX_GAP = 200
# sample 50 for dev purposes
# import random
# DEKLARACIJA_ID_RANGE = random.sample(DEKLARACIJA_ID_RANGE, 5000)
GUEST_ID = 9179496
SKIP_ROLES = {
    "Gydytojas, odontologas ar farmacijos specialistas",  # doctor, dentist or pharmacist
    "Darbuotojas",  # employee
    "Juridinio asmens darbuotojas",  # employee of a legal entity
    "Karjeros valstybės tarnautojas",  # career civil servant, as opposed to "Karjeros valstybės tarnautojas struktūrinio padalinio vadovas ar jo pavaduotojas" - Career civil servant, head of a structural unit or his deputy
    "Statutinis valstybės tarnautojas",  # statutory civil servant
    "Asmenys, perkančiosios organizacijos ar perkančiojo subjekto vadovo paskirti atlikti supaprastintus pirkimus",  # persons appointed by the contracting authority or contracting entity to carry out simplified procurements
}


class PinregSession:
    """object for interactacting with PINREG portal"""

    def __init__(self, context: Context):
        self.context = context
        self._guest_token = f"c{GUEST_ID}"

    def get_deklaracija_by_id(self, id: int) -> Optional[dict[any]]:
        id_str = f"{id:06d}"
        self.context.log.info(f"fetching declaration {id_str}")
        r = self.context.http.get(
            url=f"https://pinreg.vtek.lt/external/deklaracijos/{id_str}/perziura/viesa",
            params={"v": self._guest_token},
            headers={
                "Accept": "application/json",
                "Referer": f"https://pinreg.vtek.lt/app/pid-perziura/{id_str}",
            },
        )
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            r.raise_for_status()
        return r.json()


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
    person.add("sourceUrl", f"https://pinreg.vtek.lt/app/pid-perziura/{declaration_id}")
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
                context.log.info(
                    "Foreign declared role",
                    name=person.get("name"),
                    entity=entity_name,
                    role=role,
                    affiliation=affiliation,
                    position=position_name,
                )
                continue
            main_duty = role.pop("pareiguTipasPavadinimas")
            if main_duty in SKIP_ROLES:
                continue
            position = h.make_position(
                context,
                name=main_duty,
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
            occupancy.add("description", ", ".join([position_name, entity_name]))
            context.audit_data(role)
            if occupancy:
                entities.append((position, occupancy))
        context.audit_data(
            affiliation,
            [
                "privaluDeklaruoti",  # must declare
                "yraJuridinisAsmuo",  # is legal person
                "jaKodas",  # code
                "darbovietesTipas",  # workplace type
                "duomenuSaltiniai",  # data sources
                "uzpildytaAutomatiskai",  # filled automatically
                "jaTeisinesFormosPavadinimas",  # legal form
            ],
        )
    return entities


# CATEGORIES_URL = "https://pinreg.vtek.lt/external/klasifikatoriai/grupuoti/viesi?savybesKodasGrupavimui=PAREIGU_POBUDZIO_GRUPE"

# def search_category():
#     categories = context.fetch_json(CATEGORIES_URL)
#     for group in categories:
#         group_name = group.get("grupesPavadinimas")
#         group_res = context.lookup("groups", group_name)
#         if not group_res:
#             context.log.warning("Unknown group", group=group)
#             continue
#         for category in group.get("klasifikatoriai"):
#             if group_res.include == "some":
#                 category_res = context.lookup("categories", category.get("pavadinimas"))
#                 if not category_res:
#                     context.log.warning("Unknown category", category=category, group_name=group_name)
#                     continue
#                 if category_res.include == "none":
#                     continue
#                 assert category_res.include == "all", (category, category_res.include)
#             else:
#                 assert group_res.include == "all", (group, group_res.include)
#
#             # search the category


def crawl(context: Context) -> None:
    """exhaustively scans PINREG portal and emits all deklaracijos"""

    pinreg = PinregSession(context)
    gap = 0
    for deklaracija_id in DEKLARACIJA_ID_RANGE:
        if deklaracija_id % 1000 == 0:
            context.flush()
        if not (record := pinreg.get_deklaracija_by_id(deklaracija_id)):
            gap += 1
            if gap > MAX_GAP:
                context.log.info("gap threshold reached, stopping.")
                break
            continue
        gap = 0

        assert record.pop("id") == deklaracija_id

        # declarant data
        declarant = make_person(context, deklaracija_id, record.pop("teikejas"))
        declarant_affiliations: list[dict] = record.pop("darbovietes")
        declarant_offices = parse_affiliations(
            context, declarant, declarant_affiliations
        )
        for position, occupancy in declarant_offices:
            context.emit(position)
            context.emit(occupancy)
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

        context.emit(declarant)
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
                "deklaracijosAtsiradesGalimasRysys",  # possible relationship
                "senosPidId",  # old PID ID
                "teikimoPriezastys",  # reasons for submission
                "deklaracijosFaRysiai",  # relationships
                "yraNaujausiaVersija",  # newest version
                "deklaracijosIndividualiosVeiklos",  # individual activities
            ],
        )
