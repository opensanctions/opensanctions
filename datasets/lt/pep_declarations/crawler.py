import random
from time import sleep
from typing import Optional

from requests import HTTPError

from zavod import Context, Entity
from zavod import helpers as h

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
        self.context.log.info(f"Processing deklaracija {id_str}")
        try:
            return self.context.fetch_json(
                url=f"https://pinreg.vtek.lt/external/deklaracijos/{id_str}/perziura/viesa",
                params={"v": self._guest_token},
                headers={
                    "Accept": "application/json",
                    "Referer": f"https://pinreg.vtek.lt/app/pid-perziura/{id_str}",
                    "DNT": "1",
                    "Connection": "keep-alive",
                },
                cache_days=90,
            )
        except HTTPError as ex:
            response = ex.response.json()
            if status_code := response.pop("status") == 404:
                self.context.log.info(f"No record for deklaracija {id_str}")
            else:
                self.context.log.error(f"{status_code} error for deklaracija {id_str}")


def make_person(context: Context, data: dict) -> Entity:
    person = context.make("Person")
    first_name = data.pop("vardas")
    last_name = data.pop("pavarde")
    person_id = data.pop("asmensKodas", None)  # this identifier is often missing
    person.id = context.make_id(person_id, first_name, last_name)
    person.add("firstName", first_name)
    person.add("registrationNumber", person_id)
    person.add("lastName", last_name)
    person.add("birthDate", data.pop("gimimoData", None))
    person.add("legalForm", data.pop("asmensTipas", None))
    context.audit_data(data)
    return person


def make_marriage(context: Context, person: Entity, spouse: Entity) -> Entity:
    marriage = context.make("Family")
    marriage.id = context.make_id(person.id, spouse.id)
    marriage.add("relationship", "Spouse")
    marriage.add("person", person)
    marriage.add("relative", spouse)
    return marriage


def crawl(context: Context) -> None:
    """exhaustively scans PINREG portal and emits all deklaracijos"""

    pinreg = PinregSession(context)
    for deklaracija_id in DEKLARACIJA_ID_RANGE:
        sleep(0.3)
        if not (record := pinreg.get_deklaracija_by_id(deklaracija_id)):
            continue

        assert record.pop('id') == deklaracija_id

        submission_date = record.pop('pateikimoData')
        reasons_for_submission = record.pop("teikimoPriezastysPavadinimai")
        submission_status = record.pop("viesumoStatusas")
        nondisclosure_reason = record.pop("neviesinimoPriezastis")
        declaration_type = record.pop("deklaracijosTipas")

        # uncertain values
        submission_session_end_date = record.pop("viesumoNutraukimoData")
        other_data = record.pop("kitiDuomenys")
        other_data_fa = record.pop("kitiDuomenysFa")
        is_employee_of_institution = record.pop("teikejasYraIstaigosDarbuotojas")
        potential_related_declarations = record.pop('deklaracijosAtsiradesGalimasRysys')
        individual_activities_declarations = record.pop("deklaracijosIndividualiosVeiklos")
        declaration_fa = record.pop("deklaracijosFaRysiai")

        # likely irrelevant values
        is_latest_version = record.pop("yraNaujausiaVersija")
        reasons_for_submission_code = record.pop("teikimoPriezastys")
        old_declaration_id:Optional[str] = record.pop("senosPidId", None)
        declarant = make_person(context, record.pop("teikejas"))
        positions:list[dict] = record.pop("darbovietes")

        spouse_data:Optional[dict] = record.pop("sutuoktinis", None)
        spouse_positions:Optional[list[dict]] = record.pop("sutuoktinioDarbovietes", None)
        if spouse_data is not None:
            spouse = make_person(context, spouse_data)
            marriage = make_marriage(context, declarant, spouse)
            context.emit(spouse, target=False)
            context.emit(marriage)
        related_transactions:Optional[list[dict]] = record.pop("rysiaiDelSandoriu", None)
        related_legal_entities:Optional[list[dict]] = record.pop("rysiaiSuJa", None)
        context.emit(declarant, target=True)
        context.audit_data(record)
