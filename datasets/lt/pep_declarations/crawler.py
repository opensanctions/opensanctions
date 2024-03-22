import random

from requests import HTTPError

from zavod import Context
from zavod import helpers as h


TEST_IDS = [635390, 635_301, 0]

class PinregSession:
    """object for interactacting with PINREG portal"""

    def __init__(self, context:Context):
        self.context = context
        guest_id = random.randint(0, 9_999_999) # random guest token
        self._guest_token = f"c{guest_id:07d}"

    def get_deklaracija_by_id(self, id: int) -> dict[any]:
        self.context.log.info(f'Processing deklaracija {id:06d}')
        try: 
            return self.context.fetch_json(
                url=f"https://pinreg.vtek.lt/external/deklaracijos/{id:06d}/perziura/viesa",
                params = {'v': self._guest_token},
                headers = {
                    'Accept': 'application/json',
                    'Referer': f'https://pinreg.vtek.lt/app/pid-perziura/{id:06d}',
                    'DNT': '1',
                    'Connection': 'keep-alive'
                },
            cache_days=90,
            )
        except HTTPError as ex:
            response = ex.response.json()
            if response.pop('message') == 'Klaida' and response.pop('status') == 404:
                self.context.log.info(f'No record for deklaracija {id:06d}')

def parse_declarant(context:Context, declarant_data:dict) -> None:
    declarant = context.make("Person")
    first_name = declarant_data.pop('vardas')
    last_name = declarant_data.pop('pavarde')
    person_id = declarant_data.pop('asmensKodas') # this identifier is often missing
    declarant.id = context.make_id(person_id, first_name, last_name)
    declarant.add('firstName', first_name)
    declarant.add('registrationNumber', person_id)
    declarant.add('lastName', last_name)
    declarant.add('birthDate', declarant_data.pop('gimimoData'))
    declarant.add('legalForm', declarant_data.pop('asmensTipas'))
    context.audit_data(declarant_data)
    context.emit(declarant, target=True)

def crawl(context: Context) -> None:
    """exhaustively scans PINREG portal and emits all deklaracijos"""
    pinreg = PinregSession(context)
    for deklaracija_id in TEST_IDS: # range(0, 999_999):
        if not (record := pinreg.get_deklaracija_by_id(deklaracija_id)): 
            continue
        print(record)
        parse_declarant(context, record.pop('teikejas'))
