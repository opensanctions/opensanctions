
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


def crawl(context: Context) -> None:
    """exhaustively scans PINREG portal and emits all deklaracijos"""
    pinreg = PinregSession(context)
    for deklaracija_id in TEST_IDS: # range(0, 999_999):
        if not (record := pinreg.get_deklaracija_by_id(deklaracija_id)): 
            continue
        print(record)
