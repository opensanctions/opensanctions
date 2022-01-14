import structlog
from opensanctions.wikidata.api import get_label
from opensanctions.wikidata.value import snak_value_to_string

log = structlog.getLogger(__name__)


class Snak(object):
    """Some Notation About Knowledge (TM)."""

    def __init__(self, data):
        datavalue = data.pop("datavalue", {})
        self.value_type = datavalue.pop("type", None)
        self._value = datavalue.pop("value", None)
        data.pop("hash", None)
        self.type = data.pop("datatype", None)
        self.property = data.pop("property", None)
        self.snaktype = data.pop("snaktype", None)
        # self._data = data

    async def property_label(self, context):
        return await get_label(context, self.property)

    @property
    def qid(self):
        if self.value_type == "wikibase-entityid":
            return self._value.get("id")

    async def text(self, context):
        return await snak_value_to_string(context, self.value_type, self._value)


class Reference(object):
    def __init__(self, data):
        self.snaks = {}
        for prop, snak_data in data.pop("snaks", {}).items():
            self.snaks[prop] = [Snak(s) for s in snak_data]

    def get(self, prop):
        return self.snaks.get(prop, [])


class Claim(Snak):
    def __init__(self, data):
        self.id = data.pop("id")
        self.rank = data.pop("rank")
        super().__init__(data.pop("mainsnak"))
        self.qualifiers = {}
        for prop, snaks in data.pop("qualifiers", {}).items():
            self.qualifiers[prop] = [Snak(s) for s in snaks]

        self.references = [Reference(r) for r in data.pop("references", [])]
        # self._claim = data

    def get_qualifier(self, prop):
        return self.qualifiers.get(prop, [])
