import structlog
from nomenklatura.resolver import Identifier
from opensanctions.wikidata.api import get_label

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

    @property
    def property_label(self):
        return get_label(self.property)

    @property
    def qid(self):
        if self.value_type == "wikibase-entityid":
            return self._value.get("id")

    @property
    def text(self):
        if self.value_type is None:
            return None
        elif self.value_type == "time":
            value = self._value.get("time")
            if value is not None:
                value = value.strip("+")
                if "T" in value:
                    value, _ = value.split("T", 1)
            return value
        elif self.value_type == "wikibase-entityid":
            return get_label(self._value.get("id"))
        elif self.value_type == "monolingualtext":
            return self._value.get("text")
        elif self.value_type == "quantity":
            # Resolve unit name and make into string:
            value = self._value.get("amount", "")
            value = value.lstrip("+")
            unit = self._value.get("unit", "")
            unit = unit.split("/")[-1]
            if Identifier.QID.match(unit):
                unit = get_label(unit)
                value = f"{value} {unit}"
            return value
        elif isinstance(self._value, str):
            return self._value
        else:
            log.warning("Unhandled value type", type=self.value_type, value=self._value)


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
