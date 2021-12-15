import structlog

from opensanctions.wikidata.api import get_label

# from opensanctions.wikidata.lang import pick_lang_text

log = structlog.getLogger(__name__)


class Snak(object):
    def __init__(self, data):
        self._data = data
        datavalue = data.pop("datavalue", None)
        self.value_type = datavalue.pop("type")
        self._value = datavalue.pop("value")
        data.pop("hash", None)
        self.type = data.pop("datatype", None)
        self.property = data.pop("property", None)
        self.snaktype = data.pop("snaktype", None)

    @property
    def property_label(self):
        return get_label(self.property)

    @property
    def qid(self):
        if self.value_type == "wikibase-entityid":
            return self._value.get("id")

    @property
    def text(self):
        if self.value_type == "time":
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
        elif isinstance(self._value, str):
            return self._value
        else:
            print("XXXX", self.value_type, self._value)


class Claim(Snak):
    def __init__(self, data):
        self.id = data.pop("id")
        self.rank = data.pop("rank")
        super().__init__(data.pop("mainsnak"))
