from typing import Dict, Any, List, Optional


class SparqlValue(object):
    WD_PREFIX = "http://www.wikidata.org/entity/"

    def __init__(self, data: Dict[str, Any]) -> None:
        self.type: str = data["type"]
        self.value: str = data["value"]
        if self.type == "uri" and self.value.startswith(self.WD_PREFIX):
            self.value = self.value[len(self.WD_PREFIX) :]
        self.lang: Optional[str] = data.get("xml:lang")

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"<SV({self.type!r}, {self.value!r})>"

    def __hash__(self) -> int:
        return hash(repr(self))


class SparqlBinding(object):
    def __init__(self, response: "SparqlResponse", data: Dict[str, Any]) -> None:
        self.response = response
        self.values: Dict[str, SparqlValue] = {}
        for var, value in data.items():
            self.values[var] = SparqlValue(value)

    def wrapped(self, var: str) -> Optional[SparqlValue]:
        if var not in self.response.vars:
            raise KeyError("No such var: %s (in: %r)" % (var, self.response.vars))
        value = self.values.get(var)
        if value is None:
            return None
        return value

    def plain(self, var: str) -> Optional[str]:
        if var not in self.response.vars:
            raise KeyError("No such var: %s (in: %r)" % (var, self.response.vars))
        if var in self.values:
            value = self.wrapped(var)
            if value is not None:
                return value.value
        return None

    def __repr__(self) -> str:
        return f"<SparqlBinding({self.values!r})>"


class SparqlResponse(object):
    def __init__(self, query: str, response: Dict[str, Any]) -> None:
        self.query = query
        self.vars: List[str] = response["head"]["vars"]
        self.results: List[SparqlBinding] = []
        for bind in response["results"]["bindings"]:
            self.results.append(SparqlBinding(self, bind))

    def __len__(self) -> int:
        return len(self.results)

    def __iter__(self) -> List[SparqlBinding]:
        return self.results

    def __repr__(self) -> str:
        return f"<SparqlResponse({self.vars!r}, {len(self)})>"
