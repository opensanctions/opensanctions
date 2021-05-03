import structlog
from banal import ensure_list, ensure_dict
from normality import normalize, stringify

log = structlog.get_logger(__name__)


class Lookup(object):
    """Lookups are ways of patching up broken input data from a source."""

    def __init__(self, name, config):
        self.name = name
        self.required = config.pop("required", False)
        self.normalize = config.pop("normalize", False)
        self.lowercase = config.pop("lowercase", False)
        self.options = []
        for option in ensure_list(config.pop("options", [])):
            self.options.append(Option(self, option))
        for match, value in ensure_dict(config.pop("map", {})).items():
            option = {"match": match, "value": value}
            self.options.append(Option(self, option))

    def match(self, value):
        results = []
        for option in self.options:
            if option.matches(value):
                results.append(option.result)
        if len(results) > 1:
            log.error("Ambiguous result", value=value)
        for result in results:
            return result
        if self.required:
            log.error("Missing lookup result", value=value)

    def get_value(self, value, default=None):
        res = self.match(value)
        if res is not None:
            return res.value
        return default

    def get_values(self, value, default=None):
        res = self.match(value)
        if res is not None:
            return res.values
        return ensure_list(default)


class Option(object):
    """One possible lookup rule that might match a value."""

    def __init__(self, lookup, config):
        self.lookup = lookup
        self.normalize = config.pop("normalize", lookup.normalize)
        self.lowercase = config.pop("lowercase", lookup.lowercase)
        contains = ensure_list(config.pop("contains", []))
        self.contains = [self.normalize_value(c) for c in contains]
        match = ensure_list(config.pop("match", []))
        self.match = [self.normalize_value(m) for m in match]
        self.result = Result(config)

    def normalize_value(self, value):
        if self.normalize:
            value = normalize(value, ascii=True)
        else:
            value = stringify(value)
            if value is not None:
                if self.lowercase:
                    value = value.lower()
                value = value.strip()
        return value

    def matches(self, value):
        norm_value = self.normalize_value(value)
        if norm_value is not None:
            for cand in self.contains:
                if cand in norm_value:
                    return True
        return norm_value in self.match


class Result(object):
    def __init__(self, data):
        self._data = data

    @property
    def values(self):
        values = self._data.pop("values", self.value)
        return ensure_list(values)

    def __getattr__(self, name):
        try:
            return self._data[name]
        except KeyError:
            return None

    def __repr__(self):
        return "<Result(%r, %r)>" % (self.values, self._data)
