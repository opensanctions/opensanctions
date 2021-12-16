import structlog
from prefixdate import Precision
from nomenklatura.resolver import Identifier
from opensanctions.wikidata.api import get_label

log = structlog.getLogger(__name__)
PRECISION = {
    11: Precision.DAY,
    10: Precision.MONTH,
    9: Precision.YEAR,
}


def snak_value_to_string(value_type, value):
    if value_type is None:
        return None
    elif value_type == "time":
        time = value.get("time")
        if time is not None:
            time = time.strip("+")
            prec = PRECISION.get(value.get("precision"), Precision.DAY)
            time = time[: prec.value]
        return time
    elif value_type == "wikibase-entityid":
        return get_label(value.get("id"))
    elif value_type == "monolingualtext":
        return value.get("text")
    elif value_type == "quantity":
        # Resolve unit name and make into string:
        value = value.get("amount", "")
        value = value.lstrip("+")
        unit = value.get("unit", "")
        unit = unit.split("/")[-1]
        if Identifier.QID.match(unit):
            unit = get_label(unit)
            value = f"{value} {unit}"
        return value
    elif isinstance(value, str):
        return value
    else:
        log.warning("Unhandled value type", type=value_type, value=value)
