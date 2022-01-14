import structlog
from prefixdate import Precision
from nomenklatura.resolver import Identifier
from opensanctions.wikidata.api import get_label

from opensanctions.core import Context

log = structlog.getLogger(__name__)
PRECISION = {
    11: Precision.DAY,
    10: Precision.MONTH,
    9: Precision.YEAR,
}


async def snak_value_to_string(context: Context, value_type, value):
    if value_type is None:
        return None
    elif value_type == "time":
        time = value.get("time")
        if time is not None:
            time = time.strip("+")
            prec = PRECISION.get(value.get("precision"), Precision.DAY)
            time = time[: prec.value]
            # Date limit in FtM. These will be removed by the death filter:
            time = max("1001", time)
        return time
    elif value_type == "wikibase-entityid":
        return await get_label(context, value.get("id"))
    elif value_type == "monolingualtext":
        return value.get("text")
    elif value_type == "quantity":
        # Resolve unit name and make into string:
        value = value.get("amount", "")
        value = value.lstrip("+")
        unit = value.get("unit", "")
        unit = unit.split("/")[-1]
        if Identifier.QID.match(unit):
            unit = await get_label(context, unit)
            value = f"{value} {unit}"
        return value
    elif isinstance(value, str):
        return value
    else:
        log.warning("Unhandled value type", type=value_type, value=value)
