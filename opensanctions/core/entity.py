from normality import stringify
from zavod.logs import get_logger
from functools import cached_property
from prefixdate.precision import Precision
from typing import Any, Dict, Generator, List, Optional, Tuple, Union
from followthemoney import model
from followthemoney.exc import InvalidData
from followthemoney.util import value_list
from followthemoney.types import registry
from followthemoney.proxy import P
from followthemoney.schema import Schema
from followthemoney.property import Property
from nomenklatura.statement import StatementProxy

from opensanctions.core.lookups import type_lookup
from opensanctions.util import pick_name

log = get_logger(__name__)


class Entity(StatementProxy):
    """Entity for sanctions list entries and adjacent objects.

    Add utility methods to the :py:class:`followthemoney.proxy:EntityProxy` for
    extracting data from sanctions lists and for auditing parsing errors to
    structured logging.
    """

    @cached_property
    def caption(self) -> str:
        """The user-facing label to be used for this entity. This checks a list
        of properties defined by the schema (caption) and returns the first
        available value. If no caption is available, return the schema label."""
        is_thing = self.schema.is_a("Thing")
        for prop in self.schema.caption:
            values = self.get(prop)
            if is_thing and prop == "name" and len(values) > 1:
                all_names = self.get_type_values(registry.name)
                name = pick_name(tuple(values), tuple(all_names))
                if name is not None:
                    return name
            for value in values:
                return value
        return self.schema.label

    def make_id(self, *parts: Any) -> str:
        raise NotImplementedError

    def clean_value(
        self,
        prop: Property,
        value: Any,
        cleaned: bool = False,
        fuzzy: bool = False,
        format: Optional[str] = None,
    ) -> List[str]:
        results: List[str] = []
        if value is None:
            return results
        if cleaned:
            return [value]
        for form in type_lookup(prop.type, value):
            if form is None or len(str(form).strip()) == 0:
                continue
            clean = prop.type.clean(
                form,
                proxy=self,
                fuzzy=fuzzy,
                format=format,
            )
            if clean is not None:
                if prop.type == registry.date:
                    # none of the information in OpenSanctions is time-critical
                    clean = clean[: Precision.DAY.value]
                results.append(clean)
                continue
            if prop.type == registry.phone:
                results.append(value)
                continue
            log.warning(
                "Rejected property value",
                entity_id=self.id,
                prop=prop.name,
                value=value,
            )
        return results

    def clean_values(
        self,
        prop: Property,
        values: Any,
        cleaned: bool = False,
        fuzzy: bool = False,
        format: Optional[str] = None,
    ) -> Generator[Tuple[str, str], None, None]:
        if values is None:
            return
        for val in value_list(values):
            for clean in self.clean_value(
                prop, val, cleaned=cleaned, fuzzy=fuzzy, format=format
            ):
                if prop.type == registry.entity:
                    val = None
                else:
                    val = stringify(val)
                    if val == clean:
                        val = None
                yield (val, clean)

    def add(
        self,
        prop: P,
        values: Any,
        cleaned: bool = False,
        quiet: bool = False,
        fuzzy: bool = False,
        format: Optional[str] = None,
    ) -> None:
        prop_name = self._prop_name(prop, quiet=quiet)
        if prop_name is None:
            return None
        prop = self.schema.properties[prop_name]
        for original, value in self.clean_values(
            prop,
            values,
            cleaned=cleaned,
            fuzzy=fuzzy,
            format=format,
        ):
            self.claim(prop, value, original_value=original, cleaned=True)
        return None

    def add_cast(
        self,
        schema: str,
        prop: str,
        values: Any,
        cleaned: bool = False,
        fuzzy: bool = False,
        format: Optional[str] = None,
    ):
        """Set a property on an entity. If the entity is of a schema that doesn't
        have the given property, also modify the schema (e.g. if something has a
        birthDate, assume it's a Person, not a LegalEntity).
        """
        prop_ = self.schema.get(prop)
        if prop_ is not None:
            return self.add(prop, values, cleaned=cleaned, fuzzy=fuzzy, format=format)

        schema_ = model.get(schema)
        if schema_ is None:
            raise RuntimeError("Invalid schema: %s" % schema)
        prop_ = schema_.get(prop)
        if prop_ is None:
            raise RuntimeError("Invalid prop: %s" % prop)
        for original, value in self.clean_values(
            prop_,
            values,
            cleaned=cleaned,
            fuzzy=fuzzy,
            format=format,
        ):
            self.add_schema(schema)
            self.claim(prop_, value, original_value=original, cleaned=True)

    def add_schema(self, schema: Union[str, Schema]) -> None:
        """Try to apply the given schema to the current entity, making it more
        specific (e.g. turning a `LegalEntity` into a `Company`). This raises an
        exception if the current and new type are incompatible."""
        try:
            self.schema = model.common_schema(self.schema, schema)
        except InvalidData as exc:
            raise InvalidData(f"{self.id}: {exc}") from exc

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["caption"] = self.caption
        return data
