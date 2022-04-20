import structlog
from banal import ensure_list
from prefixdate.precision import Precision
from typing import Any, Dict, Optional, Union
from followthemoney import model
from followthemoney.exc import InvalidData
from followthemoney.types import registry
from followthemoney.model import Model
from followthemoney.schema import Schema
from followthemoney.property import Property
from nomenklatura.entity import CompositeEntity

from opensanctions.core.lookups import type_lookup
from opensanctions.core.dataset import Dataset

log = structlog.get_logger(__name__)


class Entity(CompositeEntity):
    """Entity for sanctions list entries and adjacent objects.

    Add utility methods to the :py:class:`followthemoney.proxy:EntityProxy` for
    extracting data from sanctions lists and for auditing parsing errors to
    structured logging.
    """

    def __init__(
        self,
        model: Model,
        data: Dict[str, Any],
        key_prefix: Optional[str] = None,
        cleaned: bool = True,
    ) -> None:
        super().__init__(model, data, key_prefix=key_prefix, cleaned=cleaned)
        self.target = data.get("target", False)
        self.first_seen = data.get("first_seen", None)
        self.last_seen = data.get("last_seen", None)

    def make_id(self, *parts: Any) -> str:
        raise NotImplementedError

    def _lookup_values(self, prop, values):
        for value in ensure_list(values):
            yield from type_lookup(prop.type, value)

    def _verbose_clean(self, prop, value, fuzzy, format):
        if value is None or len(str(value).strip()) == 0:
            return None
        clean = prop.type.clean(value, proxy=self, fuzzy=fuzzy, format=format)
        if clean is not None:
            if prop.type == registry.date:
                # none of the information in OpenSanctions is time-critical
                clean = clean[: Precision.DAY.value]
            return clean
        if prop.type == registry.phone:
            return clean
        log.warning(
            "Rejected property value",
            entity=self,
            prop=prop.name,
            value=value,
        )
        return None

    def add(
        self,
        prop: Union[str, Property],
        values: Any,
        cleaned: bool = False,
        quiet: bool = False,
        fuzzy: bool = False,
        format: Optional[str] = None,
    ):
        if cleaned:
            super().add(
                prop, values, cleaned=True, quiet=quiet, fuzzy=fuzzy, format=format
            )
            return

        prop_name = self._prop_name(prop, quiet=quiet)
        if prop_name is None:
            return
        prop = self.schema.properties[prop_name]

        for value in self._lookup_values(prop, values):
            clean = self._verbose_clean(prop, value, fuzzy, format)
            self.unsafe_add(prop, clean, cleaned=True)

    def add_cast(
        self,
        schema: Union[str, Schema],
        prop: Union[str, Property],
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
        for value in self._lookup_values(prop_, values):
            clean = self._verbose_clean(prop_, value, fuzzy, format)
            if clean is not None:
                self.add_schema(schema)
                self.unsafe_add(prop_, clean, cleaned=True)

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
        data["first_seen"] = self.first_seen
        data["last_seen"] = self.last_seen
        data["target"] = self.target
        data["caption"] = self.caption
        return data
