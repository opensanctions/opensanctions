from zavod.logs import get_logger
from prefixdate.precision import Precision
from typing import Any, Dict, List, Optional, Union
from followthemoney import model
from followthemoney.exc import InvalidData
from followthemoney.util import gettext
from followthemoney.types import registry
from followthemoney.schema import Schema
from followthemoney.property import Property
from nomenklatura.entity import CompositeEntity
from nomenklatura.statement import Statement
from nomenklatura.util import string_list

from opensanctions.core.dataset import Dataset
from opensanctions.core.lookups import type_lookup

log = get_logger(__name__)


class Entity(CompositeEntity):
    """Entity for sanctions list entries and adjacent objects.

    Add utility methods to the :py:class:`followthemoney.proxy:EntityProxy` for
    extracting data from sanctions lists and for auditing parsing errors to
    structured logging.
    """

    def __init__(
        self,
        dataset: Dataset,
        data: Dict[str, Any],
        cleaned: bool = True,
    ):
        super().__init__(dataset, data, cleaned=cleaned)
        self.dataset: Dataset = dataset
        self.last_change: Optional[str] = None

    def make_id(self, *parts: Any) -> str:
        raise NotImplementedError

    def lookup_clean(
        self,
        prop: Property,
        value: Optional[str],
        cleaned: bool = False,
        fuzzy: bool = False,
        format: Optional[str] = None,
    ) -> List[str]:
        results: List[str] = []
        for item in type_lookup(self.dataset, prop.type, value):
            clean: Optional[str] = item
            if not cleaned:
                clean = prop.type.clean_text(
                    item,
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
                # Do not have capacity to clean all phone numbers, allow broken ones
                results.append(item)
                continue
            log.warning(
                "Rejected property value",
                entity_id=self.id,
                prop=prop.name,
                value=value,
            )
        return results

    def unsafe_add(
        self,
        prop: Property,
        value: Optional[str],
        cleaned: bool = False,
        fuzzy: bool = False,
        format: Optional[str] = None,
        quiet: bool = False,
        schema: Optional[str] = None,
        dataset: Optional[str] = None,
        seen: Optional[str] = None,
        lang: Optional[str] = None,
        original_value: Optional[str] = None,
    ) -> None:
        """Add a statement to the entity, possibly the value."""
        if value is None or len(value) == 0:
            return

        # Don't allow setting the reverse properties:
        if prop.stub:
            if quiet:
                return None
            msg = gettext("Stub property (%s): %s")
            raise InvalidData(msg % (self.schema, prop))

        if lang is not None:
            lang = registry.language.clean_text(lang)

        for clean in self.lookup_clean(
            prop, value, cleaned=cleaned, fuzzy=fuzzy, format=format
        ):
            if original_value is None and clean != value:
                original_value = value

            stmt = Statement(
                entity_id=self.id,
                prop=prop.name,
                schema=schema or self.schema.name,
                value=clean,
                dataset=dataset or self.dataset.name,
                lang=lang,
                original_value=original_value,
                first_seen=seen,
            )
            self.add_statement(stmt)

    def add_cast(
        self,
        schema: str,
        prop: str,
        values: Any,
        cleaned: bool = False,
        fuzzy: bool = False,
        format: Optional[str] = None,
        lang: Optional[str] = None,
        original_value: Optional[str] = None,
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
        for text in string_list(values):
            for clean in self.lookup_clean(
                prop_, text, cleaned=cleaned, fuzzy=fuzzy, format=format
            ):
                if original_value is None and clean != text:
                    original_value = text
                self.add_schema(schema)
                self.unsafe_add(
                    prop_,
                    clean,
                    cleaned=True,
                    lang=lang,
                    original_value=original_value,
                )

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
        data["target"] = self.target or False
        data["last_change"] = self.last_change
        return data
