from typing import Any, Dict, Optional, Union
from followthemoney import model
from followthemoney.exc import InvalidData, InvalidModel
from followthemoney.util import gettext
from followthemoney.types import registry
from followthemoney.schema import Schema
from followthemoney.property import Property
from nomenklatura.entity import CompositeEntity
from nomenklatura.statement import Statement
from nomenklatura.util import string_list

from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.runtime.cleaning import value_clean

log = get_logger(__name__)


class Entity(CompositeEntity):
    """Entity for sanctions list entries and adjacent objects.

    Add utility methods to the [EntityProxy](https://followthemoney.tech/reference/python/followthemoney/proxy.html#EntityProxy) for
    extracting data from sanctions lists and for auditing parsing errors to structured logging.
    """  # noqa

    def __init__(self, dataset: Dataset, data: Dict[str, Any], cleaned: bool = True):
        super().__init__(dataset, data, cleaned=cleaned)
        self.dataset: Dataset = dataset

    def make_id(self, *parts: Any) -> str:
        raise NotImplementedError

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

        if self.id is None:
            raise InvalidData("Cannot add statement to entity without ID!")

        if lang is not None:
            lang = registry.language.clean_text(lang)

        for prop_, clean in value_clean(
            self, prop, value, cleaned=cleaned, fuzzy=fuzzy, format=format
        ):
            if original_value is None and clean != value:
                original_value = value

            stmt = Statement(
                entity_id=self.id,
                prop=prop_.name,
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
    ) -> None:
        """Set a property on an entity. If the entity is of a schema that doesn't
        have the given property, also modify the schema (e.g. if something has a
        birthDate, assume it's a Person, not a LegalEntity).
        """
        prop_ = self.schema.get(prop)
        if prop_ is not None:
            return self.add(prop, values, cleaned=cleaned, fuzzy=fuzzy, format=format)

        schema_ = model.get(schema)
        if schema_ is None:
            raise InvalidModel("Invalid schema: %s" % schema)
        prop_ = schema_.get(prop)
        if prop_ is None:
            raise InvalidModel("Invalid prop: %s" % prop)
        for text in string_list(values):
            for norm_prop_, clean in value_clean(
                self, prop_, text, cleaned=cleaned, fuzzy=fuzzy, format=format
            ):
                if original_value is None and clean != text:
                    original_value = text
                self.add_schema(schema)
                self.unsafe_add(
                    norm_prop_,
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
        return data
