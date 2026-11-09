"""Dataset tags derived from other metadata.

Some tags restate metadata the dataset already carries, so instead of copying
them into every dataset YAML they are derived when the dataset is loaded:

* ``publisher.<country>`` from ``publisher.country``, for official, non-PEP
  publishers with a specific country.
* ``publisher.<bloc>`` for each bloc in ``PUBLISHER_BLOCS`` that the
  publisher country belongs to, under the same conditions.

Derived tags are part of ``dataset.model.tags``, so they appear in the
exported metadata and ``#tag`` dataset queries can select on them. Deriving is
idempotent, so metadata loaded back from the archive, which already carries
the derived tags, keeps the same tags.
"""

from typing import TYPE_CHECKING

from zavod.constants import (
    GLOBAL,
    EU_MEMBERS,
    EFTA_MEMBERS,
    EUROPEAN_MICROSTATES,
    EUROPE_OTHER,
)

if TYPE_CHECKING:
    from zavod.meta.dataset import Dataset

PUBLISHER_BLOCS: dict[str, frozenset[str]] = {
    "europe": EU_MEMBERS | EFTA_MEMBERS | EUROPEAN_MICROSTATES | EUROPE_OTHER | {"eu"},
}


def derive_publisher_tags(dataset: "Dataset") -> list[str]:
    """Derive ``publisher.<country>`` and ``publisher.<bloc>`` from the publisher.

    Nothing is derived for unofficial publishers, for PEP datasets (where the
    publisher country adds no insight), or when the country is missing or
    global.
    """
    publisher = dataset.model.publisher
    if publisher is None or publisher.official is not True:
        return []
    if "list.pep" in dataset.model.tags:
        return []
    country = publisher.country
    if country is None or country == GLOBAL:
        return []
    tags = [f"publisher.{country}"]
    for bloc, members in PUBLISHER_BLOCS.items():
        if country in members:
            tags.append(f"publisher.{bloc}")
    return tags


def derive_tags(dataset: "Dataset") -> list[str]:
    """Return the dataset's tags with the derived ones appended.

    Hand-set tags come first, in their original order; derived tags that are
    already set by hand aren't repeated. Collections get no derived tags.
    """
    tags = list(dataset.model.tags)
    if dataset.is_collection:
        return tags
    for tag in derive_publisher_tags(dataset):
        if tag not in tags:
            tags.append(tag)
    return tags
