"""Dataset tags derived from other metadata at export time.

Some tags restate metadata the dataset already carries, so instead of copying
them into every dataset YAML they are derived when the metadata is exported:

* ``publisher.<country>`` from ``publisher.country``, for official, non-PEP
  publishers with a specific country.
* ``publisher.<bloc>`` for each bloc in ``PUBLISHER_BLOCS`` that the
  publisher country belongs to, under the same conditions.

Derived tags only appear in the exported metadata (``index.json`` and the
catalogs). They are not added to ``dataset.model.tags``, so ``#tag`` queries
against a catalog loaded from the dataset YAMLs can't select on them. Catalogs
loaded from the published metadata do include them.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from zavod.meta.dataset import Dataset

# Pseudo-territory for global/international publishers. It says nothing about
# where a list comes from, so it's never a tag.
GLOBAL = "zz"

EU_MEMBERS = frozenset(
    "at be bg cy cz de dk ee es fi fr gr hr hu ie it lt lu lv mt nl pl pt ro se si sk".split()
)
EFTA_MEMBERS = frozenset({"ch", "is", "li", "no"})
# TODO: fact check that all of these should go into `europe`
UK_AND_CROWN_DEPENDENCIES = frozenset({"gb", "gg", "im", "je"})
EUROPEAN_MICROSTATES = frozenset({"ad", "mc", "sm", "va"})
EUROPE_OTHER = frozenset({"al", "ba", "md", "me", "mk", "rs", "ua", "xk"})

# Blocs of publisher countries. A dataset whose publisher country is a member
# also gets ``publisher.<bloc>``.
# TODO: move these somewhere more central for reuse once they're settled
PUBLISHER_BLOCS: dict[str, frozenset[str]] = {
    "europe": EU_MEMBERS
    | EFTA_MEMBERS
    | UK_AND_CROWN_DEPENDENCIES
    | EUROPEAN_MICROSTATES
    | EUROPE_OTHER
    | {"eu"},
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
