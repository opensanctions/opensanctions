"""Dataset tags derived from other metadata at export time.

Some tags restate metadata the dataset already carries, so instead of copying
them into every dataset YAML they are derived when the metadata is exported:

* ``publisher.<country>`` from ``publisher.country``, for official, non-PEP
  publishers with a specific country.
* ``target.<territory>`` from the ``target_territories`` of the sanctions
  programs that name the dataset as their primary ``dataset``.

Derived tags only appear in the exported metadata (``index.json`` and the
catalogs). They are not added to ``dataset.model.tags``, so ``#tag`` queries
against a catalog loaded from the dataset YAMLs can't select on them. Catalogs
loaded from the published metadata do include them.
"""

from typing import TYPE_CHECKING

from zavod.stateful.programs import get_all_programs_by_key

if TYPE_CHECKING:
    from zavod.meta.dataset import Dataset

# Pseudo-territory for global/international publishers and programs. It says
# nothing about where a list comes from or who it targets, so it's never a tag.
GLOBAL = "zz"

EU_MEMBERS = frozenset(
    "at be bg cy cz de dk ee es fi fr gr hr hu ie it lt lu lv mt nl pl pt ro se si sk".split()
)
EFTA_MEMBERS = frozenset({"ch", "is", "li", "no"})

# Blocs of publisher countries. A dataset whose publisher country is a member
# also gets ``publisher.<bloc>``.
# TODO: placeholder bloc to exercise the mechanism; replace with the agreed
# blocs (e.g. ``west``) once their membership is settled.
# TODO: move these somewhere more central for reuse once they're settled
# and if we decide to keep them.
PUBLISHER_BLOCS: dict[str, frozenset[str]] = {
    "eu_efta": EU_MEMBERS | EFTA_MEMBERS,
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


def derive_target_tags(dataset: "Dataset") -> list[str]:
    """Derive ``target.<territory>`` from the programs that name this dataset.

    Programs that have ended are skipped, as is the global pseudo-territory.
    Legacy programs count, since their designations persist.
    """
    tags: list[str] = []
    for program in get_all_programs_by_key().values():
        if program.dataset != dataset.name or program.status == "ended":
            continue
        for territory in program.target_territories:
            if territory == GLOBAL:
                continue
            tag = f"target.{territory}"
            if tag not in tags:
                tags.append(tag)
    return tags


def derive_tags(dataset: "Dataset") -> list[str]:
    """Return the dataset's tags with the derived ones appended.

    Hand-set tags come first, in their original order; derived tags that are
    already set by hand aren't repeated. Collections get no derived tags.
    """
    tags = list(dataset.model.tags)
    if dataset.is_collection:
        return tags
    for tag in derive_publisher_tags(dataset) + derive_target_tags(dataset):
        if tag not in tags:
            tags.append(tag)
    return tags
