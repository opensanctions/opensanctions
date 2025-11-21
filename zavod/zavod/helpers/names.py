import re
from typing import List, Optional, cast

from followthemoney.util import join_text
from normality import squash_spaces
from rigour.data.names import data
from rigour.names.check import is_nullword

from zavod import settings
from zavod.context import Context
from zavod.entity import Entity
from zavod.extract.names.clean import (
    LLM_MODEL_VERSION,
    CleanNames,
)

# alias clean_names so that it could be imported from here
from zavod.extract.names.clean import clean_names as clean_names
from zavod.stateful.review import (
    Review,
    TextSourceValue,
    review_extraction,
)

REGEX_AND = re.compile(r"(\band\b|&|\+)", re.I)
REGEX_LNAME_FNAME = re.compile(r"^\w+, \w+$", re.I)
REGEX_CLEAN_COMMA = re.compile(
    r", \b(LLC|L\.L\.C|Inc|Jr|INC|L\.P|LP|Sr|III|II|IV|S\.A|LTD|USA INC|\(?A/K/A|\(?N\.K\.A|\(?N/K/A|\(?F\.K\.A|formerly known as|INCORPORATED)\b",  # noqa
    re.I,
)
KNOWN_AS_PATTERNS = [re.escape(phrase) for phrase in data.STOPPHRASES]
PATTERN_KNOWN_AS = rf"(\b({'|'.join(KNOWN_AS_PATTERNS)})\b)"
REGEX_KNOWN_AS = re.compile(PATTERN_KNOWN_AS, re.I)


def make_name(
    full: Optional[str] = None,
    name1: Optional[str] = None,
    first_name: Optional[str] = None,
    given_name: Optional[str] = None,
    name2: Optional[str] = None,
    second_name: Optional[str] = None,
    middle_name: Optional[str] = None,
    name3: Optional[str] = None,
    patronymic: Optional[str] = None,
    matronymic: Optional[str] = None,
    name4: Optional[str] = None,
    name5: Optional[str] = None,
    tail_name: Optional[str] = None,
    last_name: Optional[str] = None,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
) -> Optional[str]:
    """Provides a standardised way of assembling the components of a human name.
    This does a whole lot of cultural ignorance work, so YMMV.

    Args:
        full: The full name if available (this will otherwise be generated).
        name1: The first name if numeric parts are used.
        first_name: The first name.
        given_name: The given name (also first name).
        name2: The second name if numeric parts are used.
        second_name: The second name.
        middle_name: The middle name.
        name3: The third name if numeric parts are used.
        patronymic: The patronymic (father-derived) name.
        matronymic: The matronymic (mother-derived) name.
        name4: The fourth name if numeric parts are used.
        name5: The fifth name if numeric parts are used.
        tail_name: A secondary last name.
        last_name: The last/family name name.
        prefix: A prefix to the name (e.g. "Mr").
        suffix: A suffix to the name (e.g. "Jr").

    Returns:
        The full name.
    """
    if full is not None:
        full = squash_spaces(full)
        if len(full) > 0:
            return full
    return join_text(
        prefix,
        name1,
        first_name,
        given_name,
        name2,
        second_name,
        middle_name,
        name3,
        patronymic,
        matronymic,
        name4,
        name5,
        tail_name,
        last_name,
        suffix,
    )


def set_name_part(
    entity: Entity,
    prop: str,
    value: Optional[str],
    quiet: bool,
    lang: Optional[str],
    origin: Optional[str],
) -> None:
    if value is None:
        return
    prop_ = entity.schema.get(prop)
    if prop_ is None:
        if quiet:
            return
        raise TypeError("Invalid prop: %s [value: %r]" % (prop, value))
    entity.add(prop_, value, lang=lang, origin=origin)


def apply_name(
    entity: Entity,
    full: Optional[str] = None,
    name1: Optional[str] = None,
    first_name: Optional[str] = None,
    given_name: Optional[str] = None,
    name2: Optional[str] = None,
    second_name: Optional[str] = None,
    middle_name: Optional[str] = None,
    name3: Optional[str] = None,
    patronymic: Optional[str] = None,
    matronymic: Optional[str] = None,
    name4: Optional[str] = None,
    name5: Optional[str] = None,
    tail_name: Optional[str] = None,
    last_name: Optional[str] = None,
    maiden_name: Optional[str] = None,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    alias: bool = False,
    name_prop: str = "name",
    is_weak: bool = False,
    quiet: bool = False,
    lang: Optional[str] = None,
    origin: Optional[str] = None,
) -> None:
    """A standardised way to set a name for a person or other entity, which handles
    normalising the categories of names found in source data to the correct properties
    (e.g. "family name" becomes "lastName").

    Args:
        entity: The entity to set the name on.
        full: The full name if available (this will otherwise be generated).
        name1: The first name if numeric parts are used.
        first_name: The first name.
        given_name: The given name (also first name).
        name2: The second name if numeric parts are used.
        second_name: The second name.
        middle_name: The middle name.
        name3: The third name if numeric parts are used.
        patronymic: The patronymic (father-derived) name.
        matronymic: The matronymic (mother-derived) name.
        name4: The fourth name if numeric parts are used.
        name5: The fifth name if numeric parts are used.
        tail_name: A secondary last name.
        last_name: The last/family name name.
        maiden_name: The maiden name (before marriage).
        prefix: A prefix to the name (e.g. "Mr").
        suffix: A suffix to the name (e.g. "Jr").
        alias: If this is an alias name.
        name_prop: The property to set the full name on.
        is_weak: If this is a weak alias name.
        quiet: If this should not raise errors on invalid properties.
        lang: The language of the name.
        origin: The origin of the name (e.g. a GPT model).
    """
    if not is_weak:
        set_name_part(entity, "firstName", given_name, quiet, lang, origin)
        set_name_part(entity, "firstName", first_name, quiet, lang, origin)
        set_name_part(entity, "secondName", second_name, quiet, lang, origin)
        set_name_part(entity, "middleName", middle_name, quiet, lang, origin)
        set_name_part(entity, "fatherName", patronymic, quiet, lang, origin)
        set_name_part(entity, "motherName", matronymic, quiet, lang, origin)
        set_name_part(entity, "lastName", last_name, quiet, lang, origin)
        set_name_part(entity, "lastName", maiden_name, quiet, lang, origin)
        set_name_part(entity, "firstName", name1, quiet, lang, origin)
        set_name_part(entity, "secondName", name2, quiet, lang, origin)
        set_name_part(entity, "middleName", name3, quiet, lang, origin)
        set_name_part(entity, "middleName", name4, quiet, lang, origin)
        set_name_part(entity, "middleName", name5, quiet, lang, origin)
        set_name_part(entity, "lastName", tail_name, quiet, lang, origin)
    if alias:
        name_prop = "alias"
    if is_weak:
        name_prop = "weakAlias"
    full = make_name(
        full=full,
        name1=name1,
        first_name=first_name,
        given_name=given_name,
        name2=name2,
        second_name=second_name,
        middle_name=middle_name,
        name3=name3,
        patronymic=patronymic,
        matronymic=matronymic,
        name4=name4,
        name5=name5,
        tail_name=tail_name,
        last_name=last_name,
        prefix=prefix,
        suffix=suffix,
    )
    if full is not None and len(full):
        entity.add(name_prop, full, quiet=quiet, lang=lang, origin=origin)


def split_comma_names(context: Context, text: str) -> List[str]:
    """Split a string of multiple names that may contain company and individual names,
    some including commas, into individual names without breaking partnership names
    like "A, B and C Inc" or individuals like "Smith, Jane".

    To make life easier, commas are stripped from company type suffixes like "Blue, LLC"

    If the string can't be split into whole names reliably, a datapatch is looked up
    under the `comma_names` key, which should contain a list of names in the `names`
    attribute. If no match is found, the name is returned as a single item list,
    and a warning emitted.
    """
    text = squash_spaces(text)
    if len(text) == 0:
        return []

    text = REGEX_CLEAN_COMMA.sub(r" \1", text)
    # If the string ends in a comma, the last comma is unnecessary (e.g. Goldman Sachs & Co. LLC,)
    if text.endswith(","):
        text = text[:-1]

    if not REGEX_AND.search(text) and not REGEX_LNAME_FNAME.match(text):
        names = [n.strip() for n in text.split(",")]
        return names
    else:
        if ("," in text) or (" and " in text):
            res = context.lookup("comma_names", text)
            if res:
                return cast("List[str]", res.names)
            else:
                context.log.warning(
                    "Not sure how to split on comma or and.", text=text.lower()
                )
                return [text]
        else:
            return [text]


def is_name_irregular(entity: Entity, string: Optional[str]) -> bool:
    """Determine whether a name string potentially needs cleaning."""
    string = squash_spaces(string or "")

    if not string:
        return False

    for spec in entity.dataset.names.specs_for_schema(entity.schema):
        # Contains rejected characters
        for char in spec.reject_chars_consolidated:
            if char in string:
                return True

        # is nullword
        if not spec.allow_nullwords and is_nullword(string, normalize=True):
            return True

        # min length
        if len(string) < spec.min_chars:
            return True

        # requires space
        if spec.require_space and " " not in string:
            return True

    # contains a known-as phrase
    if REGEX_KNOWN_AS.search(string):
        return True

    return False


def apply_names(
    entity: Entity,
    review: Review[CleanNames],
    alias: bool = False,
    lang: Optional[str] = None,
) -> None:
    field_props = [
        ("full_name", "alias" if alias else "name"),
        ("alias", "alias"),
        ("weak_alias", "weakAlias"),
        ("previous_name", "previousName"),
    ]
    if not review.accepted:
        prop = "alias" if alias else "name"
        entity.add(prop, review.source_value, lang=lang)
        return

    for field_name, prop in field_props:
        for name in getattr(review.extracted_data, field_name):
            entity.add(
                prop,
                name,
                lang=lang,
                origin=review.origin,
                original_value=review.source_value,
            )


def review_names(
    context: Context,
    entity: Entity,
    string: Optional[str],
    lang: Optional[str] = None,
    alias: bool = False,
) -> Optional[Review[CleanNames]]:
    """
    Clean names if needed, then post them for review.

    Args:
        context: The current context.
        entity: The entity to apply names to.
        string: The raw name(s) string.
        alias: If this is known to be an alias and not a primary name.
        lang: The language of the name, if known.
    """
    if not string:
        return None

    if settings.CI or not is_name_irregular(entity, string):
        prop = "alias" if alias else "name"
        entity.add(prop, string, lang=lang)
        return None

    names = clean_names(context, string)

    source_value = TextSourceValue(key_parts=string, label="name", text=string)
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=names,
        origin=LLM_MODEL_VERSION,
    )
    return review


def apply_reviewed_names(
    context: Context,
    entity: Entity,
    string: Optional[str],
    lang: Optional[str] = None,
    alias: bool = False,
) -> None:
    """
    Clean names if needed, then post them for review.
    Cleaned names are applied to an entity if accepted, falling back
    to applying the original string as the name or alias if not.

    Also falls back to applying the original string if the CI environment
    variable is truthy, so that crawlers using this can run in CI.

    Args:
        context: The current context.
        entity: The entity to apply names to.
        string: The raw name(s) string.
        alias: If this is known to be an alias and not a primary name.
        lang: The language of the name, if known.
    """
    review = review_names(context, entity, string, lang=lang, alias=alias)
    if review is None:
        return
    apply_names(entity, review, alias=alias, lang=lang)
