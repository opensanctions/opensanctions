"""Helpers for the Umbraco Content Delivery API behind the Israeli Ministry of
Defense's National Bureau for Counter Terror Financing (NBCTF) site."""

import re
from typing import Any

from zavod import Context, Entity
from zavod import helpers as h

BASE_URL = "https://matal.mod.gov.il"
API_URL = f"{BASE_URL}/umbraco/delivery/api/v2/content"
LANGS = {"en": "eng", "he": "heb", "ar": "ara"}
SPLITS = [";", "Id Number", "a) ", "b) ", "c) ", " :", "\n", "• "]
HEBREW = re.compile(r"[֐-׿]")
ARABIC = re.compile(r"[؀-ۿ]")
# e.g. 'the source states only the year ("2010"), so the day shown is a placeholder.'
PARTIAL_DATE = re.compile(
    r'^the source states only the (?:year|month and year) \("([^"]+)"\)'
)
# Stand-ins for "no date"; a comment explains 9999-09-09 where it's used
NO_DATES = {"9999-09-09", "0001-01-01"}

Item = dict[str, Any]


def fetch_content(context: Context, content_type: str, lang: str) -> dict[str, Item]:
    """Fetch all items of a content type in one culture, keyed by item ID."""
    items: dict[str, Item] = {}
    total = None
    while total is None or len(items) < total:
        data = context.fetch_json(
            API_URL,
            params={
                "filter": f"contentType:{content_type}",
                "sort": "createDate:asc",
                "skip": len(items),
                "take": 1000,
            },
            headers={"Accept-Language": lang},
        )
        total = data["total"]
        if len(data["items"]) == 0:
            break
        for item in data["items"]:
            items[item["id"]] = item
    if len(items) != total:
        raise ValueError(f"Got {len(items)} of {total} {content_type} items ({lang})")
    return items


def fetch_variants(context: Context, content_type: str) -> dict[str, list[Item]]:
    """Fetch all items of a content type, each as a list of its [en, he, ar]
    culture variants, keyed by item ID."""
    by_lang = {lang: fetch_content(context, content_type, lang) for lang in LANGS}
    return {
        item_id: [by_lang[lang][item_id] for lang in LANGS] for item_id in by_lang["en"]
    }


def source_url(item: Item) -> str:
    return f"{BASE_URL}{item['route']['path']}"


def drop_fallbacks(
    variants: list[Item], prop: str
) -> tuple[str | None, str | None, str | None]:
    """Names in [en, he, ar], dropping copies from another language."""
    name_en, name_he, name_ar = [item["properties"][prop] for item in variants]
    if name_en == name_he and HEBREW.search(name_he or "") is not None:
        name_en = None
    if name_ar in (name_en, name_he) and ARABIC.search(name_ar or "") is None:
        name_ar = None
    return name_en, name_he, name_ar


def apply_aliases(entity: Entity, variants: list[Item], prop: str) -> None:
    for item, lang in zip(variants, LANGS.values()):
        for alias in item["properties"][prop] or []:
            entity.add("alias", h.multi_split(alias, SPLITS), lang=lang)


def pop_blocks(
    props: dict[str, Any], key: str, element_type: str
) -> list[dict[str, Any]]:
    """Pop a block list property and return the properties of each block.

    Umbraco block list values look like
    `[{"elementType": "addressBlock", "properties": {"city": ..., ...}}]`.
    Raises if a block isn't of the expected `element_type`. Callers pop the
    fields they use from each block and audit the rest.
    """
    blocks: list[dict[str, Any]] = []
    for block in props.pop(key) or []:
        if block["elementType"] != element_type:
            raise ValueError(f"Unexpected block type in {key}: {block}")
        blocks.append(dict(block["properties"]))
    return blocks


def split_ids(value: str | None) -> list[str]:
    if value is None:
        return []
    # Keep labels such as "מס' דרכון סורי:" on the same line as their number
    return h.multi_split(value.replace(":\n", ": "), SPLITS)


def apply_date(entity: Entity, prop: str, value: str | None) -> None:
    if value is not None and value not in NO_DATES:
        h.apply_date(entity, prop, value)


def apply_partial_date(
    context: Context,
    entity: Entity,
    prop: str,
    dates: list[str | None],
    comments: list[str] | None,
) -> None:
    """Takes a date and a list of comments about that date field.

    Applies the date unless there's a comment. Known comments indicate that part
    of the date is a placeholder and shouldn't be used as whole, so we use the
    year from the comment instead."""
    if comments is None:
        for date in dates:
            apply_date(entity, prop, date)
        return
    for comment in comments:
        match = PARTIAL_DATE.match(comment)
        if match is None:
            context.log.warning("Unexpected date comment", prop=prop, comment=comment)
            continue
        h.apply_date(entity, prop, match.group(1))


def apply_operative_details(
    context: Context,
    entity: Entity,
    props: dict[str, Any],
    dob_comments: list[str] | None,
) -> None:
    """Apply the personal details of an operative item to a Person.

    Pops the properties it uses from `props`, leaving the rest to the caller."""
    dobs = [b.pop("date") for b in pop_blocks(props, "datesOfBirth", "dateBlock")]
    apply_partial_date(context, entity, "birthDate", dobs, dob_comments)
    for block in pop_blocks(props, "passportDetails", "passportDetailsBlock"):
        entity.add("nationality", h.multi_split(block.pop("nationality"), ["\n"]))
        number = block.pop("passportNumber")
        if number != "Unknown":
            entity.add("idNumber", split_ids(number))
        context.audit_data(block, ignore=["country"])
    for block in pop_blocks(
        props, "identificationDocuments", "identificationDocumentBlock"
    ):
        entity.add("idNumber", split_ids(block.pop("identificationNumber")))
        context.audit_data(block, ignore=["country", "documentType"])
    entity.add("phone", props.pop("phoneNumbers"))
    entity.add("email", props.pop("emailAddresses"))
