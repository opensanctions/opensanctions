"""Helpers for Chinese corporate identifier checks.

See https://github.com/opensanctions/opensanctions/issues/5444

Pre-2015 Chinese business registration numbers (注册号) are 15 digits:
``AAAAAA`` (administrative division of the first registering authority)
+ 8-digit sequence + 1 check digit.

The 18-character Unified Social Credit Code (USCC / 统一社会信用代码) embeds
the same 6-digit division code in characters 3–8 (1-based).

When a record carries both, the prefixes normally agree. They can
legitimately differ after a re-registration in another division, so a
mismatch is a maintenance flag (warning), not a hard reject.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from zavod.logs import get_logger

if TYPE_CHECKING:
    from zavod.entity import Entity

log = get_logger(__name__)

# Pre-2015 注册号: exactly 15 ASCII digits.
_REG_NUMBER_RE = re.compile(r"^\d{15}$")
# Compact USCC shape before check-digit validation (GB 32100-2015 charset
# excludes I, O, Z, S, V). Length alone is enough for prefix extraction.
_USCC_SHAPE_RE = re.compile(r"^[0-9A-HJ-NP-RT-UW-Y]{18}$")


def division_code_from_registration_number(value: str) -> str | None:
    """Return the 6-digit GB/T 2260 division prefix of a 15-digit 注册号."""
    compact = value.strip()
    if not _REG_NUMBER_RE.fullmatch(compact):
        return None
    return compact[:6]


def division_code_from_uscc(value: str) -> str | None:
    """Return the 6-digit division code embedded in an 18-char USCC (chars 3–8)."""
    compact = value.strip().upper().replace(" ", "")
    if not _USCC_SHAPE_RE.fullmatch(compact):
        return None
    return compact[2:8]


def iter_china_division_mismatches(
    registration_numbers: list[str],
    usc_codes: list[str],
) -> list[tuple[str, str, str, str]]:
    """Pairs of (reg, uscc, reg_division, uscc_division) that disagree.

    Only values that parse as 15-digit registration numbers and 18-char USCC
    shapes participate; other identifier formats are ignored.
    """
    reg_divs: list[tuple[str, str]] = []
    for reg in registration_numbers:
        division = division_code_from_registration_number(reg)
        if division is not None:
            reg_divs.append((reg, division))

    uscc_divs: list[tuple[str, str]] = []
    for uscc in usc_codes:
        division = division_code_from_uscc(uscc)
        if division is not None:
            uscc_divs.append((uscc, division))

    mismatches: list[tuple[str, str, str, str]] = []
    for reg, reg_division in reg_divs:
        for uscc, uscc_division in uscc_divs:
            if reg_division != uscc_division:
                mismatches.append((reg, uscc, reg_division, uscc_division))
    return mismatches


def check_china_division_agreement(
    entity: "Entity",
    *,
    pending_registration_number: str | None = None,
    pending_uscc: str | None = None,
) -> None:
    """Warn when an entity's 15-digit 注册号 and USCC disagree on division code.

    ``pending_*`` values are included so the check can run from property
    cleaning before the new statement is stored on the entity.

    No-op when either side is missing or not in the expected shape. Does not
    reject values — re-registration can make a mismatch legitimate.
    """
    if not entity.schema.is_a("LegalEntity"):
        return
    if entity.schema.get("registrationNumber") is None:
        return
    if entity.schema.get("uscCode") is None:
        return

    registration_numbers = list(entity.get("registrationNumber"))
    usc_codes = list(entity.get("uscCode"))
    if pending_registration_number is not None:
        registration_numbers.append(pending_registration_number)
    if pending_uscc is not None:
        usc_codes.append(pending_uscc)
    if not registration_numbers or not usc_codes:
        return

    for reg, uscc, reg_division, uscc_division in iter_china_division_mismatches(
        registration_numbers, usc_codes
    ):
        log.warning(
            "Chinese registration number and USCC division codes disagree",
            entity_id=entity.id,
            registration_number=reg,
            usc_code=uscc,
            registration_division=reg_division,
            uscc_division=uscc_division,
        )
