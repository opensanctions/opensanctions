"""Tests for Chinese registration number / USCC division cross-check (#5444)."""

from structlog.testing import capture_logs

from zavod.runtime.china_ids import (
    check_china_division_agreement,
    division_code_from_registration_number,
    division_code_from_uscc,
    iter_china_division_mismatches,
)


# Yangzhou Yangjie example from #5444 / #4374
MATCHING_REG = "321000400012591"
MATCHING_USCC = "913210007908906337"
# Same USCC shape but Beijing division (110000) vs Yangzhou (321000)
MISMATCH_USCC = "91110000600037341L"


def test_division_from_registration_number() -> None:
    assert division_code_from_registration_number(MATCHING_REG) == "321000"
    assert division_code_from_registration_number("12345") is None
    assert division_code_from_registration_number(MATCHING_USCC) is None
    assert division_code_from_registration_number(" 321000400012591 ") == "321000"


def test_division_from_uscc() -> None:
    assert division_code_from_uscc(MATCHING_USCC) == "321000"
    assert division_code_from_uscc(MISMATCH_USCC) == "110000"
    assert division_code_from_uscc(MATCHING_REG) is None
    assert division_code_from_uscc("91 321000 7908906337") == "321000"


def test_iter_mismatches_agree() -> None:
    assert (
        iter_china_division_mismatches([MATCHING_REG], [MATCHING_USCC]) == []
    )


def test_iter_mismatches_disagree() -> None:
    assert iter_china_division_mismatches([MATCHING_REG], [MISMATCH_USCC]) == [
        (MATCHING_REG, MISMATCH_USCC, "321000", "110000")
    ]


def test_iter_ignores_non_china_shapes() -> None:
    assert (
        iter_china_division_mismatches(
            ["DE12345", MATCHING_REG],
            ["not-a-uscc", MATCHING_USCC],
        )
        == []
    )


def test_check_warns_on_mismatch(vcontext) -> None:
    entity = vcontext.make("Company")
    entity.id = "test-cn-mismatch"
    entity.add("registrationNumber", MATCHING_REG)
    with capture_logs() as cap_logs:
        check_china_division_agreement(entity, pending_uscc=MISMATCH_USCC)
    events = [log["event"] for log in cap_logs if log["log_level"] == "warning"]
    assert "Chinese registration number and USCC division codes disagree" in events


def test_check_silent_when_agree(vcontext) -> None:
    entity = vcontext.make("Company")
    entity.id = "test-cn-agree"
    entity.add("registrationNumber", MATCHING_REG)
    with capture_logs() as cap_logs:
        check_china_division_agreement(entity, pending_uscc=MATCHING_USCC)
    warnings = [log for log in cap_logs if log["log_level"] == "warning"]
    assert warnings == []


def test_check_noop_without_sibling(vcontext) -> None:
    entity = vcontext.make("Company")
    entity.id = "test-cn-solo"
    with capture_logs() as cap_logs:
        check_china_division_agreement(entity, pending_uscc=MATCHING_USCC)
        check_china_division_agreement(
            entity, pending_registration_number=MATCHING_REG
        )
    warnings = [
        log
        for log in cap_logs
        if log["log_level"] == "warning"
        and "division codes disagree" in log["event"]
    ]
    assert warnings == []


def test_check_noop_for_non_legal_entity(vcontext) -> None:
    entity = vcontext.make("Address")
    entity.id = "test-cn-address"
    # Address is not a LegalEntity and has neither identifier prop.
    with capture_logs() as cap_logs:
        check_china_division_agreement(
            entity,
            pending_registration_number=MATCHING_REG,
            pending_uscc=MISMATCH_USCC,
        )
    warnings = [
        log
        for log in cap_logs
        if "division codes disagree" in log.get("event", "")
    ]
    assert warnings == []


def test_cleaning_path_warns(vcontext) -> None:
    """Adding both identifiers via entity.add triggers the cleaning hook."""
    entity = vcontext.make("Company")
    entity.id = "test-cn-clean-path"
    entity.add("registrationNumber", MATCHING_REG)
    with capture_logs() as cap_logs:
        entity.add("uscCode", MISMATCH_USCC)
    events = [log["event"] for log in cap_logs if log["log_level"] == "warning"]
    assert "Chinese registration number and USCC division codes disagree" in events
