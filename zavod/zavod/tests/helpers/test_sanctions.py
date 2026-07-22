from datetime import timedelta

import pytest
import structlog

from zavod import Entity, settings
from zavod.context import Context
from zavod.helpers.sanctions import make_sanction, is_active


def test_sanctions_helper(vcontext: Context):
    person = vcontext.make("Person")
    with pytest.raises(AssertionError):
        make_sanction(vcontext, person)

    person.id = "jeff"
    sanction = make_sanction(vcontext, person)
    assert "OpenSanctions" in sanction.get("authority")
    assert "jeff" in sanction.get("entity")

    sanction2 = make_sanction(vcontext, person)
    assert sanction.id == sanction2.id

    sanction3 = make_sanction(vcontext, person, key="other")
    assert sanction.id != sanction3.id


def test_sanctions_helper_with_program(vcontext: Context):
    person = vcontext.make("Person")
    person.id = "jeff"
    sanction = make_sanction(
        vcontext, person, program_name="Test Program", program_key="US-BIS-DPL"
    )

    assert sanction.get("program")[0] == "Test Program"
    assert sanction.get("programUrl") == [
        "https://www.bis.gov/licensing/end-user-guidance/denied-persons-list-dpl"
    ]
    assert sanction.get("programId")[0] == "US-BIS-DPL"


def test_sanctions_helper_with_unknown_program(vcontext: Context):
    person = vcontext.make("Person")
    person.id = "jeff"

    with structlog.testing.capture_logs() as caplogs:
        sanction = make_sanction(
            vcontext, person, program_name="Test Program", program_key="OS-TEST"
        )

    assert sanction.get("program")[0] == "Test Program"
    assert sanction.get("programUrl") == []
    assert sanction.get("programId") == []
    assert {
        "event": "Program with key 'OS-TEST' not found.",
        "log_level": "warning",
        "entity_id": person.id,
    } in caplogs


def test_sanctions_status_agrees_with_is_active(vcontext: Context):
    person = vcontext.make("Person")
    person.id = "jeff"

    # Future start and end date: not yet active, status must agree.
    future_start = (settings.RUN_TIME + timedelta(days=20)).date().isoformat()
    future_end = (settings.RUN_TIME + timedelta(days=30)).date().isoformat()
    sanction = make_sanction(
        vcontext, person, start_date=future_start, end_date=future_end
    )
    assert not is_active(sanction)
    assert sanction.get("status") == ["inactive"]

    # Started in the past, ends in the future: active.
    past_start = (settings.RUN_TIME - timedelta(days=20)).date().isoformat()
    sanction = make_sanction(
        vcontext, person, key="b", start_date=past_start, end_date=future_end
    )
    assert is_active(sanction)
    assert sanction.get("status") == ["active"]

    # Ended in the past: inactive.
    past_end = (settings.RUN_TIME - timedelta(days=10)).date().isoformat()
    sanction = make_sanction(
        vcontext, person, key="c", start_date=past_start, end_date=past_end
    )
    assert not is_active(sanction)
    assert sanction.get("status") == ["inactive"]


def test_sanctions_unparseable_end_date_raises(vcontext: Context):
    person = vcontext.make("Person")
    person.id = "jeff"

    with pytest.raises(ValueError, match=r"'see annex'.*'jeff'"):
        make_sanction(vcontext, person, end_date="see annex")


@pytest.fixture
def person(vcontext: Context):
    person = vcontext.make("Person")
    person.id = "jeff"
    return person


@pytest.fixture
def sanction(vcontext: Context, person: Entity):
    return make_sanction(vcontext, person)


def test_sanctions_is_active_no_end_date(sanction: Entity):
    sanction.set("endDate", None)
    assert is_active(sanction)


def test_sanctions_is_active_with_end_date_tomorrow(sanction: Entity):
    tomorrow = (settings.RUN_TIME + timedelta(days=1)).date().isoformat()
    sanction.set("endDate", tomorrow)
    assert is_active(sanction)


def test_sanctions_is_active_with_end_date_yesterday(sanction: Entity):
    yesterday = (settings.RUN_TIME - timedelta(days=1)).date().isoformat()
    sanction.set("endDate", yesterday)
    assert not is_active(sanction)


def test_sanctions_is_active_with_multiple_end_dates(sanction: Entity):
    past_date = (settings.RUN_TIME - timedelta(days=20)).date().isoformat()
    future_date = (settings.RUN_TIME + timedelta(days=20)).date().isoformat()
    sanction.set("endDate", [past_date, future_date])
    assert is_active(sanction)


def test_sanctions_is_active_with_future_start_date(sanction: Entity):
    future_date = (settings.RUN_TIME + timedelta(days=20)).date().isoformat()
    far_future_date = (settings.RUN_TIME + timedelta(days=30)).date().isoformat()
    sanction.set("startDate", future_date)
    sanction.set("endDate", far_future_date)
    assert not is_active(sanction)


def test_sanctions_is_active_with_end_date_today(sanction: Entity):
    today = settings.RUN_TIME.date().isoformat()
    sanction.set("endDate", today)
    assert is_active(sanction)


def test_sanctions_is_active_with_prefix_end_dates(sanction: Entity):
    # A sanction ending some time this year is still active today.
    sanction.set("endDate", str(settings.RUN_TIME.year))
    assert is_active(sanction)
    # Same for month precision in the current month.
    sanction.set("endDate", settings.RUN_TIME.date().isoformat()[:7])
    assert is_active(sanction)
    # A sanction ending last year is over.
    sanction.set("endDate", str(settings.RUN_TIME.year - 1))
    assert not is_active(sanction)
    # A start date in the current year (at year precision) may have passed already.
    sanction.set("endDate", None)
    sanction.set("startDate", str(settings.RUN_TIME.year))
    assert is_active(sanction)
    sanction.set("startDate", str(settings.RUN_TIME.year + 1))
    assert not is_active(sanction)


def test_make_sanction_prefix_end_date_status(vcontext: Context, person: Entity):
    # endDate="<current year>" means the sanction runs until some point this
    # year - it must not be reported inactive from the 1st of January.
    sanction = make_sanction(
        vcontext, person, key="this-year", end_date=str(settings.RUN_TIME.year)
    )
    assert sanction.get("status") == ["active"]

    sanction = make_sanction(
        vcontext, person, key="today", end_date=settings.RUN_TIME.date().isoformat()
    )
    assert sanction.get("status") == ["active"]

    sanction = make_sanction(
        vcontext, person, key="last-year", end_date=str(settings.RUN_TIME.year - 1)
    )
    assert sanction.get("status") == ["inactive"]
