import dataclasses
from datetime import timedelta

import pytest
import structlog

from zavod import Entity, settings
from zavod.context import Context
from zavod.helpers.sanctions import make_sanction, is_active
from zavod.stateful.model import program_table
from zavod.stateful.programs import Program


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
    vcontext.conn.execute(
        program_table.insert().values(
            dataclasses.asdict(
                Program(
                    id=1,
                    key="OS-TEST",
                    title="Blabla",
                    url="http://important.authority/test",
                )
            )
        )
    )

    person = vcontext.make("Person")
    person.id = "jeff"
    sanction = make_sanction(
        vcontext, person, program_name="Test Program", program_key="OS-TEST"
    )

    assert sanction.get("program")[0] == "Test Program"
    assert sanction.get("programUrl") == ["http://important.authority/test"]
    assert sanction.get("programId")[0] == "OS-TEST"


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
    } in caplogs


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
