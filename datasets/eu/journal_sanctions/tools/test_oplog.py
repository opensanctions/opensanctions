"""Replay and validation rules on small synthetic stacks.

Run from the dataset directory: `python -m pytest --import-mode=importlib tools/`.
"""

from pathlib import Path

import pytest
from zavod.shed.ojeu.cellar import RelatedAct

from . import common, validate
from .common import OPLOG_COLUMNS, write_oplog
from .lookup import decide, parse_date, rank
from .replay import replay

FRAMEWORK = "32099R0001"
ACTS = [("32099R0002", "2099-02-01"), ("32099R0003", "2099-03-01")]
ALICE = "a" * 40
BOB = "b" * 40


def make_row(op: str, id: str, published: str, **values: str) -> dict[str, str]:
    row = dict.fromkeys(OPLOG_COLUMNS, "")
    row.update(op=op, id=id, publishedDate=published, effectiveDate=published)
    row.update(programKey="EU-UKR", measure="Asset freeze", schema="Person")
    row.update(values)
    return row


@pytest.fixture
def stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(common, "REGULATIONS_DIR", tmp_path / "regulations")
    acts = [RelatedAct(a, FRAMEWORK, "amends", None, d, "REG_IMPL") for a, d in ACTS]
    monkeypatch.setattr(validate, "related_acts", lambda framework: acts)
    directory = tmp_path / "regulations" / FRAMEWORK
    directory.mkdir(parents=True)
    write_oplog(
        directory / f"{FRAMEWORK}.csv",
        [
            make_row(
                "ADD",
                ALICE,
                "2099-01-01",
                recordId="1",
                name="Alice Smith",
                listingDate="1.1.2099",
                reason="Listed.",
            ),
            make_row(
                "ADD",
                BOB,
                "2099-01-01",
                recordId="2",
                name="Bob Jones",
                birthDate="2.2.1960",
                listingDate="1.1.2099",
                reason="Listed.",
            ),
        ],
    )
    return directory


def errors(framework: str = FRAMEWORK) -> list[str]:
    return [
        i.message for i in validate.validate_framework(framework) if i.level == "error"
    ]


def test_replay_mod_del_and_dates(stack: Path) -> None:
    write_oplog(
        stack / "32099R0002.csv",
        [
            make_row(
                "MOD",
                ALICE,
                "2099-02-01",
                recordId="1",
                name="Alice SMITH",
                listingDate="1.1.2099",
                reason="Changed.",
            ),
            make_row("DEL", BOB, "2099-02-01", recordId="2", name="Bob Jones"),
        ],
    )
    state = replay(FRAMEWORK)
    assert state.errors == []
    (alice,) = state.live.values()
    assert alice.row["name"] == "Alice SMITH"
    assert alice.listed_published == "2099-01-01"
    assert alice.modified == "2099-02-01"
    assert len(state.deleted) == 1
    # A view before the amending act still has both designations.
    assert len(replay(FRAMEWORK, as_of="2099-01-15").live) == 2
    assert errors() == []


def test_relisting_starts_a_new_period(stack: Path) -> None:
    write_oplog(
        stack / "32099R0002.csv",
        [
            make_row("DEL", BOB, "2099-02-01", recordId="2", name="Bob Jones"),
        ],
    )
    write_oplog(
        stack / "32099R0003.csv",
        [
            make_row(
                "ADD",
                BOB,
                "2099-03-01",
                recordId="1",
                name="Bob Jones",
                listingDate="1.3.2099",
                reason="Relisted.",
            ),
        ],
    )
    state = replay(FRAMEWORK)
    assert state.errors == []
    bob = next(e for e in state.live.values() if e.row["id"] == BOB)
    assert bob.listed_published == "2099-03-01"


def test_replay_errors(stack: Path) -> None:
    write_oplog(
        stack / "32099R0002.csv",
        [
            make_row("ADD", ALICE, "2099-02-01", name="Alice Smith", reason="Again."),
            make_row("MOD", "c" * 40, "2099-02-01", name="Carol", reason="X."),
            make_row("DEL", BOB, "2099-02-01", recordId="2", name="Robert Jones"),
        ],
    )
    found = errors()
    assert any("ADD of live" in e for e in found)
    assert any("MOD of missing" in e for e in found)
    assert any("does not match the live" in e for e in found)


def test_row_contract_errors(stack: Path) -> None:
    write_oplog(
        stack / "32099R0002.csv",
        [
            make_row("DEL", BOB, "2099-02-01", name="Bob Jones", reason="Not allowed."),
            make_row("MOD", "short", "2099-02-03", name="Alice Smith"),
            make_row(
                "ADD",
                "d" * 40,
                "2099-02-01",
                name="Dan",
                schema="Person",
                flag="Russia",
                reason="Vessel property.",
            ),
        ],
    )
    found = errors()
    assert any("DEL rows fill only" in e for e in found)
    assert any("40 lowercase hex" in e for e in found)
    assert any("publishedDate differs" in e for e in found)
    assert any("flag is not a property of Person" in e for e in found)


def test_unknown_act_and_coverage(stack: Path) -> None:
    write_oplog(stack / "32099R0003.csv", [])
    write_oplog(stack / "32099R0099.csv", [])
    issues = validate.validate_framework(FRAMEWORK, online=True)
    messages = [i.message for i in issues]
    assert "CELLAR relates no such act to the framework" in messages
    assert any("no file for 32099R0002" in m for m in messages)


def test_lookup_reordered_and_transliterated(stack: Path) -> None:
    state = replay(FRAMEWORK)
    query = make_row(
        "MOD",
        "",
        "2099-02-01",
        name="JONES, Bob",
        alias="Боб Джонс",
        birthDate="02.02.1960",
        listingDate="1.1.2099",
    )
    chosen = decide(rank(query, list(state.live.values())))
    assert chosen is not None and chosen.entry.row["id"] == BOB
    stranger = make_row(
        "MOD", "", "2099-02-01", name="Carol White", listingDate="5.5.2099"
    )
    assert decide(rank(stranger, list(state.live.values()))) is None


def test_parse_date_shapes() -> None:
    assert parse_date("28.2.2011") == (2011, 2, 28)
    assert parse_date("2 December 1985") == (1985, 12, 2)
    assert parse_date("26 Feb. 2011") == (2011, 2, 26)
    assert parse_date("4 Sept. 2003") == (2003, 9, 4)
    assert parse_date("26 Feb 2011") is None
    assert parse_date("approximately 1962") is None
