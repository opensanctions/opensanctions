"""Replay and validation rules on small synthetic stacks.

Run from the dataset directory: `python -m pytest --import-mode=importlib tools/`.
"""

import json
from pathlib import Path

import click
import pytest
from zavod.shed.ojeu.cellar import RelatedAct

from . import check, common, lookup, migrate
from . import replay as replay_module
from .__main__ import cli
from .common import OPLOG_COLUMNS, cell_values, encode_row, parse_date, write_oplog
from .match import decide, haystack, missing_values, rank
from .replay import Op, replay

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
    monkeypatch.setattr(check, "related_acts", lambda framework: acts)
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
        i.message for i in check.validate_framework(framework) if i.level == "error"
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
    issues = check.validate_framework(FRAMEWORK, online=True)
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


def test_encode_row_round_trip() -> None:
    row = encode_row(
        {"op": "ADD", "name": "Foo", "alias": ["A; B", 'C "D"'], "reason": "R."}
    )
    assert row["alias"] == '"A; B"; "C ""D"""'
    assert cell_values(row, "alias") == ["A; B", 'C "D"']
    assert cell_values(row, "reason") == ["R."]
    assert cell_values(row, "notes") == []
    assert cell_values(row, "name") == ["Foo"]
    with pytest.raises(ValueError):
        encode_row({"name": ["A", "B"]})
    with pytest.raises(ValueError):
        encode_row({"alias": ["A", "A"]})
    with pytest.raises(ValueError):
        encode_row({"nickname": "A"})


def test_missing_values() -> None:
    text = haystack(
        "▼M3 (1) | Ivan PETROV ¶ Born 1.1.1960. ►M4 Adviser◄ to the ¶ minister."
    )
    row = encode_row(
        {
            "op": "ADD",
            "id": "a" * 40,
            "programKey": "EU-XYZ",
            "name": "Ivan PETROV",
            "birthDate": "1.1.1960",
            "reason": "Adviser to the minister.",
            "alias": ["Ivan Petrov", "Ivan PETROV"],
        }
    )
    assert missing_values(row, text) == [("alias", "Ivan Petrov")]


def test_alias_markers_warn() -> None:
    issues: list[check.Issue] = []
    for alias in ["a) Sangin b) Sangeen", "X a.k.a. Y"]:
        row = encode_row({"op": "ADD", "id": "a" * 40, "name": "N", "alias": alias})
        check.check_row(Op("32099R0001", 2, row), issues)
    warnings = [i for i in issues if "label or enumeration" in i.message]
    assert len(warnings) == 2


def test_end_date_lapses_in_as_of_views(stack: Path) -> None:
    write_oplog(
        stack / "32099R0002.csv",
        [
            make_row(
                "ADD",
                "c" * 40,
                "2099-02-01",
                name="Vessel X",
                schema="Vessel",
                listingDate="1.2.2099",
                endDate="30 April 2099",
                reason="Listed.",
            ),
        ],
    )
    assert len(replay(FRAMEWORK, as_of="2099-04-30").lapsed) == 0
    later = replay(FRAMEWORK, as_of="2099-05-01")
    assert [e.row["name"] for e in later.lapsed.values()] == ["Vessel X"]
    assert all(e.row["name"] != "Vessel X" for e in later.live.values())
    assert errors() == []


def test_missing_values_skips_carried() -> None:
    text = haystack("for: ‘Pak Yon Sik’, read: ‘Pak Yong Sik’")
    before = encode_row(
        {"name": "Pak Yon Sik", "nationality": "DPRK", "reason": "Old."}
    )
    after = encode_row(
        {"name": "Pak Yong Sik", "nationality": "DPRK", "reason": "Old."}
    )
    assert missing_values(after, text, carried=before) == []
    assert missing_values(after, text) == [("nationality", "DPRK"), ("reason", "Old.")]


def test_one_row_per_measure(stack: Path) -> None:
    write_oplog(
        stack / "32099R0002.csv",
        [
            make_row(
                "ADD",
                "d" * 40,
                "2099-02-01",
                name="Ship",
                schema="Vessel",
                measure="Asset freeze",
                reason="R.",
            ),
            make_row(
                "ADD",
                "d" * 40,
                "2099-02-01",
                name="Ship",
                schema="Vessel",
                measure="Asset freeze",
                reason="R2.",
            ),
        ],
    )
    assert any("twice in one act" in e for e in errors())


def test_empty_id_stops_validation(stack: Path) -> None:
    write_oplog(
        stack / "32099R0002.csv",
        [
            make_row("MOD", "", "2099-02-01", name="Alice Smith", reason="R."),
        ],
    )
    assert errors() == ["empty id; run `python -m tools resolve`"]


def test_migrate_adds_columns(tmp_path: Path) -> None:
    path = tmp_path / "32099R0001.csv"
    path.write_text("op,id,name,reason\nADD,abc,Foo,Bar\n", encoding="utf-8")
    rows = migrate.migrate(path)
    assert rows is not None
    assert (
        rows[0]["name"] == "Foo" and rows[0]["extra"] == "" and rows[0]["endDate"] == ""
    )
    path.write_text("op,nickname\nADD,x\n", encoding="utf-8")
    with pytest.raises(click.ClickException):
        migrate.migrate(path)


def test_replay_before_an_act_without_a_file(
    stack: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_oplog(
        stack / "32099R0003.csv",
        [
            make_row("DEL", BOB, "2099-03-01", recordId="2", name="Bob Jones"),
        ],
    )
    monkeypatch.setattr(replay_module, "OUT_DIR", tmp_path / "out")
    with pytest.raises(click.UsageError):
        replay(FRAMEWORK, before="32099R0002")
    meta = tmp_path / "out" / "32099R0002" / "meta.json"
    meta.parent.mkdir(parents=True)
    meta.write_text(
        json.dumps({"publication_date": "2099-02-01", "entry_into_force": []})
    )
    # The corrigendum-like act sits between the framework and 32099R0003.
    assert len(replay(FRAMEWORK, before="32099R0002").live) == 2


def test_annex_moves_and_separate_listings(stack: Path) -> None:
    # A designee listed in a second annex has its own id.
    write_oplog(
        stack / "32099R0002.csv",
        [
            make_row(
                "ADD",
                "e" * 40,
                "2099-02-01",
                annex="XV",
                name="Alice Smith",
                reason="EU.",
            ),
        ],
    )
    assert len(replay(FRAMEWORK).live) == 3
    # A MOD may move an entry to another annex, with a warning to review.
    write_oplog(
        stack / "32099R0003.csv",
        [
            make_row(
                "MOD",
                BOB,
                "2099-03-01",
                recordId="2",
                annex="XIV.B",
                name="Bob Jones",
                reason="R.",
            ),
        ],
    )
    bob = next(e for e in replay(FRAMEWORK).live.values() if e.row["id"] == BOB)
    assert bob.row["annex"] == "XIV.B" and bob.listed_published == "2099-01-01"
    issues = check.validate_framework(FRAMEWORK)
    assert [i.level for i in issues if "moves the entry" in i.message] == ["warning"]
    assert errors() == []


def test_row_as_mod_and_find(
    stack: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from click.testing import CliRunner

    monkeypatch.setattr(lookup, "OUT_DIR", tmp_path / "out")
    monkeypatch.setattr(replay_module, "OUT_DIR", tmp_path / "out")
    meta = tmp_path / "out" / "32099R0002" / "meta.json"
    meta.parent.mkdir(parents=True)
    meta.write_text(
        json.dumps(
            {"publication_date": "2099-02-01", "entry_into_force": ["2099-02-02"]}
        )
    )
    out = tmp_path / "row.csv"
    result = CliRunner().invoke(
        lookup.entry,
        [
            FRAMEWORK,
            "--annex",
            "",
            "--record",
            "2",
            "--as-mod",
            "32099R0002",
            "--out",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    (row,) = common.read_oplog(out)
    assert (row["op"], row["id"], row["effectiveDate"]) == ("MOD", BOB, "2099-02-02")
    write_oplog(stack / "32099R0002.csv", [])
    result = CliRunner().invoke(
        lookup.entry,
        [FRAMEWORK, "--before", "32099R0002", "--name", "Jones, Bob"],
    )
    assert result.exit_code == 0, result.output
    assert BOB[:10] in result.output.splitlines()[0]


def test_cited_acts_with_and_without_markers() -> None:
    header = (
        "Amended by:\n"
        "►M1 | COUNCIL IMPLEMENTING REGULATION (EU) 2016/466 of 31 March 2016 | L 85 | 1 | 1.4.2016\n"
        "M2 | COUNCIL IMPLEMENTING REGULATION (EU) No 284/2014 of 21 March 2014 | L 86 | 27 | 21.3.2014\n"
        "Corrected by:\n"
        "C1 | Corrigendum, OJ L 98, 14.4.2016, p. 6 (2016/466)\n"
        "▼B\n"
        "►M9 | not part of the header\n"
    )
    amended, corrected = check.cited_acts(header)
    assert amended == {"32016R0466", "32014R0284"}
    assert corrected == {"32016R0466": 1}


def test_loose_matching_folds_lookalikes() -> None:
    # The act prints Ukrainian і (U+0456); the consolidation a Latin i.
    text = haystack("Олексiй Мозговий ¶ Name: 1: ABU 2: ZAYD 3: UMAR", loose=True)
    row = encode_row({"name": "Олексій Мозговий", "alias": "ABU ZAYD UMAR"})
    assert missing_values(row, text, loose=True) == []


def test_run_makes_the_tools_importable(tmp_path: Path) -> None:
    from click.testing import CliRunner

    script = tmp_path / "build.py"
    out = tmp_path / "rows.csv"
    script.write_text(
        "import sys\n"
        "from pathlib import Path\n"
        "from tools.common import encode_row, write_oplog\n"
        "write_oplog(Path(sys.argv[1]), [encode_row({'op': 'ADD', 'name': 'X'})])\n"
    )
    result = CliRunner().invoke(cli, ["run", str(script), str(out)])
    assert result.exit_code == 0, result.output
    (row,) = common.read_oplog(out)
    assert row["name"] == "X"


def test_label_in_short_column_warns() -> None:
    issues: list[check.Issue] = []
    row = encode_row(
        {
            "op": "ADD",
            "id": "a" * 40,
            "name": "N",
            "nationality": "Afghan. Passport no.: D 1",
        }
    )
    check.check_row(Op("32099R0001", 2, row), issues)
    assert any("nationality holds a printed label" in i.message for i in issues)
