from zavod.context import Context
from zavod.helpers import strip_name_titles
from zavod.meta.names import NamesSpec


def configure_titles(
    context: Context,
    prefixes: list[str] | None = None,
    suffixes: list[str] | None = None,
) -> None:
    context.dataset.names.prefixes_strip = prefixes or []
    context.dataset.names.suffixes_strip = suffixes or []


def test_names_spec_accepts_title_strip_config() -> None:
    spec = NamesSpec.model_validate(
        {
            "prefixes_strip": ["Hon ", "(Dr.)"],
            "suffixes_strip": [", MP"],
        }
    )

    assert spec.prefixes_strip == ["Hon ", "(Dr.)"]
    assert spec.suffixes_strip == [", MP"]


def test_strip_stacked_malaysian_prefixes(vcontext: Context) -> None:
    configure_titles(
        vcontext,
        prefixes=[
            "Yang Berhormat ",
            "Datuk Seri Panglima ",
            "Datuk Seri ",
            "YB ",
            "Dato' ",
            "Datuk ",
        ],
    )

    assert (
        strip_name_titles(
            vcontext,
            "Yang Berhormat Datuk Seri Panglima Tengku Zafrul bin Tengku Abdul Aziz",
        )
        == "Tengku Zafrul bin Tengku Abdul Aziz"
    )
    assert strip_name_titles(vcontext, "YB Dato' Syed Ibrahim") == "Syed Ibrahim"


def test_strip_preserves_unconfigured_malaysian_particles(vcontext: Context) -> None:
    configure_titles(vcontext, prefixes=["YB ", "Datuk ", "Dato' "])

    assert strip_name_titles(vcontext, "Tengku Zafrul bin Tengku Abdul Aziz") == (
        "Tengku Zafrul bin Tengku Abdul Aziz"
    )
    assert strip_name_titles(vcontext, "Syed Saddiq bin Syed Abdul Rahman") == (
        "Syed Saddiq bin Syed Abdul Rahman"
    )
    assert strip_name_titles(vcontext, "Raja Kamarul Bahrin Shah") == (
        "Raja Kamarul Bahrin Shah"
    )
    assert strip_name_titles(vcontext, "Wan Junaidi bin Tuanku Jaafar") == (
        "Wan Junaidi bin Tuanku Jaafar"
    )


def test_strip_kenyan_prefixes_and_parenthesized_titles(vcontext: Context) -> None:
    configure_titles(vcontext, prefixes=["Hon. ", "(Dr.)", "(Rtd)", "Gen "])

    assert strip_name_titles(vcontext, "Hon. (Dr.) Jane Doe") == "Jane Doe"
    assert strip_name_titles(vcontext, "(Rtd) Gen John Doe") == "John Doe"
    assert strip_name_titles(vcontext, "Hon. (CPA) Jane Doe") == "(CPA) Jane Doe"


def test_strip_repeated_suffixes(vcontext: Context) -> None:
    configure_titles(vcontext, suffixes=[", CBS", ", MP", " OGW", " MP", " (MP)"])

    assert strip_name_titles(vcontext, "Jane Doe, CBS, MP") == "Jane Doe"
    assert strip_name_titles(vcontext, "Jane Doe OGW MP") == "Jane Doe"
    assert strip_name_titles(vcontext, "Jane Doe (MP)") == "Jane Doe"


def test_strip_leaves_unknown_comma_tail_visible(vcontext: Context) -> None:
    configure_titles(vcontext, suffixes=[", CBS", ", MP"])

    assert strip_name_titles(vcontext, "Jane Doe, Party Leader") == (
        "Jane Doe, Party Leader"
    )
    assert strip_name_titles(vcontext, "Jane Doe, CBS, Party Leader") == (
        "Jane Doe, CBS, Party Leader"
    )


def test_strip_is_idempotent_for_unmatched_names(vcontext: Context) -> None:
    configure_titles(vcontext, prefixes=["Hon "], suffixes=[", MP"])

    assert strip_name_titles(vcontext, "Honour Mwangi") == "Honour Mwangi"
    assert strip_name_titles(vcontext, "Hon. Jane Doe") == "Hon. Jane Doe"
    assert strip_name_titles(vcontext, "Jane Doe") == "Jane Doe"
    assert strip_name_titles(vcontext, None) is None
