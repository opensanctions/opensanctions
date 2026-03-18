from zavod.extract.names.clean import LangText, Names


def test_lang_text_equality():
    assert LangText(text="John Doe", lang="eng") == LangText(
        text="John Doe", lang="eng"
    )
    assert LangText(text="John Doe", lang=None) == LangText(text="John Doe", lang=None)
    assert LangText(text="John Doe", lang="eng") != LangText(text="John Doe", lang=None)
    assert LangText(text="John Doe", lang="eng") != LangText(
        text="Jane Doe", lang="eng"
    )
    assert LangText(text="John Doe", lang="eng") != LangText(
        text="John Doe", lang="fra"
    )


def test_names_equality():
    assert Names() == Names()
    assert Names(name="John Doe") == Names(name="John Doe")
    assert Names(name="John Doe") == Names(name="John Doe")
    assert Names(name="John Doe") != Names(name="Jane Doe")
    assert Names(name="John Doe") == Names(name=["John Doe"])
    assert Names(name="John Doe") == Names(name=[LangText(text="John Doe", lang=None)])
    assert Names(name="John Doe") != Names(name=[LangText(text="John Doe", lang="eng")])
    assert Names(name=["A", "B"]) == Names(name=["A", "B"])
    assert Names(name=["A", "B"]) == Names(name=["B", "A"])
    assert Names(name=["A"]) != Names(alias=["A"])
    assert Names(name=["A"]) != Names()


def test_names_simplified():
    # Single-item list is unwrapped
    assert Names(name=["John Doe"]).simplified().name == "John Doe"

    # Multi-item list stays as list
    assert Names(name=["John Doe", "Jane Doe"]).simplified().name == [
        "John Doe",
        "Jane Doe",
    ]

    # Single-item list of LangText with lang=None is unwrapped and simplified to str
    assert (
        Names(name=[LangText(text="John Doe", lang=None)]).simplified().name
        == "John Doe"
    )

    # Single-item list of LangText with lang set stays as a list (can't be a bare LangText scalar)
    assert Names(name=[LangText(text="John Doe", lang="eng")]).simplified().name == [
        LangText(text="John Doe", lang="eng")
    ]

    # Mixed list: lang=None items become strs, lang-set items stay as LangText
    assert Names(
        name=[LangText(text="John Doe", lang=None), LangText(text="جون دو", lang="ara")]
    ).simplified().name == ["John Doe", LangText(text="جون دو", lang="ara")]

    # Empty list simplifies to None
    assert Names(name=[]).simplified().name is None
