from zavod.helpers.text import multi_split, remove_bracketed, is_empty, clean_note, clean_br_cnpj, clean_br_cpf


def test_multi_split():
    assert len(multi_split(None, ["a)"])) == 0
    assert len(multi_split("", ["a)"])) == 0
    text = "banana"
    assert len(multi_split(text, ["a)"])) == 1
    text = "a) banana b) peach"
    assert len(multi_split(text, ["a)", "b)"])) == 2
    text = ["a) banana b) peach", None]
    assert len(multi_split(text, ["a)", "b)"])) == 2


def test_remove_bracketed():
    assert remove_bracketed(None) is None
    text = "banana"
    assert remove_bracketed(text) == text
    text = "banana (with peaches)"
    assert remove_bracketed(text).strip() == "banana"


def test_is_empty():
    assert is_empty(None) is True
    assert is_empty("") is True
    assert is_empty("  ") is True
    assert is_empty(" hello ") is False
    assert is_empty(5) is False


def test_clean_note():
    assert clean_note([]) == []
    assert clean_note(None) == []
    assert clean_note(["hello"]) == ["hello"]
    assert clean_note(["hello", None]) == ["hello"]

def test_clean_br_cpf():

    # expected input
    assert clean_br_cpf("123.456.789-00") == "12345678900"

    # if it's already clean, then returns as is
    assert clean_br_cpf("12345678900") == "12345678900"

    # if it's invalid then returns as is
    assert clean_br_cpf("abc") == "abc"

    # if it's invalid then returns as is
    assert clean_br_cpf("1234567890)[]") == "1234567890)[]"

def test_clean_br_cnpj():

    # expected input
    assert clean_br_cnpj("12.345.678/9101-12") == "12345678910112"

    # if it's already clean, then returns as is
    assert clean_br_cnpj("12345678910112") == "12345678910112"

    # if it's invalid then returns as is
    assert clean_br_cnpj("abc") == "abc"

    # if it's invalid then returns as is
    assert clean_br_cpf("12345678910112)[]") == "12345678910112)[]"