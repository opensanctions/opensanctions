from .crawler import clean_name


def test_clean_name():
    # REMOVE_PATTERNS
    assert ["EKONOMSKI FAKULTET SARAJEVO"] == clean_name(
        "(BRISAN USLJED PRIPAJANJA) EKONOMSKI FAKULTET SARAJEVO"
    )
    assert ['"ZEMANA" d.o.o. Sarajevo'] == clean_name(
        'BRISAN iz sudskog registra-"ZEMANA" d.o.o. Sarajevo'
    )
    assert ["GRAFEX d.o.o. Mostar"] == clean_name(
        "GRAFEX d.o.o. Mostar- BRISAN LIKVIDACIJOM"
    )

    # SPLITS
    assert [
        '"EKO JASMINA" društvo sa ograničenom odgovornošću',
        '"EKO JASMINA" d.o.o.',
    ] == clean_name(
        '"EKO JASMINA" društvo sa ograničenom odgovornošću (skraćeni naziv: "EKO JASMINA" d.o.o.)'
    )
