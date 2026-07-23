from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

# Khmer digits U+17E0..U+17E9 -> ASCII, so a row's ordinal cell can be recognised without
# embedding Khmer numerals in the source.
KHMER_DIGITS = {0x17E0 + i: str(i) for i in range(10)}


def is_ordinal(text: str) -> bool:
    return text.translate(KHMER_DIGITS).isdigit()


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Cambodia",
        country="kh",
        wikidata_id="Q21295974",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    count = 0
    # The roster is a four-column table: ordinal | name | constituency | party. Member rows
    # are those whose first cell is a Khmer numeral (skips the header and nested layout).
    for row in h.xpath_elements(doc, "//tr[count(./td) = 4]"):
        cells = [h.element_text(td) for td in h.xpath_elements(row, "./td")]
        ordinal, name, constituency, party = cells
        if not is_ordinal(ordinal):
            continue
        assert name, "Empty member name"

        person = context.make("Person")
        person.id = context.make_id(name, constituency)
        person.add("name", name, lang="khm")
        person.add("political", party or None, lang="khm")
        # Candidates for the National Assembly must hold Khmer nationality by birth
        # (Constitution of Cambodia, Article 76).
        # https://constitutionnet.org/sites/default/files/Cambodia%20Constitution.pdf
        person.add("citizenship", "kh")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        occupancy.add("constituency", constituency or None, lang="khm")
        context.emit(occupancy)
        context.emit(person)
        count += 1

    if count == 0:
        raise ValueError("No member rows found in the National Assembly roster")
