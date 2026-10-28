import re

from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element, LangText

from zavod import Context, Entity
from zavod import helpers as h

# Profile pages label the term as "المدة النيابية: 2024-2029" or
# "الدورة النيابية: 2024-2029".
TERM_RE = re.compile(r"^(?:المدة|الدورة) النيابية: (\d{4})-(\d{4})$")


def fetch_term(context: Context, url: str) -> tuple[str, str] | None:
    """Read a member's term start and end years from their profile page."""
    doc = context.fetch_html(url, cache_days=7)
    terms: list[tuple[str, str]] = []
    titles = h.xpath_elements(
        doc, '//div[contains(@class, "e-n-accordion-item-title-text")]'
    )
    for title in titles:
        match = TERM_RE.match(h.element_text(title))
        if match is not None:
            terms.append((match.group(1), match.group(2)))
    if len(terms) != 1:
        context.log.warning("Expected one term label", url=url, terms=terms)
        return None
    return terms[0]


def crawl_row(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, Element],
) -> None:
    name_cell = row.pop("name")
    name = h.element_text(name_cell)
    governorate = h.element_text(row.pop("governorate"))

    person = context.make("Person")
    person.id = context.make_id(name, governorate)
    person.add("name", name, lang="ara")
    apply_translit_full_name(context, person, LangText(name, "ara"))
    # Only about half the members have a profile page ("بطاقة نائب"), the sole
    # place the term is stated. The rest stay undated.
    term: tuple[str, str] | None = None
    for profile_url in h.xpath_strings(name_cell, "./a/@href"):
        person.add("sourceUrl", profile_url)
        term = fetch_term(context, profile_url)
    # Council candidates must be Tunisian voters holding no other nationality
    # (Decree-Law 2023-10, Art. 16), and only council members can stand for the
    # CNRD (Art. 25).
    # https://legislation-securite.tn/wp-content/uploads/sites/5/2023/11/مرسوم-عدد-10-لسنة-2023-مؤرّخ-في-8-مارس-2023.pdf
    person.add("citizenship", "tn")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=term[0] if term is not None else None,
        period_end=term[1] if term is not None else None,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    # One member per district sits for the district rather than the governorate,
    # but the table doesn't say which, so we record the governorate within its
    # district.
    constituency = f"{governorate}, {h.element_text(row.pop('district'))}"
    occupancy.add("constituency", constituency, lang="ara")
    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Council of Regions and Districts of Tunisia",
        country="tn",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    table = h.xpath_element(doc, '//table[@id="tablepress-1"]')
    for cells in h.parse_html_table(table, slugify_headers=False):
        # The table ends with an empty row.
        if all(h.element_text(cell) == "" for cell in cells.values()):
            continue
        row = {
            context.lookup_value("columns", key, warn_unmatched=True) or key: cell
            for key, cell in cells.items()
        }
        crawl_row(context, position, categorisation, row)
