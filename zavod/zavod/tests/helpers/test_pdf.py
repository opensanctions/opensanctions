import pytest

from zavod import helpers as h
from zavod.tests.conftest import FIXTURES_PATH

# Both PDFs are generated from the .docx of the same name in the fixtures
# directory: `soffice --headless --convert-to pdf <name>.docx`
PDF_PATH = FIXTURES_PATH / "test_pdf.pdf"
DUPLICATE_HEADERS_PDF_PATH = FIXTURES_PATH / "test_pdf_duplicate_headers.pdf"
PAGE_SETTINGS = {"join_y_tolerance": 100}


def basic_settings_func(page):
    return (page, PAGE_SETTINGS)


def test_parse_pdf_table_gap(vcontext):
    """Gap in border cuts table off"""
    rows = list(h.parse_pdf_table(vcontext, PDF_PATH, end_page=1))
    assert len(rows) == 1, rows
    assert rows[0]["first_name"] == "Forenames"
    assert rows[0]["last_name"] == "Surname"


def test_parse_pdf_table_preserve_header_newlines(vcontext):
    rows = list(
        h.parse_pdf_table(vcontext, PDF_PATH, preserve_header_newlines=True, end_page=1)
    )
    assert len(rows) == 1, rows
    assert rows[0]["first\nname"] == "Forenames"


def test_parse_pdf_table_page_settings(vcontext):
    """page returned by page_settings is used for extraction"""

    def settings_func(page):
        cropped = page.crop((0, 93, page.width, page.height))
        # im = cropped.to_image()
        # im.save(f"page-{cropped.page_number}.png")
        return (cropped, PAGE_SETTINGS)

    rows = list(
        h.parse_pdf_table(vcontext, PDF_PATH, end_page=1, page_settings=settings_func)
    )
    assert len(list(rows)) == 2, rows
    assert rows[0]["forenames"] == "Jon"
    assert rows[0]["surname"] == "Smith"


def test_parse_pdf_table_simpler_settings_func(vcontext):
    rows = list(
        h.parse_pdf_table(
            vcontext, PDF_PATH, end_page=1, page_settings=basic_settings_func
        )
    )
    assert len(list(rows)) == 3, rows
    assert rows[0]["first_name"] == "Forenames"
    assert rows[1]["first_name"] == "Jon"
    assert rows[2]["first_name"] == "Fred"


def test_parse_pdf_table_skiprows(vcontext):
    rows = list(
        h.parse_pdf_table(
            vcontext,
            PDF_PATH,
            end_page=1,
            skiprows=1,
            page_settings=basic_settings_func,
        )
    )
    assert len(list(rows)) == 2, rows
    assert rows[0]["forenames"] == "Jon"
    assert rows[0]["surname"] == "Smith"
    assert rows[1]["forenames"] == "Fred"
    assert rows[1]["surname"] == "Bloggs"


def test_parse_pdf_table_multiple_pages(vcontext):
    rows = list(
        h.parse_pdf_table(
            vcontext, PDF_PATH, skiprows=1, page_settings=basic_settings_func
        )
    )
    assert len(list(rows)) == 5, rows
    assert rows[0]["forenames"] == "Jon"
    # This is just documenting how skiprows doesn't skip on subsequent pages
    # if headers_per_page is False. The fixture just has two different sets of
    # header rows for different cases. In practice there's sometimes one or more
    # comment rows, then a header row, then data. Sometimes this is all repeated
    # on each page.
    assert rows[2]["forenames"] == "First\nName"
    # Page 2 repeats the header row ("Forenames"/"Surname"); it is skipped
    # rather than emitted as a data row.
    assert rows[3]["forenames"] == "Jane"
    assert rows[4]["forenames"] == "Frederica"


def test_parse_pdf_table_duplicate_headers(vcontext):
    # Headers that collide after slugification would silently drop the earlier
    # column's cell ({"name": "latin script", "dob": "1970"}).
    with pytest.raises(AssertionError, match="Duplicate headers"):
        list(h.parse_pdf_table(vcontext, DUPLICATE_HEADERS_PDF_PATH))


def test_parse_pdf_table_headers_per_page(vcontext):
    rows = list(
        h.parse_pdf_table(
            vcontext,
            PDF_PATH,
            headers_per_page=True,
            skiprows=1,
            page_settings=basic_settings_func,
        )
    )
    assert len(list(rows)) == 4, rows
    assert rows[0]["forenames"] == "Jon"
    assert rows[2]["forenames"] == "Jane"
    assert rows[3]["forenames"] == "Frederica"
    assert rows[3]["surname"] == "Bullen\nJones"
