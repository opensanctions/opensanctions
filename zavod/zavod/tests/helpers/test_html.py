import pytest
from lxml import html
from rigour.text import text_hash

from zavod import Dataset
from zavod import helpers as h


HTML = """
<html>
  <table>
    <thead>
      <tr>
        <th>First Name</th>
        <th>Read More</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>James Bond</td>
        <td>
          <a href="/james-bond">Read More</a>
          but also
          <a href="/james-bond-extra">Extra</a>
        </td>
      </tr>
      <tr>
        <td>Jason Bourne</td>
        <td>12345</td>
    </tbody>
  </table>
</html>
"""


def test_parse_html_table(testdataset1: Dataset):
    doc = html.fromstring(HTML)
    table = doc.xpath(".//table")[0]
    rows = list(h.parse_html_table(table))

    assert len(rows) == 2, rows
    assert rows[0]["first_name"].text_content() == "James Bond", rows[0]

    str_row_1 = h.cells_to_str(rows[0])
    assert str_row_1["first_name"] == "James Bond", str_row_1
    assert str_row_1["read_more"] == "Read More but also Extra", str_row_1
    str_row_2 = h.cells_to_str(rows[1])
    assert str_row_2["read_more"] == "12345", str_row_2

    links_dict = h.links_to_dict(rows[0]["read_more"])
    assert links_dict["read_more"] == "/james-bond", links_dict
    assert links_dict["extra"] == "/james-bond-extra", links_dict


DUPLICATE_HEADERS_HTML = """
<html>
  <table>
    <tr><th>Name</th><th>Name</th><th>DOB</th></tr>
    <tr><td>original script</td><td>latin script</td><td>1970</td></tr>
  </table>
</html>
"""

NESTED_TABLE_HTML = """
<html>
  <table>
    <tr><th>Name</th><th>Info</th></tr>
    <tr>
      <td>Alice</td>
      <td>
        <table><tr><td>inner1</td><td>inner2</td></tr></table>
      </td>
    </tr>
  </table>
</html>
"""


def test_parse_html_table_duplicate_headers():
    # Headers that collide after slugification would silently drop the earlier
    # column's cell ({"name": "latin script", "dob": "1970"}).
    doc = html.fromstring(DUPLICATE_HEADERS_HTML)
    table = doc.xpath(".//table")[0]
    with pytest.raises(AssertionError, match="Duplicate headers"):
        list(h.parse_html_table(table))


def test_parse_html_table_nested_table():
    # Rows of a table nested inside a cell must not be emitted as rows of the
    # outer table.
    doc = html.fromstring(NESTED_TABLE_HTML)
    table = doc.xpath(".//table")[0]
    rows = list(h.parse_html_table(table))
    assert len(rows) == 1, rows
    str_row = h.cells_to_str(rows[0])
    assert str_row["name"] == "Alice", str_row


def test_element_text():
    doc = html.fromstring("<span>&nbsp; </span>")
    assert h.element_text(doc) == "", doc
    assert h.element_text(doc, squash=False) == "\xa0 ", doc
    doc = html.fromstring("<span> Hello, <div>World!</div> &nbsp;</span>")
    assert h.element_text(doc) == "Hello, World!", doc


def test_element_text_hash():
    doc = html.fromstring("<span>&nbsp; </span>")
    assert h.element_text_hash(doc) == "da39a3ee5e6b4b0d3255bfef95601890afd80709", doc

    hash = text_hash("Hello, World!")
    doc = html.fromstring("<span> Hello, <div>World!</div> &nbsp;</span>")
    assert h.element_text_hash(doc) == hash, (doc, hash)
    doc = html.fromstring("<span> Hello, <div>World!</div><h3>&nbsp;</h3></span>")
    assert h.element_text_hash(doc) == hash, (doc, hash)
    doc = html.fromstring("<span> HELLO, <div>WORLD</div> &nbsp;</span>")
    assert h.element_text_hash(doc) == hash, (doc, hash)


def test_split_html_newline_tags():
    split = h.split_html_newline_tags
    assert split("John Smith<br>Jane Doe") == ["John Smith", "Jane Doe"]
    assert split("<p>Ground one</p><p>Ground two</p>") == ["Ground one", "Ground two"]
    # Self-closing and upper-case variants
    assert split("one<br/>two") == ["one", "two"]
    assert split("one<BR>two") == ["one", "two"]
    assert split("one<br />two") == ["one", "two"]
    # Empty and whitespace-only chunks are dropped
    assert split("one<br>  <br>two") == ["one", "two"]
    assert split("") == []
    assert split("no tags here") == ["no tags here"]
