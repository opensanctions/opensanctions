from lxml import html

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
