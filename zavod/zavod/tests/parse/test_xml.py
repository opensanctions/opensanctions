from lxml import etree
from zavod.parse.xml import remove_namespace
from zavod.tests.conftest import XML_DOC


def test_xml_remove_namespace():
    with open(XML_DOC, "r") as fh:
        doc = etree.parse(fh)
    assert doc.find(".//name") is None
    new_doc = remove_namespace(doc)
    assert new_doc.findtext(".//name") == "Peter Smith"
