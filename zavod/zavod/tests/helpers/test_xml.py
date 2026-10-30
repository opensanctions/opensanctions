from lxml import etree, html
from zavod.helpers.xml import remove_namespace, xml_node_hash
from zavod.tests.conftest import XML_DOC


def test_xml_remove_namespace():
    with open(XML_DOC) as fh:
        doc = etree.parse(fh)
    assert doc.find(".//name") is None
    new_doc = remove_namespace(doc)
    assert new_doc.findtext(".//name") == "Peter Smith"


def test_xml_node_hash():
    doc = html.fromstring("<html><body>hello, world!</body></html>")
    assert xml_node_hash(doc) == "4b5f0790985812c511f5d01e807c739625104288"
    doc = html.fromstring("<html><body>Hello,   World!</body></html>")
    assert xml_node_hash(doc) == "4b5f0790985812c511f5d01e807c739625104288"

    styled = html.fromstring("<html><body><b>hello, world!</b></body></html>")
    assert xml_node_hash(styled) != xml_node_hash(doc)
    assert xml_node_hash(styled, text_only=True) == xml_node_hash(doc, text_only=True)

    assert xml_node_hash(None) is None
    assert xml_node_hash(html.fromstring("<p> </p>"), text_only=True) is None
