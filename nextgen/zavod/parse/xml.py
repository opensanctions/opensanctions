from typing import Union
from lxml import etree

ElementOrTree = Union[etree._Element, etree._ElementTree]


def remove_namespace(el: ElementOrTree) -> ElementOrTree:
    """Remove namespace in the passed XML/HTML document in place and
    return an updated element tree.

    If the namespaces in a document define multiple tags with the same
    local tag name, this will create ambiguity and lead to errors. Most
    XML documents, however, only actively use one namespace."""
    for elem in el.iter():
        elem.tag = etree.QName(elem).localname
        for key, value in list(elem.attrib.items()):
            local_key = etree.QName(key).localname
            if key != local_key:
                elem.attrib[local_key] = value
    etree.cleanup_namespaces(el)
    return el
