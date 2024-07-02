from lxml import etree
from zavod.util import ElementOrTree


def remove_namespace(el: ElementOrTree) -> ElementOrTree:
    """Remove namespace in the passed XML/HTML document in place and
    return an updated element tree.

    If the namespaces in a document define multiple tags with the same
    local tag name, this will create ambiguity and lead to errors. Most
    XML documents, however, only actively use one namespace.

    Args:
        el: The root element or tree to remove namespaces from.

    Returns:
        An updated element tree with the namespaces removed.
    """
    for elem in el.iter():
        # https://stackoverflow.com/a/47233934
        if elem.tag is etree.Comment:  # type: ignore
            # Can't make a QName from a comment
            continue
        elem.tag = etree.QName(elem).localname
        for key, value in list(elem.attrib.items()):
            local_key = etree.QName(key).localname
            if key != local_key:
                elem.attrib[local_key] = value
    etree.cleanup_namespaces(el)
    return el
