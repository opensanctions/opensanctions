from lxml import etree


def remove_namespace(doc):
    """Remove namespace in the passed XML/HTML document in place and
    return an updated element tree.

    If the namespaces in a document define multiple tags with the same
    local tag name, this will create ambiguity and lead to errors. Most
    XML documents, however, only actively use one namespace."""
    for elem in doc.getiterator():
        elem.tag = etree.QName(elem).localname
    etree.cleanup_namespaces(doc)
    return doc
