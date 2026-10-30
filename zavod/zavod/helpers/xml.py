from hashlib import sha1

from lxml import etree
from normality import squash_spaces

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


def xml_node_hash(node: ElementOrTree | None, text_only: bool = False) -> str | None:
    """Compute a SHA1 hash of an XML/HTML node to detect changes in its content.

    The hash ignores case and whitespace. Namespace declarations are part of
    the hashed form, so hash consistently either before or after
    `remove_namespace`.

    Args:
        node: The element or tree to hash.
        text_only: Hash only the text content, ignoring markup and attributes.

    Returns:
        A hex digest, or `None` if the node is missing or serialises to nothing.
    """
    digest = sha1()
    if node is None:
        return None
    if text_only:
        serialised = etree.tostring(
            node,
            method="text",
            encoding="utf-8",
        )
    else:
        serialised = etree.tostring(
            node,
            with_comments=False,
            pretty_print=True,
            method="c14n2",
        )
    text = squash_spaces(serialised.decode("utf-8").lower())
    if len(text) == 0:
        return None
    digest.update(text.replace(" ", "").encode("utf-8"))
    return digest.hexdigest()
