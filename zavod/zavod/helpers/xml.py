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


def _normalise(text: str | None) -> str:
    if text is None:
        return ""
    return squash_spaces(text.lower()).replace(" ", "")


def _sorted_digest(el: etree._Element) -> str:
    digest = sha1(_normalise(str(el.tag)).encode("utf-8"))
    for key, value in sorted((str(k), str(v)) for k, v in el.attrib.items()):
        digest.update(f"\x1f{key}={_normalise(value)}".encode())
    digest.update(f"\x1e{_normalise(el.text)}".encode())
    children = [_sorted_digest(c) for c in el.iterchildren(etree.Element)]
    for child in sorted(children):
        digest.update(child.encode("utf-8"))
    digest.update(f"\x1d{_normalise(el.tail)}".encode())
    return digest.hexdigest()


def xml_node_sorted_hash(node: ElementOrTree | None) -> str | None:
    """Compute a SHA1 hash of an XML node that ignores the order of its elements.

    Use this instead of `h.xml_node_hash` for data-style XML whose publisher
    serialises sibling elements in no stable order, so that a reshuffled export
    doesn't register as a change. Like `h.xml_node_hash`, the hash ignores case
    and whitespace. Text after a child element moves with that element, which
    makes the hash unfit for HTML and other mixed content, where that order
    carries meaning.

    Args:
        node: The element or tree to hash.

    Returns:
        A hex digest, or `None` if the node is missing.
    """
    if node is None:
        return None
    if isinstance(node, etree._ElementTree):
        node = node.getroot()
    return _sorted_digest(node)
