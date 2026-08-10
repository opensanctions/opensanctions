"""Extract the source XML for a single INN out of an EGRUL archive.

Point this at a zip from gs://egrul.opensanctions.org (or at the local cache under
LOCAL_BUCKET_CACHE_DIR) to see what the crawler actually had to work with, e.g. when
an `origin` from the output CSVs points at a record that looks wrong. The extracted
XML is written to a file in the current directory.
"""

from pathlib import Path
from typing import Generator
from zipfile import ZipFile

import click
from lxml import etree
from lxml.etree import _Element as Element

# Legal entities carry the INN on СвЮЛ/@ИНН, sole traders on СвИП/@ИННФЛ.
INN_XPATHS = [
    "//СвЮЛ[@ИНН=$inn]",
    "//СвИП[@ИННФЛ=$inn]",
]


def find_entities(data: bytes, inn: str) -> Generator[Element, None, None]:
    """Yield the entity elements matching the INN in one XML document."""
    doc = etree.fromstring(data)
    for xpath in INN_XPATHS:
        for el in doc.xpath(xpath, inn=inn):
            yield el


@click.command()
@click.argument("archive", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("inn")
def main(archive: Path, inn: str) -> None:
    """Extract the entity with INN from the EGRUL archive at ARCHIVE.

    Writes <INN>-<archive name>.xml in the current directory. All matches in the
    archive go into that file, each preceded by a comment naming the file it came
    from, in the same format as the `origin` field of the generated CSVs.
    """
    out_path = Path("%s-%s.xml" % (inn, archive.stem))
    found = 0
    with ZipFile(archive, "r") as zip, out_path.open("w") as out_fh:
        for name in zip.namelist():
            if not name.lower().endswith(".xml"):
                continue
            data = zip.read(name)
            # Parsing every document in a full dump takes minutes, and an INN that
            # isn't in the raw bytes can't be in the parsed tree either.
            if inn.encode("utf-8") not in data:
                continue
            for el in find_entities(data, inn):
                found += 1
                out_fh.write("<!-- %s/%s -->\n" % (archive.name, name))
                out_fh.write(etree.tostring(el, pretty_print=True, encoding="unicode"))

    if found == 0:
        out_path.unlink()
        raise click.ClickException("No entity with INN %s in %s" % (inn, archive))

    click.echo("Wrote %d match(es) to %s" % (found, out_path))


if __name__ == "__main__":
    main()
