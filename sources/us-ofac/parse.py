import sys
from lxml import etree


def parse_list(xml_file):
    doc = etree.parse(xml_file)
    print doc


if __name__ == '__main__':
    xml_file = sys.argv[1]
    parse_list(xml_file)
