from lxml import html
from typing import Optional
from pantomime.types import HTML

from zavod import Context
from opensanctions import helpers as h

FORMATS = ["%d/%m/%Y", "%d %B %Y"]

TAGS = {
    "Phone": ("LegalEntity", "phone"),
    "Address": ("LegalEntity", "address"),
    "Passport No": ("Person", "passportNumber"),
    "DOB": ("Person", "birthDate"),
    "AKA": ("LegalEntity", "alias"),
}


def split_narrative(line: str, tag: str) -> Optional[str]:
    try:
        _, value = line.split(tag, 1)
        return value.strip()
    except ValueError:
        return None


def parse_date(date: str):
    date = date.strip()
    return h.parse_date(date, FORMATS)


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.data_url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)
    for item in doc.findall('.//div[@class="accordion-item"]'):
        entity = context.make("LegalEntity")
        entity.id = context.make_id(item.findtext(".//h5"))

        # Manual parsing ftw
        result = context.lookup("props", entity.id)
        if result is not None:
            for prop, value in result.props.items():
                entity.add(prop, value)

        press_release: Optional[str] = None
        narrative: Optional[str] = None
        for div in item.findall(".//div"):
            div_id = div.get("id")
            if div_id is not None and div_id.startswith("London"):
                press_release = div.text_content().strip()
            if div_id is not None and div_id.startswith("Paris"):
                narrative = div.findtext(".//textarea")
        sanction = h.make_sanction(context, entity)
        sanction.add("description", press_release)
        if narrative is None:
            context.log.warn("No narrative", id=entity.id, press_release=press_release)
            continue
        texts = narrative.strip().split("\n")
        notes_parsed = False
        if len(texts) > 1:
            header, lines = texts[0], texts[1:]
            if "-" in header:
                auth_id, name = header.split("-", 1)
                entity.add("name", name)
                sanction.add("authorityId", auth_id)
                for line in lines:
                    reason = split_narrative(line, "Reason for designation:")
                    if reason is not None:
                        sanction.add("reason", reason)
                        continue
                    reg_date = split_narrative(line, "Date of Registration:")
                    if reg_date is not None:
                        sanction.add("startDate", parse_date(reg_date))
                        continue
                    if ":" in line:
                        tag, value = line.split(":", 1)
                        if tag in TAGS:
                            tag_schema, tag_prop = TAGS[tag]
                            if tag_prop == "birthDate":
                                value = parse_date(value)
                            if tag_prop == "phone":
                                value = value.split(",")
                            entity.add_cast(tag_schema, tag_prop, value)
                            continue
                    context.log.warn("Unparsed line", line=line)

        if not notes_parsed:
            entity.add("notes", narrative.strip())

        if not entity.has("name"):
            context.log.info("Entity has no name", id=entity.id, narrative=narrative)
        # print(entity.id, narrative, text)
        context.emit(entity, target=True)
        context.emit(sanction)
