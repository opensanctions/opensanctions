import re
from lxml import html
from rigour.mime.types import HTML

from zavod import Context
from zavod import helpers as h

IN_BRACKETS = re.compile(r"\(([^\)]*)\)")
PROGRAM = "Terrorism (Suppression of Financing) Act 2002; Schedule 2"
DOB = "Date of Birth:"
PASSPORT = "Passport No."


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.data_url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)

    for node in doc.findall(".//td[@class='tailSTxt']"):
        if not node.text_content().startswith("2."):
            continue
        for item in node.findall(".//tr"):
            number = item.find(".//td[@class='sProvP1No']").text_content()
            text = item.find(".//td[@class='sProvP1']").text_content()
            if text.startswith("[Deleted"):
                continue
            text = text.strip().rstrip(";").rstrip(".")
            name, _ = text.split("(", 1)
            names = h.multi_split(name, ["s/o", "@"])

            entity = context.make("Person")
            entity.id = context.make_slug(number, name)
            entity.add("name", names)
            entity.add("topics", "sanction")

            sanction = h.make_sanction(context, entity)
            sanction.add("program", PROGRAM)

            for match in IN_BRACKETS.findall(text):
                # match = match.replace("\xa0", "")
                res = context.lookup("props", match)
                if res is not None:
                    for prop, value in res.props.items():
                        entity.add(prop, value)
                    continue
                if match.endswith("citizen"):
                    cit = match.replace("citizen", "")
                    entity.add("citizenship", cit)
                    continue
                if match.startswith(DOB):
                    dob = match.replace(DOB, "").strip()
                    h.apply_date(entity, "birthDate", dob)
                    continue
                if match.startswith(PASSPORT):
                    passport = match.replace(PASSPORT, "").strip()
                    entity.add("passportNumber", passport)
                    continue
                context.log.warn("Unparsed bracket term", term=match)

            context.emit(entity, target=True)
            context.emit(sanction)
