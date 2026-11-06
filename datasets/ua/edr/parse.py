import re
from zipfile import ZipFile
from lxml import etree
from typing import IO
from lxml.etree import _Element as Element, tostring

from followthemoney import registry
from followthemoney.util import make_entity_id

from zavod import Context
from zavod.shed.internal_data import fetch_internal_data

# Beneficial owners written as prose into a FOUNDER field, e.g.
# "КІНЦЕВИЙ БЕНЕФІЦІАРНИЙ ВЛАСНИК (КОНТРОЛЕР) - ПРІЗВИЩЕ ІМ'Я ПО БАТЬКОВІ, адреса".
# The label is spelled many ways (БЕНЕФЕЦІАРНИЙ, БЕНІФІЦІАРНИЙ, БЕЕНФІЦІАРНИЙ, ...).
BO_LABEL = re.compile(
    r"^.{0,80}?(?:"
    r"(?:БЕНЕ|БЕНІ|БЕЕН)\w*\s+ВЛ\w*(?:\s*[(/]?\s*КОН\w*\s*[)/]?)?"
    r"(?:\s+(?:У\s+)?ЮРИДИЧНОЇ\s+ОСОБИ)?"
    r"|ВИГОДООДЕРЖУВАЧ\w*"
    r"|ВЛАСНИК\s+ІСТОТНОЇ\s+УЧАСТІ"
    r")\s*[-–:]?",
    re.IGNORECASE | re.DOTALL,
)
BO_ABSENT = re.compile(r"^(ВІДСУТН|ВІДОМОСТІ ВІДСУТН|НЕМАЄ)", re.IGNORECASE)
# Label fragments and a company code (EDRPOU) that may precede the name.
BO_PREFIX = re.compile(
    r"^(?:[\s.,:;\-–()/\"]|\d{8}|КО\w*ЛЕР\w*|ЮР(?:ИДИЧНОЇ|\.)\s*ОСОБИ"
    r"|ЗАСНОВНИКА|ГР(?:ОМАДЯНИН|\.)\s*УКРАЇНИ)+",
    re.IGNORECASE,
)
# The name runs up to the first comma, digit, sentence break or dash.
BO_NAME_END = re.compile(r"[,;:(]|\d|\.\s|\s[-–]\s")
NAME_TOKEN = re.compile(r"^[^\W\d_]+(?:['’ʼ\-][^\W\d_]+)*$")
PATRONYMIC = re.compile(r"(ВИЧ|ВНА|ІЧНА|ИЧНА)$", re.IGNORECASE)
# Leftover label, legal phrase, address or company words which mean the
# candidate is not a person's name.
NOT_NAME = re.compile(
    r"ОСОБ|ЮРИДИЧН|ВЛАСНИК|ЗАСНОВН|УЧАСНИК|ТОВАРИСТВ|КОНТРОЛЕР|ВІДСУТН|УКРАЇН"
    r"|БЕНЕФ|ВОЛОДІНН|ВСТАНОВ|МОЖЛИВ|ВІДПОВІДН|ЗГІДН|ГРОМАДЯН|РЕСПУБЛІК|ПОДАЄТЬСЯ"
    r"|КОМПАН|КОРПОРАЦ|ПІДПРИЄМСТВ|ФІРМ|ФОНД|ЛІМІТЕД|LIMITED|ГМБХ|GMBH|ЄДРПОУ"
    r"|\b(?:КОД|ТИПУ?|ПРЯМОГО|НЕ|ДО|Є|ПП|ЛТД|LTD|ГРУП)\b",
    re.IGNORECASE,
)


def tag_text(el: Element) -> str:
    return tostring(el, encoding="utf-8").decode("utf-8")


def parse_bo_name(text: str) -> str | None:
    """Extract the person name from the remainder of a beneficial owner prose
    text, or return None if it doesn't look like a name."""
    text = BO_PREFIX.sub("", text)
    candidate = BO_NAME_END.split(text, maxsplit=1)[0]
    if '"' in candidate or "«" in candidate:
        return None
    tokens: list[str] = []
    for token in candidate.split():
        token = token.strip(".-–")
        if not NAME_TOKEN.match(token):
            return None
        tokens.append(token)
        # Surnames can end like patronymics, so only cut from the third token.
        if len(tokens) >= 3 and PATRONYMIC.search(token):
            break
    name = " ".join(tokens)
    if len(tokens) < 2 or len(tokens) > 4 or NOT_NAME.search(name):
        return None
    return name


def parse_owner(context: Context, company_id: str, unique_id: str, el: Element) -> bool:
    """Emit the owner given in a FOUNDER or BENEFICIARY element. Returns False if
    the text is beneficial owner prose from which no name could be extracted."""
    text = el.text
    if text is None or not text.strip():
        return True
    if "причина відсутності:" in text:
        return True
    if text.startswith("релігійна громада в кількості"):
        return True

    schema = "LegalEntity"
    citizenship: str | None = None
    address: str | None = None
    shares: str | None = None
    if el.tag == "BENEFICIARY":
        # "ПІБ; громадянство; адреса"
        parts = [p.strip() for p in text.split(";")]
        if len(parts) < 3:
            context.log.warning("Unexpected beneficiary format", text=text)
            return True
        schema = "Person"
        name: str | None = parts[0]
        citizenship = parts[1]
        address = "; ".join(p for p in parts[2:] if p)
    else:
        name = text
        if ", розмір частки -" in text:
            name, shares = text.rsplit(", розмір частки -", 1)
        label = BO_LABEL.match(name)
        if label is not None:
            rest = name[label.end() :].strip(' -–:")/')
            if not rest or BO_ABSENT.match(rest):
                return True
            name = parse_bo_name(rest)
            if name is None:
                context.log.info("Cannot parse beneficial owner name", text=text)
                return False
            schema = "Person"
        else:
            name = name.strip().strip("-").strip()
            # Over-long texts are a name followed by details about it.
            if len(name) > registry.name.max_length:
                name = name.split(",", 1)[0].strip()
            if len(name) > registry.name.max_length:
                context.log.info("Owner name too long", text=text)
                return True

    owner = context.make(schema)
    owner.id = context.make_id(unique_id, text)
    owner.add("name", name)
    if not owner.has("name"):
        return True
    if citizenship is not None:
        owner.add("citizenship", citizenship)
    owner.add("address", address)
    owner.add("notes", text)
    ownership = context.make("Ownership")
    ownership.id = context.make_id(unique_id, el.tag, text)
    ownership.add("owner", owner)
    ownership.add("asset", company_id)
    ownership.add("role", el.tag)
    ownership.add("sharesValue", shares)
    context.emit(owner)
    context.emit(ownership)
    return True


def parse_uo(context: Context, fh: IO[bytes]) -> None:
    unparsed_bo = 0
    for idx, (_, el) in enumerate(etree.iterparse(fh, tag="RECORD")):
        if idx > 0 and idx % 10000 == 0:
            context.log.info(f"Parse UO records: {idx}...")

        company = context.make("Company")
        long_name = el.findtext("./NAME")
        short_name = el.findtext("./SHORT_NAME")
        edrpou = el.findtext("./EDRPOU")
        unique_id = make_entity_id(edrpou, short_name, long_name)
        company.id = context.make_slug(edrpou, unique_id, strict=False)
        if unique_id is None or company.id is None:
            context.log.info("Skipping record without name or code", xml=tag_text(el))
            el.clear()
            continue
        if short_name and len(short_name.strip()):
            company.add("name", short_name)
            company.add("alias", long_name)
        else:
            company.add("name", long_name)

        company.add("registrationNumber", edrpou)
        company.add("jurisdiction", "ua")
        company.add("address", el.findtext("./ADDRESS"))
        company.add("classification", el.findtext("./KVED"))
        company.add("status", el.findtext("./STAN"))
        context.emit(company)

        for boss in el.findall(".//BOSS"):
            name = boss.text
            director = context.make("Person")
            director.id = context.make_id(unique_id, name)
            director.add("name", name)
            if not len(director.properties):
                continue
            context.emit(director)

            directorship = context.make("Directorship")
            directorship.id = context.make_id(unique_id, "BOSS", name)
            directorship.add("organization", company)
            directorship.add("director", director)
            context.emit(director)

        owners = el.findall("./FOUNDERS/FOUNDER") + el.findall(
            "./BENEFICIARIES/BENEFICIARY"
        )
        for owner_el in owners:
            if not parse_owner(context, company.id, unique_id, owner_el):
                unparsed_bo += 1
        el.clear()
    context.log.info(f"Skipped {unparsed_bo} unparseable beneficial owner texts")


def crawl(context: Context) -> None:
    path = context.get_resource_path("source.zip")
    fetch_internal_data("ua_edr/23022022.zip", path)
    context.log.info(f"Parsing: {path}")
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            if not name.lower().endswith(".xml"):
                continue
            with zip.open(name, "r") as fh:
                if "EDR_UO" in name:
                    parse_uo(context, fh)
