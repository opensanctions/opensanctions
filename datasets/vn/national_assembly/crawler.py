import re

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.util import Element

# The 15th legislature has ~500 seats. A count far outside this band means the site
# changed or a new legislature (khóa XVI, elected ~mid-2026) has superseded the pinned
# /XV/ path — fail loudly rather than emit a stale or truncated roster.
MIN_DEPUTIES = 450
MAX_DEPUTIES = 520

# Anti-bot wall: the first response is a 177-byte stub that sets a "D1N" cookie via
# JavaScript and reloads. We read the hash and set it on the session; no JS engine needed.
COOKIE_RE = re.compile(r"D1N=([0-9a-f]+)")

# Profile link: /daibieu/{province_id}/{deputy_id}/{tab}/{slug}.aspx — tab 1 is the bio.
LINK_RE = re.compile(r"/daibieu/(\d+)/(\d+)/(\d+)/[^/]+\.aspx")

DATE_RE = re.compile(r"\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{4}")

# Phrases in the occupation / note fields that mark the deputy losing their mandate.
REMOVAL_MARKERS = ["bãi nhiệm", "miễn nhiệm", "cho thôi làm nhiệm vụ"]
DECEASED_MARKER = "từ trần"

# Profile field labels.
NAME = "Họ và tên"
COMMON_NAME = "Tên thường gọi"
BIRTH_DATE = "Ngày sinh"
GENDER = "Giới tính"
ETHNICITY = "Dân tộc"
RELIGION = "Tôn giáo"
NATIVE_PLACE = "Quê quán"
# Note: the residence field ("Nơi cư trú") is wrapped in an HTML comment in the source
# (a publisher privacy choice), so it is never parsed — we deliberately don't emit it.
EDUCATION = "Trình độ chuyên môn"
OCCUPATION = "Nghề nghiệp, chức vụ"
WORKPLACE = "Nơi làm việc"
NOTE = "Phụ chú"

# Fields we read for context but do not emit directly.
IGNORE_FIELDS = [
    COMMON_NAME,
    NOTE,
    "Trình độ chính trị",  # political-theory level
    "Ngày vào đảng",  # date joined the party
    "Đoàn ĐBQH",  # delegation (province) — constituency, not emitted on position
    "Đại biểu Quốc hội khoá",  # legislatures served
    "Đại biểu chuyên trách",  # full-time deputy scope
    "Đại biểu Hội đồng Nhân dân",  # People's Council member
]


def parse_fields(container: Element) -> dict[str, str]:
    """Parse the ``<p><strong>Label:</strong> value</p>`` profile rows into a dict.

    Labels are normalised by stripping the trailing colon (some carry surrounding
    spaces, e.g. ``Nghề nghiệp, chức vụ :``).
    """
    fields: dict[str, str] = {}
    for para in h.xpath_elements(container, ".//p[strong]"):
        strong = para.find("strong")
        if strong is None:
            continue
        label_text = h.element_text(strong)
        full = h.element_text(para)
        value = full[len(label_text) :].strip() if full.startswith(label_text) else full
        label = re.sub(r"\s*:\s*$", "", label_text).strip()
        if label:
            fields[label] = value
    return fields


def crawl_deputy(context: Context, url: str, province: str, deputy: str) -> None:
    doc = context.fetch_html(url, cache_days=7)
    container = h.xpath_element(doc, ".//div[contains(@class, 'content-view')]")
    fields = parse_fields(container)

    person = context.make("Person")
    # Province and deputy ids are opaque source identifiers (no PII).
    person.id = context.make_slug(province, deputy)
    person.add("sourceUrl", url)
    # National parliament: deputies must be Vietnamese citizens per the Constitution of
    # Vietnam (2013), Article 27 (read with Article 17 defining "citizen").
    # https://www.constituteproject.org/constitution/Socialist_Republic_of_Vietnam_2013
    person.add("citizenship", "vn")

    name = fields.pop(NAME, None)
    if name is None:
        context.log.warning("Deputy profile without a name", url=url)
        return
    person.add("name", name)
    common_name = fields.get(COMMON_NAME)
    if common_name is not None and common_name != name:
        person.add("alias", common_name)

    h.apply_date(person, "birthDate", fields.pop(BIRTH_DATE, None))
    # "Nam"/"Nữ" are translated by the type.gender lookup in the dataset YAML.
    person.add("gender", fields.pop(GENDER, None))
    person.add("ethnicity", fields.pop(ETHNICITY, None))
    religion = fields.pop(RELIGION, None)
    if religion is not None and religion != "Không":  # "Không" = none
        person.add("religion", religion)
    person.add("birthPlace", fields.pop(NATIVE_PLACE, None))
    person.add("education", fields.pop(EDUCATION, None))
    person.add("position", fields.pop(WORKPLACE, None))

    # Detect deputies who lost their mandate during the term. The marker may be in the
    # occupation field (which is then a status note, not a role) or only in the note
    # field (where the occupation still describes a genuine former role).
    occupation = fields.pop(OCCUPATION, None)
    note = fields.get(NOTE, "")
    occupation_lower = (occupation or "").lower()
    is_removed = any(m in f"{occupation_lower} {note.lower()}" for m in REMOVAL_MARKERS)
    is_deceased = DECEASED_MARKER in f"{occupation_lower} {note.lower()}"
    occupation_is_status = any(
        m in occupation_lower for m in REMOVAL_MARKERS + [DECEASED_MARKER]
    )
    if occupation is not None and not occupation_is_status:
        person.add("position", occupation)

    end_date = None
    if is_removed or is_deceased:
        dates = DATE_RE.findall(occupation or "") or DATE_RE.findall(note)
        if len(dates) == 0:
            context.log.warning(
                "Deputy marked removed/deceased without a parseable date",
                url=url,
                occupation=occupation,
            )
        else:
            # Source sometimes writes "18 /01/2023"; drop spaces for date parsing.
            end_date = re.sub(r"\s+", "", dates[-1])
        if is_deceased:
            h.apply_date(person, "deathDate", end_date)

    context.audit_data(fields, ignore=IGNORE_FIELDS)

    position = h.make_position(
        context,
        name="Member of the National Assembly of Vietnam",
        country="vn",
        topics=["gov.national"],
        wikidata_id="Q10841192",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        end_date=end_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(person)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    # Bootstrap the anti-bot cookie: the bare request returns the JS stub with the hash.
    stub = context.fetch_text(context.data_url)
    match = COOKIE_RE.search(stub or "")
    if match is None:
        raise RuntimeError(
            "Could not extract the D1N anti-bot cookie from the stub page"
        )
    context.http.cookies["D1N"] = match.group(1)

    doc = context.fetch_html(context.data_url, cache_days=1)

    deputies: dict[tuple[str, str], str] = {}
    for anchor in h.xpath_elements(doc, ".//a[contains(@href, '/daibieu/')]"):
        href = anchor.get("href")
        if href is None:
            continue
        link = LINK_RE.search(href)
        if link is None:
            continue
        province, deputy, tab = link.groups()
        if tab != "1":  # other tabs are alternate views of the same deputy
            continue
        deputies[(province, deputy)] = href

    count = len(deputies)
    if not (MIN_DEPUTIES <= count <= MAX_DEPUTIES):
        context.log.error("Unexpected number of deputies", count=count)
        raise ValueError(
            f"Expected {MIN_DEPUTIES}-{MAX_DEPUTIES} deputies, found {count}"
        )

    for (province, deputy), url in deputies.items():
        crawl_deputy(context, url, province, deputy)
