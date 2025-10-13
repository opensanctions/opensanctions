from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h


def crawl(context: Context) -> None:
    res = context.fetch_json(context.data_url)  # , method="POST")
    rows = res.pop("rows", [])
    for row in rows:
        person = context.make("Person")
        name, gremium, gender, period, parties, attr, akgr, klub, district, bio = row
        person.id = context.make_slug(attr["uri"])
        person.add("name", name)
        person.add("name", attr["zit"])
        person.add("citizenship", "at")
        person.add("title", akgr)  # akgr: Akademischer Grad
        person.add("gender", gender)
        url = urljoin(context.data_url, bio)
        person.add("sourceUrl", url)

        for mand in attr["mandate_kompakt"]["mandate"]:
            position = h.make_position(
                context,
                name=mand["funktion_text"],
                country="at",
                topics=["gov.legislative", "gov.national"],
            )
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                start_date=mand["mandatVon"],
                end_date=mand["mandatBis"],
            )
            if occupancy is not None:
                context.emit(occupancy)
                context.emit(position)

        if "role.pep" not in person.get("topics"):
            continue

        html = context.fetch_html(url, cache_days=7)
        for para in html.findall(".//section/p"):
            note = h.element_text(para)
            if note.startswith("Geb.:"):
                dob = note[5:]
                if ", " in dob:
                    dob, pob = dob.split(",", 1)
                    person.add("birthPlace", pob.strip())
                h.apply_date(person, "birthDate", dob.strip())
            elif note.startswith("Verst.:"):
                dod = note[7:].strip().split(",")[0]
                h.apply_date(person, "deathDate", dod)
            elif note.startswith("Berufliche Tätigkeit:"):
                note = note[22:].strip()
                person.add("education", note)

        context.emit(person)
