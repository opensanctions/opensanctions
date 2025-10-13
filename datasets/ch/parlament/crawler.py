from itertools import count

from requests import RequestException
from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h

IGNORE_FIELDS = {
    "id",
    "number",
    "code",
    "council",
    "updated",
    "concerns",
    "party",
    "partyId",
    "domicile",
    "cantonName",
    "displayLanguage",
    "salutationLetter",
    "dateOfDeath",
    "committeeMemberships",
    "militaryGrade",
    "professions",
}


def crawl_councillor(context: Context, councillor_id: int) -> None:
    """Fetch and process detailed profile for a single councillor."""
    data = context.fetch_json(
        f"http://ws-old.parlament.ch/councillors/{councillor_id}",
        params={"lang": "en", "format": "json"},
        cache_days=1,
    )

    date_of_death = data.pop("dateOfDeath", None)
    if date_of_death is not None:
        return  # Skip deceased persons
    for field in IGNORE_FIELDS:
        data.pop(field, None)

    entity = context.make("Person")
    entity.id = context.make_slug("councillor", str(councillor_id))
    h.apply_name(
        entity,
        first_name=data.pop("firstName"),
        last_name=data.pop("lastName"),
        lang="eng",
    )
    h.apply_date(entity, "birthDate", data.pop("birthDate", None))
    birth_place = data.pop("birthPlace", {})
    entity.add("birthPlace", birth_place.get("city"))
    entity.add("gender", data.pop("gender", None))
    entity.add("title", data.pop("title", None))
    entity.add("title", data.pop("salutationTitle", None))
    entity.add("nationality", "ch")
    entity.add("spokenLanguage", data.pop("language", None))
    entity.add("spokenLanguage", data.pop("workLanguage", None))
    entity.add("political", data.pop("partyName", None))
    entity.add("religion", data.pop("officialDenomination", None))
    entity.add("notes", data.pop("mandate", None))

    contact = data.pop("contact", {})
    entity.add("email", contact.get("emailWork"))
    entity.add("website", contact.get("homepagePrivate"))
    entity.add("website", contact.get("homepageWork"))
    entity.add("phone", contact.get("phoneMobileWork"))
    entity.add("phone", contact.get("phoneWork"))

    entity.add("address", data.pop("canton", None))
    for place in data.pop("homePlaces", []):
        entity.add("address", place.get("city"))

    postal = data.pop("postalAddress", {})
    if postal.get("city") is not None:
        addr = h.make_address(
            context,
            street=postal.get("addressLine"),
            postal_code=postal.get("zip"),
            city=postal.get("city"),
            country_code="ch",
        )
        h.copy_address(entity, addr)

    is_pep = data.pop("active", False)
    if is_pep:
        entity.add("topics", "role.pep")
    for mem in data.pop("councilMemberships", []):
        body = mem.get("council")
        assert isinstance(body, dict)
        name = body.get("name")
        assert name is not None
        position = h.make_position(
            context,
            name=name,
            country="ch",
            topics=["gov.legislative", "gov.national"],
            # lang="eng",
        )
        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue
        context.emit(position)
        occupancy = h.make_occupancy(
            context,
            entity,
            position,
            start_date=mem.get("entryDate"),
            end_date=mem.get("leavingDate"),
            no_end_implies_current=True,
            propagate_country=False,
        )
        if occupancy is None:
            continue
        occupancy.add("description", mem.get("cantonName"))
        context.emit(occupancy)

    context.emit(entity)


def crawl(context: Context) -> None:
    """Crawl Swiss Federal Assembly members from the Parliament API."""

    for page in count(start=1):
        params = {"pageNumber": page, "lang": "en", "format": "json"}
        try:
            data = context.fetch_json(context.data_url, params=params)
        except RequestException as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return
            raise

        if not isinstance(data, list):
            context.log.error("Expected list of councillors", data=data)
            return

        for councillor in data:
            if not isinstance(councillor, dict):
                context.log.warning("Invalid councillor data", data=councillor)
                continue

            councillor_id = councillor.get("id")
            if councillor_id is None:
                context.log.warning("Councillor missing ID", councillor=councillor)
                continue

            crawl_councillor(context, councillor_id)
