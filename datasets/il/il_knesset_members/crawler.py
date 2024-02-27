from datetime import datetime
import json
from zavod import Context, helpers as h
from zavod.logic.pep import categorise

DATE_FORMATS = ["%B %d, %Y"]


def get_members_urls(context: Context) -> list:
    # return list with dictionary containing id, name, url of member detail page and flag if member is currenttly within the knesset

    # load members json file (note: it's XML when using the browser)
    path = context.fetch_resource("members.json", context.data_url)

    # save json file locally
    context.export_resource(path, "text/json", title=context.SOURCE_TITLE)

    # load json into list with dictionaries
    with open(path, "r") as fh:
        list_dict_members = json.load(fh)

    # add url of member detail-page to the data
    [
        member.update(
            {
                "url": f'https://knesset.gov.il/WebSiteApi/knessetapi/MKs/GetMkDetailsContent?mkId={member["ID"]}&languageKey=en'
            }
        )
        for member in list_dict_members
    ]

    return list_dict_members


def crawl_item(dict_member: dict, context: Context):
    # fetch data from member detail-page and create the entities

    try:
        path = context.fetch_resource(
            f"member_detail_{dict_member['ID']}.json", dict_member["url"]
        )
        context.export_resource(path, "text/json", title=context.SOURCE_TITLE)
        with open(path, "r") as fh:
            dict_member.update(json.load(fh))
        context.log.info(
            f"Parsed details for member: {dict_member['Name']} (ID:{dict_member['ID']})"
        )
    except Exception as e:
        context.log.warning(
            f"Couldn't parse details for member: {dict_member['Name']} (ID:{dict_member['ID']}), error: {e}"
        )

    # add current members only
    if dict_member["IsCurrent"] == True:
        person = context.make("Person")
        person.id = context.make_id(dict_member["Name"])
        person.add("name", dict_member["Name"])
        person.add("country", "il")

        # parse dates
        for date_field in ["DateOfBirth", "DeathDate"]:
            if dict_member.get(date_field, False):
                dict_member[date_field] = h.parse_date(
                    dict_member[date_field], DATE_FORMATS
                )[0]

        # split first and last name
        name_parts = [part for part in dict_member["Name"].split(" ") if part != ""]
        last_name = name_parts[-1]
        first_names = " ".join(name_parts[:-1])

        h.apply_name(
            person,
            first_name=first_names,
            last_name=last_name,
        )
        person.add("sourceUrl", dict_member["url"])

        # create position
        position = h.make_position(context, "Knesset member (2022-)", country="il")
        categorisation = categorise(context, position, is_pep=True)

        if not categorisation.is_pep:
            return

        # update if new knesset is elected
        start_date_knesset_25th = datetime(2022, 11, 15)
        end_date_knesset_25th = None

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            True,
            start_date=end_date_knesset_25th,
            categorisation=categorisation,
        )

        if occupancy is None:
            return

        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context):
    list_dict_members = get_members_urls(context)

    if list_dict_members is None:
        return

    for i, dict_member in enumerate(list_dict_members):
        crawl_item(dict_member, context)
