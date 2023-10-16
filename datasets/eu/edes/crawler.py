import json
from datetime import datetime
from zavod import Context
from zavod import helpers as h


def convert_date(date_str: str) -> str:
    date = datetime.fromtimestamp(int(date_str) / 1000)
    return date.date().isoformat()


def crawl(context: Context) -> None:
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json",
    }
    data = {"ecOpCountryCode": None, "ecOpName": None, "page": 0, "take": 100}
    res = context.http.post(
        context.data_url,
        data=json.dumps(data),
        headers=headers,
    )
    res_data = json.loads(res.text.split("\n", 1)[1])
    for row in res_data["content"]:
        name = row.pop("ecOpName")
        entity = context.make("Company")
        entity.id = context.make_id(name)
        name = name.split("*")
        entity.add("name", name)
        entity.add("address", row.pop("ecOpAddress"))
        entity.add("country", row.pop("ecOpCountryCode"))
        entity.add("country", row.pop("ecOpCountryName"))

        sanction = h.make_sanction(context, entity)
        sanction.add("summary", row.pop("comments").replace("<br>", "\n"))
        sanction.add("reason", row.pop("grounds").replace("<br>", "\n"))
        sanction.add("provisions", row.pop("typeLabel"))
        sanction.add("startDate", convert_date(row.pop("from")))
        sanction.add("endDate", convert_date(row.pop("to")))

        context.emit(entity, target=True)
        context.emit(sanction)
        # print(row)
