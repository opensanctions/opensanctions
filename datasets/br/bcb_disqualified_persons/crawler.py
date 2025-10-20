import itertools
from typing import Any, Iterable, Tuple

from zavod import Context
from zavod import helpers as h


def crawl_person(context: Context, name: str, lines: Iterable[dict[str, Any]]) -> None:
    """Crawl all entries for a person."""
    first_line = next(iter(lines))
    person = context.make("Person")
    tax_number = first_line["CPF"]
    person.id = context.make_slug(tax_number, prefix="br-cpf")
    person.add("name", first_line["Nome"])
    person.add("taxNumber", tax_number)
    person.add("country", "br")
    person.add("topics", "corp.disqual")

    # When a person is listed multiple times, the end date is only listed in one of the lines.
    # We extract that one end date here so that we can use it below.
    end_dates = {line["Prazo_final_penalidade"] for line in lines}
    end_dates = {date for date in end_dates if date != "Consolidado"}

    for line in lines:
        assert line["Nome"] == first_line["Nome"]
        assert line["CPF"] == first_line["CPF"], (
            "CPF for all lines with the same name should be the same"
        )
        pas_number = line.pop("PAS")

        sanction = h.make_sanction(context, person, key=pas_number)
        # The ID of the process
        sanction.add(
            "description",
            "Administrative Sanctioning Process Number: {}".format(pas_number),
        )

        # The duration is always in years
        sanction.add("duration", "{} year(s)".format(line.pop("Prazo_em_anos")))
        # The start and end dates are in the format YYYY-MM-DD
        sanction.add("startDate", line.pop("Inicio_do_cumprimento"))
        # If the end date is "Consolidado", the end date is only listed in one of the lines
        # referring to this person.
        if line["Prazo_final_penalidade"] == "Consolidado":
            if len(end_dates) != 1:
                context.log.error(
                    'End date "Consolidado", but multiple other end dates found. '
                    "Unclear which one should be used, using the first one.",
                    end_dates=end_dates,
                )
            sanction.add("endDate", next(iter(end_dates)))
        else:
            sanction.add("endDate", line.pop("Prazo_final_penalidade"))

        context.audit_data(line)

        context.emit(sanction)
    context.emit(person)


def crawl(context: Context) -> None:
    """
    Entrypoint to the crawler.

    The crawler works by fetching the data from the URL as a JSON.
    The data is already in the format of a list of dicts, so we just need to create the entities.

    :param context: The context object.
    """
    response = context.fetch_json(context.data_url)
    if "value" not in response:
        context.log.error("Value not found in JSON")
        return
    data = response["value"]

    lines_by_name: Iterable[Tuple[str, Iterable[dict[str, Any]]]] = itertools.groupby(
        data, lambda line: line["Nome"]
    )

    for name, lines in lines_by_name:
        crawl_person(context, name, lines)
