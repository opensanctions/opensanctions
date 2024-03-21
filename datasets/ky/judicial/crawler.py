from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


def crawl_judges(context: Context, url, position, get_name, get_judges=None):
    request_response = context.fetch_html(url, cache_days=30)

    if get_judges is not None:
        parsed_judges = get_judges(request_response)
        judges = []
        for judge in parsed_judges:
            judge_name = get_name(judge)
            judges += [(judge_name, position, url)]
        return judges
    else:
        judge_name = get_name(request_response)
        return [(judge_name, position, url)]


def emit_entities(context: Context, entities_data):
    for entity in entities_data:
        name, position, url = entity
        person_proxy = context.make("Person")
        person_proxy.id = context.make_id(name)
        person_proxy.add("sourceUrl", url)
        h.apply_name(person_proxy, full=name)
        person_proxy.add("topics", "role.judge")

        position = h.make_position(
            context,
            name=position,
            country="Cayman Islands",
        )
        categorisation = categorise(context, position)
        if categorisation.is_pep:
            occupancy = h.make_occupancy(
                context, person_proxy, position, True, categorisation=categorisation
            )
            context.emit(person_proxy, target=True)
            context.emit(position)
            context.emit(occupancy)


def crawl(context: Context):
    all_judges = []

    # Get Chief Justice
    chief_justice_url = urljoin(context.data_url, "chief-justice")

    def get_name(elm):
        return (
            elm.xpath('.//div[contains(@class, "fusion-flex-column-wrapper-legacy")]')[
                2
            ]
            .text_content()
            .split(",")[0]
            .replace("Hon. ", "")
        )

    chief_justice = crawl_judges(
        context, chief_justice_url, "Chief of Justice", get_name
    )
    all_judges += chief_justice

    # Get President of court
    president_court_url = urljoin(context.data_url, "president-court-of-appeal")

    def get_name(elm):
        return (
            elm.xpath('.//h4[contains(@class, "panel-title")]')[0]
            .text_content()
            .split("Sir")[-1]
            .strip()
        )

    president_court = crawl_judges(
        context, president_court_url, "President of the Court of Appeal", get_name
    )
    all_judges += president_court

    # Get Justices of Appeal
    justices_of_appeal_url = urljoin(context.data_url, "judges-of-appeal")

    def get_name(elm):
        elm_text = elm.text_content()
        if "Sir" in elm_text:
            return elm_text.split("Sir")[-1].strip()
        else:
            return elm_text.split("Hon.")[-1].replace("Justice", "").strip()

    def get_judges(page):
        return page.xpath('.//div[contains(@class, "accordian")]')[0].xpath(".//h4")

    justices_of_appeal = crawl_judges(
        context, justices_of_appeal_url, "Judge of Appeal", get_name, get_judges
    )
    all_judges += justices_of_appeal

    # Get Judges
    def get_name(elm):
        return (
            elm.xpath('.//span[contains(@class, "fusion-toggle-heading")]')[0]
            .text.replace("Hon. ", "")
            .split(",")[0]
            .replace(" CBE", "")
        )

    def get_judges(page):
        return page.xpath("//div[contains(@class, 'accordian')]")[0].xpath(
            ".//div[contains(@class, 'fusion-panel')]"
        )

    judges = crawl_judges(
        context, context.dataset.data.url, "Judge", get_name, get_judges
    )
    all_judges += judges

    emit_entities(context, all_judges)
