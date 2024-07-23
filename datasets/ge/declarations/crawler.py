from urllib.parse import urlencode
from zavod.context import Context
from datetime import datetime
import re



DECLARATION_LIST_URL = "https://declaration.acb.gov.ge/Home/DeclarationList"
REGEX_CHANGE_PAGE = re.compile(r"changePage\((\d+), \d+\)")


def crawl_declaration(context: Context, item: dict) -> None:
    print(item["Id"], item["FirstName"], item["LastName"])


def query_declaration(context: Context, year: int, name: str, cache_days: int) -> None:
    query = {
        "Key": name,
        "YearSelectedValues": year,
    }
    url = f"{context.data_url}?{urlencode(query)}"
    declarations = context.fetch_json(url, cache_days=cache_days)
    return declarations


def crawl(context: Context) -> None:
    current_year = datetime.now().year
    for year in [current_year, current_year - 1]:
        page = 1
        max_page = None

        while max_page == None or page <= max_page:
            url = f"{DECLARATION_LIST_URL}?yearSelectedValues={year}&page={page}"
            cache_days = 1 if year == current_year else 30
            doc = context.fetch_html(url, cache_days=cache_days)
            page_count_el = doc.find(".//li[@class='PagedList-skipToLast']/a")
            page_count_match = REGEX_CHANGE_PAGE.match(page_count_el.get("onclick"))
            max_page = int(page_count_match.group(1))
            cache_days = 7 if year == current_year else 30

            for row in doc.findall(".//div[@class='declaration1']"):
                name = row.find(".//h3").text_content()
                declarations = query_declaration(context, year, name, cache_days)
                for declaration in declarations:
                    crawl_declaration(context, declaration)
            
            page += 1
