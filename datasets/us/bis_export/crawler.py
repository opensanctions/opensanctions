from typing import Tuple
import re

from zavod import Context, helpers as h

STOPPHRASES: Tuple[str, ...] = (
    "A.K.A",
    "A/K/A",
    "F/K/A",
    "a.k.a",
    "a.k.a.",
    "a/k/a",
    "aka/dba",
    "also known as",
    "d/b/a",
    "dba",
    "doing business as",
    "earlier known as",
    "f/k/a",
    "former",
    "formerly",
    "formerly known as",
    "presently known as",
    "previously known as",
    "trading as",
) # pudo's tuple of aliases from rigour.data.names.data
#AKA_PATTERNS = rf"(\b(?:{'|'.join(re.escape(p) for p in STOPPHRASES)})\b)" # from zavod.helpers.names
AKA_PATTERNS = rf"\b(?:{'|'.join(re.escape(p) for p in STOPPHRASES)})\b"
REGEX_KNOWN_AS = re.compile(AKA_PATTERNS, flags=re.IGNORECASE)


def crawl(context: Context) -> None:
    irregular_name_hits = 0
    # context.data_url with validation <== context.dataset.data.url without validation
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    table = h.xpath_elements(doc, ".//table", expect_exactly=1)

    for row in h.parse_html_table(table[0]):
        str_row = h.cells_to_str(row)

        case_id_string = str_row.pop("case_id")
        case_id_element = row.get("case_id")
        assert case_id_element is not None
        url = h.xpath_elements(case_id_element, ".//a")[0].get("href")

        case_name = str_row.pop("case_name")
        order_date = str_row.pop("order_date")
        
        for name in h.multi_split(
            case_name, ";"
            # ^ to capture some splits, split also happens by /n, ',', 'and'
        ):  # case-level names, might contain multiple entities
            entity = context.make("LegalEntity")
            entity.id = context.make_id(
                name, case_id_string
            )
            # we want to handle akas automatically but flag remaining irregular names for manual review
            if h.is_name_irregular(entity, name):
                irregular_name_hits += 1
                res = context.lookup("comma_names", name, warn_unmatched=True)
                if res: 
                    name = res.name
                    aliases = res.alias
                    entity.add("name", name)
                    entity.add("alias", aliases)

                # if irregular_name_hits == 4:
                #     # finding ['Pejman Mahmood Kosarayanifard', 'a/k/aKosarian Fard'] #2 aka without spacing 
                #     # Ali Abdullah Alhay,a/k/a Ali Alhay, a/k/a Ali Abdullah Ahmed Alhay # correct aka with spacing 
                # # aliases_test = h.split_comma_names(context, name)
                #     print(name, '\n', aliases, '\n', irregular_name_hits )
                #     breakpoint()

            # use raw name strings to generate IDs
            aliases = [
                alias.strip().rstrip(",") 
                for alias in REGEX_KNOWN_AS.split(name) 
                if alias.strip()
            ] 

            # ^ can/should we suppress warning for cases like this (working split on AKA for irregular name)? maybe not, if name contrains other weird chars or (worse) uncaptured entity separators 
            # 2026-01-26 11:44:51 [warning  ] No matching lookup found.      [us_bis_export] dataset=us_bis_export lookup=comma_names value='Kerman Aviation, a/k/a GIE Kerman Aviation'
            # Kerman Aviation, a/k/a GIE Kerman Aviation ['Kerman Aviation', 'GIE Kerman Aviation']

            entity.add("alias", aliases)
            entity.add("name", entity.get("alias")[0])
            entity.add("topics", "reg.warn")  
            entity.add("sourceUrl", url)

            sanction = h.make_sanction(context, entity)
            sanction.add("authorityId", case_id_string)
            h.apply_date(
                sanction, "listingDate", order_date
            )  # sanction object schema date

            context.emit(entity)
            context.emit(sanction)
            context.audit_data(str_row)
            print(name, aliases)

    print(f"Irregular name hits: {irregular_name_hits}")