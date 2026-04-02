import re
from lxml import html

from zavod import Context, helpers as h


ID_PATTERNS = [
    r"统一社会信用证号",
    r"社会信用代码",
    r"身份证号码",
    r"身份证号",
    r"账户代码",
]


def crawl_targets(context: Context, full_text: str, date: str, url: str) -> None:
    # extract 当事人 (target) block
    # end of the block:
    # 经查明 ("upon investigation")
    # 根据 ("according to")
    # \d{4}年: a line starting with a year
    target_section_match = re.search(
        r"当事人[：:](.*?)(?=\n经查明|\n根据|\n\d{4}年|$)", full_text, re.DOTALL
    )
    assert target_section_match is not None, (
        f"Expected to get a match on name and ID from the notice, received no match for URL: {url}."
    )
    target = target_section_match.group(1).strip()

    # some notices mention several entities divided by \n or chinese dots
    id_pattern = "|".join(ID_PATTERNS)

    for line in re.split(r"[\n。]", target):
        line = line.strip()
        if not line:
            continue

        # fetch target name and ID
        match = re.search(rf"(.+?)\s*[，,]?\s*({id_pattern})[：:]\s*(\S+)", line)
        assert match is not None

        name = match.group(1).strip()
        # id_type = match.group(2).strip()
        # note that id_redacted look like '211002XXXXXXXXXXXX', i.e. redactions applied by SZSE
        # so im not saving those but using to generate entity IDs only
        id_redacted = match.group(3).strip("。，,；;）)").strip()

        entity = context.make("LegalEntity")
        entity.id = context.make_id(name, id_redacted)
        entity.add("name", name)
        entity.add("country", "cn")
        entity.add("topics", "financial")
        entity.add("sourceUrl", url)

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "listingDate", date)

        context.emit(entity)
        context.emit(sanction)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)

    # <a> tags are injected by JavaScript and lxml never sees them
    # no API endpoint for this data
    # wacky but working solution: extracting data from <script>
    for row in h.xpath_elements(doc, ".//ul[contains(@class, 'newslist')]//li"):
        date = h.xpath_elements(row, ".//span[contains(@class, 'time')]")
        date = date[0].text_content().strip()

        script = h.xpath_element(row, ".//script")

        # fix encoding for chinese chars
        text = script.text_content().encode("latin-1").decode("utf-8")

        # get notice url
        url_match = re.search(r"var curHref = '([^']+)'", text)
        assert url_match is not None, "Couldn't extract notice URL from script tag"
        url = context.data_url.replace("index.html", "") + url_match.group(1).replace(
            "./", ""
        )
        # notice_title = re.search(r"var curTitle = '([^']+)'", text).group(1).strip()

        # fetch detailed notice with a manual decode needed at the fetch level
        # to prevent garbled text from appearing
        response = context.fetch_response(url)
        doc_notice = html.fromstring(response.content.decode("utf-8"))
        # extract notice text (inconsistent target placement across docs, sometimes in multiple p tags)
        # target structure: 当事人 ("party involved")：<company name>，身份证号码 ("ID Number")：<ID>
        lines = []
        ps = h.xpath_elements(doc_notice, ".//div[contains(@class, 'des-content')]//p")
        for p in ps:
            text = p.text_content().strip()
            lines.append(text)
        full_text = "\n".join(lines)

        # extract target names and IDs
        crawl_targets(context, full_text, date, url)
