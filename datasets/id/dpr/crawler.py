import json
import re
from string import Template
from typing import Any

from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context
from zavod import helpers as h

# Period 6 == the current (2024-2029) term. The listing defaults to it, so the
# crawler only asserts the term it received rather than requesting one.
PERIODE_ID = 6

# STATUS_OFF value marking a member who currently holds the seat. The listing
# also offers "Selesai masa jabatan" (term finished), which covers members
# replaced mid-term (Pergantian Antar Waktu) and who are no longer MPs.
IN_OFFICE = "Dalam masa jabatan"

# Upper bound for the roster page size we request. Comfortably above the 580
# seats of the chamber; the crawler fails if the response is still paginated.
PAGE_SIZE = 1000

# The id of the element that ROSTER_INTERCEPTOR appends to the page, holding the
# roster JSON.
ROSTER_CONTAINER_ID = "roster-json"

# A member's detail page is keyed on their name and numeric id, e.g.
# ".../detail-anggota/Dr-H-C-PUAN-MAHARANI-287". The site builds the name part by
# replacing every run of non-alphanumeric characters, punctuation included, with a
# single hyphen, which leaves a trailing hyphen on names ending in a title.
MEMBER_URL = (
    "https://www.dpr.go.id/en/tentang-dpr/informasi-anggota-dewan/detail-anggota/%s-%s"
)

# JavaScript run in the page, before the roster is re-requested, to get the whole
# roster in a single response and to make that response readable to us.
#
# The listing is a client-rendered Next.js page. It renders 12 members at a time
# and holds the page number in React state only, so there is no URL to fetch
# page N with. It gets its data from POST /gql, which the site's axios
# interceptor signs with x-api-token and x-api-signature headers. Reproducing
# that signature in the crawler means hardcoding the frontend's HMAC secret, so
# we let the page sign its own requests and only rewrite what it sends.
#
# The wrapper below sits under the interceptor: axios signs the request, then
# hands it to its XHR adapter, and only then does our send() see the body. That
# ordering works because the signature does not cover the body — the site sends
# a constant x-request-body digest with every request, whatever the body is.
#
# Two rewrites happen to the roster query variables:
#   - first/page: request the entire roster at once, instead of 12 rows.
#   - where: keep only members currently in office. The listing itself asks for
#     serving and finished members together, and the response carries no status
#     field, so the filter is the only way to tell the two apart.
#
# The rendered cards carry no member id, so the JSON response is the only usable
# form of the roster. The load handler copies it verbatim into a container
# element, which the crawler then reads out of the returned browser HTML.
ROSTER_INTERCEPTOR = Template("""
const originalSend = XMLHttpRequest.prototype.send;
XMLHttpRequest.prototype.send = function (body) {
  if (typeof body === 'string' && body.indexOf('getDaftarRiwayatAnggota') !== -1) {
    const payload = JSON.parse(body);
    payload.variables.page = 1;
    payload.variables.first = $page_size;
    payload.variables.where = {
      column: 'STATUS_OFF', operator: 'EQ', value: '$in_office'
    };
    body = JSON.stringify(payload);
    this.addEventListener('load', () => {
      const container = document.createElement('div');
      container.id = '$container_id';
      container.textContent = this.responseText;
      document.body.appendChild(container);
    });
  }
  return originalSend.call(this, body);
};
""").substitute(
    page_size=PAGE_SIZE, in_office=IN_OFFICE, container_id=ROSTER_CONTAINER_ID
)

# Pagination link clicked to make the page issue a second roster request. The
# request made during the initial page load is already out by the time actions
# run, so it goes out unmodified and is ignored.
NEXT_PAGE_SELECTOR = "a[aria-label='Page 2']"

ROSTER_ACTIONS: list[dict[str, Any]] = [
    {
        "action": "waitForSelector",
        "selector": {"type": "css", "value": NEXT_PAGE_SELECTOR},
    },
    {"action": "evaluate", "source": ROSTER_INTERCEPTOR},
    {"action": "click", "selector": {"type": "css", "value": NEXT_PAGE_SELECTOR}},
    {
        "action": "waitForSelector",
        "selector": {
            "type": "css",
            "value": f"#{ROSTER_CONTAINER_ID}",
            "state": "attached",
        },
        # Serving the whole roster in one response takes the site well past the
        # 5s default. 15s is the maximum Zyte API allows.
        "timeout": 15,
    },
]

CONTAINER_XPATH = f'//div[@id="{ROSTER_CONTAINER_ID}"]'


def member_url(name: str, member_id: str) -> str:
    """Build the URL of a member's detail page from their name and id."""
    return MEMBER_URL % (re.sub(r"[^A-Za-z0-9]+", "-", name), member_id)


def fetch_roster(context: Context) -> list[dict[str, Any]]:
    """Fetch the full roster of serving members as the site's own JSON records."""
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        CONTAINER_XPATH,
        actions=ROSTER_ACTIONS,
        geolocation="id",
        cache_days=1,
    )
    container = h.xpath_element(doc, CONTAINER_XPATH)
    response = json.loads(h.element_text(container, squash=False))
    if "errors" in response:
        raise RuntimeError(f"GraphQL errors in roster response: {response['errors']}")
    roster = response["data"]["getDaftarRiwayatAnggota"]

    # A truncated roster would silently drop members, so require the whole list.
    paginator = roster["paginatorInfo"]
    assert paginator["hasMorePages"] is False, paginator
    members = roster["data"]
    assert isinstance(members, list), members
    assert len(members) == paginator["total"], (len(members), paginator)
    context.log.info("Fetched roster", members=len(members))
    return members


def crawl_member(
    context: Context,
    member: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    assert member["idPeriode"] == PERIODE_ID, member
    member_data = member["anggota"]
    member_id = member_data["id"]

    person = context.make("Person")
    person.id = context.make_slug(member_id)

    raw_name = member_data["nama"]
    clean_name = h.strip_name_titles(context, raw_name)
    person.add(
        "name",
        clean_name,
        lang="ind",
        original_value=raw_name if clean_name != raw_name else None,
    )
    # DPR members must be Indonesian citizens (Law No. 7 of 2017 on General
    # Elections, Article 240 paragraph (1)). https://peraturan.bpk.go.id/Details/37644
    person.add("citizenship", "id")
    person.add("political", member["riwayatFraksi"]["fraksi"]["fraksi"], lang="ind")
    person.add("sourceUrl", member_url(raw_name, member_id))

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", member["dapil"]["dapil"], lang="ind")
    context.emit(person)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the People's Representative Council of Indonesia",
        country="id",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328632",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    for member in fetch_roster(context):
        crawl_member(context, member, position, categorisation)
