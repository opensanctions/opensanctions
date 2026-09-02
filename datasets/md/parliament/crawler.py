import json
from datetime import datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from zavod.entity import Entity
from zavod.extract.zyte_api import fetch_html
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context
from zavod import helpers as h

# The endpoint is same-origin, so the script can run on the light home page. The
# roster page named by data.url would render every portrait into the DOM.
RENDER_URL = "https://parlament.md/home.nspx"

# The endpoint only answers calls made from within the rendered page; an identical
# external POST returns an empty 200.
BROWSER_SCRIPT = """
(async () => {
  const emit = (value) => {
    const pre = document.createElement("pre");
    pre.id = "payload";
    pre.textContent = JSON.stringify(value);
    document.body.appendChild(pre);
  };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 40000);
  const post = async (body) => {
    const response = await fetch("/Routing/MakePostRequest", {
      method: "POST",
      headers: {"Content-Type": "application/json; charset=utf-8"},
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`POST ${body.methodName} returned HTTP ${response.status}`);
    }
    // An empty body means the endpoint rejected the call; no results is "[]".
    return response.json();
  };

  try {
    const structures = await post({
      methodName: "GetAllStructuresForFilter",
      languageId: 1,
    });
    const terms = [];
    for (const structure of structures) {
      if (typeof structure.Id !== "string") {
        throw new Error(`Invalid structure: ${JSON.stringify(structure)}`);
      }
      const members = await post({
        methodName: "GetParliamentMPs",
        languageId: 1,
        genderId: null,
        ageFrom: null,
        ageTo: null,
        factionId: null,
        searchText: null,
        independent: null,
        nonaffiliated: null,
        membersFlag: null,
        StructureId: structure.Id,
      });
      // Each member carries ~240KB of base64 portrait, putting a roster at ~50MB.
      terms.push({structure, members: members.map(({UserImg, ...rest}) => rest)});
    }
    emit(terms);
  } catch (error) {
    emit({error: String(error)});
  } finally {
    clearTimeout(timer);
  }
})()
"""

CHISINAU = ZoneInfo("Europe/Chisinau")


def dotnet_date(value: str) -> str:
    """Convert a .NET /Date(ms)/ timestamp to the calendar date it denotes."""
    parsed = datetime.fromtimestamp(
        int(value.removeprefix("/Date(").removesuffix(")/")) / 1000, CHISINAU
    )
    # Any time but midnight means the zone is wrong and the date a day out.
    assert parsed.time() == time(0, 0), (value, parsed.isoformat())
    return parsed.date().isoformat()


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    data: dict[str, Any],
    period_start: str,
    period_end: str | None,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(data.pop("UserId"))
    raw_name = data.pop("FullName").strip()
    h.apply_name(person, full=raw_name.title(), lang="eng")
    # Candidates must hold Moldovan citizenship: Electoral Code CE325/2022, Art. 109
    # https://www.legis.md/cautare/downloadpdf/148962
    person.add("citizenship", "md")

    faction = data.pop("ParliamentaryFactionTitle").strip()
    faction_id = data.pop("ParliamentaryFactionId")
    political_group: str | None
    if faction_id is None:
        # Without an ID the title holds a status label, not a faction name. Required
        # lookups raise only via context.lookup, so an unknown label halts the crawl.
        context.lookup("faction_status", faction, warn_unmatched=True)
        political_group = None
    else:
        political_group = faction

    # RoleId marks offices within the chamber; the source documents no values. Audit
    # before the early return below, which would skip members whose term has lapsed.
    context.audit_data(data, ignore=["RoleId"])

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=period_start,
        period_end=period_end,
    )
    if occupancy is None:
        return
    occupancy.add("politicalGroup", political_group)
    context.emit(occupancy)
    context.emit(person)


def crawl_term(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term: dict[str, Any],
) -> None:
    structure = term.pop("structure")

    period_start = dotnet_date(structure.pop("DateFrom"))
    period_end = dotnet_date(structure.pop("DateTo"))
    assert period_start <= period_end, (period_start, period_end)
    is_current = structure.pop("IsCurrent")
    assert isinstance(is_current, bool), is_current
    # The roster arrives already grouped by term, so the structure ID is unused.
    context.audit_data(structure, ignore=["Id"])

    for member in term.pop("members"):
        crawl_member(
            context,
            position,
            categorisation,
            member,
            period_start=period_start,
            # The sitting term's DateTo is a scheduled end, not a fact about the
            # member; leave it open so the occupancy reads as current.
            period_end=None if is_current else period_end,
        )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of the Republic of Moldova",
        country="md",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = fetch_html(
        context,
        RENDER_URL,
        html_source="browserHtml",
        unblock_validator=(
            '//pre[@id="payload"][starts-with(normalize-space(text()), "[")]'
        ),
        actions=[
            {"action": "waitForTimeout", "timeout": 2},
            {"action": "evaluate", "source": BROWSER_SCRIPT},
        ],
        cache_days=1,
    )
    payload = h.element_text(h.xpath_element(doc, '//pre[@id="payload"]'))
    terms = json.loads(payload)
    # The in-page script emits an {"error": ...} object when a roster request fails.
    assert isinstance(terms, list), terms
    current = [t for t in terms if t["structure"]["IsCurrent"] is True]
    assert len(current) == 1, len(current)
    for term in terms:
        crawl_term(context, position, categorisation, term)
