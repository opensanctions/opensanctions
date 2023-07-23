from opensanctions import helpers as h
from opensanctions.core import Context
from zavod.parse.positions import make_position
import os
from urllib.parse import urlencode
import json

API_KEY = os.environ.get("OPENSANCTIONS_US_CONGRESS_KEY")
LIMIT = 250


def make_member(context, member):
  print(member.get("name", member.get("directOrderName", None)))
  if terms := member.get("terms"):
    for term in terms["item"]:
      print("  ", term["chamber"], term.get("startYear", None), term.get("endYear", None))

    # make person
    # make positions
    # make occupancies for terms that are within the periods we care about
    # iff there are occupancies, emit all the things


def fetch(context: Context, offset):
    query = {"limit": LIMIT, "offset": offset}
    url = f"{ context.source.data.url }?{ urlencode(query) }"
    headers = {"x-api-key": API_KEY}
    path = context.fetch_resource(f"members-{offset}.json", url, headers=headers)
    context.export_resource(path, title=context.SOURCE_TITLE + f"offset {offset}")
    with open(path, "r") as fh:
        return json.load(fh)["members"], offset + LIMIT


def crawl(context: Context):
  offset = 0
  while True:
    members, offset = fetch(context, offset)
    if not members:
      break

    for member in members:
      make_member(context, member)

    # entity = context.make("Person")
    # entity.id = "123"
    # entity.add("name", "Fred Bloggs")
    # entity.add("topics", "role.pep")
    # entity.add("country", "us")
    # context.emit(entity, target=True)
# 
    # position = make_position(context, "United states senator", country="us", summary="This is a summary from us_congress", description="This is a description from us_congress")
    # context.emit(position)
# 
    # occupancy = context.make("Occupancy")
    # occupancy.id = context.make_id("Fred Bloggs", "United statessenator")
    # occupancy.add("holder", entity)
    # occupancy.add("post", position)
    # context.emit(occupancy)