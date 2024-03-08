# Wikidata

We import [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) to
OpenSanctions in two ways: using a [crawler](tutorial.md) which
imports persons who have held any of a set of wikidata positions we have categorised as
[Politically Exposed Person positions](https://opensanctions.org/pep), and using
our [Wikidata Enricher](https://www.opensanctions.org/datasets/wikidata/).

We also occasionally publish data for a small selection of properties to Wikidata.
The current publishing process is interactive and completely supervised by a
human.

## Publishing to Wikidata using zavod

The zavod command line tool can publish data from a specific dataset to Wikidata.
The tool iterates over the entities in the specified dataset until it
finds an entity for which it can perform some action:

1. If an entity has a Wikidata QID, it proposes any edits it can make, awaiting 
   user confirmation to publish.
3. If an entity does not have a QID, it searches for existing Wikidata items to
   [resolve the entity to](https://www.opensanctions.org/docs/identifiers/),
   and proposes the wikidata edits it would make if the user
   instead chooses to create a new Wikidata item.

Resolving the entity to an existing Wikidata item repeats the check for potential
edits. If no edits are proposed for the current entity, the next entity with possible
actions is loaded.

Publishing changes to wikidata can take a number of seconds, since the Wikidata
API imposes throttling to avoid overload. Once changes are published, the next entity
with possible actions is loaded.

### Running zavod `wd-up`

In addition to basic zavod setup, the following environment variables:


    ZAVOD_ARCHIVE_BUCKET=data.opensanctions.org
    ZAVOD_ARCHIVE_BACKEND=GoogleCloudBackend
    ZAVOD_WD_CONSUMER_TOKEN
    ZAVOD_WD_CONSUMER_SECRET
    ZAVOD_WD_ACCESS_TOKEN
    ZAVOD_WD_ACCESS_SECRET
    ZAVOD_WD_USER
    PYWIKIBOT_DIR=.pywikibot

Get OAuth credentials by registering an 
[OAuth 1.0a consumer](https://meta.wikimedia.org/wiki/Special:OAuthConsumerRegistration/propose/oauth1a) 
with permission to:

- edit existing pages
- create, edit and move pages

Set `ZAVOD_WD_USER` to the username used for the OAuth consumer.

Set `PYWIKIBOT_DIR` to the directory directory with your `user-config.py` - 
`.pywikibot` in this repository is probably sufficient.

Copy an up to date resolve.ijson file to your `ZAVOD_RESOLVER_PATH`.

Run `wd-up` as follows, changing for the dataset and country you'd like to sync up:

```
zavod wd-up \
  --clear \
  datasets/de/abgeordnetenwatch/de_abgeordnetenwatch.yml \
  datasets/_analysis/ann_pep_positions/ann_pep_positions.yml \
  --country-adjective German \
  --country-code de
```

- The panel on the left shows the current OpenSanctions entity, and below that the 
  proposed actions.
- The middle panel shows the search results for Wikidata items if the current entity
  does not have a QID. Highlight the right option using up/down arrows.
- The panel on the right shows log of operations by `wd-up` and instructs your next step.
- Press save after creating or resolving a wikidata item and remember to copy your resolve.ijson back and upstream the changes.

Country adjective is used to generate descriptions like `German politician`.

Country code is used to sanity check that the position refers to the country of
the supplied nationality adjective. It only works if you supply matching arguments -
it isn't clever. Use lowercase like the data does.

`wd-up` has the following limitations (and probably many more)

- Collection datasets don't work very well because it does not properly generate sources for referencing.
- Only Person entities which are targets are considered.
- Supported properties:
  - labels of any language
  - descriptions only in `en` (English)
  - 'instance of' Human
  - 'sex or gender'
  - 'birth date'
  - 'position held'
- Only positions which have QIDs are considered. You might benefit from doing an [xref](https://www.opensanctions.org/docs/identifiers/) between your dataset and wd_peps first.
- Edits are only proposed if sources can be provided, except for labels, descriptions, and 'instance of'.
- Sources are only proposed if a `sourceUrl` property is available for the entity with the same source dataset as the property being considered.