# Wikidata

We import [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) to
OpenSanctions in two ways: using a [crawler](/tutorial) which
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

Set the following environment variables:

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

Run `wd-up` as follows:

```
zavod wd-up datasets/de/abgeordnetenwatch/de_abgeordnetenwatch.yml --country-adjective German --country-code de
```

- The panel on the left shows the current OpenSanctions entity, and below that the 
  proposed actions.
- The middle panel shows the search results for Wikidata items if the current entity
  does not have a QID. Highlight the right option using up/down arrows.
- The panel on the right shows log of operations by `wd-up` and instructs your next step.

Country adjective is used to generate descriptions like `German politician`.
Country code is used to sanity check that the position refers to the country of
the supplied nationality adjective. It only works if you supply matching arguments -
it isn't clever.

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
- Only positions which have QIDs are considered.
- Edits are only proposed if sources can be provided, except for labels, descriptions, and 'instance of'.
- Sources are only proposed if a `sourceUrl` property is available for the entity with the same source dataset as the property being considered.