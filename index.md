---
layout: default
title: Open-source data for due diligence
description: >
  OpenSanctions is a database of persons and companies of political, criminal,
  or economic interest. We combine sanctioned entities, politically exposed
  persons, and other public information.
---

# Welcome to OpenSanctions!

OpenSanctions is a global database of persons and companies of political,
criminal, or economic interest. We combine sanctioned entities, politically
exposed persons, and other public information into a single dataset.

* Cross-check leaks and public databases for possible conflicts of interests
  and signs of illicit activity.

* Track political conflicts and compare world-wide sanctions policies.

* Check potential customers and partners in international dealings.


## Downloads

{% for source in site.data.sources %}
  {{ source.title }}
{% endfor %}

## Frequently Asked Questions

### What is OpenSanctions?

OpenSanctions brings together sanctions lists, lists of politically exposed
persons (PEPs), and parties excluded from government contracts in different countries.
It is a list of persons and companies of political, criminal, or economic
significance that merit further investigation.

It is meant to be a resource for journalists and civil society who need to
perform due diligence-style tasks (e.g. searching for persons of interest in
a leak or open dataset).

OpenSanctions is an open project, anyone is invited to use the dataset and
contribute additional information.

### Where does the data come from?

Our preferred sources are official datasets published by governments all over
the world; including commonly used sanctions and ban lists. In the future, we
hope to also include information from media reporting, Wikidata and Wikipedia
and litigation.

### How frequently is OpenSanctions updated?

The data is updated daily and processed in several stages, so there can be
delays of up to 72 hours for a new entity to make its way into the master
dataset.

### Does OpenSanctions only contain sanctions lists?

No, we are just bad at naming things. The project includes sanctions lists,
lists of politicians, ban lists used in government procurement, lists of known
terrorists and other data sources relevant to journalistic research and due
diligence.

### Can I contribute a new data source?

Yes! We're particularly keen to add sources that include information from
criminal cases, and family and associates of politically exposed persons.

Our data sources are currently set up as independent repositories inside the
OpenSanctions organization on GitHub. Please file a ticket with the main
repository to suggest a new source and get access to contribute.

Data import scripts are self-running Python scrapers hosted on morph.io. If
you're keen to write or improve scrapers, we'd love your help!


### Who is behind OpenSanctions?



### Links

* MrWatchlist
