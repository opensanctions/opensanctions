---
title: "How to sanctions-check a spreadsheet using OpenRefine"
summary: |
    OpenRefine, a power tool for data cleaning, offers a way to quickly check hundreds or thousands of names against the OpenSanctions database to find the ones that might be persons of interest in an investigation.
draft: false
---

As an investigator, you will sometimes find yourself with a large list of companies or people that you need to vet: a database of government contract awards, people involved in offshore finance, or even criminal indictments. One way to find leads for reporting is to look for people in the public eye: politicians, criminals, those connected to illicit behaviour.

In this introduction, we'll show how to use [OpenRefine](https://openrefine.org/) (née Google Refine) to match each entry in a spreadsheet against the OpenSanctions database to find reporting leads. In Refine, the process of matching against a data source like OpenSanctions is known as [reconciliation](https://docs.openrefine.org/manual/reconciling).

As a spreadsheet for testing, we'll use the ICIJ OffshoreLeaks database: the published part of the data from the Panama Papers, Pandora Leaks etc. On its web site, ICIJ invites you to [download the data in CSV format](https://offshoreleaks.icij.org/pages/database) as a `.zip` bundle. Once downloaded, we'll use the `nodes-officers.csv` file, which lists all people linked to an offshore company in the dataset.

After we've installed OpenRefine on our own own computer ([guide](https://docs.openrefine.org/manual/installing)) and started a project based on this CSV file ([guide](https://docs.openrefine.org/manual/starting)), we can inspect the material: nearly 750,000 rows of names. Searching for people of interest in this by hand would drive any investigator into madness. 

<a href="https://assets.pudo.org/opensanctions/images/openrefine/overview.png">
    <img class="img-fluid" src="https://assets.pudo.org/opensanctions/images/openrefine/overview.png">
</a>

We can also use the facet function to show a grouping by country in the sidebar. Clicking on a country name in the sidebar will filter the rows in the table section to only those that mention the country.

<a href="https://assets.pudo.org/opensanctions/images/openrefine/country-facet.png">
    <img class="img-fluid" src="https://assets.pudo.org/opensanctions/images/openrefine/country-facet.png">
</a>

For no particular reason, we'll choose to filter for "Kazakhstan" today, and vet only 501 of the 750,000 original entries.

### Connecting Refine to the OpenSanctions database

Next, it's time to connect Refine to OpenSanctions: using the dropdown arrow next to the `name` column, we'll select *Reconcile*, then *Start reconciling*.

<a href="https://assets.pudo.org/opensanctions/images/openrefine/start-reconciling.png">
    <img class="img-fluid" src="https://assets.pudo.org/opensanctions/images/openrefine/start-reconciling.png">
</a>

In the dialog that pops up, we can see the data sources that the Refine tool already knows about. Using the *Add standard service...* button in the bottom left of that screen, we can  add OpenSanctions by inserting this URL:

```
https://api.opensanctions.org/reconcile/default
```

<a href="https://assets.pudo.org/opensanctions/images/openrefine/add-service.png">
    <img class="img-fluid" src="https://assets.pudo.org/opensanctions/images/openrefine/add-service.png">
</a>

Once you've added OpenSanctions, the software will propose what type of entity is in your list: is it people, companies, or even aircraft and ships? In our case, the sheet contains a mix of people and companies. In OpenSanctions, the entity type `LegalEntity` exists to identify both people, companies and other organziations.

Additional to the entity type, we can also select additional information to be supplied to the matching algorithm. For example, we can select the `countries` field in the table and choose to associate it with the `Country` property recognised by OpenSanctions. This way, the searches for matching people of interest will be much more precise.

<a href="https://assets.pudo.org/opensanctions/images/openrefine/extra-props.png">
    <img class="img-fluid" src="https://assets.pudo.org/opensanctions/images/openrefine/extra-props.png">
</a>

After we kick this off by pressing *Start Reconciling...* in the bottom right corner of the screen, Refine will begin querying the OpenSanctions database and find potential matches between both sets. Depending on how many rows your spreadsheet has, this can take a few seconds (for dozens or hundreds of rows) up to an hour for a very large list (100,000 or millions of rows).

### Reviewing potential matches

When the processing has finished, we'll be sent back to the table viewer. Now, however, both the sidebar and the table view will have additional controls that allow us to review the reconciliation results. In the main table, you'll see matching candidates for many rows, showing the most likely matches from the OpenSanctions database. In the sidebar to the left, you'll see new filter options to filter the table based on how good the reconciliation matches are.

As a first step, we'll use the filters in the left-hand sidebar to filter out low-scored matches:

<a href="https://assets.pudo.org/opensanctions/images/openrefine/score-filter.png">
    <img class="img-fluid" src="https://assets.pudo.org/opensanctions/images/openrefine/score-filter.png">
</a>

Using this filter, we're now down to ten rows in our data: people who are mentioned in the OffshoreLeaks database that score very highly for being persons of interest in OpenSanctions. 

By clicking the light blue links in each row, we can pop open the OpenSanctions entity profile for each of them and see if they're people whose offshore dealings may merit some further investigation:

<a href="https://assets.pudo.org/opensanctions/images/openrefine/entity-link.png">
    <img class="img-fluid" src="https://assets.pudo.org/opensanctions/images/openrefine/entity-link.png">
</a>

Of course, the connections made using the reconciliation tool are only starting points. Further research must be invested into each possible hit to verify that the person in the data is really the same one as the politician in OpenSanctions, and to unveil what story lies behind their presence in the OffshoreLeaks data.

By the way: the method used by Refine and OpenSanctions to talk to each other, the Reconciliation API, is [a standard](https://reconciliation-api.github.io/specs/latest/) used by [dozens of data sources](https://reconciliation-api.github.io/testbench/), [database apps](https://github.com/drkane/datasette-reconcile) and [command-line tools](https://github.com/maxharlow/reconcile) to create better-linked data.

We hope this introduction on how OpenRefine can be used to cross-check many entries against the OpenSanctions database has helped you to see how both can be used as a powerful combination for data mining in investigative work!