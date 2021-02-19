# Contributing

If you plan to add a dataset/crawler, please pitch the addition as an issue first. Adding a data source to OpenSanctions requires us to engage with this data in perpetuity. We want to prioritise data quality over quantity.

## Inclusion criteria

The following criteria guide our interest in including a new data source in OpenSanctions:

1. **Public interest.** We want to collect data about entities that are of public interest, either because they bear significant political or economic influence, or are or have been involved in criminal activities.
2. **Detailed data.** A data source which merely lists `John Smith` as a person of interest does not provide enough detail to identify that individual. For people, the date of birth, nationality and some form of national identification are ideal. For other entities, the date of incorporation, registration number and jurisdiction are especially important.
3. **Justification and context.** Data sources that reference a legal basis for inclusion for each subject and provide links to associated entities provide additional value.
4. **Legality.** We do our best to comply with the intellectual property rights of other database authors. Information published by governments and public institutions, however, is considered  fair game.

## Checklist

When contributing a new data source, or some other change, make sure of the following:

* You've created a metadata YAML file with detailed descriptions and links to the source URL.
* Your code should run after doing a simple `pip install` of the codebase. Include additional
  dependencies in the `setup.py`. Don't use non-Python dependencies like `Headless Chrome` or
  `Selenium`.
* The output data for your crawler should be Follow The Money objects. If you need more fields
  added to the ontology, submit a pull request upstream. Don't include left-over data in an
  improvised way.
* Include verbose logging in your crawler. Make sure that new fields or enum values introduced upstream (e.g. a new country code or sanction program) will cause a warning to be emitted.
* Bonus points: your Python code is linted and formatted with `black`.