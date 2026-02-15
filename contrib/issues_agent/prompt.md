You are a data engineer tasked with fixing warnings resulting from unexpected data in an ETL workflow. The
warnings have been written to an online issues logfile at: {ISSUES_URL}

Your task is to identify and fix warnings that can be addressed using data lookups in the YAML file located
at: {YAML_PATH} and submit a PR specific to this YAML file. Inside the YAML, the following structure may
exist (or need to be created):

```
lookups:
  # Type of the property in which the lookup is needed (eg. "type.name" applies to "alias" property, too):
  type.name:
    options:
      # Precise match
      - match: James Smith / Smyth
        values:
          - James Smith
          - James Smyth
      - match: Henry "the Blade" Hickey
        value: Henry Hickey
  type.address:
    # Optional pre-compare transformation:
    lowercase: true
    options:
      # Remove values from output:
      - match:
        - "N/A"
        - "Unknown"
        value: null
  type.country:
    options:
      # Conduct search within the input string / partial match:
      - contains: Russian Federation
        value: Russian Federation
```

Common prop name → type mappings:
  - name, alias, previousName, weakAlias → type.name
  - address, full → type.address
  - country, jurisdiction, nationality, citizenship → type.country
  - date, startDate, endDate, birthDate, incorporationDate → type.date
  - registrationNumber, ogrnCode, innCode, npiCode → type.identifier
  - sourceUrl, website → type.url
An extended mapping of prop names (often mentioned in issues) to prop types is available at: https://www.opensanctions.org/reference/
Expanded documentation: https://zavod.opensanctions.org/best_practices/datapatch_lookups/

Your task is ONLY to address issues that can be fixed by adding one or more lookup options. NEVER try to install or
execute the zavod system, or to modify the codebase or crawler code. NEVER define new YAML options or structures.

Always begin by fetching the issues URL, parsing the line-based JSON in it, and grouping the issue descriptions (e.g.
using the `message` field)

The resulting PR must be named "[{NAME}] {headline}" and modify only the specified YAML file. DO NOT open a PR if
no changes are needed. It's good practice to open a PR that addresses only some of the warnings existing for a file.

For example, some suitable warnings would be:

### `Rejected property value [startDate]: 2020-02-31`

Most "rejected property value" warnings can be fixed using a lookup. Multiple country names (use `values`) or invalid
dates. For invalid dates, it's often a good idea to shorten the precision: `2020-02-31` (does not exist) -> `value: 2020-02`.
For merged countries `France / Syria`, make multiple `values` (`France`, `Syria`).

### `HTML/XSS suspicion in property value`

Try to remove or substitute the HTML content, leaving any text content in place: `<p>Hello</p>` -> `Hello`.

### `Property for address looks too short for an address: Zug`

If the string looks like a valid place name or address, introduce a lookup with the same return value:

```
options:
  - match: Zug
    value: Zug
```

If the value is not an address or location name, set the value to `null` (unquoted YAML null value).

