# Extracting designations from official notices

This guide describes the manual extraction process used for MOFCOM Announcement
No. 27 of 2026. It is intended as a checklist for future contributions to
`sanctions.csv`.

## Read the official page

Use the official notice as the source of record. Search results, translations, and
secondary reporting can help locate a notice but should not supply designation data.

Some MOFCOM pages are not reliably exposed through text-only browser tools. In that
case, download the HTML directly and extract its visible text. For example:

```bash
curl -L --fail --silent --show-error \
  'https://www.mofcom.gov.cn/zwgk/zcfb/art/2026/art_df87be1437044874a35f85cf6e076f3d.html' \
  -o notice.html
```

The page contains navigation and unrelated footer content, so isolate the notice body
before extracting records. Confirm the following notice-level facts:

- Issuing authority.
- Announcement number.
- Publication and effective dates.
- List or measure being changed.
- Number of entities stated in the notice.
- Whether the action adds, removes, suspends, or amends designations.

For Announcement No. 27, MOFCOM stated that 20 Japanese entities were added to the
Export Control List and that the measure took effect on publication, 29 June 2026.

## Extract one row per numbered entry

The attachment presents each entry as a numbered block containing:

1. Chinese name and official English rendering.
2. Address.
3. Postal code.

Use the official English rendering as `Name` and the Chinese rendering as
`Chinese name`. Preserve source spellings even when they look unusual. For example,
the notice renders one company as `MHI Oceanincs Co., Ltd.`; silently correcting it
would make the extraction diverge from its citation. A verified correction can be
made separately, retaining the source form as an alias where useful.

Do not invent aliases, identifiers, or translated addresses. Leave unsupported fields
empty.

When the notice supplies multiple common names, place them in `Alias` separated by
semicolons. The crawler emits each value as a separate alias.

## Choose the entity type

The notice calls all targets “entities” and does not provide schema types. Classify
them from their institutional form:

- Government research institutes and research centers are `Organization`.
- Incorporated businesses are `Company`.

For Announcement No. 27, the first four entries are government research bodies and
the remaining sixteen are companies. If the institutional form is genuinely unclear,
stop and research it rather than guessing from the English name alone.

## Map the notice to CSV fields

Use these values for an Export Control List addition:

| CSV field | Value |
| --- | --- |
| `Country` | `Japan` |
| `Topics` | `export.control` |
| `Body` | `Ministry of Commerce` |
| `List` | `List of Export Controls` |
| `Date` | Effective date in `DD.MM.YYYY` format |
| `Source URL` | Canonical official notice URL |

The crawler maps `List of Export Controls` to program `CN-ECL`. Do not place the
internal program key in the CSV unless the source-facing convention is changed for
the entire dataset.

Combine the source address and postal code into `Address`, retaining the Chinese text:

```text
日本东京都新宿区市谷本村町5番1号，邮编：162-8808
```

Keep the address in the language used by the notice. CSV values containing an ASCII
comma must be quoted; Chinese punctuation does not introduce additional CSV
delimiters.

## Verify the extraction

Validation should check both CSV syntax and notice-specific invariants:

```bash
qsv validate sanctions.csv
```

For this notice, verify that:

- Exactly 20 rows use its source URL.
- The type split is 16 `Company` and 4 `Organization`.
- Every row has `Date` set to `29.06.2026`.
- Every row maps to `List of Export Controls`.
- Every row contains an address.
- The CSV still has the expected column count and all older rows remain present.

Finally, run the dataset in dry-run mode. This catches unknown columns, invalid schema
properties, unmapped programs, malformed dates, and assertion failures:

```bash
/Users/pudo/.python/wrangle/bin/zavod crawl -d \
  datasets/cn/sanctions/cn_sanctions.yml
```

Review the Git diff before committing. A source addition should change only the rows,
schema mapping, and contributor documentation required for that notice.
