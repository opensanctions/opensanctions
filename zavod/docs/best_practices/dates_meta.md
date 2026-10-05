# Date parsing with dataset metadata

`zavod` provides helpers for date parsing that can handle multiple date formats and re-write
international date strings into ones that Python can parse.

Consider using [`h.apply_date`][zavod.helpers.dates.apply_date] (and [`h.apply_dates`][zavod.helpers.dates.apply_dates] for lists) when parsing dates. This will:

a) Use the dataset-level date parsing instructions (see below)
b) Cause warnings to be emitted for all invalid dates
c) Store the unparsed `original_value` alongside the parsed form.

## Dataset metadata

In the dataset metadata YAML file, you can add a section like this:

```yaml
dates:
    formats: ['%d. %m. %Y']
```

This will instruct the parsers to use the given formats. If your input data is formatted in ISO 8661 style (eg. `2024-01-23`), you do not need to supply a format at all.

Sometimes, you will also see date strings involving a non-English month specification (eg. `12. März 2024`). For this, you can add a section like this:

```yaml
dates:
    formats: ['%d. %b %Y']
    months:
        Mar:
            - März
            - Maerz
        Jul: Juli
```

Note that this mapping is essentially a simple string replacement. In this case, we're mapping German month names onto the short English month form parsed by `%b` in the format string. You could also map months onto month numbers or long month names.

Finally, some datasets are just too messy to fully parse all contained dates. In these cases, it can be useful to simply extract years, instead of parsing the full date string. Use this as a last resort, with caution:

```yaml
dates:
    formats: ['%m %Y']
    year_only: true
```

This will parse any string that contains a valid year, such as `Approximately 1960`, or `circa 2007`.

## Two-digit years

A `%y` format needs a `two_digit_year_base`. The two-digit year is read as the first matching year that is not before that year. `h.apply_date`, `h.apply_dates` and `h.extract_date` accept the argument, and warn if a `%y` format matches without it.

```python
# Birth date: 100 years ago.
h.apply_date(
    person, "birthDate", "16-07-68", two_digit_year_base=h.TWO_DIGIT_BIRTH_YEAR_BASE
)
# Case date: the earliest possible event year.
h.apply_date(sanction, "startDate", "16-07-68", two_digit_year_base=2000)
```

Helpers that apply dates themselves take the argument too, and you should prefer that
over parsing the date before calling them. `h.make_occupancy` keys the occupancy ID on
the date strings as given, so pre-parsing them silently renumbers every occupancy in
the dataset.

```python
h.make_occupancy(
    context, person, position, start_date="16-Jul-68", two_digit_year_base=1945
)
```
