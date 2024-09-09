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
    formats: ['%m %y']
    year_only: true
```

This will parse any string that contains a valid year, such as `Approximately 1960`, or `circa 2007`.