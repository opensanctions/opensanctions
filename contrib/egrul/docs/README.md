# EGRUL/EGRIP XML format references

Vendored copies of the FNS (Russian Federal Tax Service) format
specifications that the parser in `..` is written against. The XSDs inside
are `windows-1251`-encoded (open with `iconv -f windows-1251 -t utf-8 …`).

## Files

| File | Formats | Notes |
|---|---|---|
| `13673441xsd.zip` | EGRUL 4.07 (legal entities) + EGRIP 4.06 (sole traders) | Current. Contains `VO_RUGF_2_311_26_04_07_01.xsd` and `VO_RIGF_2_311_27_04_06_01.xsd`. |

`13673441xsd.zip` was downloaded from
<https://www.nalog.gov.ru/html/sites/www.new.nalog.ru/2023/about_fts/docs_fts/13673441xsd.zip>,
linked from the FNS order page
<https://www.nalog.gov.ru/rn77/about_fts/docs/13673441/> — FNS order
ЕД-7-14/382@ of 2023-06-06, which established formats EGRUL 4.07 and
EGRIP 4.06.

## Upcoming format (not yet vendored)

A new format pair — EGRUL 4.08 and EGRIP 4.07 — was approved by FNS order
ЕД-7-14/613@ of 2025-07-08, effective 2026-02-01 and mandatory from
2026-08-01. XSDs are published as `16493030_xsd.zip` from
<https://www.nalog.gov.ru/rn77/about_fts/docs/16493030/>. Drop the zip in
here and update this table when we start ingesting that format.

## Where to look for future format updates

- Landing page (overview, regulations, FTP access procedure):
  <https://www.nalog.gov.ru/rn77/service/egrip2/egrip_vzayim/>
- All EGRUL/EGRIP format orders are linked from there.
