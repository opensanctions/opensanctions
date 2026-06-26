# EGRUL/EGRIP XML format references

Vendored copies of the FNS (Russian Federal Tax Service) format
specifications that the parser in `..` is written against. The XSDs inside
are `windows-1251`-encoded (open with `iconv -f windows-1251 -t utf-8 …`).

## Files

### `13673441xsd.zip` — current formats

EGRUL 4.07 (legal entities) and EGRIP 4.06 (sole traders), established by
FNS order ЕД-7-14/382@ of 2023-06-06.

Source: <https://www.nalog.gov.ru/html/sites/www.new.nalog.ru/2023/about_fts/docs_fts/13673441xsd.zip>
(linked from <https://www.nalog.gov.ru/rn77/about_fts/docs/13673441/>).

Contents:

| File | Format |
|---|---|
| `VO_RUGF_2_311_26_04_07_01.xsd` | EGRUL 4.07 (legal entities) |
| `VO_RIGF_2_311_27_04_06_01.xsd` | EGRIP 4.06 (sole traders) |

### `16493030_xsd.zip` — upcoming formats

EGRUL 4.08 and EGRIP 4.07, approved by FNS order ЕД-7-14/613@ of
2025-07-08. Effective 2026-02-01, mandatory from 2026-08-01.

Source: <https://www.nalog.gov.ru/html/sites/www.new.nalog.ru/files/about_fts/docs/xsd/16493030_xsd.zip>
(linked from <https://www.nalog.gov.ru/rn77/about_fts/docs/16493030/>).

Contents:

| File | Format |
|---|---|
| `VO_RUGF_2_311_26_04_08_01.xsd` | EGRUL 4.08 (legal entities) |
| `VO_RIGF_2_311_27_04_07_01.xsd` | EGRIP 4.07 (sole traders) |

## Where to look for future format updates

- Landing page (overview, regulations, FTP access procedure):
  <https://www.nalog.gov.ru/rn77/service/egrip2/egrip_vzayim/>
- All EGRUL/EGRIP format orders are linked from there.
