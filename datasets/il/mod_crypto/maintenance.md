# Maintaining il_mod_crypto

The crawler reads its entity data from `seizures.csv` next to this file, not from the
NBCTF web page. It is edited by hand, as a pull request against that file; the Google
Sheet the data used to live in is retired, and `seizures.csv` began as a verbatim export
of it, so the two produce identical entities. The page is only fetched to snapshot the two
tables into `releases.csv` and `wallets.csv`, so that `git diff` shows what changed on
the source. When the diff shows a new order or new wallets, add the rows by hand.

The crawler reads the file out of the checkout, so a run always uses the copy in front
of it and a pull request is crawled with its own edit. `data.url` in `il_mod_crypto.yml`
points at the file on `main` for provenance only — it is published as the dataset's
source URL and is not fetched. `*.csv` is in `.gitignore`, so the file needs
`git add -f datasets/il/mod_crypto/seizures.csv` — without the `-f` the edit is silently
left out of the commit.

Source page: <https://nbctf.mod.gov.il/he/MinisterSanctions/PropertyPerceptions/Pages/Blockchain.aspx>
(Hebrew — it is more complete and more current than the English translation).

**Always open the Seizure Order PDF for the order you are adding.** The web tables carry
only wallet addresses and a few names; almost everything else — dates, platforms, whether
the holder is a person or a company, ID and passport numbers — is in the PDF and its
annexes. Review every file linked for the order before filling in the row.

## Fields that need care

### `schema` — `Person`, `Wallet`, `Account` or `LegalEntity`

Decide from the Seizure Order PDF, not from the name. Companies, exchanges and money
changers are `LegalEntity`; individuals are `Person`. Where the order lists a wallet or an
exchange account with no identified holder, use `Wallet` or `Account` respectively — the
crawler emits the `CryptoWallet` from `wallet_address`/`account_id` either way, and only
builds a holder entity for `Person` and `LegalEntity` rows.

Only `Person` rows use `dob`, `id_no`/`id_country`, `residency_no`/`residency_country`,
`passport_no`/`passport_country`, `email` and `phone`. A `Person` may be recorded by ID or
passport number alone where the order gives no name.

### `start_date` — date the order was issued

Not in the web table. Take it from the date field inside the Seizure Order PDF, which is
usually **handwritten**, though it is sometimes printed. In most orders currently in the
file it precedes `end_date` by exactly two years, which makes each a good check on the
other when the handwriting is hard to read. Leave it empty rather than guess — the ASO
number and `last_updated` still identify the order.

### `end_date` — "Validity of Issue" (`תוקף הצו`)

From the releases table, where that column holds a date (e.g. `20.04.2028`). Getting this wrong
is consequential: an `end_date` in the past makes the sanction inactive, and the crawler then
drops the `crime.terror` topic from the wallet.

### `last_updated` — "Last Updated" (`תאריך עדכון`)

Copied straight from the releases table. Note it is the date the *listing* was last
touched, which is often later than `start_date` (an amendment, a re-publication).

### `platform` — managing exchange

The exchange or service holding the wallet or account (e.g. `Binance`, `OKX`, `NOBITEX` —
the three in use so far). Rarely in the web table; usually named in the Seizure Order PDF.
Fill it in especially for rows that carry an `account_id` rather than an on-chain
`wallet_address`, since the account number is meaningless without the platform it belongs
to. The two columns are mutually exclusive: an on-chain address goes in `wallet_address`, a
platform account number in `account_id`.

### `order_url` — link to the Seizure Order PDF

The URL of the "Seizure order (ASO n/yy) of the Minister of Defense" file in the releases
table. **Never leave this empty when a PDF is available**: it is the document underlying the
designation, and it is what a user needs in order to verify the entry. Two related columns:

- `annex_url` — the "Annex of the Seizure Order — Wallet Details" file, where one exists.
- `forfeiture_order_url` — the forfeiture order linked from the "Validity of Issue" column.

## Dates

Write dates as `YYYY-MM-DD`, or `YYYY-MM` / `YYYY` where the order gives only that
much. ISO is the only form the crawler parses; anything else is kept verbatim as
unparsed text.

## After editing

Run the crawler and check `data/datasets/il_mod_crypto/issues.log` for warnings, then
confirm the entity counts still sit inside the `assertions` block in `il_mod_crypto.yml`,
raising the `min`/`max` bounds if a large order has pushed them.

```bash
zavod crawl datasets/il/mod_crypto/il_mod_crypto.yml
```
