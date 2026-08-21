# Maintaining il_mod_crypto

The dataset is maintained as `seizures.csv` next to the crawler; edits are made as
pull requests against that file. It replaces the Google Sheet the data used to live
in, which is now retired.

The data is transcribed from the seizure order PDFs rather than scraped, because the
table on the source page omits some of the wallet addresses the orders name. The
crawler still fetches the page on every run, but only to monitor it: it snapshots
both tables to `page_releases.csv` and `page_wallets.csv` — committed, so a diff
shows what changed, and exported as resources, so the tables can be read back from
an archived run — and warns about any order or value the page lists that
`seizures.csv` does not cover.

Source page (Hebrew; more complete and more current than the English translation):
<https://nbctf.mod.gov.il/he/MinisterSanctions/PropertyPerceptions/Pages/Blockchain.aspx>

One row per wallet or account per order. A holder with several wallets under one
order gets one row per wallet, repeating the holder's details; an order that names a
wallet with no holder gets a wallet-only row. The same wallet seized under a later
order is a new row, keyed on that order.

**Always open the Seizure Order PDF for the order you are adding.** The page table
carries only wallet addresses and a few names; almost everything else — dates,
platforms, whether the holder is a person or a company, ID and passport numbers — is
in the PDF and its annexes, and the page's column headings do not hold for every
order (the ASO 1/25 block lists phone numbers under the wallet heading). Review
every document linked for the order before filling in the row.

## Fields that need care

### `schema` — `Person`, `Wallet`, `Account` or `LegalEntity`

Decide from the Seizure Order PDF, not from the name. Companies, exchanges and money
changers are `LegalEntity`; individuals are `Person`. Where the order lists a wallet
or an exchange account with no identified holder, use `Wallet` or `Account`
respectively — the crawler emits the `CryptoWallet` from `wallet_address` or
`account_id` either way, and only builds a holder entity for `Person` and
`LegalEntity` rows.

Only `Person` rows use `dob`, `id_no`/`id_country`, `residency_no`/`residency_country`,
`passport_no`/`passport_country`, `email` and `phone`. A `Person` may be recorded by
ID or passport number alone where the order gives no name.

### `start_date` — date the order was issued

Not in the page table. Take it from the date field inside the Seizure Order PDF,
which is usually **handwritten**, though it is sometimes printed. In every order in
the file so far it precedes `end_date` by exactly two years, which makes each a good
check on the other when the handwriting is hard to read. Leave it empty rather than
guess — the order number and `last_updated` still identify the order.

### `end_date` — "Validity of Issue" (`תוקף הצו`)

From the releases table, where that column holds a date (e.g. `20.04.2028`). Getting
this wrong is consequential: an `end_date` in the past makes the sanction inactive,
and the crawler then drops the `crime.terror` topic from the wallet.

### `last_updated` — "Last Updated" (`תאריך עדכון`)

Copied straight from the releases table. Note it is the date the *listing* was last
touched, which is often later than `start_date` (an amendment, a re-publication).

### `platform` — managing exchange

The exchange or service holding the wallet or account (`Binance`, `OKX` and
`NOBITEX` so far). Rarely in the page table; usually named in the Seizure Order PDF.
Fill it in especially for rows that carry an `account_id` rather than an on-chain
`wallet_address`, since the account number is meaningless without the platform it
belongs to. The two columns are mutually exclusive: an on-chain address goes in
`wallet_address`, a platform account number in `account_id`.

### `order_url` — link to the Seizure Order PDF

The URL of the "Seizure order (ASO n/yy) of the Minister of Defense" file in the
releases table. **Never leave this empty when a PDF is available**: it is the document
underlying the designation, and it is what a user needs in order to verify the entry.
Two related columns:

- `annex_url` — the "Annex of the Seizure Order — Wallet Details" file, where one exists.
- `forfeiture_order_url` — the forfeiture order linked from the "Validity of Issue" column.

## Dates

Write dates as `YYYY-MM-DD`, which is how the file is kept. `DD.MM.YYYY` and
`DD/MM/YYYY` also parse (see `dates.formats` in the yml), but only the ISO form is
unambiguous: slash dates are read day-first, so a date that a spreadsheet has
helpfully reformatted to US `MM/DD/YYYY` is parsed with day and month swapped,
silently and without a warning, for the first twelve days of any month. 82 of the
dates transcribed from the Google Sheet were in that ambiguous form and were
converted day-first, exactly as the crawler had been reading them; a reviewer with
the order PDF at hand can correct any that were entered US-style.

## Handling the crawler's warnings

The source page and the order PDFs are both behind anti-bot protection, so they need
a Zyte key to fetch and cannot be read from CI. Warnings therefore carry what is
needed to draft an update — the order, its documents, and the values the page lists —
and the full page tables can be downloaded from the latest archived run. The
instructions for each warning are in the `config.discovery` block of the dataset yml,
next to `reviewed_orders`, which mutes an order that should not be imported at all.

What the monitor does not see, and `manual_check` covers instead: a wallet added to
an order's PDF or annex without the page table changing, holder details that only
ever appear in the PDF, and a name added to the page without an accompanying number.

## After editing

Run the crawler and check `data/datasets/il_mod_crypto/issues.log` for warnings, then
confirm the entity counts still sit inside the `assertions` block in the yml, raising
the `min`/`max` bounds if a large order has pushed them.

```bash
zavod crawl datasets/il/mod_crypto/il_mod_crypto.yml
```
