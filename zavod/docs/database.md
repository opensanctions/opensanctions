# Database

Zavod keeps runtime state in a SQL database: sqlite by default, Postgres in
production. The database is shared between zavod and the nomenklatura
components it embeds:

| Tables                                                   | Schema defined in         | Migrations                            |
| -------------------------------------------------------- | ------------------------- | ------------------------------------- |
| `cache`, `resolver`, `statement` (nomenklatura)          | `nomenklatura.db`         | nomenklatura's, branch `nomenklatura` |
| `position`, `program`, `review`, `review_entity` (zavod) | `zavod/stateful/model.py` | `zavod/alembic/`, branch `zavod`      |

All of these tables are versioned with [Alembic](https://alembic.sqlalchemy.org/),
and only migrations create or change them. Nomenklatura ships its revisions in
its package; zavod's `alembic.ini` includes them via `version_locations`, so
one `alembic upgrade heads` applies both. Production runs from repository
checkouts, so zavod's own migrations live in the repository rather than the
installed package.

## Using Alembic

From the `zavod/` directory:

```bash
# Create or upgrade all tables, zavod's and nomenklatura's:
alembic upgrade heads
```

Note `heads`: zavod's and nomenklatura's revisions are separate branches, so
there is more than one head. The connection URL is taken from
`ZAVOD_DATABASE_URI`, falling back to `OPENSANCTIONS_DATABASE_URI`.

After upgrading the nomenklatura dependency, run `alembic upgrade heads` to
apply any migrations it brings.

## Changing the schema

1. Edit the table definitions in `zavod/stateful/model.py`.
2. Autogenerate a revision on the `zavod` branch and review the draft:

   ```bash
   alembic revision --autogenerate --head zavod@head -m "add column"
   ```

3. Apply it with `alembic upgrade heads`.

Changes to nomenklatura's tables are made in nomenklatura, with a revision on
its own branch, and reach zavod with the next nomenklatura release.
