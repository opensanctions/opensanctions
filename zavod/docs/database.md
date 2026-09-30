# Database

Zavod keeps runtime state in a SQL database: sqlite by default, Postgres in
production. The database is shared between zavod and the nomenklatura
components it embeds:

| Tables | Schema defined in | Created by |
| --- | --- | --- |
| `cache`, `resolver` (nomenklatura) | the component constructors | the components, on first use |
| `statement`, `position`, `program`, `review`, `review_entity` (zavod) | `zavod/stateful/model.py` | Alembic migrations |

The nomenklatura schemas are frozen, so those components create their tables
when missing. The zavod tables evolve and their data must survive each change,
so their schema is versioned with [Alembic](https://alembic.sqlalchemy.org/):
only the migrations in `zavod/alembic/` create or change these tables.
Production runs from repository checkouts, so the migrations live in the
repository rather than the installed package.

## Creating the tables

From the `zavod/` directory:

```bash
alembic upgrade head
```

The connection URL is taken from `sqlalchemy.url` in `alembic.ini` if set,
else from `ZAVOD_DATABASE_URI`, falling back to `OPENSANCTIONS_DATABASE_URI`.

## Changing the schema

1. Edit the table definitions in `zavod/stateful/model.py`.
2. Autogenerate a revision and review the draft:

   ```bash
   alembic revision --autogenerate -m "add statement table"
   ```

3. Apply it with `alembic upgrade head`.

Autogenerate considers only the zavod tables: the database also hosts the
nomenklatura tables and those of other projects, which the include filter in
`zavod/alembic/env.py` keeps out of the comparison.
