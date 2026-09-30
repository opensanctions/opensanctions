# Database

Zavod keeps runtime state in a SQL database: sqlite by default, Postgres in
production. The database is shared between zavod and the nomenklatura
components it embeds:

| Tables                                                   | Schema defined in          | Created by                   |
| -------------------------------------------------------- | -------------------------- | ---------------------------- |
| `cache`, `resolver` (nomenklatura)                       | the component constructors | the components, on first use |
| `statement` (nomenklatura)                               | `nomenklatura.db`          | `load_dataset_to_db`         |
| `position`, `program`, `review`, `review_entity` (zavod) | `zavod/stateful/model.py`  | Alembic migrations           |

The nomenklatura tables are not managed by zavod's Alembic setup: they are
created when missing by whoever uses them.

The zavod schema is versioned with [Alembic](https://alembic.sqlalchemy.org/): only the
migrations in `zavod/alembic/` create or change these tables. Production runs
from repository checkouts, so the migrations live in the repository rather
than the installed package.

## Using Alembic

From the `zavod/` directory:

```bash
# Create the zavod tables on a database that doesn't have them yet:
alembic upgrade head
```

The connection URL is taken from `ZAVOD_DATABASE_URI`, falling back to `OPENSANCTIONS_DATABASE_URI`.

## Changing the schema

1. Edit the table definitions in `zavod/stateful/model.py`.
2. Autogenerate a revision and review the draft:

   ```bash
   alembic revision --autogenerate -m "add column"
   ```

3. Apply it with `alembic upgrade head`.
