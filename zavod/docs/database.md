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

Running jobs affected by a migration will crash, restart, pull the new image, and block until the migration is complete.
It's impractical to wait until all jobs finish - they start throughout the day and some run frequently.

We don't need to go to extreme lengths to make migrations backward compatible.


## Using Alembic

From the `zavod/` directory:

```bash
# Create the zavod tables on a database that doesn't have them yet:
alembic upgrade head
```

Migrations connect to the same database as zavod. For Postgres, extend statement
timeout by setting `NOMENKLATURA_DB_STMT_TIMEOUT` (`0` to disable it).

## Changing the schema

1. Edit the table definitions in `zavod/stateful/model.py`.
2. Autogenerate a revision and review the draft:

   ```bash
   alembic revision --autogenerate -m "add column"
   ```

3. Apply it with `alembic upgrade head`.


## DB layer

We prefer a pattern of sqlalchemy-core + pydantic models for persistence in a database.

Historically this started because we liked something that's easy to dynamically generate queries yet be close to SQL we were used to writing. We don't hate it, but if we did want to migrate from it, we should do so with a lot of care.

Mixing sqlalchemy-core with sqlalchemy ORM could be problematic. We normally use a Nomenklatura Session which is basically just connection. A zavod context has a connection and we checkpoint and commit it at selective points in a run. sqlalchemy ORM has state in python which might become out of sync with the Nomenklatura session, and automatically flushes which might bring surprises and zavod isn't currently set up to work with this.

The pattern:

- Schema: tables are defined with SQLAlchemy Core on one MetaData, which Alembic manages.
- Models: one pydantic model per table, with fields matching the column names, so `model_validate(row._mapping)` works directly.
- Data access on the model:
  - `load(conn, stmt)` is where a row becomes a model.
  - `by_<key>` builds a select and hands it to load.
  - `create` inserts with .returning(id), then reads the row back through by_id so server defaults are included.
  - save runs `update ... values(model_dump(exclude={id, created_at}))`.
  - `delete` and `count_*` are classmethods.
- Return types: methods return `Self`, `Self | None`, `list[Self]` or a scalar, never Rows or tuples.
- Transactions belong to the caller: methods take the connection as their first argument and never commit.
- Postgres/SQLite differences live in shared helpers: upserts (Session.insert, upsert), UTC handling (UTCDateTime, add_utc_timezone) and SQLite foreign-key enforcement.
- History: where history matters, rows are soft-deleted with deleted_at.
