"""Add statement.id hash

Revision ID: 61e1e03bcf7a
Revises: 0943255f9b62
Create Date: 2022-01-06 23:30:35.537347

"""
from alembic import op
import sqlalchemy as sa
from hashlib import sha1


# revision identifiers, used by Alembic.
revision = "61e1e03bcf7a"
down_revision = "0943255f9b62"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("statement", sa.Column("id", sa.Unicode(length=255), nullable=True))

    bind = op.get_bind()
    meta = sa.MetaData()
    meta.bind = bind
    meta.reflect()
    table = meta.tables["statement"]
    q = sa.select([table])
    crp = bind.execute(q)
    while True:
        batch = crp.fetchmany(1000)
        if not batch:
            break
        # print("batch", len(batch))
        for row in batch:
            key = f"{row.dataset}.{row.entity_id}.{row.prop}.{row.value}"
            hashed = sha1(key.encode("utf-8")).hexdigest()
            q = sa.update(table)
            q = q.where(table.c.entity_id == row.entity_id)
            q = q.where(table.c.prop == row.prop)
            q = q.where(table.c.value == row.value)
            q = q.where(table.c.dataset == row.dataset)
            q = q.values(id=hashed)
            bind.execute(q)

    op.alter_column("statement", "id", nullable=False)
    op.alter_column(
        "statement", "entity_id", existing_type=sa.VARCHAR(length=255), nullable=False
    )
    op.drop_index("ix_statement_value", table_name="statement")
    # op.create_index(op.f("ix_statement_id"), "statement", ["id"])
    op.drop_constraint("statement_pkey", "statement", type_="primary")
    op.create_primary_key("PRIMARY", "statement", ["id"])
    # op.create_unique_constraint(None, "statement", ["id"])
    op.alter_column("statement", "first_seen", nullable=False)


def downgrade():
    pass
