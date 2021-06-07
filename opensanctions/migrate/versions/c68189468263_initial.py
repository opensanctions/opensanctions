"""initial

Revision ID: c68189468263
Revises: 
Create Date: 2021-06-06 14:10:39.120187

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "c68189468263"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "statement",
        sa.Column("entity_id", sa.Unicode(length=128), nullable=False),
        sa.Column("canonical_id", sa.Unicode(length=128), nullable=True),
        sa.Column("prop", sa.Unicode(), nullable=False),
        sa.Column("schema", sa.Unicode(), nullable=True),
        sa.Column("value", sa.Unicode(), nullable=False),
        sa.Column("dataset", sa.Unicode(), nullable=False),
        sa.Column("first_seen", sa.DateTime(), nullable=True),
        sa.Column("last_seen", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("entity_id", "prop", "value", "dataset"),
        sa.UniqueConstraint("entity_id", "prop", "value", "dataset", name="_prov"),
    )
    op.create_index(
        op.f("ix_statement_canonical_id"), "statement", ["canonical_id"], unique=False
    )
    op.create_index(
        op.f("ix_statement_entity_id"), "statement", ["entity_id"], unique=False
    )
    op.create_index(op.f("ix_statement_value"), "statement", ["value"], unique=False)


def downgrade():
    pass
