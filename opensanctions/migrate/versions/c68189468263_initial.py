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
    tbl = "statement"
    op.create_table(
        tbl,
        sa.Column("entity_id", sa.Unicode(length=255), nullable=False),
        sa.Column("prop", sa.Unicode(length=255), nullable=False),
        sa.Column("prop_type", sa.Unicode(length=255), nullable=False),
        sa.Column("schema", sa.Unicode(length=255), nullable=False),
        sa.Column("value", sa.Unicode(length=65535), nullable=False),
        sa.Column("dataset", sa.Unicode(length=255), nullable=False),
        sa.Column("first_seen", sa.DateTime(), nullable=True),
        sa.Column("last_seen", sa.DateTime(), nullable=True),
        sa.Column("target", sa.Boolean(), nullable=False),
        sa.Column("unique", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("entity_id", "prop", "value", "dataset"),
    )
    op.create_index(op.f("ix_statement_entity_id"), tbl, ["entity_id"], unique=False)
    op.create_index(op.f("ix_statement_value"), tbl, ["value"], unique=False)
    op.create_index(op.f("ix_statement_dataset"), tbl, ["dataset"], unique=False)
    op.create_index(op.f("ix_statement_last_seen"), tbl, ["last_seen"], unique=False)


def downgrade():
    pass
