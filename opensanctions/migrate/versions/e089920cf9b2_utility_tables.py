"""Cache and canonical utility tables

Revision ID: e089920cf9b2
Revises: 61e1e03bcf7a
Create Date: 2022-01-07 20:01:31.792678

"""
from alembic import op
import sqlalchemy as sa


revision = "e089920cf9b2"
down_revision = "61e1e03bcf7a"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "cache",
        sa.Column("url", sa.Unicode(), nullable=False),
        sa.Column("text", sa.Unicode(), nullable=True),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
    )
    op.create_index(op.f("ix_cache_timestamp"), "cache", ["timestamp"], unique=False)
    op.create_index(op.f("ix_cache_url"), "cache", ["url"], unique=True)
    op.create_table(
        "canonical",
        sa.Column("entity_id", sa.Unicode(length=255), nullable=False),
        sa.Column("canonical_id", sa.Unicode(length=255), nullable=True),
    )
    op.create_index(
        op.f("ix_canonical_canonical_id"), "canonical", ["canonical_id"], unique=False
    )
    op.create_index(
        op.f("ix_canonical_entity_id"), "canonical", ["entity_id"], unique=False
    )
    op.create_unique_constraint(None, "statement", ["id"])


def downgrade():
    pass
