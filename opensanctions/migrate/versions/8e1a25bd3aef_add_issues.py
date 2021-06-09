"""Add data issues tracking

Revision ID: 8e1a25bd3aef
Revises: c68189468263
Create Date: 2021-06-08 10:04:28.897676

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "8e1a25bd3aef"
down_revision = "c68189468263"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "issue",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("level", sa.Unicode(), nullable=False),
        sa.Column("module", sa.Unicode(), nullable=True),
        sa.Column("dataset", sa.Unicode(), nullable=False),
        sa.Column("message", sa.Unicode(), nullable=True),
        sa.Column("entity_id", sa.Unicode(length=128), nullable=True),
        sa.Column("entity_schema", sa.Unicode(), nullable=True),
        sa.Column("data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_issue_dataset"), "issue", ["dataset"], unique=False)
    op.create_index(op.f("ix_issue_entity_id"), "issue", ["entity_id"], unique=False)


def downgrade():
    pass
