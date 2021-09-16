"""Add canonical_id to statement

Revision ID: 0943255f9b62
Revises: 800f1358c93f
Create Date: 2021-09-16 12:14:23.618448

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0943255f9b62"
down_revision = "800f1358c93f"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "statement", sa.Column("canonical_id", sa.Unicode(length=255), nullable=True)
    )
    op.create_index(
        op.f("ix_statement_canonical_id"), "statement", ["canonical_id"], unique=False
    )


def downgrade():
    pass
