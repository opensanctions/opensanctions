"""Indexes on statement table.

Revision ID: dc9369e80cf7
Revises: 800f1358c93f
Create Date: 2021-07-02 11:40:33.147510

"""
from alembic import op


revision = "dc9369e80cf7"
down_revision = "800f1358c93f"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        op.f("ix_statement_dataset"), "statement", ["dataset"], unique=False
    )
    op.create_index(
        op.f("ix_statement_last_seen"), "statement", ["last_seen"], unique=False
    )


def downgrade():
    pass
