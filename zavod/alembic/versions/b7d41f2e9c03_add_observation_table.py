"""add observation table

Revision ID: b7d41f2e9c03
Revises: 664de5de0bc5
Create Date: 2026-11-15 12:00:00.000000

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b7d41f2e9c03"
down_revision: str | Sequence[str] | None = "664de5de0bc5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "observation",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dataset", sa.Unicode(length=255), nullable=False),
        sa.Column("key", sa.Unicode(length=255), nullable=False),
        sa.Column("checksum", sa.Unicode(length=255), nullable=False),
        sa.Column("first_seen", sa.DateTime(), nullable=False),
        sa.Column("last_seen", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_observation_dataset_key_checksum",
        "observation",
        ["dataset", "key", "checksum"],
        unique=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_observation_dataset_key_checksum", table_name="observation")
    op.drop_table("observation")
