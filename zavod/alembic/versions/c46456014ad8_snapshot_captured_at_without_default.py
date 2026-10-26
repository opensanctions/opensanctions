"""Snapshot captured_at without server default

Revision ID: c46456014ad8
Revises: 664de5de0bc5
Create Date: 2026-10-19 12:00:00.000000

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c46456014ad8"
down_revision: str | Sequence[str] | None = "664de5de0bc5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # Pravda mints captured_at at capture; no insert path can omit it,
    # so the column has no default. (Pravda 0.9 removed the database
    # write that carried the original server default.)
    with op.batch_alter_table("snapshot") as batch:
        batch.alter_column(
            "captured_at",
            server_default=None,
            existing_type=sa.DateTime(timezone=True),
            existing_nullable=False,
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("snapshot") as batch:
        batch.alter_column(
            "captured_at",
            server_default=sa.func.now(),
            existing_type=sa.DateTime(timezone=True),
            existing_nullable=False,
        )
