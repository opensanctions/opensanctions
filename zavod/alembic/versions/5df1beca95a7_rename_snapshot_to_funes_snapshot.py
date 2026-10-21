"""Rename snapshot to funes_snapshot

Revision ID: 5df1beca95a7
Revises: c46456014ad8
Create Date: 2026-10-19 12:30:00.000000

"""

from collections.abc import Sequence

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "5df1beca95a7"
down_revision: str | Sequence[str] | None = "c46456014ad8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # The table is a funes state-store table like the others; the bare
    # name predates funes (it was pravda's own table definition). Rename
    # before the runner ships: nothing else knows the table yet.
    op.rename_table("snapshot", "funes_snapshot")


def downgrade() -> None:
    """Downgrade schema."""
    op.rename_table("funes_snapshot", "snapshot")
