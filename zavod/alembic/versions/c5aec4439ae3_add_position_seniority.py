"""add position.seniority

Revision ID: c5aec4439ae3
Revises: 664de5de0bc5
Create Date: 2026-10-30 12:00:00.000000

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c5aec4439ae3"
down_revision: str | Sequence[str] | None = "664de5de0bc5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "position", sa.Column("seniority", sa.Unicode(length=255), nullable=True)
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("position", "seniority")
