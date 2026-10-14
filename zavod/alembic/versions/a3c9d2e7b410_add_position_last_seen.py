"""add position.last_seen

Revision ID: a3c9d2e7b410
Revises: f6e4a1f539b1
Create Date: 2026-10-05 12:00:00.000000

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a3c9d2e7b410"
down_revision: str | Sequence[str] | None = "f6e4a1f539b1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("position", sa.Column("last_seen", sa.DateTime(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("position", "last_seen")
