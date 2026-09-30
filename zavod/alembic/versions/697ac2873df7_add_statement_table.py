"""statement table from nomenklatura

Revision ID: 697ac2873df7
Revises: f6e4a1f539b1
Create Date: 2026-09-30 12:22:07.124410

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "697ac2873df7"
down_revision: str | Sequence[str] | None = "f6e4a1f539b1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "statement",
        sa.Column("id", sa.Unicode(length=255), nullable=False),
        sa.Column("entity_id", sa.Unicode(length=255), nullable=False),
        sa.Column("canonical_id", sa.Unicode(length=255), nullable=False),
        sa.Column("prop", sa.Unicode(length=255), nullable=False),
        sa.Column("prop_type", sa.Unicode(length=255), nullable=False),
        sa.Column("schema", sa.Unicode(length=255), nullable=False),
        sa.Column("value", sa.Unicode(length=65535), nullable=False),
        sa.Column("original_value", sa.Unicode(length=65535), nullable=True),
        sa.Column("dataset", sa.Unicode(length=255), nullable=True),
        sa.Column("origin", sa.Unicode(length=255), nullable=True),
        sa.Column("lang", sa.Unicode(length=255), nullable=True),
        sa.Column("external", sa.Boolean(), nullable=False),
        sa.Column("first_seen", sa.DateTime(), nullable=True),
        sa.Column("last_seen", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id"),
    )
    op.create_index(
        op.f("ix_statement_canonical_id"), "statement", ["canonical_id"], unique=False
    )
    op.create_index(
        op.f("ix_statement_dataset"), "statement", ["dataset"], unique=False
    )
    op.create_index(
        op.f("ix_statement_entity_id"), "statement", ["entity_id"], unique=False
    )
    op.create_index(op.f("ix_statement_origin"), "statement", ["origin"], unique=False)
    op.create_index(op.f("ix_statement_prop"), "statement", ["prop"], unique=False)
    op.create_index(
        op.f("ix_statement_prop_type"), "statement", ["prop_type"], unique=False
    )
    op.create_index(op.f("ix_statement_schema"), "statement", ["schema"], unique=False)
    op.create_index(
        "ix_statement_value_entity",
        "statement",
        ["value"],
        unique=False,
        sqlite_where=sa.text("prop_type = 'entity'"),
        postgresql_where=sa.text("prop_type = 'entity'"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_statement_value_entity",
        table_name="statement",
        sqlite_where=sa.text("prop_type = 'entity'"),
        postgresql_where=sa.text("prop_type = 'entity'"),
    )
    op.drop_index(op.f("ix_statement_schema"), table_name="statement")
    op.drop_index(op.f("ix_statement_prop_type"), table_name="statement")
    op.drop_index(op.f("ix_statement_prop"), table_name="statement")
    op.drop_index(op.f("ix_statement_origin"), table_name="statement")
    op.drop_index(op.f("ix_statement_entity_id"), table_name="statement")
    op.drop_index(op.f("ix_statement_dataset"), table_name="statement")
    op.drop_index(op.f("ix_statement_canonical_id"), table_name="statement")
    op.drop_table("statement")
