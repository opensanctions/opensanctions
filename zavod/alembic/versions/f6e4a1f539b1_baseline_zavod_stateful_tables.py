"""baseline: zavod stateful tables

Revision ID: f6e4a1f539b1
Revises:
Create Date: 2026-09-28 19:16:01.486470

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f6e4a1f539b1"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "position",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("entity_id", sa.Unicode(length=255), nullable=False),
        sa.Column("caption", sa.Unicode(length=65535), nullable=False),
        sa.Column("countries", sa.JSON(), nullable=False),
        sa.Column("subnational_areas", sa.JSON(), nullable=True),
        sa.Column("is_pep", sa.Boolean(), nullable=True),
        sa.Column("topics", sa.JSON(), nullable=False),
        sa.Column("dataset", sa.Unicode(length=65535), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("modified_at", sa.DateTime(), nullable=True),
        sa.Column("modified_by", sa.Unicode(length=255), nullable=True),
        sa.Column("deleted_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_position_deleted_at"), "position", ["deleted_at"], unique=False
    )
    op.create_index(
        op.f("ix_position_entity_id"), "position", ["entity_id"], unique=False
    )
    op.create_table(
        "program",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("key", sa.Unicode(length=255), nullable=False),
        sa.Column("title", sa.Unicode(length=65535), nullable=True),
        sa.Column("url", sa.Unicode(length=65535), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key"),
    )
    op.create_table(
        "review",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("key", sa.Unicode(length=255), nullable=False),
        sa.Column("dataset", sa.Unicode(length=255), nullable=False),
        sa.Column("extraction_schema", sa.JSON(), nullable=False),
        sa.Column("source_value", sa.Unicode(length=1048576), nullable=True),
        sa.Column("source_mime_type", sa.Unicode(length=65535), nullable=True),
        sa.Column("source_label", sa.Unicode(length=65535), nullable=True),
        sa.Column("source_url", sa.Unicode(length=65535), nullable=True),
        sa.Column("accepted", sa.Boolean(), nullable=False),
        sa.Column("crawler_version", sa.Integer(), nullable=False),
        sa.Column("original_extraction", sa.JSON(), nullable=False),
        sa.Column("origin", sa.Unicode(length=65535), nullable=False),
        sa.Column("extracted_data", sa.JSON(), nullable=False),
        sa.Column("last_seen_version", sa.Unicode(length=255), nullable=False),
        sa.Column("modified_at", sa.DateTime(), nullable=False),
        sa.Column("modified_by", sa.Unicode(length=255), nullable=False),
        sa.Column("deleted_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_review_accepted"), "review", ["accepted"], unique=False)
    op.create_index(op.f("ix_review_dataset"), "review", ["dataset"], unique=False)
    op.create_index(
        op.f("ix_review_deleted_at"), "review", ["deleted_at"], unique=False
    )
    op.create_index(op.f("ix_review_key"), "review", ["key"], unique=False)
    op.create_index(
        "ix_review_key_dataset_unique_not_deleted",
        "review",
        ["key", "dataset"],
        unique=True,
        sqlite_where=sa.text("deleted_at IS NULL"),
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        op.f("ix_review_last_seen_version"),
        "review",
        ["last_seen_version"],
        unique=False,
    )
    op.create_table(
        "review_entity",
        sa.Column("dataset", sa.Unicode(length=255), nullable=False),
        sa.Column("review_key", sa.Unicode(length=255), nullable=False),
        sa.Column("entity_id", sa.Unicode(length=255), nullable=False),
        sa.Column("last_seen_version", sa.Unicode(length=255), nullable=False),
    )
    op.create_index(
        op.f("ix_review_entity_dataset"), "review_entity", ["dataset"], unique=False
    )
    op.create_index(
        op.f("ix_review_entity_last_seen_version"),
        "review_entity",
        ["last_seen_version"],
        unique=False,
    )
    op.create_index(
        op.f("ix_review_entity_review_key"),
        "review_entity",
        ["review_key"],
        unique=False,
    )
    op.create_index(
        "ix_review_entity_unique_review_key_entity_id_dataset",
        "review_entity",
        ["review_key", "entity_id", "dataset"],
        unique=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_review_entity_unique_review_key_entity_id_dataset",
        table_name="review_entity",
    )
    op.drop_index(op.f("ix_review_entity_review_key"), table_name="review_entity")
    op.drop_index(
        op.f("ix_review_entity_last_seen_version"), table_name="review_entity"
    )
    op.drop_index(op.f("ix_review_entity_dataset"), table_name="review_entity")
    op.drop_table("review_entity")
    op.drop_index(op.f("ix_review_last_seen_version"), table_name="review")
    op.drop_index(
        "ix_review_key_dataset_unique_not_deleted",
        table_name="review",
        sqlite_where=sa.text("deleted_at IS NULL"),
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index(op.f("ix_review_key"), table_name="review")
    op.drop_index(op.f("ix_review_deleted_at"), table_name="review")
    op.drop_index(op.f("ix_review_dataset"), table_name="review")
    op.drop_index(op.f("ix_review_accepted"), table_name="review")
    op.drop_table("review")
    op.drop_table("program")
    op.drop_index(op.f("ix_position_entity_id"), table_name="position")
    op.drop_index(op.f("ix_position_deleted_at"), table_name="position")
    op.drop_table("position")
