"""funes pipeline catalogue tables

Revision ID: 664de5de0bc5
Revises: e0dbc0b5c6a6
Create Date: 2026-10-02 12:30:00.000000

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "664de5de0bc5"
down_revision: str | Sequence[str] | None = "e0dbc0b5c6a6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "funes_dataset",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("people_sought", sa.Text(), nullable=False),
        sa.Column("subject_label", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "funes_subject",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("dataset_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"], ["funes_dataset.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dataset_id", "name"),
    )
    op.create_table(
        "funes_candidate",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("attempt_id", sa.Uuid(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["subject_id"], ["funes_subject.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("subject_id", "url"),
    )
    op.create_table(
        "funes_attempt",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("candidate_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("model", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["candidate_id"], ["funes_candidate.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["snapshot_id"], ["snapshot.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("snapshot_id"),
        sa.CheckConstraint(
            "status IN ('usable', 'broken')", name="ck_funes_attempt_status_domain"
        ),
        sa.CheckConstraint(
            "outcome IS NOT NULL OR status = 'broken'",
            name="ck_funes_attempt_outcome_required_if_usable",
        ),
        sa.CheckConstraint(
            "outcome IS NULL OR (status = 'usable' AND outcome IN ('hit', 'miss'))",
            name="ck_funes_attempt_outcome_implies_usable",
        ),
        sa.CheckConstraint(
            "reason IS NULL OR status = 'broken' OR outcome = 'miss'",
            name="ck_funes_attempt_hit_without_reason",
        ),
        sa.CheckConstraint(
            "reason IS NOT NULL OR (status = 'usable' AND outcome = 'hit')",
            name="ck_funes_attempt_reason_required_unless_hit",
        ),
        sa.CheckConstraint(
            "model IS NOT NULL OR status = 'broken'",
            name="ck_funes_attempt_model_required_unless_broken",
        ),
    )
    op.create_index(
        "ix_funes_attempt_candidate_created",
        "funes_attempt",
        ["candidate_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_funes_attempt_candidate_created", table_name="funes_attempt"
    )
    op.drop_table("funes_attempt")
    op.drop_table("funes_candidate")
    op.drop_table("funes_subject")
    op.drop_table("funes_dataset")
