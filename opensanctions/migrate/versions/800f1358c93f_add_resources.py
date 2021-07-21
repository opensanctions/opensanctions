"""Record resources in the database

Revision ID: 800f1358c93f
Revises: 8e1a25bd3aef
Create Date: 2021-06-18 20:31:01.392640

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "800f1358c93f"
down_revision = "8e1a25bd3aef"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "resource",
        sa.Column("path", sa.Unicode(length=255), nullable=False),
        sa.Column("checksum", sa.Unicode(length=255), nullable=False),
        sa.Column("dataset", sa.Unicode(length=255), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("mime_type", sa.Unicode(length=255), nullable=True),
        sa.Column("size", sa.Integer(), nullable=True),
        sa.Column("title", sa.Unicode(length=65535), nullable=True),
        sa.PrimaryKeyConstraint("path", "dataset"),
    )
    op.create_index(op.f("ix_resource_dataset"), "resource", ["dataset"], unique=False)


def downgrade():
    pass
