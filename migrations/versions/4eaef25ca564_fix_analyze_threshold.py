"""fix analyze threshold

Revision ID: 4eaef25ca564
Revises: 0b93817fb877
Create Date: 2026-02-14 13:38:09.559959

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4eaef25ca564'
down_revision = '0b93817fb877'
branch_labels = None
depends_on = None


def set_storage_params(table, **kwargs):
    storage_params = ', '.join(
        f"{param} = {str(value).lower()}" for param, value in kwargs.items()
    )
    op.execute(f"ALTER TABLE {table} SET ({storage_params})")


def reset_storage_params(table, param_names):
    op.execute(f"ALTER TABLE {table} RESET ({', '.join(param_names)})")


def upgrade():
    reset_storage_params(
        'document',
        [
            'autovacuum_analyze_threshold',
        ]
    )

    # Signals:
    reset_storage_params(
        'configure_account_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'prepare_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'finalize_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )


def downgrade():
    set_storage_params(
        'document',
        autovacuum_analyze_threshold=2000000000,
    )

    # Signals:
    set_storage_params(
        'configure_account_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'prepare_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'finalize_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
