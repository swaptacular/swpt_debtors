"""storage params

Revision ID: 8f4fc8d34dde
Revises: a2f7d66208c9
Create Date: 2025-12-13 14:48:44.669030

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8f4fc8d34dde'
down_revision = 'a2f7d66208c9'
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
    op.execute("ALTER TABLE debtor ALTER COLUMN config_data SET STORAGE EXTERNAL")
    op.execute("ALTER TABLE running_transfer ALTER COLUMN transfer_note SET STORAGE EXTERNAL")

    set_storage_params(
        'debtor',
        toast_tuple_target=430,
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'running_transfer',
        fillfactor=100,
        autovacuum_vacuum_threshold=10000,
        autovacuum_vacuum_scale_factor=0.004,
        autovacuum_vacuum_insert_threshold=10000,
        autovacuum_vacuum_insert_scale_factor=0.004,
    )
    set_storage_params(
        'document',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )

    # Signals:
    set_storage_params(
        'configure_account_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'prepare_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'finalize_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )


def downgrade():
    op.execute("ALTER TABLE debtor ALTER COLUMN config_data SET STORAGE DEFAULT")
    op.execute("ALTER TABLE running_transfer ALTER COLUMN transfer_note SET STORAGE DEFAULT")

    reset_storage_params(
        'debtor',
        [
            'toast_tuple_target',
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'running_transfer',
        [
            'fillfactor',
            'autovacuum_vacuum_threshold',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'document',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )

    # Signals:
    reset_storage_params(
        'configure_account_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'prepare_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'finalize_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
