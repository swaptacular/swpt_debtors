"""create_pktypes

Revision ID: 0b93817fb877
Revises: 8f4fc8d34dde
Create Date: 2026-02-08 19:26:45.816906

"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime
from sqlalchemy.inspection import inspect

# revision identifiers, used by Alembic.
revision = '0b93817fb877'
down_revision = '8f4fc8d34dde'
branch_labels = None
depends_on = None


def _pg_type(column_type):
    if column_type.python_type == datetime:
        if column_type.timezone:
            return "TIMESTAMP WITH TIME ZONE"
        else:
            return "TIMESTAMP"

    return str(column_type)


def _pktype_name(model):
    return f"{model.__table__.name}_pktype"


def create_pktype(model):
    mapper = inspect(model)
    type_declaration = ','.join(
        f"{c.key} {_pg_type(c.type)}" for c in mapper.primary_key
    )
    op.execute(
        f"CREATE TYPE {_pktype_name(model)} AS ({type_declaration})"
    )


def drop_pktype(model):
    op.execute(f"DROP TYPE IF EXISTS {_pktype_name(model)}")


def upgrade():
    from swpt_debtors import models

    create_pktype(models.Debtor)
    create_pktype(models.ConfigureAccountSignal)
    create_pktype(models.PrepareTransferSignal)
    create_pktype(models.FinalizeTransferSignal)


def downgrade():
    from swpt_debtors import models

    drop_pktype(models.Debtor)
    drop_pktype(models.ConfigureAccountSignal)
    drop_pktype(models.PrepareTransferSignal)
    drop_pktype(models.FinalizeTransferSignal)
