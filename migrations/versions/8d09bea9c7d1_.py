"""empty message

Revision ID: 8d09bea9c7d1
Revises: 
Create Date: 2019-11-21 16:36:52.340664

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.schema import Sequence, CreateSequence, DropSequence


# revision identifiers, used by Alembic.
revision = '8d09bea9c7d1'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute(CreateSequence(Sequence('issuing_coordinator_request_id_seq')))
    op.execute(CreateSequence(Sequence('debtor_reservation_id_seq')))


def downgrade():
    op.execute(DropSequence(Sequence('issuing_coordinator_request_id_seq')))
    op.execute(DropSequence(Sequence('debtor_reservation_id_seq')))
