import pytest
import uuid
from swpt_debtors.models import Debtor, RunningTransfer

D_ID = -1
C_ID = 1


@pytest.fixture
def debtor(db_session):
    debtor = Debtor(debtor_id=D_ID, status_flags=Debtor.STATUS_IS_ACTIVATED_FLAG)
    db_session.add(debtor)
    db_session.commit()
    return debtor


def test_sibnalbus_burst_count(app):
    from swpt_debtors import models as m
    assert isinstance(m.ConfigureAccountSignal.signalbus_burst_count, int)
    assert isinstance(m.PrepareTransferSignal.signalbus_burst_count, int)
    assert isinstance(m.FinalizeTransferSignal.signalbus_burst_count, int)


def test_running_transfer_attrs(debtor, db_session, current_ts):
    debtor_id = debtor.debtor_id
    transfer_uuid = uuid.uuid4()
    t = RunningTransfer(debtor_id=debtor_id, transfer_uuid=transfer_uuid,
                        recipient_uri='swpt:2/1', recipient='1',
                        amount=1, transfer_note_format='fmt', transfer_note='a test note')
    db_session.add(t)
    db_session.commit()
    t = RunningTransfer.query.filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid).one()
    assert t.transfer_note_format == 'fmt'
    assert t.transfer_note == 'a test note'
    assert not t.is_settled
    t.transfer_id = 666
    assert t.is_settled


def test_debtor_attrs(debtor, db_session, current_ts):
    debtor.status = Debtor.STATUS_IS_ACTIVATED_FLAG
    debtor.deactivate()
    assert debtor.is_activated
    assert debtor.is_deactivated
