import pytest
import uuid
from datetime import date, datetime
from sqlalchemy.exc import IntegrityError
from swpt_debtors.lower_limits import LowerLimit, LowerLimitSequence
from swpt_debtors.models import Debtor, InitiatedTransfer, RunningTransfer

D_ID = -1
C_ID = 1


@pytest.fixture
def debtor(db_session):
    debtor = Debtor(debtor_id=D_ID)
    db_session.add(debtor)
    db_session.commit()
    return debtor


def test_limit_properties(db_session):
    lower_limits = LowerLimitSequence([
        LowerLimit(10, date(2000, 1, 1)),
        LowerLimit(20, date(2000, 1, 2)),
    ])
    assert len(lower_limits.current_limits(date(2000, 1, 1))) == 2
    assert len(lower_limits.current_limits(date(2000, 1, 2))) == 1
    assert len(lower_limits.current_limits(date(2000, 1, 3))) == 0
    assert lower_limits.current_limits(date(2000, 1, 1)).apply_to_value(0) == 20
    assert lower_limits.current_limits(date(2000, 1, 2)).apply_to_value(0) == 20
    assert lower_limits.current_limits(date(2000, 1, 3)).apply_to_value(0) == 0

    d = Debtor(debtor_id=1)
    assert len(d.balance_lower_limits) == 0
    assert len(d.interest_rate_lower_limits) == 0
    d.balance_lower_limits = lower_limits
    d.interest_rate_lower_limits = lower_limits
    assert list(d.balance_lower_limits) == list(lower_limits)
    assert list(d.interest_rate_lower_limits) == list(lower_limits)
    db_session.add(d)
    db_session.commit()

    # Set to an empty list.
    d = Debtor.get_instance(1)
    assert list(d.balance_lower_limits) == list(lower_limits)
    assert d.bll_values is not None
    assert d.bll_cutoffs is not None
    d.balance_lower_limits = LowerLimitSequence()
    assert d.bll_values is None
    assert d.bll_cutoffs is None


def test_initiated_transfer_attrs(debtor, db_session, current_ts):
    debtor_id = debtor.debtor_id
    transfer_uuid = uuid.uuid4()
    t = InitiatedTransfer(debtor_id=debtor_id, transfer_uuid=transfer_uuid, recipient_uri='', amount=1)
    db_session.add(t)
    db_session.commit()
    t = InitiatedTransfer.query.filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid).one()
    assert t.transfer_info == {}
    assert not t.is_finalized
    assert t.errors == []
    assert isinstance(t.initiated_at_ts, datetime)
    assert not t.is_successful
    t.finalized_at_ts = current_ts
    t.error_code = 'Uups!'
    t.error_message = 'Just testing'
    assert t.is_finalized
    assert t.errors == [{'error_code': 'Uups!', 'message': 'Just testing'}]
    t.is_successful = True
    assert t.errors == []


def test_running_transfer_attrs(debtor, db_session, current_ts):
    debtor_id = debtor.debtor_id
    transfer_uuid = uuid.uuid4()
    t = RunningTransfer(debtor_id=debtor_id, transfer_uuid=transfer_uuid, recipient_creditor_id=C_ID,
                        amount=1, transfer_info={'note': 'a test note'})
    db_session.add(t)
    db_session.commit()
    t = RunningTransfer.query.filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid).one()
    assert t.transfer_info == {'note': 'a test note'}
    assert not t.is_finalized
    t.finalized_at_ts = current_ts
    assert t.is_finalized


def test_debtor_attrs(debtor, db_session, current_ts):
    assert debtor.interest_rate == 0.0
    assert not debtor.is_active
    debtor.interest_rate_target = 6.66
    debtor.status = Debtor.STATUS_IS_ACTIVE_FLAG
    assert debtor.is_active
    assert debtor.interest_rate == 6.66
    debtor.deactivated_at_date = current_ts
    with pytest.raises(IntegrityError):
        db_session.commit()
