import pytest
from uuid import UUID
from datetime import datetime, date, timedelta
from swpt_debtors import __version__
from swpt_debtors.models import Account, ChangeInterestRateSignal, InitiatedTransfer, \
    INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL
from swpt_debtors import procedures as p
from swpt_debtors.lower_limits import LowerLimit

D_ID = -1
C_ID = 1


@pytest.fixture
def debtor():
    return p.get_or_create_debtor(D_ID)


def test_version(db_session):
    assert __version__


def test_is_later_event(current_ts):
    assert p._is_later_event((1, current_ts), (None, None))


def test_get_or_create_debtor(db_session):
    debtor = p.get_or_create_debtor(D_ID)
    assert debtor.debtor_id == D_ID
    debtor = p.get_or_create_debtor(D_ID)
    assert debtor.debtor_id == D_ID


def test_process_account_change_signal(db_session, debtor):
    change_seqnum = 1
    change_ts = datetime.fromisoformat('2019-10-01T00:00:00+00:00')
    last_outgoing_transfer_date = date.fromisoformat('2019-10-01')
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=change_seqnum,
        change_ts=change_ts,
        principal=1000,
        interest=12.5,
        interest_rate=-0.5,
        last_outgoing_transfer_date=last_outgoing_transfer_date,
        status=0,
    )
    assert len(Account.query.all()) == 1
    a = Account.get_instance((D_ID, C_ID))
    assert a.change_seqnum == change_seqnum
    assert a.change_ts == change_ts
    assert a.principal == 1000
    assert a.interest == 12.5
    assert a.interest_rate == -0.5
    assert a.last_outgoing_transfer_date == last_outgoing_transfer_date
    assert a.status == 0
    cirs = ChangeInterestRateSignal.query.all()
    assert len(cirs) == 1
    assert cirs[0].debtor_id == D_ID
    assert cirs[0].creditor_id == C_ID

    # Older message
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=change_seqnum - 1,
        change_ts=change_ts,
        principal=1001,
        interest=12.5,
        interest_rate=-0.5,
        last_outgoing_transfer_date=last_outgoing_transfer_date,
        status=0,
    )
    assert len(Account.query.all()) == 1
    a = Account.get_instance((D_ID, C_ID))
    assert a.principal == 1000
    cirs = ChangeInterestRateSignal.query.all()
    assert len(cirs) == 1

    # Newer message
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=change_seqnum + 1,
        change_ts=change_ts + timedelta(seconds=5),
        principal=1001,
        interest=12.6,
        interest_rate=-0.6,
        last_outgoing_transfer_date=last_outgoing_transfer_date + timedelta(days=1),
        status=Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG,
    )
    assert len(Account.query.all()) == 1
    a = Account.get_instance((D_ID, C_ID))
    assert a.change_seqnum == change_seqnum + 1
    assert a.change_ts == change_ts + timedelta(seconds=5)
    assert a.principal == 1001
    assert a.interest == 12.6
    assert a.interest_rate == -0.6
    assert a.last_outgoing_transfer_date == last_outgoing_transfer_date + timedelta(days=1)
    assert a.status == Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    cirs = ChangeInterestRateSignal.query.all()
    assert len(cirs) == 1


def test_interest_rate_absolute_limits(db_session, debtor):
    debtor.interest_rate_target = -100.0
    assert debtor.interest_rate == INTEREST_RATE_FLOOR
    debtor.interest_rate_target = 1e100
    assert debtor.interest_rate == INTEREST_RATE_CEIL


def test_update_debtor_policy(db_session, debtor, current_ts):
    date_years_ago = (current_ts - timedelta(days=5000)).date()
    with pytest.raises(p.DebtorDoesNotExistError):
        p.update_debtor_policy(1234567890, 6.66, [], [])

    p.update_debtor_policy(D_ID, 6.66, [LowerLimit(0.0, date_years_ago)], [LowerLimit(-1000, date_years_ago)])
    debtor = p.get_debtor(D_ID)
    assert debtor.interest_rate_target == 6.66
    assert len(debtor.interest_rate_lower_limits) == 1
    assert debtor.interest_rate_lower_limits[0] == LowerLimit(0.0, date_years_ago)
    assert len(debtor.balance_lower_limits) == 1
    assert debtor.balance_lower_limits[0] == LowerLimit(-1000, date_years_ago)

    p.update_debtor_policy(D_ID, None, [], [])
    debtor = p.get_debtor(D_ID)
    assert debtor.interest_rate_target == 6.66
    assert len(debtor.interest_rate_lower_limits) == 0
    assert len(debtor.balance_lower_limits) == 0

    with pytest.raises(p.ConflictingPolicyError):
        p.update_debtor_policy(D_ID, None, 11 * [LowerLimit(0.0, current_ts.date())], [])
    with pytest.raises(p.ConflictingPolicyError):
        p.update_debtor_policy(D_ID, None, [], 11 * [LowerLimit(-1000, current_ts.date())])


def test_initiated_transfers(db_session, debtor):
    test_uuid = UUID('123e4567-e89b-12d3-a456-426655440000')
    db_session.add(InitiatedTransfer(
        debtor_id=D_ID,
        transfer_uuid=test_uuid,
        recipient_uri='https://example.com/creditors/1111',
        amount=1001,
    ))
    db_session.commit()
    with pytest.raises(p.DebtorDoesNotExistError):
        p.get_debtor_transfer_uuids(1234567890)
    uuids = p.get_debtor_transfer_uuids(D_ID)
    assert uuids == [test_uuid]

    assert p.get_initiated_transfer(1234567890, test_uuid) is None
    t = p.get_initiated_transfer(D_ID, test_uuid)
    assert t.debtor_id == D_ID
    assert t.transfer_uuid == test_uuid
    assert t.amount == 1001
    assert t.recipient_uri == 'https://example.com/creditors/1111'

    p.delete_initiated_transfer(D_ID, test_uuid)
    assert p.get_initiated_transfer(D_ID, test_uuid) is None


def test_create_new_debtor(db_session, debtor):
    with pytest.raises(p.DebtorExistsError):
        p.create_new_debtor(D_ID)
    debtor = p.create_new_debtor(1234567890)
    assert debtor.debtor_id == 1234567890
