import pytest
from datetime import datetime, date, timedelta, timezone
from swpt_debtors import __version__
from swpt_debtors.models import LowerLimit, LowerLimitSequence, Account, ChangeInterestRateSignal, \
    INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL
from swpt_debtors import procedures as p

D_ID = -1
C_ID = 1


@pytest.fixture(scope='function')
def current_ts():
    return datetime.now(tz=timezone.utc)


@pytest.fixture
def debtor():
    return p.get_or_create_debtor(D_ID)


def test_version(db_session):
    assert __version__


def test_is_later_event(current_ts):
    assert p._is_later_event((1, current_ts), (None, None))


def test_add_limit_to_list():
    limits = LowerLimitSequence()
    limits.add_limit(LowerLimit(30, date(2000, 1, 1)))
    limits.add_limit(LowerLimit(20, date(2000, 1, 2)))
    limits.add_limit(LowerLimit(10, date(2000, 1, 3)))
    assert [l.value for l in limits] == [30, 20, 10]
    limits.add_limit(LowerLimit(25, date(2000, 1, 4)))
    assert [l.value for l in limits] == [30, 25]
    assert [l.cutoff for l in limits] == [date(2000, 1, 1), date(2000, 1, 4)]
    limits.add_limit(LowerLimit(30, date(2000, 1, 3)))
    assert [l.value for l in limits] == [30, 25]
    assert [l.cutoff for l in limits] == [date(2000, 1, 3), date(2000, 1, 4)]

    # Add an already existing limit.
    limits.add_limit(LowerLimit(30, date(2000, 1, 3)))
    assert [l.value for l in limits] == [30, 25]


def test_add_limit_to_list_eliminator():
    limits = LowerLimitSequence([
        LowerLimit(10, date(2000, 1, 1)),
        LowerLimit(20, date(2000, 1, 2)),
        LowerLimit(30, date(2000, 1, 3)),
    ])
    assert [l.value for l in limits] == [10, 20, 30]
    limits.add_limit(LowerLimit(10, date(2000, 1, 1)))
    assert [l.value for l in limits] == [30]


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


def test_interest_rate_absolute_limits(db_session, debtor, current_ts):
    debtor.interest_rate_target = -100.0
    assert p._calc_interest_rate(current_ts.date(), debtor) == INTEREST_RATE_FLOOR
    debtor.interest_rate_target = 1e100
    assert p._calc_interest_rate(current_ts.date(), debtor) == INTEREST_RATE_CEIL
