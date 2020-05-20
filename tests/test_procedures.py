import pytest
import time
from uuid import UUID
from datetime import datetime, date, timedelta
from swpt_debtors import __version__
from swpt_debtors.models import Debtor, Account, ChangeInterestRateSignal, InitiatedTransfer, RunningTransfer, \
    PrepareTransferSignal, FinalizePreparedTransferSignal, ConfigureAccountSignal, \
    INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, ROOT_CREDITOR_ID
from swpt_debtors import procedures as p
from swpt_debtors.lower_limits import LowerLimit

D_ID = -1
C_ID = 1
TEST_UUID = UUID('123e4567-e89b-12d3-a456-426655440000')
TEST_UUID2 = UUID('123e4567-e89b-12d3-a456-426655440001')


@pytest.fixture
def debtor(db_session):
    return p.lock_or_create_debtor(D_ID)


def test_version(db_session):
    assert __version__


def test_lock_or_create_debtor(db_session):
    debtor = p.lock_or_create_debtor(D_ID)
    assert debtor.debtor_id == D_ID
    assert not debtor.is_active
    assert debtor.deactivated_at_date is None
    cas = ConfigureAccountSignal.query.one()
    assert cas.debtor_id == D_ID

    debtor = p.lock_or_create_debtor(D_ID)
    assert debtor.debtor_id == D_ID
    assert not debtor.is_active
    assert debtor.deactivated_at_date is None
    assert len(ConfigureAccountSignal.query.all()) == 1


def test_deactivate_debtor(db_session, debtor):
    p.deactivate_debtor(D_ID)
    debtor = p.get_debtor(D_ID)
    assert not debtor.is_active
    assert debtor.deactivated_at_date is not None

    p.deactivate_debtor(D_ID)
    debtor = p.get_debtor(D_ID)
    assert not debtor.is_active
    assert debtor.deactivated_at_date is not None

    p.deactivate_debtor(1234567890)
    assert p.get_debtor(1234567890) is None


def test_process_account_change_signal(db_session, debtor, current_ts):
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
        creation_date=date(2018, 10, 20),
        negligible_amount=5.5,
        status=0,
        ts=current_ts,
        ttl=1e30,
    )
    assert len(Account.query.all()) == 1
    a = Account.get_instance((D_ID, C_ID))
    assert a.change_seqnum == change_seqnum
    assert a.change_ts == change_ts
    assert a.principal == 1000
    assert a.interest == 12.5
    assert a.interest_rate == -0.5
    assert a.last_outgoing_transfer_date == last_outgoing_transfer_date
    assert a.negligible_amount == 5.5
    assert a.status == 0
    cirs = ChangeInterestRateSignal.query.all()
    assert len(cirs) == 1
    assert cirs[0].debtor_id == D_ID
    assert cirs[0].creditor_id == C_ID
    last_heartbeat_ts = a.last_heartbeat_ts

    # Account heartbeat
    time.sleep(0.1)
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=change_seqnum,
        change_ts=change_ts,
        principal=1000,
        interest=12.5,
        interest_rate=-0.5,
        last_outgoing_transfer_date=last_outgoing_transfer_date,
        creation_date=date(2018, 10, 20),
        negligible_amount=5.5,
        status=0,
        ts=current_ts + timedelta(seconds=12),
        ttl=1e30,
    )
    a = Account.get_instance((D_ID, C_ID))
    assert 11.0 <= (a.last_heartbeat_ts - last_heartbeat_ts).total_seconds() <= 13.0
    assert a.change_seqnum == change_seqnum
    assert a.change_ts == change_ts
    assert a.principal == 1000
    assert a.interest == 12.5
    assert a.interest_rate == -0.5
    assert a.last_outgoing_transfer_date == last_outgoing_transfer_date
    assert a.negligible_amount == 5.5
    assert a.status == 0
    assert len(ChangeInterestRateSignal.query.all()) == 1

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
        creation_date=date(2018, 10, 20),
        negligible_amount=5.5,
        status=0,
        ts=current_ts,
        ttl=1e30,
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
        creation_date=date(2018, 10, 20),
        negligible_amount=5.5,
        status=Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG,
        ts=current_ts,
        ttl=1e30,
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

    # Ignored message
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=change_seqnum + 2,
        change_ts=change_ts + timedelta(seconds=5),
        principal=1002,
        interest=12.6,
        interest_rate=-0.6,
        last_outgoing_transfer_date=last_outgoing_transfer_date + timedelta(days=1),
        creation_date=date(2018, 10, 20),
        negligible_amount=5.5,
        status=Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG,
        ts=current_ts - timedelta(seconds=1000),
        ttl=500,
    )
    assert len(Account.query.all()) == 1
    a = Account.get_instance((D_ID, C_ID))
    assert a.change_seqnum == change_seqnum + 1
    assert a.principal == 1001


def test_process_account_change_signal_no_debtor(db_session, current_ts):
    assert len(Debtor.query.all()) == 0

    change_seqnum = 1
    change_ts = datetime.fromisoformat('2019-10-01T00:00:00+00:00')
    last_outgoing_transfer_date = date.fromisoformat('2019-10-01')
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        change_seqnum=change_seqnum,
        change_ts=change_ts,
        principal=-1000,
        interest=0.0,
        interest_rate=0.0,
        last_outgoing_transfer_date=last_outgoing_transfer_date,
        creation_date=date(2018, 10, 20),
        negligible_amount=2.0,
        status=0,
        ts=current_ts,
        ttl=1e30,
    )
    assert len(Account.query.all()) == 1
    a = Account.get_instance((D_ID, ROOT_CREDITOR_ID))
    assert a.change_seqnum == change_seqnum
    assert a.change_ts == change_ts
    assert a.principal == -1000
    assert a.interest == 0.0
    assert a.interest_rate == 0.0
    assert a.last_outgoing_transfer_date == last_outgoing_transfer_date
    assert a.negligible_amount == 2.0
    assert a.status == 0
    assert len(ChangeInterestRateSignal.query.all()) == 0
    d = Debtor.query.filter_by(debtor_id=D_ID).one()
    assert d.deactivated_at_date is not None
    assert d.initiated_transfers_count == 0
    assert d.balance == -1000
    assert d.balance_ts == change_ts


def test_process_root_account_change_signal(db_session, debtor, current_ts):
    change_seqnum = 1
    change_ts = datetime.fromisoformat('2019-10-01T00:00:00+00:00')
    last_outgoing_transfer_date = date.fromisoformat('2019-10-01')
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        change_seqnum=change_seqnum,
        change_ts=change_ts,
        principal=-9999,
        interest=0,
        interest_rate=0.0,
        last_outgoing_transfer_date=last_outgoing_transfer_date,
        creation_date=date(2018, 10, 20),
        negligible_amount=5.5,
        status=0,
        ts=current_ts,
        ttl=1e30,
    )
    d = p.get_debtor(D_ID)
    assert d.balance == -9999
    assert d.balance_ts == change_ts


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
    assert debtor.is_active
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
    Debtor.get_instance(D_ID).initiated_transfers_count = 1
    db_session.add(InitiatedTransfer(
        debtor_id=D_ID,
        transfer_uuid=TEST_UUID,
        recipient_creditor_id=C_ID,
        amount=1001,
    ))
    db_session.commit()
    assert p.get_debtor(D_ID).initiated_transfers_count == 1
    with pytest.raises(p.DebtorDoesNotExistError):
        p.get_debtor_transfer_uuids(1234567890)
    uuids = p.get_debtor_transfer_uuids(D_ID)
    assert uuids == [TEST_UUID]

    assert p.get_initiated_transfer(1234567890, TEST_UUID) is None
    t = p.get_initiated_transfer(D_ID, TEST_UUID)
    assert t.debtor_id == D_ID
    assert t.transfer_uuid == TEST_UUID
    assert t.amount == 1001
    assert t.recipient_creditor_id == C_ID

    result = p.delete_initiated_transfer(D_ID, TEST_UUID)
    assert result is True
    assert p.get_debtor(D_ID).initiated_transfers_count == 0
    assert p.get_initiated_transfer(D_ID, TEST_UUID) is None


def test_delete_non_existing_initiated_transfer(db_session):
    assert p.delete_initiated_transfer(D_ID, TEST_UUID) is False


def test_create_new_debtor(db_session, debtor):
    with pytest.raises(p.DebtorExistsError):
        p.create_new_debtor(D_ID)
    debtor = p.create_new_debtor(1234567890)
    assert debtor.debtor_id == 1234567890
    assert len(ConfigureAccountSignal.query.all()) == 2
    assert ConfigureAccountSignal.query.filter_by(debtor_id=D_ID).one()
    assert ConfigureAccountSignal.query.filter_by(debtor_id=1234567890).one()


def test_initiate_transfer(db_session, debtor):
    assert len(RunningTransfer.query.all()) == 0
    assert len(InitiatedTransfer.query.all()) == 0
    assert p.get_debtor_transfer_uuids(D_ID) == []
    t = p.initiate_transfer(D_ID, TEST_UUID, C_ID, 1000, {'note': 'test'})
    debtor = p.get_debtor(D_ID)
    assert debtor.is_active
    assert len(InitiatedTransfer.query.all()) == 1
    assert t.debtor_id == D_ID
    assert t.transfer_uuid == TEST_UUID
    assert t.recipient_creditor_id == C_ID
    assert t.amount == 1000
    assert t.transfer_notes == {'note': 'test'}
    assert not t.is_finalized
    running_transfers = RunningTransfer.query.all()
    assert len(running_transfers) == 1
    rt = running_transfers[0]
    assert rt.debtor_id == D_ID
    assert rt.transfer_uuid == TEST_UUID
    assert rt.recipient_creditor_id == C_ID
    assert rt.amount == 1000
    assert rt.transfer_notes == {'note': 'test'}
    assert not t.is_finalized
    with pytest.raises(p.TransferExistsError):
        p.initiate_transfer(D_ID, TEST_UUID, C_ID, 1000, {'note': 'test'})
    with pytest.raises(p.TransfersConflictError):
        p.initiate_transfer(D_ID, TEST_UUID, C_ID, 1001, {'note': 'test'})
    with pytest.raises(p.DebtorDoesNotExistError):
        p.initiate_transfer(1234567890, TEST_UUID, C_ID, 1001, {'note': 'test'})
    assert len(p.get_debtor_transfer_uuids(D_ID)) == 1
    assert len(RunningTransfer.query.all()) == 1

    p.delete_initiated_transfer(D_ID, TEST_UUID)
    assert len(RunningTransfer.query.all()) == 1
    with pytest.raises(p.TransfersConflictError):
        p.initiate_transfer(D_ID, TEST_UUID, C_ID, 1000, {'note': 'test'})


def test_too_many_initiated_transfers(db_session, debtor):
    Debtor.get_instance(D_ID).initiated_transfers_count = 1
    db_session.add(InitiatedTransfer(
        debtor_id=D_ID,
        transfer_uuid=TEST_UUID,
        recipient_creditor_id=C_ID,
        amount=1000,
    ))
    db_session.commit()
    assert len(InitiatedTransfer.query.all()) == 1
    assert p.get_debtor(D_ID).initiated_transfers_count == 1
    for i in range(1, 10):
        suffix = '{:0>4}'.format(i)
        uuid = f'123e4567-e89b-12d3-a456-42665544{suffix}',
        p.initiate_transfer(D_ID, uuid, C_ID, 1000, {})
    assert len(InitiatedTransfer.query.all()) == 10
    assert p.get_debtor(D_ID).initiated_transfers_count == 10
    with pytest.raises(p.TransfersConflictError):
        p.initiate_transfer(D_ID, f'123e4567-e89b-12d3-a456-426655440010', C_ID, 1000, {})


def test_successful_transfer(db_session, debtor):
    assert len(PrepareTransferSignal.query.all()) == 0
    p.initiate_transfer(D_ID, TEST_UUID, C_ID, 1000, {'note': 'test'})
    pts_list = PrepareTransferSignal.query.all()
    assert len(pts_list) == 1
    pts = pts_list[0]
    assert pts.debtor_id == D_ID
    assert pts.coordinator_request_id is not None
    assert pts.min_amount == pts.max_amount == 1000
    assert pts.sender_creditor_id == ROOT_CREDITOR_ID
    assert pts.recipient_creditor_id == C_ID
    assert pts.minimum_account_balance == debtor.minimum_account_balance
    coordinator_request_id = pts.coordinator_request_id

    p.process_prepared_issuing_transfer_signal(
        debtor_id=D_ID,
        sender_creditor_id=ROOT_CREDITOR_ID,
        transfer_id=777,
        recipient_identity=str(C_ID),
        sender_locked_amount=1000,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
    )
    assert len(PrepareTransferSignal.query.all()) == 1
    fpts_list = FinalizePreparedTransferSignal.query.all()
    assert len(fpts_list) == 1
    fpts = fpts_list[0]
    assert fpts.debtor_id == D_ID
    assert fpts.sender_creditor_id == ROOT_CREDITOR_ID
    assert fpts.transfer_id is not None
    assert fpts.committed_amount == 1000
    assert fpts.transfer_notes == {'note': 'test'}
    assert fpts.__marshmallow_schema__.dump(fpts)['transfer_message'] == '{"note": "test"}'

    rt_list = RunningTransfer.query.all()
    assert len(rt_list) == 1
    rt = rt_list[0]
    assert rt.is_finalized
    assert rt.issuing_transfer_id is not None
    it_list = InitiatedTransfer.query.all()
    assert len(it_list) == 1
    it = it_list[0]
    assert not it.is_finalized
    assert not it.is_successful

    p.process_finalized_issuing_transfer_signal(
        debtor_id=D_ID,
        sender_creditor_id=p.ROOT_CREDITOR_ID,
        transfer_id=777,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
        recipient_identity=str(C_ID),
        committed_amount=1000,
        status_code='OK',
    )
    it_list = InitiatedTransfer.query.all()
    assert len(it_list) == 1
    it = it_list[0]
    assert it.is_finalized
    assert it.is_successful


def test_failed_transfer(db_session, debtor):
    p.initiate_transfer(D_ID, TEST_UUID, C_ID, 1000, {'note': 'test'})
    pts = PrepareTransferSignal.query.all()[0]
    p.process_rejected_issuing_transfer_signal(D_ID, pts.coordinator_request_id, 'TEST', 1000, D_ID, p.ROOT_CREDITOR_ID)
    assert len(FinalizePreparedTransferSignal.query.all()) == 0

    assert len(RunningTransfer.query.all()) == 0
    it_list = InitiatedTransfer.query.all()
    assert len(it_list) == 1
    it = it_list[0]
    assert it.is_finalized
    assert not it.is_successful

    p.process_rejected_issuing_transfer_signal(D_ID, pts.coordinator_request_id, 'TEST', 1000, D_ID, p.ROOT_CREDITOR_ID)
    assert len(RunningTransfer.query.all()) == 0
    it_list == InitiatedTransfer.query.all()
    assert len(it_list) == 1 and it_list[0].is_finalized


def test_process_account_purge_signal(db_session, debtor, current_ts):
    creation_date = date(2020, 1, 10)
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        change_seqnum=1,
        change_ts=current_ts,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_outgoing_transfer_date=current_ts.date(),
        creation_date=creation_date,
        negligible_amount=2.0,
        status=0,
        ts=current_ts,
        ttl=1e30,
    )
    assert len(Account.query.all()) == 1
    p.process_account_purge_signal(D_ID, p.ROOT_CREDITOR_ID, creation_date)
    assert len(Account.query.all()) == 0
    d = Debtor.query.one()
    assert d
    assert not d.status & Debtor.STATUS_HAS_ACCOUNT_FLAG
    assert d.deactivated_at_date is not None
    assert d.initiated_transfers_count == 0
    assert len(InitiatedTransfer.query.filter_by(debtor_id=D_ID).all()) == 0
    assert len(Account.query.all()) == 0


def test_process_account_maintenance_signal(db_session, debtor, current_ts):
    db_session.add(Account(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=0,
        change_ts=current_ts,
        principal=-10,
        interest=0.0,
        interest_rate=0.0,
        last_outgoing_transfer_date=current_ts,
        creation_date=current_ts.date(),
        negligible_amount=2.0,
        status=0,
        last_maintenance_request_ts=current_ts,
        is_muted=True,
    ))
    db_session.commit()
    p.process_account_maintenance_signal(D_ID, C_ID, current_ts - timedelta(seconds=10))
    a = Account.get_instance((D_ID, C_ID))
    assert a.is_muted is True
    assert a.last_maintenance_request_ts == current_ts
    p.process_account_maintenance_signal(D_ID, C_ID, current_ts)
    a = Account.get_instance((D_ID, C_ID))
    assert a.is_muted is False
    assert a.last_maintenance_request_ts == current_ts


def test_cancel_transfer_success(db_session, debtor):
    p.initiate_transfer(D_ID, TEST_UUID, C_ID, 1000, {'note': 'test'})
    coordinator_request_id = PrepareTransferSignal.query.one().coordinator_request_id

    t = p.cancel_transfer(D_ID, TEST_UUID)
    assert t.is_finalized
    assert not t.is_successful

    t = p.cancel_transfer(D_ID, TEST_UUID)
    assert t.is_finalized
    assert not t.is_successful

    p.process_prepared_issuing_transfer_signal(
        debtor_id=D_ID,
        sender_creditor_id=ROOT_CREDITOR_ID,
        transfer_id=777,
        recipient_identity=str(C_ID),
        sender_locked_amount=1000,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
    )
    t = p.cancel_transfer(D_ID, TEST_UUID)
    assert t.is_finalized
    assert not t.is_successful


def test_cancel_transfer_failure(db_session, debtor):
    p.initiate_transfer(D_ID, TEST_UUID, C_ID, 1000, {'note': 'test'})
    coordinator_request_id = PrepareTransferSignal.query.one().coordinator_request_id

    p.process_prepared_issuing_transfer_signal(
        debtor_id=D_ID,
        sender_creditor_id=ROOT_CREDITOR_ID,
        transfer_id=777,
        recipient_identity=str(C_ID),
        sender_locked_amount=1000,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
    )
    with pytest.raises(p.TransferUpdateConflictError):
        p.cancel_transfer(D_ID, TEST_UUID)
    with pytest.raises(p.TransferUpdateConflictError):
        p.cancel_transfer(D_ID, TEST_UUID)

    p.process_finalized_issuing_transfer_signal(
        debtor_id=D_ID,
        sender_creditor_id=p.ROOT_CREDITOR_ID,
        transfer_id=777,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
        recipient_identity=str(C_ID),
        committed_amount=1000,
        status_code='OK',
    )
    with pytest.raises(p.TransferUpdateConflictError):
        p.cancel_transfer(D_ID, TEST_UUID)
    with pytest.raises(p.TransferUpdateConflictError):
        p.cancel_transfer(D_ID, TEST_UUID)
