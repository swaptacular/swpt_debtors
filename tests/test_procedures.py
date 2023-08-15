import pytest
from uuid import UUID
from datetime import datetime, date, timedelta
from swpt_pythonlib.utils import i64_to_u64
from swpt_debtors.extensions import db
from swpt_debtors import __version__
from swpt_debtors.models import (
    Debtor,
    RunningTransfer,
    PrepareTransferSignal,
    FinalizeTransferSignal,
    ConfigureAccountSignal,
    ROOT_CREDITOR_ID,
    TS0,
    DATE0,
    SC_OK,
    SC_CANCELED_BY_THE_SENDER,
    HUGE_NEGLIGIBLE_AMOUNT,
    DEFAULT_CONFIG_FLAGS,
)
from swpt_debtors import procedures as p

D_ID = 4294967296
C_ID = 1
TEST_UUID = UUID("123e4567-e89b-12d3-a456-426655440000")
TEST_UUID2 = UUID("123e4567-e89b-12d3-a456-426655440001")


def acc_id(debtor_id, credior_id):
    recipient = str(i64_to_u64(credior_id))
    return f"swpt:{i64_to_u64(debtor_id)}/{recipient}", recipient


@pytest.fixture
def debtor(db_session):
    debtor = Debtor(debtor_id=D_ID, status_flags=0)
    debtor.activate()
    db.session.add(debtor)
    db.session.commit()

    return p.get_debtor(D_ID)


def test_version():
    assert __version__


def test_deactivate_debtor(debtor):
    assert debtor.is_activated
    assert not debtor.is_deactivated
    p.deactivate_debtor(D_ID)
    debtor = p.get_debtor(D_ID)
    assert debtor.is_activated
    assert debtor.is_deactivated
    assert debtor.deactivation_date is not None

    p.deactivate_debtor(D_ID)
    debtor = p.get_debtor(D_ID)
    assert debtor.is_deactivated
    assert debtor.deactivation_date is not None

    p.deactivate_debtor(1234567890)
    assert p.get_debtor(1234567890) is None


def test_process_account_update_signal_no_debtor(db_session, current_ts):
    assert len(Debtor.query.all()) == 0

    change_seqnum = 1
    change_ts = datetime.fromisoformat("2019-10-01T00:00:00+00:00")
    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        last_change_seqnum=change_seqnum,
        last_change_ts=change_ts,
        principal=-1000,
        interest_rate=0.0,
        creation_date=date(2018, 10, 20),
        last_config_ts=TS0,
        last_config_seqnum=0,
        config_data="",
        account_id="0",
        transfer_note_max_bytes=100,
        negligible_amount=2.0,
        config_flags=0,
        ts=current_ts,
        ttl=1000000,
    )
    assert len(Debtor.query.filter_by(debtor_id=D_ID).all()) == 0
    cas = ConfigureAccountSignal.query.one()
    assert cas.debtor_id == D_ID
    assert cas.config_data == ""
    assert cas.config_flags & Debtor.CONFIG_SCHEDULED_FOR_DELETION_FLAG


def test_process_account_update_signal_old_ts(debtor, current_ts):
    account_last_heartbeat_ts = debtor.account_last_heartbeat_ts
    change_seqnum = 1
    change_ts = datetime.fromisoformat("2019-10-01T00:00:00+00:00")
    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        last_change_seqnum=change_seqnum,
        last_change_ts=change_ts,
        principal=-9999,
        interest_rate=10.0,
        creation_date=date(2018, 10, 20),
        last_config_ts=TS0,
        last_config_seqnum=0,
        config_data="",
        account_id="0",
        transfer_note_max_bytes=100,
        negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
        config_flags=DEFAULT_CONFIG_FLAGS,
        ts=current_ts - timedelta(days=1),
        ttl=10000,
    )
    d = p.get_debtor(D_ID)
    assert d.balance == 0
    assert d.account_last_heartbeat_ts == account_last_heartbeat_ts

    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        last_change_seqnum=0,
        last_change_ts=TS0,
        principal=-9999,
        interest_rate=10.0,
        creation_date=DATE0,
        last_config_ts=TS0,
        last_config_seqnum=0,
        config_data="",
        account_id="0",
        transfer_note_max_bytes=100,
        negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
        config_flags=DEFAULT_CONFIG_FLAGS,
        ts=current_ts,
        ttl=1000000,
    )
    d = p.get_debtor(D_ID)
    assert d.balance == 0
    assert d.account_last_heartbeat_ts == current_ts


def test_account_change_signal_effectual_config(debtor, current_ts):
    change_seqnum = 1
    change_ts = datetime.fromisoformat("2019-10-01T00:00:00+00:00")
    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        last_change_seqnum=change_seqnum,
        last_change_ts=change_ts,
        principal=-9999,
        interest_rate=10.0,
        creation_date=date(2018, 10, 20),
        last_config_ts=TS0,
        last_config_seqnum=0,
        config_data="",
        account_id="0",
        transfer_note_max_bytes=100,
        negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
        config_flags=DEFAULT_CONFIG_FLAGS,
        ts=current_ts,
        ttl=1000000,
    )
    d = p.get_debtor(D_ID)
    assert d.account_last_heartbeat_ts == current_ts
    assert d.balance == -9999
    assert d.account_id == "0"
    assert d.transfer_note_max_bytes == 100
    assert d.has_server_account
    assert d.account_creation_date == date(2018, 10, 20)
    assert d.account_last_change_seqnum == change_seqnum
    assert d.account_last_change_ts == change_ts
    assert d.is_config_effectual


def test_account_change_signal_ineffectual_config(debtor, current_ts):
    change_seqnum = 1
    change_ts = datetime.fromisoformat("2019-10-01T00:00:00+00:00")
    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        last_change_seqnum=change_seqnum,
        last_change_ts=change_ts,
        principal=-9999,
        interest_rate=10.0,
        creation_date=date(2018, 10, 20),
        last_config_ts=TS0,
        last_config_seqnum=0,
        config_data="",
        account_id="0",
        transfer_note_max_bytes=100,
        negligible_amount=0.0,
        config_flags=DEFAULT_CONFIG_FLAGS,
        ts=current_ts,
        ttl=1000000,
    )
    d = p.get_debtor(D_ID)
    assert d.balance == -9999
    assert d.account_id == "0"
    assert d.transfer_note_max_bytes == 100
    assert d.has_server_account
    assert d.account_creation_date == date(2018, 10, 20)
    assert d.account_last_change_seqnum == change_seqnum
    assert d.account_last_change_ts == change_ts
    assert not d.is_config_effectual


def test_running_transfers(debtor):
    recipient_uri, recipient = acc_id(D_ID, C_ID)
    Debtor.query.filter_by(debtor_id=D_ID).one().running_transfers_count = 1
    db.session.add(
        RunningTransfer(
            debtor_id=D_ID,
            transfer_uuid=TEST_UUID,
            recipient=recipient,
            recipient_uri=recipient_uri,
            transfer_note_format="fmt",
            transfer_note="note",
            amount=1001,
        )
    )
    db.session.commit()
    assert p.get_debtor(D_ID).running_transfers_count == 1
    with pytest.raises(p.DebtorDoesNotExist):
        p.get_debtor_transfer_uuids(1234567890)
    uuids = p.get_debtor_transfer_uuids(D_ID)
    assert uuids == [TEST_UUID]

    assert p.get_running_transfer(1234567890, TEST_UUID) is None
    t = p.get_running_transfer(D_ID, TEST_UUID)
    assert t.debtor_id == D_ID
    assert t.transfer_uuid == TEST_UUID
    assert t.amount == 1001
    assert t.recipient == recipient
    assert t.recipient_uri == recipient_uri

    p.delete_running_transfer(D_ID, TEST_UUID)
    assert p.get_debtor(D_ID).running_transfers_count == 0
    assert p.get_running_transfer(D_ID, TEST_UUID) is None


def test_delete_non_existing_initiated_transfer(db_session):
    with pytest.raises(p.TransferDoesNotExist):
        p.delete_running_transfer(D_ID, TEST_UUID)


def test_initiate_running_transfer(debtor):
    recipient_uri, recipient = acc_id(D_ID, C_ID)
    assert len(RunningTransfer.query.all()) == 0
    assert p.get_debtor_transfer_uuids(D_ID) == []
    t = p.initiate_running_transfer(
        D_ID, TEST_UUID, recipient_uri, recipient, 1000, "fmt", "test"
    )
    assert len(RunningTransfer.query.all()) == 1
    assert t.debtor_id == D_ID
    assert t.transfer_uuid == TEST_UUID
    assert t.recipient == recipient
    assert t.recipient_uri == recipient_uri
    assert t.amount == 1000
    assert t.transfer_note_format == "fmt"
    assert t.transfer_note == "test"
    assert not t.is_settled
    assert not t.is_finalized
    with pytest.raises(p.TransferExists):
        p.initiate_running_transfer(
            D_ID, TEST_UUID, *acc_id(D_ID, C_ID), 1000, "fmt", "test"
        )
    with pytest.raises(p.TransfersConflict):
        p.initiate_running_transfer(
            D_ID, TEST_UUID, *acc_id(D_ID, C_ID), 1001, "fmt", "test"
        )
    with pytest.raises(p.DebtorDoesNotExist):
        p.initiate_running_transfer(
            1234567890,
            TEST_UUID,
            *acc_id(1234567890, C_ID),
            1001,
            "fmt",
            "test",
        )
    assert len(p.get_debtor_transfer_uuids(D_ID)) == 1
    assert len(RunningTransfer.query.all()) == 1

    p.delete_running_transfer(D_ID, TEST_UUID)
    assert len(RunningTransfer.query.all()) == 0


def test_too_many_initiated_transfers(debtor):
    recipient_uri, recipient = acc_id(D_ID, C_ID)
    Debtor.query.filter_by(debtor_id=D_ID).one().running_transfers_count = 1
    db.session.add(
        RunningTransfer(
            debtor_id=D_ID,
            transfer_uuid=TEST_UUID,
            recipient=recipient,
            recipient_uri=recipient_uri,
            amount=1000,
            transfer_note_format="",
            transfer_note="",
        )
    )
    db.session.commit()
    assert len(RunningTransfer.query.all()) == 1
    assert p.get_debtor(D_ID).running_transfers_count == 1
    for i in range(1, 10):
        suffix = "{:0>4}".format(i)
        uuid = UUID(f"123e4567-e89b-12d3-a456-42665544{suffix}")
        p.initiate_running_transfer(
            D_ID, uuid, *acc_id(D_ID, C_ID), 1000, "", "", 10
        )
    assert len(RunningTransfer.query.all()) == 10
    assert p.get_debtor(D_ID).running_transfers_count == 10
    with pytest.raises(p.TooManyRunningTransfers):
        p.initiate_running_transfer(
            D_ID,
            "123e4567-e89b-12d3-a456-426655440010",
            *acc_id(D_ID, C_ID),
            1000,
            "",
            "",
            10,
        )


def test_successful_transfer(debtor):
    recipient_uri, recipient = acc_id(D_ID, C_ID)
    assert len(PrepareTransferSignal.query.all()) == 0
    p.initiate_running_transfer(
        D_ID, TEST_UUID, recipient_uri, recipient, 1000, "fmt", "test"
    )
    pts_list = PrepareTransferSignal.query.all()
    assert len(pts_list) == 1
    pts = pts_list[0]
    assert pts.debtor_id == D_ID
    assert pts.coordinator_request_id is not None
    assert pts.amount == 1000
    assert pts.recipient == recipient
    coordinator_request_id = pts.coordinator_request_id

    p.process_prepared_issuing_transfer_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        transfer_id=777,
        recipient=str(C_ID),
        locked_amount=1000,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
    )
    assert len(PrepareTransferSignal.query.all()) == 1
    fpts_list = FinalizeTransferSignal.query.all()
    assert len(fpts_list) == 1
    fpts = fpts_list[0]
    assert fpts.debtor_id == D_ID
    assert fpts.creditor_id == ROOT_CREDITOR_ID
    assert fpts.transfer_id is not None
    assert fpts.committed_amount == 1000
    assert fpts.transfer_note == "test"
    assert fpts.__marshmallow_schema__.dump(fpts)["transfer_note"] == "test"

    rt_list = RunningTransfer.query.all()
    assert len(rt_list) == 1
    rt = rt_list[0]
    assert rt.is_settled
    assert rt.transfer_id is not None
    it_list = RunningTransfer.query.all()
    assert len(it_list) == 1
    it = it_list[0]
    assert not it.is_finalized
    assert it.error_code is None

    p.process_finalized_issuing_transfer_signal(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        transfer_id=777,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
        committed_amount=1000,
        status_code=SC_OK,
        total_locked_amount=0,
    )
    it_list = RunningTransfer.query.all()
    assert len(it_list) == 1
    it = it_list[0]
    assert it.is_finalized
    assert it.error_code is None


def test_rejected_transfer(debtor):
    p.initiate_running_transfer(
        D_ID, TEST_UUID, *acc_id(D_ID, C_ID), 1000, "fmt", "test"
    )
    pts = PrepareTransferSignal.query.all()[0]
    p.process_rejected_issuing_transfer_signal(
        coordinator_id=D_ID,
        coordinator_request_id=pts.coordinator_request_id,
        status_code="TEST",
        total_locked_amount=0,
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
    )
    assert len(FinalizeTransferSignal.query.all()) == 0

    it_list = RunningTransfer.query.all()
    assert len(it_list) == 1
    it = it_list[0]
    assert it.is_finalized
    assert it.error_code == "TEST"

    p.process_rejected_issuing_transfer_signal(
        coordinator_id=D_ID,
        coordinator_request_id=pts.coordinator_request_id,
        status_code="TEST",
        total_locked_amount=0,
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
    )
    it_list == RunningTransfer.query.all()
    assert len(it_list) == 1 and it_list[0].is_finalized


def test_failed_transfer(debtor):
    recipient_uri, recipient = acc_id(D_ID, C_ID)
    assert len(PrepareTransferSignal.query.all()) == 0
    p.initiate_running_transfer(
        D_ID, TEST_UUID, recipient_uri, recipient, 1000, "fmt", "test"
    )
    pts_list = PrepareTransferSignal.query.all()
    assert len(pts_list) == 1
    pts = pts_list[0]
    assert pts.debtor_id == D_ID
    assert pts.coordinator_request_id is not None
    assert pts.amount == 1000
    assert pts.recipient == recipient
    coordinator_request_id = pts.coordinator_request_id

    p.process_prepared_issuing_transfer_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        transfer_id=777,
        recipient=str(C_ID),
        locked_amount=1000,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
    )
    assert len(PrepareTransferSignal.query.all()) == 1
    fpts_list = FinalizeTransferSignal.query.all()
    assert len(fpts_list) == 1
    fpts = fpts_list[0]
    assert fpts.debtor_id == D_ID
    assert fpts.creditor_id == ROOT_CREDITOR_ID
    assert fpts.transfer_id is not None
    assert fpts.committed_amount == 1000
    assert fpts.transfer_note == "test"
    assert fpts.__marshmallow_schema__.dump(fpts)["transfer_note"] == "test"

    rt_list = RunningTransfer.query.all()
    assert len(rt_list) == 1
    rt = rt_list[0]
    assert rt.is_settled
    assert rt.transfer_id is not None
    it_list = RunningTransfer.query.all()
    assert len(it_list) == 1
    it = it_list[0]
    assert not it.is_finalized
    assert it.error_code is None

    p.process_finalized_issuing_transfer_signal(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        transfer_id=777,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
        committed_amount=0,
        status_code="TEST_ERROR",
        total_locked_amount=666,
    )
    it_list = RunningTransfer.query.all()
    assert len(it_list) == 1
    it = it_list[0]
    assert it.is_finalized
    assert it.error_code == "TEST_ERROR"
    assert it.total_locked_amount == 666


def test_process_account_purge_signal(debtor, current_ts):
    creation_date = date(2020, 1, 10)
    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        last_change_seqnum=1,
        last_change_ts=current_ts,
        principal=0,
        interest_rate=0.0,
        creation_date=creation_date,
        last_config_ts=TS0,
        last_config_seqnum=0,
        config_data="",
        account_id="0",
        transfer_note_max_bytes=100,
        negligible_amount=2.0,
        config_flags=0,
        ts=current_ts,
        ttl=1000000,
    )
    d = p.get_active_debtor(D_ID)
    assert d.has_server_account

    p.process_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        creation_date=creation_date,
    )
    d = p.get_active_debtor(D_ID)
    assert not d.has_server_account


def test_cancel_running_transfer_success(debtor):
    p.initiate_running_transfer(
        D_ID, TEST_UUID, *acc_id(D_ID, C_ID), 1000, "fmt", "test"
    )
    coordinator_request_id = (
        PrepareTransferSignal.query.one().coordinator_request_id
    )

    t = p.cancel_running_transfer(D_ID, TEST_UUID)
    assert t.is_finalized
    assert t.error_code == SC_CANCELED_BY_THE_SENDER

    t = p.cancel_running_transfer(D_ID, TEST_UUID)
    assert t.is_finalized
    assert t.error_code == SC_CANCELED_BY_THE_SENDER

    p.process_prepared_issuing_transfer_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        transfer_id=777,
        recipient=str(C_ID),
        locked_amount=1000,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
    )
    t = p.cancel_running_transfer(D_ID, TEST_UUID)
    assert t.is_finalized
    assert t.error_code == SC_CANCELED_BY_THE_SENDER


def test_cancel_running_transfer_failure(debtor):
    p.initiate_running_transfer(
        D_ID, TEST_UUID, *acc_id(D_ID, C_ID), 1000, "fmt", "test"
    )
    coordinator_request_id = (
        PrepareTransferSignal.query.one().coordinator_request_id
    )

    p.process_prepared_issuing_transfer_signal(
        debtor_id=D_ID,
        creditor_id=ROOT_CREDITOR_ID,
        transfer_id=777,
        recipient=str(C_ID),
        locked_amount=1000,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
    )
    with pytest.raises(p.ForbiddenTransferCancellation):
        p.cancel_running_transfer(D_ID, TEST_UUID)
    with pytest.raises(p.ForbiddenTransferCancellation):
        p.cancel_running_transfer(D_ID, TEST_UUID)

    p.process_finalized_issuing_transfer_signal(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        transfer_id=777,
        coordinator_id=D_ID,
        coordinator_request_id=coordinator_request_id,
        committed_amount=1000,
        status_code=SC_OK,
        total_locked_amount=0,
    )
    with pytest.raises(p.ForbiddenTransferCancellation):
        p.cancel_running_transfer(D_ID, TEST_UUID)
    with pytest.raises(p.ForbiddenTransferCancellation):
        p.cancel_running_transfer(D_ID, TEST_UUID)


def test_activate_new_debtor(db_session):
    debtor = p.reserve_debtor(D_ID)
    assert debtor.debtor_id == D_ID
    assert not debtor.is_activated
    assert len(Debtor.query.all()) == 1
    with pytest.raises(p.DebtorExists):
        p.reserve_debtor(D_ID)

    assert not p.get_active_debtor(D_ID)
    with pytest.raises(p.InvalidReservationId):
        p.activate_debtor(D_ID, "-123")
    p.activate_debtor(D_ID, str(debtor.reservation_id))
    debtor = p.get_active_debtor(D_ID)
    assert debtor
    assert debtor.is_activated
    cas = ConfigureAccountSignal.query.one()
    assert cas.debtor_id == D_ID
    assert cas.config_data == ""
    assert cas.config_flags == 0

    with pytest.raises(p.DebtorExists):
        p.reserve_debtor(D_ID)


def test_update_debtor_config(debtor):
    last_config_ts = debtor.last_config_ts
    last_config_seqnum = debtor.last_config_seqnum

    p.update_debtor_config(D_ID, config_data="TEST", latest_update_id=2)
    debtor = p.get_active_debtor(D_ID)
    assert debtor
    assert debtor.config_data == "TEST"
    assert debtor.last_config_seqnum == last_config_seqnum + 1
    assert debtor.last_config_ts >= last_config_ts

    cas = ConfigureAccountSignal.query.one()
    assert cas.debtor_id == D_ID
    assert cas.config_data == "TEST"
    assert cas.config_flags == 0
    assert cas.ts == debtor.last_config_ts
    assert cas.seqnum == debtor.last_config_seqnum


def test_process_rejected_config_signal(debtor):
    d = p.get_active_debtor(D_ID)
    assert d.config_error is None

    params = {
        "debtor_id": D_ID,
        "creditor_id": ROOT_CREDITOR_ID,
        "config_ts": debtor.last_config_ts,
        "config_seqnum": debtor.last_config_seqnum,
        "negligible_amount": HUGE_NEGLIGIBLE_AMOUNT,
        "config_data": "",
        "config_flags": debtor.config_flags,
        "rejection_code": "TEST_CODE",
    }
    p.process_rejected_config_signal(**{**params, "config_data": "UNEXPECTED"})
    p.process_rejected_config_signal(
        **{**params, "negligible_amount": HUGE_NEGLIGIBLE_AMOUNT * 1.0001}
    )
    p.process_rejected_config_signal(
        **{**params, "config_flags": d.config_flags ^ 1}
    )
    p.process_rejected_config_signal(
        **{**params, "config_seqnum": d.last_config_seqnum - 1}
    )
    p.process_rejected_config_signal(
        **{**params, "config_seqnum": d.last_config_seqnum + 1}
    )
    p.process_rejected_config_signal(
        **{**params, "config_ts": d.last_config_ts + timedelta(seconds=-1)}
    )
    p.process_rejected_config_signal(
        **{**params, "config_ts": d.last_config_ts + timedelta(seconds=1)}
    )
    d = p.get_active_debtor(D_ID)
    assert d.config_error is None

    p.process_rejected_config_signal(**params)
    d = p.get_active_debtor(D_ID)
    assert d.config_error == "TEST_CODE"

    p.process_rejected_config_signal(**params)
    assert d.config_error == "TEST_CODE"
