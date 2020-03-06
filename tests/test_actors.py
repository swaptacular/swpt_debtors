from datetime import datetime, timezone
from swpt_debtors import procedures as p
from swpt_debtors import actors as a

D_ID = -1
C_ID = 1


def test_create_debtor(db_session):
    a.create_debtor(
        debtor_id=D_ID,
    )


def test_deactivate_debtor(db_session):
    a.deactivate_debtor(
        debtor_id=D_ID,
    )


def test_on_account_change_signal(db_session):
    a.on_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=0,
        change_ts='2019-10-01T00:00:00Z',
        principal=1000,
        interest=12.5,
        interest_rate=-0.5,
        last_transfer_seqnum=0,
        last_outgoing_transfer_date='2018-10-01',
        last_config_signal_ts='1900-01-01T00:00:00Z',
        last_config_signal_seqnum=0,
        creation_date='2018-10-1',
        negligible_amount=2.0,
        status=0,
        signal_ts=datetime.now(tz=timezone.utc).isoformat(),
        signal_ttl=1e30,
    )


def test_on_prepared_issuing_transfer_signal(db_session):
    a.on_prepared_issuing_transfer_signal(
        debtor_id=D_ID,
        sender_creditor_id=2,
        transfer_id=1,
        coordinator_type='issuing',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        sender_locked_amount=1000,
        recipient_creditor_id=C_ID,
        prepared_at_ts='2019-10-01T00:00:00Z',
    )


def test_on_rejected_issuing_transfer_signal(db_session):
    a.on_rejected_issuing_transfer_signal(
        coordinator_type='issuing',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        details={'errorCode': '123456', 'message': 'Oops!'},
    )


def test_on_finalized_issuing_transfer_signal(db_session):
    a.on_finalized_issuing_transfer_signal(
        debtor_id=D_ID,
        sender_creditor_id=p.ROOT_CREDITOR_ID,
        transfer_id=123,
        coordinator_type='issuing',
        coordinator_id=D_ID,
        coordinator_request_id=678,
        recipient_creditor_id=1234,
        prepared_at_ts='2019-10-01T00:00:00Z',
        finalized_at_ts='2019-10-01T00:00:00Z',
        committed_amount=100,
    )


def test_on_account_purge_signal(db_session):
    a.on_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date='2019-10-01',
    )


def test_on_account_maintenance_signal(db_session):
    a.on_account_maintenance_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        request_ts='2019-10-01T00:00:00Z',
    )
